"""Generate an PMTiles of contours from Mapzen / Amazon elevation tiles
(https://registry.opendata.aws/terrain-tiles/)"""


from multiprocessing import Pool
from subprocess import CalledProcessError, run
import argparse
import asyncio
import json
import os
import random
# import tempfile
import time

import aiofiles
from fiona.transform import transform
import httpcore
import httpx
import mercantile
from supermercado import burntiles, super_utils
from tqdm import tqdm

from database import make_database, DBNAME, SRID
from sources import util


TABLE_NAME = "contours"
CACHE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "elevation-tiles")


# pylint: disable=invalid-name
def tile_file_path(x, y, z, ext="tif"):
    """Return tile file path for coordinates"""
    return os.path.join(CACHE_DIR, str(z), str(x), f"{y}.{ext}")
# pylint: enable=invalid-name


def tiles_from_bbox(swlon, swlat, nelon, nelat, zooms):
    """Tiles from a bounding box specified by southwest and northeast coordinates
    and a list of zooms
    """
    return list(
        mercantile.tiles(swlon, swlat, nelon, nelat, zooms, truncate=False)
    )


def tiles_from_geojson(geojson, zooms):
    """Tiles from a GeoJSON feature passed in as a dict"""
    tiles = []
    if geojson["type"] == "FeatureCollection":
        features = geojson["features"]
    # supermercado expects features, so if this is a just a geometry, wrap it in a
    # Feature
    elif geojson["type"] != "Feature":
        features = [{"type": "Feature", "geometry": geojson}]
    else:
        features = [geojson]
    for zoom in zooms:
        # So much more complicated than it needs to be. burntiles.burn only
        # accepts a list of polygons, not features, not multipolygons, just
        # polygons, so super_utils.filter_features gets the polygons out of
        # diverse GeoJSON-y dicts. It also returns a list of numpy.ndarray
        # objects, which are not lists and need to be turned into lists with
        # tolist()
        tiles += [
            ftiles.tolist()
            for ftiles in burntiles.burn(
                list(super_utils.filter_features(features)), zoom
            )
        ]
    return [mercantile.Tile(*tile) for tile in tiles]

# https://stackoverflow.com/questions/51525604/how-to-iterate-over-a-range-asynchronously#51781911
async def async_range(start, stop):
    """Async version of range()"""
    for i in range(start, stop):
        yield i
        await asyncio.sleep(0.0)

async def cache_tile(tile, client, clean=False, max_retries=3, debug=False):
    """Caches a tile"""
    tile_path = f"{tile.z}/{tile.x}/{tile.y}.tif"
    url = f"https://s3.amazonaws.com/elevation-tiles-prod/geotiff/{tile_path}"
    file_path = tile_file_path(tile.x, tile.y, tile.z)
    dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)
    if os.path.exists(file_path):
        if debug:
            util.log(f"cache_tile, path exists: {file_path}")
        if clean:
            os.remove(file_path)
        else:
            return file_path
    # TODO handle errors, client abort, server abort
    async for try_num in async_range(1, max_retries + 1):
        try:
            if debug:
                util.log(f"getting {url}")
            download = await client.get(url)
            if download.status_code != 200:
                util.log(
                    f"Request for {url} failed with {download.status_code}, "
                    "skipping..."
                )
                return
            async with aiofiles.open(file_path, 'wb') as outfile:
                async for chunk in download.aiter_bytes():
                    await outfile.write(chunk)
            break
        except (
            asyncio.exceptions.TimeoutError,
            httpx.ConnectTimeout,
            httpx.PoolTimeout,
            httpcore.ConnectTimeout
        ) as timeout_error:
            util.log(f"Caught timeout error: {type(timeout_error).__name__}")
            if try_num > max_retries:
                util.log(
                    f"Request for {url} timed out {max_retries} times, "
                    "skipping..."
                )
            else:
                # Wait to retry, with a little randomness
                sleepytime = (try_num ** 3) + random.randrange(5, 20)
                if debug:
                    util.log(f"Sleeping for {sleepytime}s...")
                await asyncio.sleep(sleepytime)
    if os.path.exists(file_path):
        return file_path
    raise FileNotFoundError(f"Failed to download {url}")


# Cache DEM tiles using asyncio for this presumably IO-bound process
async def cache_tiles(tiles, clean=False):
    """Cache multiple tiles"""
    async with httpx.AsyncClient() as client:
        # Break it into chunks
        chunk_size = 200
        for i in range(0, len(tiles), chunk_size):
            idx = i
            # using as_completed with tqdm (https://stackoverflow.com/a/37901797)
            tasks = [
                cache_tile(tile, client, clean=clean, debug=True)
                for tile in tiles[idx:idx+chunk_size]
            ]
            pbar = tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc=f"Downloading DEM TIFs {idx}-{idx+chunk_size} ({len(tiles)} total)",
                unit=" tiles"
            )
            for task in pbar:
                await task
            time.sleep(10)


def make_contours_for_tile(tile, clean=False):
    """Make contours for a file"""
    tile_path = tile_file_path(tile.x, tile.y, tile.z)
    dir_path = os.path.dirname(tile_path)
    if not os.path.exists(tile_path):
        raise FileNotFoundError(f"Tile file does not exist at {tile_path}")
    merge_contours_path = tile_file_path(
        tile.x,
        tile.y,
        tile.z,
        ext="merge-contours.shp"
    )
    if os.path.exists(merge_contours_path):
        if clean:
            os.remove(merge_contours_path)
        else:
            return
    interval = 1000
    if tile.z >= 10:
        interval = 25
    elif tile.z >= 8:
        interval = 100
    # Merge all 8 tiles that surround this tile so we don't get weird edge
    # effects
    merge_coords = [
        [tile.x - 1, tile.y - 1], [tile.x + 0, tile.y - 1], [tile.x + 1, tile.y - 1],
        [tile.x - 1, tile.y + 0], [tile.x + 0, tile.y + 0], [tile.x + 1, tile.y + 0],
        [tile.x - 1, tile.y + 1], [tile.x + 0, tile.y + 1], [tile.x + 1, tile.y + 1]
    ]
    target_merge_file_paths = [tile_file_path(xy[0], xy[1], tile.z) for xy in merge_coords]
    merge_file_paths = []
    for path in target_merge_file_paths:
        if os.path.exists(path):
            merge_file_paths.append(path)
        else:
            util.log(f"WARNING: tile path does not exist: {path}")
    # If for some reason no files exist for this tile or its buffer, just give up
    if len(merge_file_paths) == 0:
        return
    merge_path = tile_file_path(tile.x, tile.y, tile.z, "merge.tif")
    try:
        run(["gdal_merge.py", "-q", "-o", merge_path, *merge_file_paths], check=True)
    except CalledProcessError:
        util.log("gdal_merge.py not working, trying another path")
        run(["/usr/bin/gdal_merge.py", "-q", "-o", merge_path, *merge_file_paths], check=True)

    try:
        run([
            "gdal_contour", "-q", "-i", str(interval), "-a", "elevation", merge_path,
            merge_contours_path
        ], check=True)
    except CalledProcessError:
        util.log("gdal_contour not working, trying another path")
        run([
            "/usr/bin/gdal_contour", "-q", "-i", str(interval), "-a", "elevation", merge_path,
            merge_contours_path
        ], check=True)
    # Get the bounding box of this tile in lat/lon, project into the source
    # coordinate system (Pseudo Mercator) so we can clip to it before
    # importing into PostGIS
    bounds = mercantile.bounds(tile.x, tile.y, tile.z)
    bounds_x, bounds_y = transform(
        'EPSG:4326',
        'EPSG:3857',
        [bounds.west, bounds.east],
        [bounds.south, bounds.north]
    )
    clip_minx, clip_miny, clip_maxx, clip_maxy = (
        bounds_x[0], bounds_y[0], bounds_x[1], bounds_y[1]
    )
    # Clip to the tile boundaries and keep only the line parts of the result
    # with ST_CollectionExtract, all within one SQLite/Spatialite-dialect
    # query. Clipping with plain "-clipsrc" can leave stray Points alongside
    # the LineStrings, turning the clipped geometry into a GeometryCollection
    # that fails to COPY into the MULTILINESTRING contours column (issue #18).
    layer_name = util.extless_basename(merge_contours_path)
    clip_mbr = f"BuildMBR({clip_minx}, {clip_miny}, {clip_maxx}, {clip_maxy})"
    clip_sql = (
        f"SELECT id, elevation, "
        f"ST_CollectionExtract(ST_Intersection(geometry, {clip_mbr}), 2) AS geometry "
        f"FROM \"{layer_name}\" WHERE ST_Intersects(geometry, {clip_mbr})"
    )
    # Do a bunch of stuff, including clipping the lines back to the original
    # tile boundaries, projecting them into 4326, and loading them into a
    # PostGIS table
    try:
        run([
            "ogr2ogr",
            "-append",
            "-skipfailures",
            "-nln", TABLE_NAME,
            "-nlt", "MULTILINESTRING",
            "-dialect", "sqlite",
            "-sql", clip_sql,
            "-f", "PostgreSQL", f"PG:dbname={DBNAME}",
            "-t_srs", f"EPSG:{SRID}",
            "--config", "PG_USE_COPY", "YES",
            merge_contours_path
        ], check=True)
    except CalledProcessError as ogrerror:
        util.log(f"Could not extract contours for {merge_contours_path}: {ogrerror}")
        return tile
    return None


def make_contours_table(tiles, procs=2):
    """
        Make contours from DEM files using a multiprocessing pool for this
        presumably CPU-bound process
    """
    make_database()
    util.run_sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    # Create the table, with its MULTILINESTRING column, before starting the
    # pool. Otherwise every worker's `ogr2ogr -append` tries to create it on
    # its first write, and the first two workers to finish race each other to
    # do so (issue #18).
    util.run_sql(f"""
        CREATE TABLE {TABLE_NAME} (
            ogc_fid SERIAL,
            PRIMARY KEY (ogc_fid),
            id NUMERIC(8,0),
            elevation NUMERIC(23,15),
            wkb_geometry geometry(MULTILINESTRING,{SRID})
        )
    """)
    util.run_sql(
        f"CREATE INDEX {TABLE_NAME}_wkb_geometry_geom_idx "
        f"ON {TABLE_NAME} USING GIST (wkb_geometry)"
    )
    failed_tiles = []
    with Pool(processes=procs) as pool:
        pbar = tqdm(
            pool.imap_unordered(make_contours_for_tile, tiles),
            desc="Converting to contours & importing",
            unit=" tiles",
            total=len(tiles)
        )
        for result in pbar:
            if result is not None:
                failed_tiles.append(result)
    if failed_tiles:
        util.log(
            f"WARNING: failed to import contours for {len(failed_tiles)} of "
            f"{len(tiles)} tile(s): {failed_tiles}"
        )


async def make_contours_pmtiles(
    zoom,
    swlon=None,
    swlat=None,
    nelon=None,
    nelat=None,
    geojson=None,
    pmtiles_zoom=None,
    clean=False,
    procs=2,
    path="./contours.pmtiles"
):
    """Make the pmtiles for contours"""
    make_database()
    zooms = [zoom]
    if not pmtiles_zoom:
        pmtiles_zoom = zoom
    print("Clearing out existing data...")
    if os.path.exists(path):
        os.remove(path)
    if os.path.isdir(CACHE_DIR):
        util.call_cmd(
            ["find", CACHE_DIR, "-type", "f", "-name", "*.merge*", "-delete"]
        )
    else:
        os.mkdir(CACHE_DIR)
    tiles = None
    if geojson:
        tiles = tiles_from_geojson(geojson, zooms)
    elif swlon and swlat and nelon and nelat:
        tiles = tiles_from_bbox(swlon, swlat, nelon, nelat, zooms)
    if tiles is None:
        raise ValueError("You must specify a bounding box or a GeoJSON feature")
    await cache_tiles(tiles, clean=clean)
    make_contours_table(tiles, procs=procs)
    # TODO make pmtiles_zoom into pmtiles_zooms which is a mapping between the
    # desired zooms in the pmtiles and what zoom-level table in the database to
    # fill it with (i.e. what contour resolution)
    cmd = [
        "ogr2ogr",
        path,
        f"PG:dbname={DBNAME}",
        TABLE_NAME,
        "-dsco", f"MINZOOM={pmtiles_zoom}",
        "-dsco", f"MAXZOOM={pmtiles_zoom}",
        "-dsco", "DESCRIPTION=\"Elevation contours, 25m interval\""
    ]
    util.call_cmd(cmd)
    return path


def make_contours(
    zoom,
    swlon=None,
    swlat=None,
    nelon=None,
    nelat=None,
    geojson=None,
    pmtiles_zoom=None,
    clean=False,
    procs=2,
    path="./contours.pmtiles"
):
    """Seemingly useless method so we can export a synchronous method that calls async code"""
    pmtiles_path = asyncio.run(
        make_contours_pmtiles(
            zoom,
            swlon=swlon,
            swlat=swlat,
            nelon=nelon,
            nelat=nelat,
            geojson=geojson,
            pmtiles_zoom=pmtiles_zoom,
            clean=clean,
            procs=procs,
            path=path
        )
    )
    return pmtiles_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Make an PMTiles of contours given a bounding box and "
        "zoom range"
    )
    parser.add_argument("zoom", type=int, help="Single zoom or minimum zoom")
    parser.add_argument("--swlon", type=float, help="Bounding box swlon")
    parser.add_argument("--swlat", type=float, help="Bounding box swlat")
    parser.add_argument("--nelon", type=float, help="Bounding box nelon")
    parser.add_argument("--nelat", type=float, help="Bounding box nelat")
    parser.add_argument(
        "-f", "--geojson", type=str,
        help="Path to a file with a GeoJSON feature defining the target area"
    )
    parser.add_argument(
        "--procs", type=int,
        help="Number of processes to use for multiprocessing"
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Clean cached data before running"
    )
    args = parser.parse_args()
    min_zoom = args.zoom
    path = None
    if args.geojson:
        with open(args.geojson, encoding="utf-8") as f:
            geojson = json.loads(f.read())
            path = make_contours(
                min_zoom, geojson=geojson, clean=args.clean, procs=args.procs)
    else:
        path = make_contours(
            args.swlon, args.swlat, args.nelon, args.nelat, min_zoom,
            clean=args.clean, procs=args.procs
        )
    if path:
        print(f"PMTiles created at {path}")
    else:
        print("Failed to generate PMTiles")
