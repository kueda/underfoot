"""Generate an MBTiles of contours from Mapzen / Amazon elevation tiles
(https://registry.opendata.aws/terrain-tiles/)"""

from database import make_database, DBNAME, SRID
from fiona.transform import transform
from multiprocessing import Pool
from sources import util
from subprocess import run
from supermercado import burntiles, super_utils
from tqdm import tqdm
import aiofiles
import argparse
import asyncio
import httpx
import json
import mercantile
import os


TABLE_NAME = "contours"
CACHE_DIR = "./elevation-tiles"


def tile_file_path(x, y, z, ext="tif"):
    return "{}/{}/{}/{}.{}".format(CACHE_DIR, z, x, y, ext)


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
    features = [geojson]
    # supermercado expects features, so if this is a just a geometry, wrap it in a
    # Feature
    if geojson["type"] != "Feature":
        features = [{"type": "Feature", "geometry": geojson}]
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


async def cache_tile(tile, client, clean=False, max_retries=3):
    tile_path = "{}/{}/{}.tif".format(tile.z, tile.x, tile.y)
    url = f"https://s3.amazonaws.com/elevation-tiles-prod/geotiff/{tile_path}"
    dir_path = "./{}/{}/{}".format(CACHE_DIR, tile.z, tile.x)
    file_path = tile_file_path(tile.x, tile.y, tile.z)
    os.makedirs(dir_path, exist_ok=True)
    if os.path.exists(file_path):
        if clean:
            os.remove(file_path)
        else:
            return file_path
    # TODO handle errors, client abort, server abort
    for try_num in range(1, max_retries + 1):
        try:
            r = await client.get(url)
            if r.status_code != 200:
                util.log(
                    f"Request for {url} failed with {r.status_code}, "
                    "skipping..."
                )
                return
            async with aiofiles.open(file_path, 'wb') as fd:
                async for chunk in r.aiter_bytes():
                    await fd.write(chunk)
            break
        except (asyncio.exceptions.TimeoutError, httpx.ConnectTimeout):
            if try_num > max_retries:
                util.log(
                    f"Request for {url} timed out {max_retries} times, "
                    "skipping..."
                )
            else:
                # util.log(f"Sleeping for {try_num ** 3}s...")
                await asyncio.sleep(try_num ** 3)
            pass


# Cache DEM tiles using asyncio for this presumably IO-bound process
async def cache_tiles(tiles, clean=False):
    async with httpx.AsyncClient() as client:
        # using as_completed with tqdm (https://stackoverflow.com/a/37901797)
        tasks = [cache_tile(tile, client, clean=clean) for tile in tiles]
        pbar = tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Downloading DEM TIFs",
            unit=" tiles"
        )
        for task in pbar:
            await task


def make_contours_for_tile(tile, clean=False):
    # print("Making contours for {}".format(tile))
    x = tile.x
    y = tile.y
    z = tile.z
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
    if z >= 10:
        interval = 25
    elif z >= 8:
        interval = 100
    # Merge all 8 tiles that surround this tile so we don't get weird edge
    # effects
    merge_coords = [
        [x - 1, y - 1], [x + 0, y - 1], [x + 1, y - 1],
        [x - 1, y + 0], [x + 0, y + 0], [x + 1, y + 0],
        [x - 1, y + 1], [x + 0, y + 1], [x + 1, y + 1]
    ]
    merge_file_paths = [tile_file_path(xy[0], xy[1], z) for xy in merge_coords]
    merge_file_paths = [
        path for path in merge_file_paths if os.path.exists(path)
    ]
    merge_path = tile_file_path(x, y, z, "merge.tif")
    run(["gdal_merge.py", "-q", "-o", merge_path, *merge_file_paths])
    run([
        "gdal_contour", "-q", "-i", str(interval), "-a", "elevation", merge_path,
        merge_contours_path
    ])
    # Get the bounding box of this tile in lat/lon, project into the source
    # coordinate system (Pseudo Mercator) so ogr2ogr can clip it before
    # importing into PostGIS
    bounds = mercantile.bounds(x, y, z)
    xs, ys = transform(
        'EPSG:4326',
        'EPSG:3857',
        [bounds.west, bounds.east],
        [bounds.south, bounds.north]
    )
    # Do a bunch of stuff, including clipping the lines back to the original
    # tile boundaries, projecting them into 4326, and loading them into a
    # PostGIS table
    run([
        "ogr2ogr",
        "-append",
        "-skipfailures",
        "-nln", TABLE_NAME,
        "-nlt", "MULTILINESTRING",
        "-clipsrc", *[str(c) for c in [xs[0], ys[0], xs[1], ys[1]]],
        "-f", "PostgreSQL", 'PG:dbname={}'.format(DBNAME),
        "-t_srs", "EPSG:{}".format(SRID),
        "--config", "PG_USE_COPY", "YES",
        merge_contours_path
    ])


def make_contours_table(tiles, clean=False, procs=2):
    """
        Make contours from DEM files using a multiprocessing pool for this
        presumably CPU-bound process
    """
    make_database()
    zooms = set([tile.z for tile in tiles])
    for z in zooms:
        util.run_sql("DROP TABLE IF EXISTS {}".format(TABLE_NAME))
    pool = Pool(processes=procs)
    pbar = tqdm(
        pool.imap_unordered(make_contours_for_tile, tiles),
        desc="Converting to contours & importing",
        unit=" tiles",
        total=len(tiles)
    )
    for _ in pbar:
        pass


async def make_contours_mbtiles(
    zoom,
    swlon=None,
    swlat=None,
    nelon=None,
    nelat=None,
    geojson=None,
    mbtiles_zoom=None,
    clean=False, procs=2, path="./contours.mbtiles"
):
    zooms = [zoom]
    if not mbtiles_zoom:
        mbtiles_zoom = zoom
    print("Clearing out existing data...")
    if os.path.exists(path):
        os.remove(path)
    util.call_cmd(["psql", DBNAME, "-c", f"DROP TABLE IF EXISTS {TABLE_NAME}"])
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
        raise "You must specify a bounding box or a GeoJSON feature"
    await cache_tiles(tiles, clean=clean)
    make_contours_table(tiles, clean=clean, procs=procs)
    # TODO make mbtiles_zoom into mbtiles_zooms which is a mapping between the
    # desired zooms in the mbtiles and what zoom-level table in the database to
    # fill it with (i.e. what contour resolution)
    cmd = [
        "ogr2ogr",
        "-f", "MBTILES",
        path,
        f"PG:dbname={DBNAME}",
        TABLE_NAME,
        "-dsco", f"MINZOOM={mbtiles_zoom}",
        "-dsco", f"MAXZOOM={mbtiles_zoom}",
        "-dsco", "DESCRIPTION=\"Elevation contours, 25m interval\""
    ]
    util.call_cmd(cmd)
    return path


# Seemingly useless method so we can export a synchronous method that calls
# async code
def make_contours(
    zoom,
    swlon=None,
    swlat=None,
    nelon=None,
    nelat=None,
    geojson=None,
    mbtiles_zoom=None,
    clean=False,
    procs=2,
    path="./contours.mbtiles"
):
    mbtiles_path = asyncio.run(
        make_contours_mbtiles(
            zoom,
            swlon=swlon,
            swlat=swlat,
            nelon=nelon,
            nelat=nelat,
            geojson=geojson,
            mbtiles_zoom=mbtiles_zoom,
            clean=clean,
            procs=procs,
            path=path
        )
    )
    return mbtiles_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Make an MBTiles of contours given a bounding box and "
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
        with open(args.geojson) as f:
            geojson = json.loads(f.read())
            path = make_contours(
                min_zoom, geojson=geojson, clean=args.clean, procs=args.procs)
    else:
        path = make_contours(
            args.swlon, args.swlat, args.nelon, args.nelat, min_zoom,
            clean=args.clean, procs=args.procs
        )
    if path:
        print("MBTiles created at {}".format(path))
    else:
        print("Failed to generate MBTiles")
