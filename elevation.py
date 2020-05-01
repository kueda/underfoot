"""Generate an MBTiles of contours from Mapzen / Amazon elevation tiles
(https://registry.opendata.aws/terrain-tiles/)
"""
from database import make_database, DBNAME, DB_USER, DB_PASSWORD, SRID
from fiona.transform import transform
from sources import util
from subprocess import run
from tqdm import tqdm
import argparse
import mercantile
import os
import httpx
import asyncio
import aiofiles
from multiprocessing import Pool

TABLE_NAME = "contours"
CACHE_DIR = "./elevation-tiles"

def tile_file_path(x, y, z, ext="tif"):
  return "{}/{}/{}/{}.{}".format(CACHE_DIR, z, x, y, ext)

async def cache_tile(tile, client, clean=False):
  tile_path = "{}/{}/{}.tif".format(tile.z, tile.x, tile.y)
  url = "https://s3.amazonaws.com/elevation-tiles-prod/geotiff/{}".format(tile_path)
  dir_path = "./{}/{}/{}".format(CACHE_DIR, tile.z, tile.x)
  file_path = tile_file_path(tile.x, tile.y, tile.z)
  os.makedirs(dir_path, exist_ok=True)
  if os.path.exists(file_path):
    if clean:
      os.remove(file_path)
    else:
      return file_path
  # TODO handle errors, client abort, server abort
  r = await client.get(url)
  if r.status_code != 200:
    print("Request for {} failed with {}".formt(url, r.status_code))
    return
  async with aiofiles.open(file_path, 'wb') as fd:
    async for chunk in r.aiter_bytes():
      await fd.write(chunk)
    # print("Wrote {}".format(file_path))

# Cache DEM tiles using asyncio for this presumably IO-bound process
async def cache_tiles(swlon, swlat, nelon, nelat, zooms, clean=False):
  async with httpx.AsyncClient() as client:
    tiles = list(mercantile.tiles(swlon, swlat, nelon, nelat, zooms, truncate=False))
    # using as_completed with tqdm (https://stackoverflow.com/a/37901797)
    tasks = [cache_tile(tile, client, clean=clean) for tile in tiles]
    pbar = tqdm(asyncio.as_completed(tasks),
      total=len(tasks),
      desc="Downloading DEM TIFs",
      unit=" tiles")
    for task in pbar:
      await task

def make_contours_for_tile(tile):
  # print("Making contours for {}".format(tile))
  x = tile.x
  y = tile.y
  z = tile.z
  merge_contours_path = tile_file_path(tile.x, tile.y, tile.z, ext="merge-contours.shp");
  if os.path.exists(merge_contours_path):
    if clean:
      os.remove(merge_contours_path)
    else:
      return
  interval = 1000;
  if z >= 10:
    interval = 25
  elif z >= 8:
    interval = 100
  # Merge all 8 tiles that surround this tile so we don't get weird edge effects
  merge_coords = [
    [x - 1, y - 1], [x + 0, y - 1], [x + 1, y - 1],
    [x - 1, y + 0], [x + 0, y + 0], [x + 1, y + 0],
    [x - 1, y + 1], [x + 0, y + 1], [x + 1, y + 1]
  ]
  merge_file_paths = [tile_file_path(xy[0], xy[1], z) for xy in merge_coords]
  merge_file_paths = [path for path in merge_file_paths if os.path.exists(path)]
  merge_path = tile_file_path(x, y, z, "merge.tif")
  run([
    "gdal_merge.py", "-q", "-o", merge_path, *merge_file_paths
  ])
  run([
    "gdal_contour", "-q", "-i", str(interval), "-a", "elevation", merge_path, merge_contours_path
  ])
  # Get the bounding box of this tile in lat/lon, project into the source
  # coordinate system (Pseudo Mercator) so ogr2ogr can clip it before
  # importing into PostGIS
  bounds = mercantile.bounds(x, y, z)
  xs, ys = transform('EPSG:4326', 'EPSG:3857', [bounds.west, bounds.east], [bounds.south, bounds.north])
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
  # os.remove(merge_path)

# Make contours from DEM files using a multiprocessing pool for this presumably
# CPU-bound process
def make_contours_table(swlon, swlat, nelon, nelat, zooms, clean=False, procs=2):
  make_database()
  if clean:
    for z in zooms:
      util.run_sql("DROP TABLE IF EXISTS {}".format(TABLE_NAME))
  tiles = list(mercantile.tiles(swlon, swlat, nelon, nelat, zooms, truncate=False))
  pool = Pool(processes=procs)
  pbar = tqdm(pool.imap_unordered(make_contours_for_tile, tiles),
    desc="Converting to contours & importing",
    unit=" tiles",
    total=len(tiles))
  for _ in pbar:
    pass

async def make_contours_mbtiles(swlon, swlat, nelon, nelat, zoom, mbtiles_zoom=None,
    clean=False, procs=2):
  zooms = [zoom]
  if not mbtiles_zoom:
    mbtiles_zoom = zoom
  mbtiles_path = "./elevation.mbtiles"
  print("Clearing out existing data...")
  if os.path.exists(mbtiles_path):
    os.remove(mbtiles_path)
  util.call_cmd(["psql", DBNAME, "-c", "DROP TABLE {}".format(TABLE_NAME)])
  util.call_cmd(["find", "elevation-tiles/", "-type", "f", "-name", "*.merge*", "-delete"])
  await cache_tiles(swlon, swlat, nelon, nelat, zooms, clean=clean)
  make_contours_table(swlon, swlat, nelon, nelat, zooms, clean=clean, procs=procs)
  util.call_cmd([
    "./node_modules/tl/bin/tl.js", "copy",
    "-i", "elevation.json",
    "-z", str(mbtiles_zoom),
    "-Z", str(mbtiles_zoom),
    "postgis://{}:{}@localhost:5432/{}?table={}".format(
      DB_USER,
      DB_PASSWORD,
      DBNAME,
      TABLE_NAME
    ),
    "mbtiles://{}".format(mbtiles_path)
  ])
  return mbtiles_path

# Seemingly useless method so we can export a synchronous method that calls
# async code
def make_contours(swlon, swlat, nelon, nelat, min_zoom,
    mbtiles_zoom=None, clean=False, procs=2):
  path = asyncio.run(
    make_contours_mbtiles(
      swlon,
      swlat,
      nelon,
      nelat,
      min_zoom,
      mbtiles_zoom=mbtiles_zoom,
      clean=clean,
      procs=procs
    )
  )
  return path

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Make an MBTiles of contours given a bounding box and zoom range")
  parser.add_argument("swlon", type=float, help="Bounding box swlon")
  parser.add_argument("swlat", type=float, help="Bounding box swlat")
  parser.add_argument("nelon", type=float, help="Bounding box nelon")
  parser.add_argument("nelat", type=float, help="Bounding box nelat")
  parser.add_argument("zoom", type=int, help="Single zoom or minimum zoom")
  parser.add_argument("--procs", type=int, help="Number of processes to use for multiprocessing")
  parser.add_argument("--clean", action="store_true", help="Clean cached data before running")
  args = parser.parse_args()
  min_zoom = args.zoom
  path = make_contours(args.swlon, args.swlat, args.nelon, args.nelat, min_zoom,
    clean=args.clean, procs=args.procs)
  print("MBTiles created at {}".format(path))