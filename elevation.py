from database import make_database, DBNAME, DB_USER, DB_PASSWORD, SRID
from fiona.transform import transform
from sources import util
from subprocess import run
from tqdm import tqdm
import argparse
import mercantile
import os
import requests
import shutil
import sys
import time
import urllib.request

TABLE_NAME = "contours"
CACHE_DIR = "./elevation-tiles"

def tile_file_path(x, y, z, ext="tif"):
  return "{}/{}/{}/{}.{}".format(CACHE_DIR, z, x, y, ext)

def cache_tile(tile, clean=False):
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
  # print("Downloading {}...".format(url))
  # with urllib.request.urlopen(url) as response, open(file_path, "wb") as out_file:
  #   shutil.copyfileobj(response, out_file)
  #   print("Wrote {}".format(out_file))
  r = requests.get(url, stream=True)
  if r.status_code != 200:
    print("Request for {} failed with {}".formt(url, r.status_code))
    return
  with open(file_path, 'wb') as fd:
    for chunk in r.iter_content(chunk_size=128):
      fd.write(chunk)
    # print("Wrote {}".format(file_path))

def cache_tiles(swlon, swlat, nelon, nelat, zooms, clean=False):
  tiles = list(mercantile.tiles(swlon, swlat, nelon, nelat, zooms, truncate=False))
  pbar = tqdm(tiles, desc="Downloading DEM TIFs", unit=" tiles")
  for tile in pbar:
    # TODO parallelize
    cache_tile(tile, clean=clean)

def make_contours(swlon, swlat, nelon, nelat, zooms, clean=False):
  make_database()
  if clean:
    for z in zooms:
      util.run_sql("DROP TABLE IF EXISTS {}".format(TABLE_NAME))
  tiles = list(mercantile.tiles(swlon, swlat, nelon, nelat, zooms, truncate=False))
  pbar = tqdm(tiles, desc="Converting to contours & importing", unit=" tiles")
  for tile in pbar:
    # print("Making contours for {}".format(tile))
    x = tile.x
    y = tile.y
    z = tile.z
    merge_contours_path = tile_file_path(tile.x, tile.y, tile.z, ext="merge-contours.shp");
    if os.path.exists(merge_contours_path):
      if clean:
        os.remove(merge_contours_path)
      else:
        continue
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

def make_elevation_mbtiles(swlon, swlat, nelon, nelat, zoom, mbtiles_zoom=None,
    clean=False):
  zooms = [zoom]
  if not mbtiles_zoom:
    mbtiles_zoom = zoom
  mbtiles_path = "./elevation.mbtiles"
  print("Clearing out existing data...")
  if os.path.exists(mbtiles_path):
    os.remove(mbtiles_path)
  util.call_cmd(["psql", DBNAME, "-c", "DROP TABLE {}".format(TABLE_NAME)])
  util.call_cmd(["find", "elevation-tiles/", "-type", "f", "-name", "*.merge*", "-delete"])
  cache_tiles(swlon, swlat, nelon, nelat, zooms, clean=clean)
  make_contours(swlon, swlat, nelon, nelat, zooms, clean=clean)
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

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Make an MBTiles of contours given a bounding box and zoom range")
  parser.add_argument("swlon", type=float, help="Bounding box swlon")
  parser.add_argument("swlat", type=float, help="Bounding box swlat")
  parser.add_argument("nelon", type=float, help="Bounding box nelon")
  parser.add_argument("nelat", type=float, help="Bounding box nelat")
  parser.add_argument("zoom", type=int, help="Single zoom or minimum zoom")
  parser.add_argument("--clean", action="store_true", help="Clean cached data before running")
  args = parser.parse_args()
  min_zoom = args.zoom
  path = make_elevation_mbtiles(args.swlon, args.swlat, args.nelon, args.nelat, min_zoom, clean=args.clean)
  print("MBTiles created at {}".format(path))
