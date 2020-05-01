"""Script for creating data 'packs' for use in Underfoot

Diverging from the README, starting from a a fresh checkout you would make a
pack like:

python setup.py
python packs.py us-ca-oakland

Which *should* create all the necessary files and wrap them up in a zip archive

"""

import argparse
import os
import shutil
import tempfile
from rocks import make_rocks
from osm import make_ways
from elevation import make_contours
from database import make_database
from sources import util

PACKS = {
  "us-ca": {
    "description": "US state of California",
    "rock": [
      "mf2342c",      # Oakland, CA
      "mf2337c",      # SF, most of Marin County, CA
      "of94_622",     # Contra Costa County, CA
      "of97_489",     # Santa Cruz County, CA
      "of98_354",     # South SF
      "mf2403c",      # Napa and Lake Counties, in part
      "of98_137",     # San Mateo County, CA
      "mf2402",       # Western Sonoma County
      "sim2858",      # Mark West Springs, Sonoma County (Pepperwood)
      "of96_252",     # Alameda County, CA
      "of97_456",     # Point Reyes, Marin County, CA
      "of97_744",     # San Francisco Bay Area
      "ofr20151175",  # Joshua Tree National Park
      "of99_014",     # Carrizo Plain
      "of2005_1305",  # all of California, coarse
    ],
    "osm": "http://download.geofabrik.de/north-america/us/california-latest.osm.pbf"
  },
  "us-ca-sfba": {
    "description": "Nine counties of the San Francisco Bay Area in California, USA, plus Santa Cruz County",
    "rock": [
      "mf2342c",      # Oakland, CA
      "mf2337c",      # SF, most of Marin County, CA
      "of94_622",     # Contra Costa County, CA
      "of97_489",     # Santa Cruz County, CA
      "of98_354",     # South SF
      "mf2403c",      # Napa and Lake Counties, in part
      "of98_137",     # San Mateo County, CA
      "mf2402",       # Western Sonoma County
      "sim2858",      # Mark West Springs, Sonoma County (Pepperwood)
      "of96_252",     # Alameda County, CA
      "of97_456",     # Point Reyes, Marin County, CA
      "of97_744",     # San Francisco Bay Area
    ],
    "osm": "http://download.geofabrik.de/north-america/us/california/norcal-latest.osm.pbf"
  },
  "us-ca-oakland": {
    "description": "Oakland, CA, USA. Mostly for testing some place small.",
    "rock": [
      "mf2342c",      # Oakland, CA
    ],
    "osm": "http://download.geofabrik.de/north-america/us/california/norcal-latest.osm.pbf",
    "bbox": {
      "top": 37.9999225069647,
      "bottom": 37.6249329829376,
      "left": -122.37608299613,
      "right": -122.00107120948
    }
  }
}

def list_packs():
  for pack_name in PACKS:
    print("\t{}: {}".format(pack_name, PACKS[pack_name]["description"]))

def make_pack(pack_name, clean=False):
  make_database()
  pack = PACKS[pack_name]
  paths = []
  paths.append(make_rocks(pack["rock"], args))
  paths.append(make_ways(pack["osm"], bbox=pack["bbox"]))
  # TODO make this work with a boundary. Mercantile can do this and we don't
  # need to download most of Nevada for an mbtiles covering California
  paths.append(
    make_contours(
      pack["bbox"]["left"],
      pack["bbox"]["bottom"],
      pack["bbox"]["right"],
      pack["bbox"]["top"],
      12,
      mbtiles_zoom=14,
      clean=clean))
  with tempfile.TemporaryDirectory() as tmpdirname:
    pack_path = os.path.join(tmpdirname, pack_name)
    os.makedirs(pack_path)
    for path in paths:
      shutil.copy(path, pack_path)
    return shutil.make_archive(pack_name, "zip", pack_path)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Make a data pack for Underfoot")
  parser.add_argument("pack", metavar="PACK_NAME", type=str, help="Make the specified pack. Use `list` to list available packs")
  parser.add_argument("--clean", action="store_true", help="Clean all cached files before building")
  args = parser.parse_args()

  if args.pack == "list":
    print("Available packs:")
    list_packs()
  else:
    print("making pack: {}".format(args.pack))
    pack_path = make_pack(args.pack, clean=args.clean)
    print(f"Pack available at {pack_path}")
