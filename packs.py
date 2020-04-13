"""Script for creating data 'packs' for use in Underfoot

Diverging from the README, starting from a a fresh checkout you would make a
pack like:

python setup.py
python packs.py us-ca-oakland

Which *should* create all the necessary files and wrap them up in a zip archive

"""

import argparse
from rocks import make_rocks
from database import make_database

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
    "osm": "http://download.geofabrik.de/north-america/us/california/norcal-latest.osm.pbf"
  }
}

def list_packs():
  for pack_name in PACKS:
    print("\t{}: {}".format(pack_name, PACKS[pack_name]["description"]))

def make_pack(pack_name):
  make_database()
  pack = PACKS[pack_name]
  paths = make_rocks(pack["rock"])
  # These should happen last b/c they depend on the spatial scope of the
  # database tables populated above
  # TODO Make the OSM mbtiles
  # TODO Make the contours mbtiles
  # TODO zip up all relevant files

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Make a data pack for Underfoot")
  parser.add_argument("pack", metavar="PACK_NAME", type=str, help="Make the specified pack. Use `list` to list available packs")
  args = parser.parse_args()

  if args.pack == "list":
    print("Available packs:")
    list_packs()
  else:
    print("making pack: {}".format(args.pack))
    make_pack(args.pack)
