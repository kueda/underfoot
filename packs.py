"""Script for creating data 'packs' for use in Underfoot

Diverging from the README, starting from a a fresh checkout you would make a
pack like:

python setup.py
python packs.py us-ca-oakland

Which *should* create all the necessary files and wrap them up in a zip archive

"""

from database import make_database
from elevation import make_contours
from glob import glob
from osm import make_ways
from rocks import make_rocks
from sources import util
from urllib.parse import urlparse
import argparse
import json
import os
import pathlib
import shutil
import tempfile

PACKS = {}
pack_glob = os.path.join(pathlib.Path(__file__).parent.absolute(), "packs/*.json")
for pack_path in glob(pack_glob):
    with open(pack_path) as pack_f:
        pack = json.load(pack_f)
        if pack["geojson"] and pack["geojson"]["$ref"]:
            parsed_uri = urlparse(pack["geojson"]["$ref"])
            geojson_path = os.path.join(
                pathlib.Path(pack_path).parent.absolute(),
                f"{parsed_uri.netloc}{parsed_uri.path}")
            with open(geojson_path) as geojson_f:
                pack["geojson"] = json.load(geojson_f)
        PACKS[os.path.basename(os.path.splitext(pack_path)[0])] = pack

def list_packs():
    for pack_name in PACKS:
        print("\t{}: {}".format(pack_name, PACKS[pack_name]["description"]))

def make_pack(pack_name, clean=False):
    make_database()
    pack = PACKS[pack_name]
    paths = []
    paths.append(make_rocks(pack["rock"], args))
    paths.append(make_ways(pack["osm"], bbox=pack["bbox"]))
    if pack["geojson"]:
        paths.append(
            make_contours(12, geojson=pack["geojson"], mbtiles_zoom=14, clean=clean))
    else:
        paths.append(
            make_contours(
                12,
                swlon=pack["bbox"]["left"],
                swlat=pack["bbox"]["bottom"],
                nelon=pack["bbox"]["right"],
                nelat=pack["bbox"]["top"],
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
