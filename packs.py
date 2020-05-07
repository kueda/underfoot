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

def make_pack(pack_name, clean=False, clean_rocks=False, clean_ways=False,
        clean_contours=False, procs=2):
    make_database()
    pack = PACKS[pack_name]
    build_dir = os.path.join(pathlib.Path(__file__).parent.absolute(), "build" )
    pack_dir = os.path.join(build_dir, pack_name)
    if not os.path.isdir(pack_dir):
        os.makedirs(pack_dir)
    rocks_mbtiles_path = os.path.join(pack_dir, "rocks.mbtiles")
    if clean or clean_rocks == "rocks" or not os.path.isfile(rocks_mbtiles_path):
        make_rocks(pack["rock"], clean=(clean or clean_rocks), path=rocks_mbtiles_path)
    elif os.path.isfile(rocks_mbtiles_path):
        print(f"{rocks_mbtiles_path} exists, skipping...")
    ways_mbtiles_path = os.path.join(pack_dir, "ways.mbtiles")
    if clean or clean_ways or not os.path.isfile(ways_mbtiles_path):
        make_ways(pack["osm"], bbox=pack["bbox"], clean=(clean or clean_ways),
            path=ways_mbtiles_path)
    elif os.path.isfile(ways_mbtiles_path):
        print(f"{ways_mbtiles_path} exists, skipping...")
    contours_mbtiles_path = os.path.join(pack_dir, "contours.mbtiles")
    if clean or clean_contours or not os.path.isfile(contours_mbtiles_path):
        if pack["geojson"]:
            make_contours(12, geojson=pack["geojson"], mbtiles_zoom=14,
                clean=(clean or clean_contours), procs=procs,
                path=contours_mbtiles_path)
        else:
            make_contours(
                12,
                swlon=pack["bbox"]["left"],
                swlat=pack["bbox"]["bottom"],
                nelon=pack["bbox"]["right"],
                nelat=pack["bbox"]["top"],
                mbtiles_zoom=14,
                path=contours_mbtiles_path,
                clean=(clean or clean_contours))
    elif os.path.isfile(contours_mbtiles_path):
        print(f"{contours_mbtiles_path} exists, skipping...")
    with tempfile.TemporaryDirectory() as tmpdirname:
        return shutil.make_archive(pack_dir, format="zip", root_dir=pack_dir,
            base_dir=build_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make a data pack for Underfoot")
    parser.add_argument("pack", metavar="PACK_NAME", type=str, help="Make the specified pack. Use `list` to list available packs")
    parser.add_argument("--clean", action="store_true", help="Clean all cached files before building")
    parser.add_argument("--clean-rocks", action="store_true", help="Clean all cached files for geologic data before building")
    parser.add_argument("--clean-ways", action="store_true", help="Clean all cached files for ways before building")
    parser.add_argument("--clean-contours", action="store_true", help="Clean all cached files for contours before building")
    parser.add_argument("--procs", type=int, default=2, help="Number of processes to run in parallel when multiprocessing")
    args = parser.parse_args()

    if args.pack == "list":
        print("Available packs:")
        list_packs()
    else:
        print("making pack: {}".format(args.pack))
        pack_path = make_pack(args.pack, clean=args.clean,
            clean_rocks=args.clean_rocks, clean_ways=args.clean_ways,
            clean_contours=args.clean_contours, procs=args.procs)
        print(f"Pack available at {pack_path}")
