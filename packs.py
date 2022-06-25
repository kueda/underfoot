"""Script for creating data 'packs' for use in Underfoot

Diverging from the README, starting from a a fresh checkout you would make a
pack like:

python setup.py
python packs.py us-ca-oakland

Which *should* create all the necessary files and wrap them up in a zip archive

"""

from datetime import datetime
from glob import glob
from urllib.parse import urlparse
from urllib.request import urlopen
import argparse
import xml.etree.ElementTree as ET
import json
import os
import pathlib
import shutil
import re

from database import make_database
from elevation import make_contours
from osm import make_ways
from rocks import make_rocks
from water import make_water

REQUIRED_ATTRIBUTES = [
    "admin1",
    "id",
    "name"
]

ATTRIBUTES_FOR_METADATA = [
    "admin1",
    "admin2",
    "description",
    "id",
    "name"
]


def validate_pack(path, pack):
    """Validate a pack"""
    for attribute in REQUIRED_ATTRIBUTES:
        if attribute not in pack or not pack[attribute]:
            raise Exception(f"{path} is not valid: missing {attribute}")

PACKS = {}
pack_glob = os.path.join(
    pathlib.Path(__file__).parent.absolute(),
    "packs/*.json"
)
for pack_path in glob(pack_glob):
    with open(pack_path, encoding="utf-8") as pack_f:
        pack = json.load(pack_f)
        validate_pack(pack_path, pack)
        if "geojson" in pack and "$ref" in pack["geojson"]:
            parsed_uri = urlparse(pack["geojson"]["$ref"])
            geojson_path = os.path.join(
                pathlib.Path(pack_path).parent.absolute(),
                f"{parsed_uri.netloc}{parsed_uri.path}"
            )
            with open(geojson_path, encoding="utf-8") as geojson_f:
                pack["geojson"] = json.load(geojson_f)
        PACKS[os.path.basename(os.path.splitext(pack_path)[0])] = pack


def list_packs():
    """Print a list of all possible packs"""
    for pack_id, pack in PACKS.items():
        print(f"\t{pack_id}: {pack['description']}")


def get_build_dir():
    """Return absolute path to the directory where pack files will be written"""
    return os.path.join(pathlib.Path(__file__).parent.absolute(), "build")


def add_metadata_to_pack(pack):
    """Augments a pack dict that has info about a specific file with descriptive static metadata"""
    if pack["id"] not in PACKS:
        return pack
    full_pack = PACKS[pack["id"]]
    filtered = dict((k, full_pack[k]) for k in ATTRIBUTES_FOR_METADATA if k in full_pack)
    return {
        **pack,
        **filtered
    }


def make_manifest(manifest_url=None, s3_bucket_url=None):
    """
    Make a JSON file that describes all generated packs, optionall including
    packs from a remote manifest or remote s3 bucket in the definition
    of "all"
    """
    build_dir = get_build_dir()
    remote_packs = remote_built_packs(manifest_url) if manifest_url else {}
    s3_packs = s3_built_packs(s3_bucket_url) if s3_bucket_url else {}
    local_packs = local_built_packs()
    merged_packs = {**s3_packs, **remote_packs, **local_packs}
    final_packs = [add_metadata_to_pack(pack) for pack_id, pack in merged_packs.items()]
    manifest = {"packs": final_packs, "updated_at": max([p["updated_at"] for p in final_packs])}
    with open(os.path.join(build_dir, "manifest.json"), "w", encoding="utf-8") as manifest_f:
        json.dump(manifest, manifest_f)


def local_built_packs():
    """List locally built packs"""
    build_dir = get_build_dir()
    packs = {}
    for path in glob(os.path.join(build_dir, "*.zip")):
        pack_id = os.path.basename(re.sub(r"\.zip$", "", path))
        pack_path = os.path.join(build_dir, f"{pack_id}.zip")
        packs[pack_id] = {
            "id": pack_id,
            "path": os.path.relpath(pack_path, build_dir),
            "updated_at": datetime.isoformat(
                datetime.fromtimestamp(
                    os.path.getmtime(pack_path)
                )
            )
        }
    return packs


def remote_built_packs(manifest_url):
    """List packs from a remote manifest.json"""
    with urlopen(manifest_url) as response:
        remote_json = json.loads(response.read().decode())
        return {pack["id"]: pack for pack in remote_json["packs"]}


def s3_built_packs(s3_bucket_url):
    """List packs from a publicly-accessible S3 bucket"""
    packs = {}
    s3ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    with urlopen(s3_bucket_url) as response:
        xml = ET.fromstring(response.read())
        for file in xml.findall("s3:Contents", s3ns):
            file_name = file.find("s3:Key", s3ns).text
            if not file_name.endswith(".zip"):
                continue
            pack_id = os.path.basename(os.path.splitext(file_name)[0])
            packs[pack_id] = {
                "id": pack_id,
                "path": file_name,
                "updated_at": file.find("s3:LastModified", s3ns).text
            }
    return packs


def make_pack(pack_id, clean=False, clean_rocks=False, clean_water=False,
              clean_ways=False, clean_contours=False, procs=2):
    """Generate a pack and write it to the build directory"""
    make_database()
    pack = PACKS[pack_id]
    build_dir = get_build_dir()
    pack_dir = os.path.join(build_dir, pack_id)
    if not os.path.isdir(pack_dir):
        os.makedirs(pack_dir)
    rocks_mbtiles_path = os.path.join(pack_dir, "rocks.mbtiles")
    if (
        clean
        or clean_rocks == "rocks" or not os.path.isfile(rocks_mbtiles_path)
    ):
        make_rocks(
            pack["rock"],
            bbox=pack["bbox"],
            clean=(clean or clean_rocks),
            path=rocks_mbtiles_path,
            procs=procs)
    elif os.path.isfile(rocks_mbtiles_path):
        print(f"{rocks_mbtiles_path} exists, skipping...")
    water_mbtiles_path = os.path.join(pack_dir, "water.mbtiles")
    if clean or clean_water or not os.path.isfile(water_mbtiles_path):
        make_water(
            pack["water"],
            bbox=pack["bbox"],
            # TODO make pack options to clip water / rocks / ways by the bbox
            # or not. sometimes it makes more sense to include everythign in
            # the sources
            clean=(clean or clean_water),
            path=water_mbtiles_path)
    elif os.path.isfile(water_mbtiles_path):
        print(f"{water_mbtiles_path} exists, skipping...")
    ways_mbtiles_path = os.path.join(pack_dir, "ways.mbtiles")
    if clean or clean_ways or not os.path.isfile(ways_mbtiles_path):
        make_ways(
            pack["osm"],
            bbox=pack["bbox"],
            clean=(clean or clean_ways),
            path=ways_mbtiles_path)
    elif os.path.isfile(ways_mbtiles_path):
        print(f"{ways_mbtiles_path} exists, skipping...")
    contours_mbtiles_path = os.path.join(pack_dir, "contours.mbtiles")
    if clean or clean_contours or not os.path.isfile(contours_mbtiles_path):
        if "geojson" in pack:
            make_contours(
                12,
                geojson=pack["geojson"],
                mbtiles_zoom=14,
                clean=(clean or clean_contours),
                procs=procs,
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
                clean=(clean or clean_contours),
                procs=procs)
    elif os.path.isfile(contours_mbtiles_path):
        print(f"{contours_mbtiles_path} exists, skipping...")
    # with tempfile.TemporaryDirectory() as tmpdirname:  # noqa: F841
    return shutil.make_archive(
        pack_dir,
        format="zip",
        root_dir=build_dir,
        base_dir=os.path.basename(pack_dir))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Make a data pack for Underfoot")
    parser.add_argument(
        "pack",
        metavar="pack_id",
        type=str,
        help="Make the specified pack. Use `list` to list available packs")
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean all cached files before building")
    parser.add_argument(
        "--clean-rocks",
        action="store_true",
        help="Clean all cached files for geologic data before building")
    parser.add_argument(
        "--clean-water",
        action="store_true",
        help="Clean all cached files for hydrologic data before building")
    parser.add_argument(
        "--clean-ways",
        action="store_true",
        help="Clean all cached files for ways before building")
    parser.add_argument(
        "--clean-contours",
        action="store_true",
        help="Clean all cached files for contours before building")
    parser.add_argument(
        "--procs",
        type=int,
        default=2,
        help="Number of processes to run in parallel when multiprocessing")
    parser.add_argument(
        "--manifest-url",
        type=str,
        help="""
            URL of an existing manifest.json to merge any new local packs into when the local
            manifest gets generated
        """)
    parser.add_argument(
        "--s3-bucket-url",
        type=str,
        help="URL of an existing s3 bucket to use when generating the manifest")
    args = parser.parse_args()

    if args.pack == "list":
        print("Available packs:")
        list_packs()
    elif args.pack == "manifest":
        make_manifest(manifest_url=args.manifest_url, s3_bucket_url=args.s3_bucket_url)
    else:
        print(f"Making pack: {args.pack}")
        pack_path = make_pack(
            args.pack,
            clean=args.clean,
            clean_rocks=args.clean_rocks,
            clean_water=args.clean_water,
            clean_ways=args.clean_ways,
            clean_contours=args.clean_contours,
            procs=args.procs)
        make_manifest(manifest_url=args.manifest_url, s3_bucket_url=args.s3_bucket_url)
        print(f"Pack available at {pack_path}")
