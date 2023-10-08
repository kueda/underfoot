"""Create a pack metadata file

# Create a pack metadata json file given three sources. This will 
python create-pack.py rgm_004 of2005_1305_ca of2005_1305_nv \
    --admin1="United States" \
    --admin2="California" \
    --id us-ca-tahoe \
    --name="Lake Tahoe and surrounding area, CA, USA"

"""

import argparse
import json
import os
import tempfile

from sys import exit as sys_exit
import fiona
from shapely.geometry import shape, mapping
import shapely

from sources.util import call_cmd, log


def get_attribute(attr, args):
    """Get attribute from args with interactive fallback"""
    if args.interactive:
        return input(f"{attr}: ")
    return vars(args)[attr]

def get_sources(args):
    """Get sources"""
    if args.interactive:
        sources =  input("Sources (comma-separated): ").split(",")
    else:
        sources = args.sources
    if len(sources) == 0:
        raise ValueError("You must specify some rock sources")
    return sources


def generate_sources(sources, args):
    """Check if sources have been generated and do so if not"""
    for source in sources:
        source_output_path = f"sources/work-{source}/units.geojson"
        if os.path.isfile(source_output_path):
            log(f"Output for {source} exists at {source_output_path}")
        else:
            source_py_path = f"sources/{source}.py"
            log(f"Running {source_py_path}...")
            call_cmd(["python", source_py_path])

def generate_geojson(sources, args, data):
    """Make geojson file from the convex hulls of rock geometries"""
    outpath = os.path.join("packs", f"{data['id']}.geojson")
    if os.path.isfile(outpath):
        log(f"{outpath} exists, skipping GeoJSON generation...")
        return outpath
    generate_sources(sources, args)
    pack_hulls = []
    for source in sources:
        source_output_path = f"sources/work-{source}/units.geojson"
        log(f"Making convex hull from {source_output_path}")
        with fiona.open(source_output_path) as collection:
            source_hulls = [shape(feature.geometry).convex_hull for feature in collection]
            source_hull = shapely.geometrycollections(source_hulls)
            pack_hulls.append(source_hull)
    pack_hull = shapely.geometrycollections(pack_hulls).convex_hull
    schema = {
      'geometry': 'Polygon'
    }
    log(f"Writing geojson to {outpath}")
    with fiona.open(outpath, "w", "GeoJSON", schema) as output:
        output.write({
            'geometry': mapping(pack_hull)
        })
    if args.interactive:
        proceed = input(f"Generated a boundary GeoJSON at {outpath}, which you may want to edit. "
                        "Proceed w/o editing? [Y/n]: ")
        if proceed == "n":
            sys_exit()
    return outpath


def generate_bbox(geojson_path):
    """Generate bounds from GeoJSON path"""
    with fiona.open(geojson_path) as geojson:
        left, bottom, right, top = geojson.bounds
        return {
            "bottom": bottom,
            "left": left,
            "right": right,
            "top": top
        }


def shapely_geometry_collection_from_geojson(geojson_path):
    """Return a Shapely geometry collection from a GeoJSON path"""
    with fiona.collection(geojson_path) as geojson:
        return shapely.geometrycollections([shape(feature.geometry) for feature in geojson])


def find_geofabrik_url(geojson_path):
    """Finds the URL of the smallest Geofabrik extract containing the GeoJSON shape"""
    geofabrik_index_geojson_path = "geofabrik_index.geojson"
    geofabrik_index_geojson_url = "https://download.geofabrik.de/index-v1.json"
    if not os.path.isfile(geofabrik_index_geojson_path):
        log(f"DOWNLOADING {geofabrik_index_geojson_url}")
        call_cmd(["curl", "-L", "-o", geofabrik_index_geojson_path, geofabrik_index_geojson_url])
    pack_geom = shapely_geometry_collection_from_geojson(geojson_path)
    with fiona.open('geofabrik_index.geojson') as geofabrik_index:
        containing_features = [
            feature for feature in geofabrik_index
            if shape(feature.geometry).contains(pack_geom)
        ]
    if len(containing_features) == 0:
        return None
    smallest_feature = min(containing_features,
        key=lambda feature: shapely.area(shape(feature.geometry)))
    return smallest_feature.properties['urls']['pbf']


def find_nhd_hu4_sources(geojson_path):
    """Finds relevant NHD HU4 sources"""
    simplified_wbd_hu4_geojson_path = "simplified_wbd_hu4.geojson"
    if not os.path.isfile(simplified_wbd_hu4_geojson_path):
        log(
            f"{simplified_wbd_hu4_geojson_path} doesn't exist, "
            "it's gonna take a while to generate..."
        )
        wbd_gpkg_url = (
            "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/"
            "GPKG/WBD_National_GPKG.zip"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            wbd_gpkg_zip_path = os.path.join(tmpdir, os.path.basename(wbd_gpkg_url))
            if not os.path.isfile(wbd_gpkg_zip_path):
                log(f"DOWNLOADING {wbd_gpkg_url}")
                call_cmd(["curl", "-L", "-o", wbd_gpkg_zip_path, wbd_gpkg_url])
                call_cmd(["unzip", "-u", "-o", wbd_gpkg_zip_path, "-d", tmpdir])
                call_cmd([
                    "ogr2ogr",
                    simplified_wbd_hu4_geojson_path,
                    os.path.join(tmpdir, "WBD_National_GPKG.gpkg"),
                    "WBDHU4",
                    "-simplify",
                    "0.05"
                ])
    pack_geom = shapely_geometry_collection_from_geojson(geojson_path)
    with fiona.open(simplified_wbd_hu4_geojson_path) as wbdhu4:
        intersecting_features = [
            feature for feature in wbdhu4
            if shape(feature.geometry).intersects(pack_geom)
        ]
    return [f"nhdplus_h_{feature.properties['huc4']}_hu4" for feature in intersecting_features]


def find_tiger_water_sources(geojson_path):
    """Find relevant TIGER water sources"""
    tiger_counties_geojson_path = "tiger_counties.geojson"
    if not os.path.isfile(tiger_counties_geojson_path):
        tiger_counties_shp_url = (
            "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_county_20m.zip"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            tiger_counties_zip_path = os.path.join(tmpdir, os.path.basename(tiger_counties_shp_url))
            if not os.path.isfile(tiger_counties_zip_path):
                log(f"DOWNLOADING {tiger_counties_shp_url}")
                call_cmd(["curl", "-L", "-o", tiger_counties_zip_path, tiger_counties_shp_url])
                call_cmd(["unzip", "-u", "-o", tiger_counties_zip_path, "-d", tmpdir])
                call_cmd([
                    "ogr2ogr",
                    tiger_counties_geojson_path,
                    os.path.join(tmpdir, "cb_2022_us_county_20m.shp")
                ])
    pack_geom = shapely_geometry_collection_from_geojson(geojson_path)
    with fiona.open(tiger_counties_geojson_path) as counties:
        intersecting_features = [
            feature for feature in counties
            if shape(feature.geometry).intersects(pack_geom)
        ]
    return [f"tiger_water_{feature.properties['GEOID']}" for feature in intersecting_features]


def find_water_sources(geojson_path):
    """Finds relevant water sources given a GeoJSON boundary"""
    return find_nhd_hu4_sources(geojson_path) + find_tiger_water_sources(geojson_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make a pack metadata file")
    parser.add_argument(
        "sources",
        nargs="*",
        type=str,
        help="Rock source identifiers to use")
    parser.add_argument(
        "--admin1",
        type=str,
        help="Country or other top-level political entity containing the pack")
    parser.add_argument(
        "--admin2",
        type=str,
        help="State or other secondary political entity containing the pack")
    parser.add_argument(
        "--id",
        type=str,
        help="Hyphenated, unique identifier for this pack, preferably using two letter codes for "
             "admin1 and admin2 places, e.g. us-ca-san-francisco")
    parser.add_argument(
        "--name",
        type=str,
        help="Name of this pack")
    parser.add_argument(
        "--description",
        type=str,
        help="Description of this pack")
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Solicit fields interactively")
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Print debug statements")
    args = parser.parse_args()
    data = {
        "id": get_attribute("id", args),
        "name": get_attribute("name", args),
        "description": get_attribute("description", args),
        "admin1": get_attribute("admin1", args),
        "admin2": get_attribute("admin2", args),
        "rock": get_sources(args)
    }
    if not data["id"] or len(data["id"]) == 0:
        raise ValueError("You must specify an ID")
    geojson_path = generate_geojson(data["rock"], args, data)
    data["geojson"] = {
        "$ref": f"file://./{os.path.basename(geojson_path)}"
    }
    data["bbox"] = generate_bbox(geojson_path)
    data["osm"] = find_geofabrik_url(geojson_path)
    data["water"] = find_water_sources(geojson_path)
    outfile_path = os.path.join("packs", f"{data['id']}.json",)
    with open(outfile_path, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=True)
    print(f"Pack created at {outfile_path}")
