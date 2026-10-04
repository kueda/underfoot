"""Create a pack metadata file

# Create a pack metadata json file given one or more rock sources
python create_pack.py rgm_004 of2005_1305_ca of2005_1305_nv \
    --admin1="United States" \
    --admin2="California" \
    --id us-ca-tahoe \
    --name="Lake Tahoe and surrounding area, CA, USA"

"""

import argparse
import json
import os
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request

from sys import exit as sys_exit
import fiona
from shapely.geometry import shape, mapping
import shapely

from sources.util import call_cmd, download_file, log


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

    # Pre-provided file (--geojson PATH)
    if args.geojson:
        log(f"Copying boundary from {args.geojson} to {outpath}")
        shutil.copy2(args.geojson, outpath)
        return outpath

    # Interactive boundary selection
    if args.interactive:
        print("How would you like to define the pack boundary?")
        print("  1. Auto-generate from source convex hulls (default)")
        print("  2. Look up a county from TIGER county boundaries")
        print("  3. Look up from Nominatim (OSM geocoder)")
        print("  4. Provide a GeoJSON file path")
        choice = input("Choice [1/2/3/4]: ").strip() or "1"
        if choice == "2":
            county = input("County name (without 'County', e.g. 'Inyo'): ").strip()
            state = input("State name (e.g. 'California'): ").strip()
            return find_tiger_county_geojson(county, state, outpath)
        if choice == "3":
            query = input("Nominatim search query: ").strip()
            return find_nominatim_geojson(query, outpath)
        if choice == "4":
            src = input("GeoJSON file path: ").strip()
            shutil.copy2(src, outpath)
            return outpath
        # choice "1" falls through to hull generation below

    # Auto-generate from convex hulls (original behavior)
    pack_hulls = []
    for source in sources:
        source_output_path = f"sources/work-{source}/units.geojson"
        log(f"Making convex hull from {source_output_path}")
        with fiona.open(source_output_path) as collection:
            source_hulls = [shape(feature.geometry).convex_hull for feature in collection]
            source_hull = shapely.geometrycollections(source_hulls)
            pack_hulls.append(source_hull)
    pack_hull = shapely.geometrycollections(pack_hulls).convex_hull
    schema = {'geometry': 'Polygon'}
    log(f"Writing geojson to {outpath}")
    with fiona.open(outpath, "w", "GeoJSON", schema) as output:
        output.write({'geometry': mapping(pack_hull)})
    if args.interactive:
        proceed = input(
            f"Generated a boundary GeoJSON at {outpath}, which you may want to edit. "
            "Proceed w/o editing? [Y/n]: "
        )
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


GEOFABRIK_INDEX_GEOJSON_PATH = "geofabrik_index.geojson"

# Extracts bigger than this get confirmed before they go into a pack. The
# California extract is about 1.3 GB, while a multi-state extract like
# us-northeast is 1.8 GB and all of the US is over 12 GB.
MAX_OSM_EXTRACT_BYTES = 1_500_000_000

# Extracts covering less of the pack than this aren't worth suggesting
MIN_OSM_CANDIDATE_COVERAGE = 0.9


def ensure_geofabrik_index():
    """Download the Geofabrik extract index if not present"""
    geofabrik_index_geojson_url = "https://download.geofabrik.de/index-v1.json"
    if not os.path.isfile(GEOFABRIK_INDEX_GEOJSON_PATH):
        log(f"DOWNLOADING {geofabrik_index_geojson_url}")
        download_file(geofabrik_index_geojson_url, GEOFABRIK_INDEX_GEOJSON_PATH)


def find_geofabrik_url(geojson_path):
    """Finds the URL of the smallest Geofabrik extract containing the GeoJSON shape"""
    ensure_geofabrik_index()
    # Shrink the pack by about 500 m so slivers along its edges don't rule out
    # an extract, e.g. where a state boundary in the source data differs
    # slightly from the one Geofabrik used. The OSM outline of New York
    # overhangs Geofabrik's New York polygon by about 200 m.
    pack_geom = shapely_geometry_collection_from_geojson(geojson_path).buffer(-0.005)
    with fiona.open(GEOFABRIK_INDEX_GEOJSON_PATH) as geofabrik_index:
        containing_features = [
            feature for feature in geofabrik_index
            if shape(feature.geometry).contains(pack_geom)
        ]
    if len(containing_features) == 0:
        return None
    smallest_feature = min(containing_features,
        key=lambda feature: shapely.area(shape(feature.geometry)))
    return smallest_feature.properties['urls']['pbf']


def find_geofabrik_candidates(geojson_path):
    """List Geofabrik extracts intersecting the GeoJSON shape, smallest first,
    with the fraction of the shape each one covers"""
    ensure_geofabrik_index()
    pack_geom = shapely.union_all(
        shapely_geometry_collection_from_geojson(geojson_path).geoms
    )
    candidates = []
    with fiona.open(GEOFABRIK_INDEX_GEOJSON_PATH) as geofabrik_index:
        for feature in geofabrik_index:
            # Some index polygons are invalid, and buffer(0) repairs them
            extract_geom = shape(feature.geometry).buffer(0)
            if not extract_geom.intersects(pack_geom):
                continue
            candidates.append({
                "id": feature.properties["id"],
                "url": feature.properties["urls"]["pbf"],
                "area": extract_geom.area,
                "coverage": extract_geom.intersection(pack_geom).area / pack_geom.area
            })
    return sorted(candidates, key=lambda candidate: candidate["area"])


def get_content_length(url):
    """Size of the file at a URL in bytes, or None if the server doesn't say"""
    req = urllib.request.Request(
        url, method="HEAD", headers={"User-Agent": "underfoot/create_pack.py"}
    )
    try:
        with urllib.request.urlopen(req) as response:
            length = response.headers.get("Content-Length")
    except urllib.error.URLError as err:
        log(f"Couldn't get size of {url}: {err}")
        return None
    return int(length) if length else None


def format_bytes(num_bytes):
    """Human-readable size"""
    if num_bytes is None:
        return "unknown size"
    return f"{num_bytes / 1_000_000_000:.1f} GB"


def choose_osm_url(geojson_path, interactive=False):
    """Choose a Geofabrik extract for the pack, asking for confirmation or an
    alternative if the smallest one containing it is missing or huge"""
    url = find_geofabrik_url(geojson_path)
    size = get_content_length(url) if url else None
    if url and (size is None or size <= MAX_OSM_EXTRACT_BYTES):
        return url
    candidates = [
        candidate for candidate in find_geofabrik_candidates(geojson_path)
        if candidate["coverage"] >= MIN_OSM_CANDIDATE_COVERAGE
    ]
    lines = [
        f"  {i}. {candidate['id']}: {candidate['coverage']:.2%} of pack, "
        f"{format_bytes(get_content_length(candidate['url']))}, {candidate['url']}"
        for i, candidate in enumerate(candidates, start=1)
    ]
    if url:
        problem = f"The smallest Geofabrik extract containing the pack is {format_bytes(size)}"
    else:
        problem = "No Geofabrik extract contains the pack"
    message = "\n".join([f"{problem}. Extracts covering most of it:", *lines])
    if not interactive:
        raise ValueError(f"{message}\nChoose one and pass it with --osm URL")
    print(message)
    default_hint = f", or blank for {url}" if url else ""
    while True:
        choice = input(f"Choose a number, paste a URL{default_hint}: ").strip()
        if not choice and url:
            return url
        if choice.isdigit() and 1 <= int(choice) <= len(candidates):
            return candidates[int(choice) - 1]["url"]
        if choice.startswith(("http://", "https://")):
            return choice


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
                download_file(wbd_gpkg_url, wbd_gpkg_zip_path)
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


# Matches the vintage of the TIGER water files in sources/util/tiger_water.py,
# so the tiger_water_<GEOID> sources we pick exist. Later vintages replace
# Connecticut's counties with planning regions that have different GEOIDs.
TIGER_COUNTIES_VINTAGE = "2020"
TIGER_COUNTIES_GEOJSON_PATH = f"tiger_counties_{TIGER_COUNTIES_VINTAGE}.geojson"


def ensure_tiger_counties():
    """Download TIGER county shapefile and convert to GeoJSON if not present"""
    if os.path.isfile(TIGER_COUNTIES_GEOJSON_PATH):
        return
    tiger_counties_shp_url = (
        f"https://www2.census.gov/geo/tiger/GENZ{TIGER_COUNTIES_VINTAGE}/shp/"
        f"cb_{TIGER_COUNTIES_VINTAGE}_us_county_20m.zip"
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, os.path.basename(tiger_counties_shp_url))
        log(f"DOWNLOADING {tiger_counties_shp_url}")
        download_file(tiger_counties_shp_url, zip_path)
        call_cmd(["unzip", "-u", "-o", zip_path, "-d", tmpdir])
        call_cmd([
            "ogr2ogr", TIGER_COUNTIES_GEOJSON_PATH,
            os.path.join(tmpdir, f"cb_{TIGER_COUNTIES_VINTAGE}_us_county_20m.shp")
        ])


def find_tiger_county_geojson(county_name, state_name, outpath):
    """Write a single-feature GeoJSON for the named county to outpath"""
    ensure_tiger_counties()
    with open(TIGER_COUNTIES_GEOJSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    matches = [
        feat for feat in data["features"]
        if feat["properties"]["NAME"].lower() == county_name.lower()
        and feat["properties"]["STATE_NAME"].lower() == state_name.lower()
    ]
    if not matches:
        raise ValueError(f"No county '{county_name}' found in '{state_name}'.")
    if len(matches) > 1:
        county_only = [m for m in matches if m["properties"]["LSAD"] == "06"]
        if len(county_only) == 1:
            matches = county_only
        else:
            raise ValueError(
                f"Ambiguous: {[m['properties']['NAMELSAD'] for m in matches]}. "
                "Use --geojson to provide a boundary directly."
            )
    log(f"Writing TIGER boundary for {county_name}, {state_name} to {outpath}")
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": [matches[0]]}, f)
    return outpath


def find_nominatim_geojson(query, outpath):
    """Write a single-feature GeoJSON from Nominatim geocoder to outpath"""
    params = urllib.parse.urlencode({
        "q": query, "format": "geojson", "polygon_geojson": "1", "limit": "1"
    })
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "underfoot/create_pack.py"})
    log(f"Querying Nominatim: {url}")
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode("utf-8"))
    if not data.get("features"):
        raise ValueError(f"Nominatim returned no results for: '{query}'.")
    log(f"Writing Nominatim boundary for '{query}' to {outpath}")
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": [data["features"][0]]}, f)
    return outpath


def find_tiger_water_sources(geojson_path):
    """Find relevant TIGER water sources"""
    ensure_tiger_counties()
    pack_geom = shapely_geometry_collection_from_geojson(geojson_path)
    with fiona.open(TIGER_COUNTIES_GEOJSON_PATH) as counties:
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
        "--geojson",
        type=str,
        metavar="PATH",
        help="Path to a pre-existing GeoJSON boundary file, skipping auto-generation")
    parser.add_argument(
        "--osm",
        type=str,
        metavar="URL",
        help="URL of the OSM PBF extract to use, skipping the Geofabrik lookup")
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
    generate_sources(data["rock"], args)
    geojson_path = generate_geojson(data["rock"], args, data)
    data["geojson"] = {
        "$ref": f"file://./{os.path.basename(geojson_path)}"
    }
    data["bbox"] = generate_bbox(geojson_path)
    data["osm"] = args.osm or choose_osm_url(geojson_path, interactive=args.interactive)
    data["water"] = find_water_sources(geojson_path)
    outfile_path = os.path.join("packs", f"{data['id']}.json",)
    with open(outfile_path, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4)
    print(f"Pack created at {outfile_path}")
