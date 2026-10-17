"""
Geologic Map of Alaska
"""

import csv
import os

from util import call_cmd, download_file, log, make_work_dir, unzip
from util.proj import SRS
from util.rocks import process_usgs_source

BASE_URL = "https://pubs.usgs.gov/sim/3340"
SHP_DIR = "sim3340_shp/AKgeol_web_shp"
GENSTATE_PATH = "sim3340_databases/Database_files/CSV/genstate.csv"
POLYGONS_PATH = "western_hemisphere_polygons.shp"
METADATA_PATH = "genstate_utf8.csv"


def download(filename):
    """Download a file from the publication unless we already have it"""
    if not os.path.isfile(filename):
        log(f"DOWNLOADING {filename}")
        download_file(f"{BASE_URL}/{filename}")
    return filename


def prepare_polygons():
    """
    Clip the polygons to the western hemisphere. The Aleutians west of the
    antimeridian would otherwise make the source span the globe. Also label
    the glaciers, which the source leaves unlabeled even though the unit
    descriptions include them, and drop the parts of Canada the source
    includes. Drop water too: it's mostly the sea, cut into quadrangles, with
    more vertices than many units combined, and the water layer covers it.
    """
    if os.path.isfile(POLYGONS_PATH):
        return
    call_cmd(["unzip", "-u", download("sim3340_shp.zip"), f"{SHP_DIR}/AKStategeol_poly.*"])
    call_cmd([
        "ogr2ogr",
        "-t_srs", SRS,
        "-wrapdateline",
        "-clipdst", "-180", "-90", "0", "90",
        "-makevalid",
        "-dialect", "sqlite",
        "-sql",
        "SELECT CASE STATE_UNIT WHEN 'Glaciers' THEN 'g' ELSE STATE_LA_1 "
        "END AS STATE_LA_1, geometry FROM AKStategeol_poly "
        "WHERE STATE_LA_1 IS NOT 'Canada' AND STATE_UNIT IS NOT 'Water'",
        "-overwrite",
        POLYGONS_PATH,
        f"{SHP_DIR}/AKStategeol_poly.shp"
    ], check=True)


def prepare_metadata():
    """Unit descriptions are in a separate archive, encoded as Windows-1252"""
    if os.path.isfile(METADATA_PATH):
        return
    unzip(download("sim3340_databases.zip"))
    with open(GENSTATE_PATH, encoding="cp1252") as infile:
        rows = [row for row in csv.DictReader(infile) if row["State_label"]]
    with open(METADATA_PATH, "w", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def years_from_ma(col):
    """Make a mapping that converts ages in Ma to years"""
    return lambda row: float(row[col]) * 1_000_000 if row[col] else ""


def run():
    os.chdir(make_work_dir(os.path.realpath(__file__)))
    prepare_polygons()
    prepare_metadata()
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url=f"{BASE_URL}/sim3340_shp.zip",
        extracted_file_path=POLYGONS_PATH,
        srs=SRS,
        polygons_join_col="STATE_LA_1",
        mappable_metadata_csv_path=METADATA_PATH,
        mappable_metadata_mapping={
            "code": "State_label",
            "title": "Unit_name",
            "span": "Age_range",
            "description": "Description",
            "min_age": years_from_ma("Min_age"),
            "max_age": years_from_ma("Max_age")
        }
    )
