"""Script to import  Geologic Map of the Lake Tahoe Basin, California and Nevada"""

import csv
import json
import os
import time
import util
from util.rocks import infer_metadata_from_csv, join_polygons_and_metadata


WORK_PATH = util.make_work_dir(os.path.realpath(__file__))
os.chdir(WORK_PATH)
SRS = "+proj=utm +zone=10 +datum=NAD27 +units=m +no_defs"  # noqa: E501
URL = "https://www.conservation.ca.gov/cgs/Documents/Publications/" \
    "Regional-Geologic-Maps/RGM_004/RGM_004-Tahoe-Basin-2005-100k-GIS.zip"
DATA_PATH = os.path.join("shapefiles", "TahoeBasin_geo_poly.shp")

def get_archive(url, data_path):
    """Fetch the data"""
    download_path = os.path.basename(url)
    if not os.path.isfile(download_path):
        print(f"DOWNLOADING {url}")
        util.call_cmd(["curl", "-OL", url])
    if data_path.endswith(".gdb"):
        is_extracted = os.path.isdir(data_path)
    else:
        is_extracted = os.path.isfile(data_path)
    if not is_extracted:
        print("EXTRACTING ARCHIVE...")
        util.call_cmd(["unzip", download_path])
    return os.path.realpath(data_path)


def copy_citation():
    """Copies the citation into the output directory"""
    util.call_cmd([
      "cp",
      os.path.join(os.path.dirname(__file__), "citation.json"),
      os.path.join(WORK_PATH, "citation.json")
    ])

def run():
    """Actually do the work"""
    shp_path = get_archive(URL, DATA_PATH)
    copy_citation()
    metadata_csv_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "units.csv"
    )
    metadata_path = infer_metadata_from_csv(metadata_csv_path)
    projected_shp_path = os.path.join(WORK_PATH, "units.shp")
    table_name = util.extless_basename(shp_path)
    util.call_cmd([
        "ogr2ogr",
        "-s_srs", SRS,
        "-t_srs", util.SRS,
        projected_shp_path,
        shp_path,
        "-overwrite",
        "-dialect", "sqlite",
        "-sql",
        f"SELECT PTYPE,ST_Union(geometry) as geometry FROM '{table_name}' GROUP BY PTYPE"
    ])
    join_polygons_and_metadata(
        projected_shp_path,
        metadata_path,
        polygons_join_col="PTYPE",
        polygons_table_name="units",
        output_path="units.geojson"
    )
