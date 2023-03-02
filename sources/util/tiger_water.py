"""Methods for processing tiger areawater data"""

import json
import os
import re
import time
from . import log, call_cmd, make_work_dir, SRS as UNDERFOOT_SRS

SRS = "EPSG:4269"


def download(fips):
    """Download the data and extract it"""
    work_path = make_work_dir(os.path.realpath(__file__))
    url = f"https://www2.census.gov/geo/tiger/TIGER2020/AREAWATER/tl_2020_{fips}_areawater.zip"
    # Download the data
    download_path = os.path.join(work_path, os.path.basename(url))
    if os.path.isfile(download_path):
        log(f"Download exists at {download_path}, skipping...")
    else:
        log(f"DOWNLOADING {url}")
        call_cmd(["curl", "-f", "-L", url, "--output", download_path])
    # Unpack the zip
    shp_path = os.path.join(work_path, f"tl_2020_{fips}_areawater.shp")
    if os.path.isfile(shp_path):
        log(f"Archive already extracted at {shp_path}, skipping...")
    else:
        log("EXTRACTING ARCHIVE...")
        call_cmd(["unzip", "-u", "-o", download_path, "-d", work_path])


def make_gpkg(fips, dst_path, append=False):
    """Convert data to a GeoPackage and normalize some data"""
    log(f"Making gpkg for {fips}, append: {append }")
    work_path = make_work_dir(os.path.realpath(__file__))
    basename = f"tl_2020_{fips}_areawater"
    shp_path = os.path.join(work_path, f"{basename}.shp")
    gpkg_path = os.path.join(dst_path, "waterbodies.gpkg")
    if os.path.isfile(gpkg_path) and not append:
        print(f"Removing {gpkg_path}")
        os.remove(gpkg_path)
    sql = f"""
        SELECT
            HYDROID AS waterbody_id,
            HYDROID AS source_id,
            'HYDROID' AS source_id_attr,
            CASE
            WHEN FULLNAME LIKE '%Riv' THEN REPLACE(FULLNAME, ' Riv', ' River')
            WHEN FULLNAME LIKE '%Crk' THEN REPLACE(FULLNAME, ' Crk', ' Creek')
            WHEN FULLNAME LIKE '%C v' THEN REPLACE(FULLNAME, ' C v', ' Cove')
            ELSE FULLNAME
            END AS name,
            CASE
            WHEN MTFCC = 'H2051' THEN 'bay/estuary/gulf/sound'
            WHEN MTFCC = 'H2053' THEN 'ocean/sea'
            WHEN MTFCC = 'H2081' THEN 'glacier'
            WHEN MTFCC = 'H3010' THEN 'stream'
            WHEN MTFCC = 'H3013' THEN 'stream'
            WHEN MTFCC = 'H3020' THEN 'stream'
            ELSE 'unknown'
            END AS type,
            1 AS is_natural,
            'perennial' AS permanence,
            Geometry AS geom
        FROM {basename}
        WHERE
            MTFCC IN ('H2051', 'H2053', 'H2081', 'H3010', 'H3013', 'H3020')
    """
    sql = re.sub(r'\s+', " ", sql)
    cmd = [
        "ogr2ogr",
        "-s_srs", SRS,
        "-t_srs", UNDERFOOT_SRS,
        "-overwrite",
        gpkg_path,
        shp_path,
        "-dialect", "sqlite",
        "-nln", "waterbodies",
        "-nlt", "MULTIPOLYGON",
        "-sql", sql
    ]
    if append:
        cmd += ["-append"]
    call_cmd(cmd, check=True)
    return gpkg_path


def copy_citation(dst_path):
    """Write citation.json"""
    citation_path = os.path.join(dst_path, "citation.json")
    if os.path.isfile(citation_path):
        return
    log(f"Copying citation to {citation_path}")
    now = time.localtime()
    data = [
        {
            "id": "https://www2.census.gov/geo/tiger/TIGER2020/AREAWATER",
            "type": "webpage",
            "container-title": "Census.gov",
            "title": "Index of /geo/tiger/TIGER2020/AREAWATER",
            "URL": "https://www2.census.gov/geo/tiger/TIGER2020/AREAWATER",
            "author": [
                {
                    "family": "United States Census Bureau"
                }
            ],
            "accessed": {
                "date-parts": [
                    [
                      now.tm_year,
                      now.tm_mon,
                      now.tm_mday
                    ]
                ]
            },
            "issued": {
                "date-parts": [
                    [
                        "2020"
                    ]
                ]
            }
        }
    ]
    with open(citation_path, 'w', encoding="utf-8") as outfile:
        json.dump(data, outfile)


def process_tiger_water_for_fips(fips_codes, source):
    """Generate water source data from TIGER given a list of FIPS codes"""
    dst_path = make_work_dir(source)
    for fips in fips_codes:
        download(fips)
    for idx, fips in enumerate(fips_codes):
        make_gpkg(fips, dst_path=dst_path, append=(idx > 0))
    copy_citation(dst_path)
