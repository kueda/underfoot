import os
import re
import util
from util import log, call_cmd, SRS

gdb_name = "tlgdb_2020_a_us_areawater.gdb"
work_path = util.make_work_dir(os.path.realpath(__file__))
srs = "EPSG:4269"
os.chdir(work_path)

FIPS_CODES = [
    # Belknap County
    "33001",
    # Carroll County
    "33003",
    # Cheshire County
    "33005",
    # Coös County
    "33007",
    # Grafton County
    "33009",
    # Hillsborough County
    "33011",
    # Merrimack County
    "33013",
    # Rockingham County
    "33015",
    # Strafford County
    "33017",
    # Sullivan County
    "33019",
]


def download(fips):
    url = f"https://www2.census.gov/geo/tiger/TIGER2020/AREAWATER/tl_2020_{fips}_areawater.zip"  # noqa: E501
    # Download the data
    download_path = os.path.basename(url)
    if os.path.isfile(download_path):
        log(f"Download exists at {download_path}, skipping...")
    else:
        log(f"DOWNLOADING {url}")
        call_cmd(["curl", "-OL", url])
    # Unpack the zip
    shp_path = f"tl_2020_{fips}_areawater.shp"
    if os.path.isfile(shp_path):
        log(f"Archive already extracted at {shp_path}, skipping...")
    else:
        log("EXTRACTING ARCHIVE...")
        call_cmd(["unzip", "-o", download_path])


def make_gpkg(fips, append=False):
    print(f"making gpkg for {fips}, append: {append }")
    basename = f"tl_2020_{fips}_areawater"
    shp_path = f"tl_2020_{fips}_areawater.shp"
    gpkg_path = "waterbodies.gpkg"
    if os.path.isfile(gpkg_path) and not append:
        print(f"Removing {gpkg_path}")
        os.remove(gpkg_path)
    sql = """
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
        FROM {}
        WHERE
            MTFCC IN ('H2051', 'H2053', 'H2081', 'H3010', 'H3013', 'H3020')
    """.format(basename)
    sql = re.sub(r'\s+', " ", sql)
    cmd = [
        "ogr2ogr",
        "-s_srs", srs,
        "-t_srs", SRS,
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


def run():
    for fips in FIPS_CODES:
        download(fips)
    for idx, fips in enumerate(FIPS_CODES):
        make_gpkg(fips, append=(idx > 0))
    print("COPYING CITATION")
    call_cmd([
      "cp",
      os.path.join(os.path.dirname(__file__), "citation.json"),
      os.path.join(work_path, "citation.json")
    ])
