"""Constants and functions for processing water sources"""

import json
import os
import re
import shutil
from glob import glob

import xml.etree.ElementTree as ET

from . import call_cmd, extless_basename, log, make_work_dir, unzip
from .proj import GRS80_LONGLAT, SRS

WATERWAYS_FNAME = "waterways.gpkg"
WATERBODIES_FNAME = "waterbodies.gpkg"
WATERSHEDS_FNAME = "watersheds.gpkg"
WATERWAYS_NETWORK_FNAME = "waterways-network.sqlite"
CITATION_FNAME = "citation.json"

ARTIFACT_NAMES = [
    WATERWAYS_FNAME,
    WATERBODIES_FNAME,
    WATERSHEDS_FNAME,
    WATERWAYS_NETWORK_FNAME,
    CITATION_FNAME
]

def process_omca_creeks_source(url, dir_name, waterways_shp_path,
                               watersheds_shp_path, waterways_name_col,
                               watersheds_name_col, srs):
    """Process creeks data from the Oakland Museum"""
    download_path = os.path.basename(url)
    if not os.path.isfile(download_path):
        print(f"DOWNLOADING {url}")
        call_cmd(["curl", "-OL", url])

    # Unpack
    dir_path = dir_name
    if not os.path.isdir(dir_path):
        print("EXTRACTING ARCHIVE...")
        unzip(download_path)

    # Project into EPSG 4326 along with name, type, and natural attributes
    waterways_gpkg_path = WATERWAYS_FNAME
    if os.path.isfile(waterways_gpkg_path):
        log("{waterways_gpkg_path} exists, skipping...")
    else:
        lyr_name = extless_basename(waterways_shp_path)
        sql = f"""
            SELECT
                null as waterway_id,
                {waterways_name_col} as name,
                LTYPE as type,
                (LTYPE = 'Creek') AS natural,
                geometry AS geom
            FROM '{lyr_name}'
        """
        sql = re.sub(r'\s+', " ", sql)
        cmd = f"""
            ogr2ogr \
              -s_srs '{srs}' \
              -t_srs '{SRS}' \
              -overwrite \
              {waterways_gpkg_path} \
              {waterways_shp_path} \
              -dialect sqlite \
              -nln waterways \
              -nlt MULTILINESTRING \
              -sql "{sql}"
        """
        call_cmd(cmd, shell=True, check=True)
    watersheds_gpkg_path = WATERSHEDS_FNAME
    if os.path.isfile(watersheds_gpkg_path):
        log("{watersheds_gpkg_path} exists, skipping...")
    else:
        lyr_name = extless_basename(watersheds_shp_path)
        sql = f"""
            SELECT
                OID AS source_id,
                'OID' AS source_id_attr,
                {watersheds_name_col} as name,
                geometry AS geom
            FROM '{lyr_name}'
            WHERE
                {watersheds_name_col} IS NOT NULL
                AND {watersheds_name_col} != ''
        """
        sql = re.sub(r'\s+', " ", sql)
        cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{SRS}' \
          -overwrite \
          {watersheds_gpkg_path} \
          {watersheds_shp_path} \
          -dialect sqlite \
          -nln watersheds \
          -nlt MULTIPOLYGON \
          -sql "{sql}"
        """
        call_cmd(cmd, shell=True, check=True)


def process_nhdplus_hr_source_waterways(gdb_path, srs):
    """Project into EPSG 4326 along with name, type, and natural attributes"""
    waterways_gpkg_path = WATERWAYS_FNAME
    if os.path.isfile(waterways_gpkg_path):
        log(f"{waterways_gpkg_path} exists, skipping...")
        return
    sql = """
        SELECT
          GNIS_ID AS waterway_id,
          GNIS_Name AS name,
          NHDPlusID AS source_id,
          'NHDPlusID' AS source_id_attr,
          CASE
          WHEN NHDFlowline.FCode = 42813 THEN 'siphon'
          WHEN NHDFlowline.FCode IN (33601, 42801, 42804) THEN 'aqueduct'
          WHEN NHDFlowline.FTYPE = 336 THEN 'canal/ditch'
          WHEN NHDFlowline.FTYPE = 558 THEN 'artificial'
          WHEN NHDFlowline.FTYPE = 334 THEN 'connector'
          ELSE
            'stream'
          END AS type,
          (
            -- LAKE/POND
            (NHDFlowline.FCode BETWEEN 39000 AND 39012)
            OR (NHDFlowline.FCode IN (
              -- BAY/INLET
              31200,
              -- PLAYA
              36100,
              -- FORESHORE
              36400,
              -- RAPIDS
              43100,
              -- REEF
              43400,
              -- ROCK
              44100,
              -- ROCK
              44101,
              -- ROCK
              44102,
              -- SEA/OCEAN
              44500,
              -- WASH
              48400,
              -- WATERFALL
              48700,
              -- ESTUARY
              49300,
              -- AREA OF COMPLEX CHANNELS
              53700,
              -- COASTLINE
              56600,
              -- ???
              56700
            ))
            -- SPRING/SEEP
            -- STREAM/RIVER
            -- SUBMERGED STREAM
            -- SWAMP/MARSH
            OR (NHDFlowline.FCode BETWEEN 45800 AND 46602)
          ) AS is_natural,
          LOWER(
            COALESCE(
              NULLIF(NHDFCode.RelationshipToSurface, ' '),
              'surface'
            )
          ) AS surface,
          LOWER(
            COALESCE(
              NULLIF(NHDFCode.HydrographicCategory, ' '),
              'perennial'
            )
          ) AS permanence,
          Shape AS geom
        FROM
          NHDFlowline
            JOIN NHDFcode ON NHDFcode.FCode = NHDFlowline.FCode
        WHERE
          NHDFlowLine.FCode NOT IN (56600, 56700)
    """
    # remove comments, ogr doesn't like them
    sql = re.sub(r'--.+', "", sql)
    sql = re.sub(r'\s+', " ", sql)
    cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{SRS}' \
          -overwrite \
          {waterways_gpkg_path} \
          {gdb_path} \
          -dialect sqlite \
          -nln waterways \
          -nlt MULTILINESTRING \
          -sql "{sql}"
    """
    call_cmd(cmd, shell=True, check=True)


def waterbodies_sql(lyr_name):
    """Make SQL for extracting waterbodies from an NHD layer"""
    return f"""
        SELECT
          GNIS_ID AS waterbody_id,
          GNIS_ID AS source_id,
          'GNIS_ID' AS source_id_attr,
          GNIS_Name AS name,
          CASE
          WHEN {lyr_name}.FCode = 36400 THEN 'foreshore'
          WHEN {lyr_name}.FCode = 40309 THEN 'floodplain'
          WHEN {lyr_name}.FCode = 43607 THEN 'evaporator'
          WHEN {lyr_name}.FCode = 43613 THEN 'storage'
          WHEN {lyr_name}.FCode = 43624 THEN 'treatment'
          WHEN {lyr_name}.FCode = 46006 THEN 'stream/river'
          WHEN {lyr_name}.FCode = 46600 THEN 'swamp/marsh'
          WHEN {lyr_name}.FCode = 53700 THEN 'swamp/marsh'
          WHEN {lyr_name}.FType = 436 THEN 'reservoir'
          ELSE 'lake/pond'
          END AS type,
          NOT (
            GNIS_Name LIKE '%reservoir%'
            OR NHDFcode.Description LIKE '%reservoir%'
            OR {lyr_name}.FType = 436
            OR {lyr_name}.FCode = 43607
            OR {lyr_name}.FCode = 43613
            OR {lyr_name}.FCode = 43624
          ) AS is_natural,
          LOWER(
            COALESCE(
              NULLIF(NHDFCode.HydrographicCategory, ' '),
              'perennial'
            )
          ) AS permanence,
          Shape AS geom
        FROM
          {lyr_name}
            JOIN NHDFcode ON NHDFcode.FCode = {lyr_name}.FCode
        WHERE
          {lyr_name}.FCode NOT IN (56600, 56700)
    """

def process_nhdplus_hr_source_waterbodies(gdb_path, srs):
    """Creates waterbodies.gpkg in the work dir"""
    waterbodies_gpkg_path = WATERBODIES_FNAME
    if os.path.isfile(waterbodies_gpkg_path):
        log(f"{waterbodies_gpkg_path} exists, skipping...")
        return
    sql = waterbodies_sql("NHDWaterbody")
    sql = re.sub(r'\s+', " ", sql)
    cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{SRS}' \
          -overwrite \
          {waterbodies_gpkg_path} \
          {gdb_path} \
          -dialect sqlite \
          -nln waterbodies \
          -nlt MULTIPOLYGON \
          -sql "{sql}"
    """
    call_cmd(cmd, shell=True, check=True)
    sql = waterbodies_sql("NHDArea")
    sql = re.sub(r'\s+', " ", sql)
    cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{SRS}' \
          {waterbodies_gpkg_path} \
          {gdb_path} \
          -append \
          -update \
          -dialect sqlite \
          -nln waterbodies \
          -nlt MULTIPOLYGON \
          -sql "{sql}"
    """
    call_cmd(cmd, shell=True, check=True)


def process_nhdplus_hr_source_watersheds(gdb_path, srs):
    """Creates watersheds.gpkg in the work dir"""
    watersheds_gpkg_path = "watersheds.gpkg"
    if os.path.isfile(watersheds_gpkg_path):
        log(f"{watersheds_gpkg_path} exists, skipping...")
        return
    sql = """
        SELECT
            Name AS name,
            HUC10 AS source_id,
            'HUC10' AS source_id_attr,
            Shape AS geom
        FROM WBDHU10
    """
    sql = re.sub(r'\s+', " ", sql)
    cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{SRS}' \
          -overwrite \
          {watersheds_gpkg_path} \
          {gdb_path} \
          -dialect sqlite \
          -nln watersheds \
          -nlt MULTIPOLYGON \
          -sql "{sql}"
    """
    call_cmd(cmd, shell=True, check=True)


def process_nhdplus_hr_source_waterways_network(gdb_path):
    """Adds waterways-network.csv to the work dir given Underfoot water GDB"""
    sqlite_path = WATERWAYS_NETWORK_FNAME
    if not os.path.isfile(sqlite_path):
        # Extract network data from the GDB to a sqlite database that we can
        # index for efficient queries
        call_cmd(
            f"ogr2ogr {sqlite_path} {gdb_path} NHDPlusFlowlineVAA",
            shell=True
        )
        # Make those indexes
        call_cmd(
            f"sqlite3 {sqlite_path} 'CREATE INDEX vaa_tonode ON "
            "NHDPlusFlowlineVAA (ToNode)'",
            shell=True
        )
        call_cmd(
            f"sqlite3 {sqlite_path} 'CREATE INDEX vaa_fromnode ON "
            "NHDPlusFlowlineVAA (FromNode)'",
            shell=True
        )
    csv_path = "waterways-network.csv"
    if not os.path.isfile(csv_path):
        # Dump the network from sqlite to CSV. Each segment has an NHDPlusID,
        # and we're storing the NHDPlusID of the segment upstream
        # (from_source_id) and downstream (to_source_id). You don't technically
        # need both to construct the graph, but they save a lot of calculation
        sql = """
            SELECT
                CAST(a.NHDPlusID AS INTEGER) AS source_id,
                CAST(t.NHDPlusID AS INTEGER) AS to_source_id,
                CAST(f.NHDPlusID AS INTEGER) AS from_source_id
            FROM
                NHDPlusFlowlineVAA a
                    LEFT JOIN NHDPlusFlowlineVAA t ON t.FromNode = a.ToNode
                    LEFT JOIN NHDPlusFlowlineVAA f ON f.ToNode = a.FromNode
        """
        sql = re.sub(r'\s+', " ", sql)
        call_cmd(
            f"""
                sqlite3 {sqlite_path} -csv -header "{sql}" > {csv_path}
            """,
            shell=True
        )


def process_nhdplus_hr_source_citation(url):
    """Adds citation.json to work dir"""
    citation_json_path = CITATION_FNAME
    if os.path.isfile(citation_json_path):
        return
    globs = glob("*_GDB.xml")
    if len(globs) == 0:
        log("No metadata XML found, skipping citation generation...")
        return
    tree = ET.parse(globs[0])
    citeinfo = tree.find("./idinfo/citation/citeinfo")
    pubdate = citeinfo.find("pubdate").text
    pubyear = pubdate[0:4]
    pubmonth = pubdate[4:6]
    pubday = pubdate[6:8]
    title = citeinfo.find("title").text
    citation_json = [{
        "id": url,
        "title": title,
        "container-title": "USGS National Hydrography Dataset Plus High Resolution",  # noqa: E501
        "URL": url,
        "author": [
            {"family": "U.S. Geological Survey"}
        ],
        "issued": {
            "date-parts": [
                [
                    pubyear,
                    pubmonth,
                    pubday
                ]
            ]
        }
    }]
    with open("citation.json", 'w', encoding="utf-8") as outfile:
        json.dump(citation_json, outfile)


def cleanup(work_path):
    """Remove build artifacts that aren't needed anymore or can't be rebuilt"""
    doomed = (
        glob(os.path.join(work_path, "*.gdb")) +
        glob(os.path.join(work_path, "*.zip")) +
        glob(os.path.join(work_path, "*.jpg")) +
        glob(os.path.join(work_path, "*.xml"))
    )
    for path in doomed:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


def artifacts_generated(work_path):
    """Check if all artifacts exist"""
    for fname in ARTIFACT_NAMES:
        if not os.path.isfile(os.path.join(work_path, fname)):
            return False
    return True


def process_nhdplus_hr_source(
        base_path,
        url,
        gdb_name,
        srs=GRS80_LONGLAT):
    """Process hydrologic data from an NHDPlus source"""
    work_path = make_work_dir(os.path.realpath(base_path))
    if artifacts_generated(work_path):
        log(f"Artifacts generated for {gdb_name}, skipping...")
        cleanup(work_path)
        return
    os.chdir(work_path)
    # Download the data
    download_path = os.path.basename(url)
    if os.path.isfile(download_path):
        log(f"Download exists at {download_path}, skipping...")
    else:
        log(f"DOWNLOADING {url}")
        call_cmd(["curl", "-OL", url])
    # Unpack the waterways data
    gdb_path = gdb_name
    if os.path.isdir(gdb_path):
        log(f"Archive already extracted at {gdb_path}, skipping...")
    else:
        log("EXTRACTING ARCHIVE...")
        call_cmd(["unzip", "-u", "-o", download_path])
    process_nhdplus_hr_source_waterways(gdb_path, srs)
    process_nhdplus_hr_source_waterbodies(gdb_path, srs)
    process_nhdplus_hr_source_watersheds(gdb_path, srs)
    process_nhdplus_hr_source_waterways_network(gdb_path)
    process_nhdplus_hr_source_citation(url)
    if not artifacts_generated(work_path):
        raise FileNotFoundError(f"Failed tobuild artifacts for {gdb_name}")
    cleanup(work_path)
