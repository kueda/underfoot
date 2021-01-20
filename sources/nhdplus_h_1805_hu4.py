import os
import re
import util

# EPSG 4269
srs = "+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs"
url = "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/NHDPLUS_H_1805_HU4_GDB.zip"
dir_name = "NHDPLUS_H_1805_HU4_GDB"
gdb_name = "NHDPLUS_H_1805_HU4_GDB.gdb"

# Make the work dir
work_path = util.make_work_dir(os.path.realpath(__file__))
os.chdir(work_path)

# Download the data
download_path = os.path.basename(url)
if os.path.isfile(download_path):
    util.log(f"Download exists at {download_path}, skipping...")
else:
    util.log(f"DOWNLOADING {url}")
    util.call_cmd(["curl", "-OL", url])

# Unpack the waterways data
dir_path = dir_name
gdb_path = gdb_name
if os.path.isdir(gdb_path):
    util.log(f"Archive already extracted at {gdb_path}, skipping...")
else:
    util.log("EXTRACTING ARCHIVE...")
    util.call_cmd(["unzip", "-o", download_path])

# Project into EPSG 4326 along with name, type, and natural attributes
waterways_gpkg_path = "waterways.gpkg"
if os.path.isfile(waterways_gpkg_path):
    util.log(f"{waterways_gpkg_path} exists, skipping...")
else:
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
            (NHDFlowline.FCode BETWEEN 39000 AND 39012)
            OR (NHDFlowline.FCode IN (
              31200,
              36400,
              43100,
              43400,
              44100,
              44101,
              44102,
              44500,
              48400,
              48700,
              49300,
              53700,
              56600,
              56700
            ))
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
    sql = re.sub(r'\s+', " ", sql)
    cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{util.SRS}' \
          -overwrite \
          {waterways_gpkg_path} \
          {gdb_path} \
          -dialect sqlite \
          -nln waterways \
          -nlt MULTILINESTRING \
          -sql "{sql}"
    """
    util.call_cmd(cmd, shell=True, check=True)

waterbodies_gpkg_path = "waterbodies.gpkg"
if os.path.isfile(waterbodies_gpkg_path):
    util.log(f"{waterbodies_gpkg_path} exists, skipping...")
else:
    sql = """
        SELECT
          GNIS_ID AS waterbody_id,
          GNIS_ID AS source_id,
          'GNIS_ID' AS source_id_attr,
          GNIS_Name AS name,
          CASE
          WHEN NHDWaterbody.FCode = 43624 THEN 'treatment'
          WHEN NHDWaterbody.FCode = 43613 THEN 'storage'
          WHEN NHDWaterbody.FCode = 43607 THEN 'evaporator'
          WHEN NHDWaterbody.FCode = 46600 THEN 'swamp/marsh'
          WHEN NHDWaterbody.FType = 436 THEN 'reservoir'
          ELSE 'lake/pond'
          END AS type,
          NOT (
            GNIS_Name LIKE '%reservoir%'
            OR NHDFcode.Description LIKE '%reservoir%'
          ) AS is_natural,
          LOWER(
            COALESCE(
              NULLIF(NHDFCode.HydrographicCategory, ' '),
              'perennial'
            )
          ) AS permanence,
          Shape AS geom
        FROM
          NHDWaterbody
            JOIN NHDFcode ON NHDFcode.FCode = NHDWaterbody.FCode
        WHERE
          NHDWaterbody.FCode NOT IN (56600, 56700)
    """
    sql = re.sub(r'\s+', " ", sql)
    cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{util.SRS}' \
          -overwrite \
          {waterbodies_gpkg_path} \
          {gdb_path} \
          -dialect sqlite \
          -nln waterbodies \
          -nlt MULTIPOLYGON \
          -sql "{sql}"
    """
    util.call_cmd(cmd, shell=True, check=True)

watersheds_gpkg_path = "watersheds.gpkg"
if os.path.isfile(watersheds_gpkg_path):
    util.log(f"{watersheds_gpkg_path} exists, skipping...")
else:
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
          -t_srs '{util.SRS}' \
          -overwrite \
          {watersheds_gpkg_path} \
          {gdb_path} \
          -dialect sqlite \
          -nln watersheds \
          -nlt MULTIPOLYGON \
          -sql "{sql}"
    """
    util.call_cmd(cmd, shell=True, check=True)
