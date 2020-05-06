"""Methods for generating geologica data for Underfoot

Main export is make_rocks, which should load everything into the database,
export an MBTiles file, and return a list with the files you need.
"""

import os
import shutil
import sys
import re
import psycopg2
import time
from multiprocessing import Pool

from sources import util
from database import DBNAME, SRID

NUM_PROCESSES = 4

final_table_name = "rock_units"
mask_table_name = "rock_units_masks"

# Painful process of removing polygon overlaps
def remove_polygon_overlaps(source_table_name):
  temp_source_table_name = "temp_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(temp_source_table_name), dbname=DBNAME)
  util.run_sql("ALTER TABLE {} RENAME TO {}".format(source_table_name, temp_source_table_name))
  # First we need to split the table into its constituent polygons so we can
  # sort them by size and use them to cut holes out of the larger polygons
  print("\tDumping into constituent polygons...")
  dumped_source_table_name = "dumped_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(dumped_source_table_name), dbname=DBNAME)
  util.run_sql("""
    CREATE TABLE {} AS SELECT {}, (ST_Dump(geom)).geom AS geom FROM {}
  """.format(
    dumped_source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES),
    temp_source_table_name
  ))
  util.run_sql("ALTER TABLE {} ADD COLUMN id SERIAL PRIMARY KEY, ADD COLUMN area float".format(dumped_source_table_name))
  util.run_sql("UPDATE {} SET area = ST_Area(geom)".format(dumped_source_table_name))
  # Now we iterate over each polygon order by size, and use it to cut a hole
  # out of all the other polygons that intersect it
  con = psycopg2.connect("dbname={}".format(DBNAME))
  cur1 = con.cursor()
  cur1.execute("SELECT id, ST_Area(geom) FROM {} ORDER BY ST_Area(geom) ASC".format(dumped_source_table_name))
  for idx, row in enumerate(cur1):
    print("\tCutting larger polygons by smaller polygons... ({} / {}, {}%)".format(
      idx, cur1.rowcount, round(idx / cur1.rowcount * 100, 2)), end="\r", flush=True)
    id = row[0]
    area = row[1]
    cur2 = con.cursor()
    sql = """
      UPDATE {}
      SET geom = ST_Multi(ST_Difference(geom, (SELECT geom FROM {} WHERE id = {})))
      WHERE
        ST_Intersects(geom, (SELECT geom FROM {} WHERE id = {}))
        AND id != {}
        AND area >= {}
    """.format(
      dumped_source_table_name,
      dumped_source_table_name, id,
      dumped_source_table_name, id, id,
      area
    )
    cur2.execute(sql)
    cur2.close()
  cur1.close()
  con.commit()
  print()
  print("\tRecreating multipolygons...")
  util.run_sql("CREATE TABLE {} AS SELECT {}, ST_Multi(ST_Union(geom)) AS geom FROM {} GROUP BY {}".format(
    source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES),
    dumped_source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES)
  ))
  util.run_sql("DELETE FROM {} WHERE ST_GeometryType(geom) = 'ST_GeometryCollection'".format(source_table_name))

# Run the source scripts and load their data into the database
def process_source(source_identifier):
  util.call_cmd(["python", os.path.join("sources", "{}.py".format(source_identifier))])
  path = os.path.join("sources", "{}.py".format(source_identifier))
  work_path = util.make_work_dir(path)
  units_path = os.path.join(work_path, "units.geojson")
  source_table_name = re.sub(r"\W", "_", source_identifier)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(source_table_name), dbname=DBNAME)
  time.sleep(5) # stupid hack to make sure the table is dropped before we start loading into it
  print("Loading {} into {} table...".format(units_path, source_table_name))
  util.call_cmd([
    "ogr2ogr",
      "-f", "PostgreSQL",
      "PG:dbname={}".format(DBNAME),
      units_path,
      "-nln", source_table_name,
      "-nlt", "MULTIPOLYGON",
      "-lco", "GEOMETRY_NAME=geom",
      "-skipfailures",
      "-a_srs", "EPSG:{}".format(SRID)
  ])
  work_source_table_name = "work_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(work_source_table_name), dbname=DBNAME)
  util.run_sql("CREATE TABLE {} AS SELECT * FROM {}".format(work_source_table_name, source_table_name), dbname=DBNAME)
  print("Deleting empty units...")
  util.run_sql("DELETE FROM {} WHERE code IS NULL OR code = ''".format(work_source_table_name), dbname=DBNAME)
  print("Repairing invalid geometries...")
  util.run_sql("UPDATE {} SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom)".format(work_source_table_name))
  print("Removing polygon overlaps...")
  remove_polygon_overlaps(work_source_table_name)
  util.run_sql("DELETE FROM {} WHERE ST_GeometryType(geom) = 'ST_GeometryCollection'".format(work_source_table_name))
  util.run_sql("UPDATE {} SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom)".format(work_source_table_name))

def clip_source_polygons_by_mask(source_table_name):
  print("Clipping source polygons by the mask...")
  print("\tDumping into constituent polygons...")
  dumped_source_table_name = "dumped_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(dumped_source_table_name), dbname=DBNAME)
  util.run_sql("""
    CREATE TABLE {} AS SELECT {}, (ST_Dump(geom)).geom AS geom FROM {}
  """.format(
    dumped_source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES),
    source_table_name
  ))
  util.run_sql("""
    UPDATE {}
    SET geom = ST_Difference({}.geom, {}.geom)
    FROM {}
    WHERE ST_Intersects({}.geom, {}.geom)
  """.format(
      dumped_source_table_name,
      dumped_source_table_name, mask_table_name,
      mask_table_name,
      dumped_source_table_name, mask_table_name
    ))
  print("\tRecreating multipolygons...")
  temp_source_table_name = "temp_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(source_table_name), dbname=DBNAME)
  util.run_sql("CREATE TABLE {} AS SELECT {}, ST_Multi(ST_Union(geom)) AS geom FROM {} GROUP BY {}".format(
    source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES),
    dumped_source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES)
  ))
  util.run_sql("DELETE FROM {} WHERE ST_GeometryType(geom) = 'ST_GeometryCollection'".format(source_table_name))

def load_units(sources):
  """Load geological units into the database from the specified sources
  
  Parameters
  ----------
  sources : list
    Names of sources to load
  """
  # Drop existing units and masks tables
  for table_name in [final_table_name, mask_table_name]:
    util.run_sql("DROP TABLE IF EXISTS {}".format(table_name), dbname=DBNAME)

  # Create the units table
  column_names = ['id'] + util.METADATA_COLUMN_NAMES + ['source', 'geom']
  column_defs = [
    "id BIGSERIAL PRIMARY KEY"
  ] + [
    "{} text".format(c) for c in util.METADATA_COLUMN_NAMES
  ] + [
    "source text",
    "geom geometry(MULTIPOLYGON, {})".format(SRID)
  ]
  util.run_sql("""
    CREATE TABLE {} (
      {}
    )
  """.format(
    final_table_name,
    ", ".join(column_defs)
  ))

  # Create the masks table
  util.run_sql("""
    CREATE TABLE {} (
      source varchar(255),
      geom geometry(MULTIPOLYGON, {})
    )
  """.format(mask_table_name, SRID))

  # Creaate a processing pool to max out 4 processors
  pool = Pool(processes=NUM_PROCESSES)
  pool.map(process_source, sources)

  for idx, source_identifier in enumerate(sources):
    # print()
    source_table_name = re.sub(r"\W", "_", source_identifier)
    work_source_table_name = "work_{}".format(source_table_name)
    if idx == 0:
      print("Creating {} and inserting...".format(final_table_name))
      util.run_sql("INSERT INTO {} ({}, source, geom) SELECT {}, '{}', geom FROM {}".format(
        final_table_name,
        ", ".join(util.METADATA_COLUMN_NAMES),
        ", ".join(util.METADATA_COLUMN_NAMES),
        source_identifier,
        work_source_table_name
      ))
    else:
      clip_source_polygons_by_mask(work_source_table_name)
      print("Inserting into {}...".format(final_table_name))
      util.run_sql("""
        INSERT INTO {} ({})
        SELECT {}, '{}', s.geom
        FROM {} s
      """.format(
        final_table_name, ", ".join(column_names[1:]),
        ", ".join(util.METADATA_COLUMN_NAMES), source_identifier,
        work_source_table_name
      ))
    print("Updating {}...".format(mask_table_name))
    # Remove slivers and make it valid
    if idx == 0:
      util.run_sql("""
        INSERT INTO {} (source, geom)
        SELECT
          '{}',
          ST_Multi(
            ST_Buffer(
              ST_Buffer(
                ST_MakeValid(
                  ST_Union(geom)
                ),
                0.01,
                'join=mitre'
              ),
              -0.01,
              'join=mitre'
            )
          )
        FROM {}
      """.format(
        mask_table_name, source_identifier, source_table_name
      ))
    else:
      util.run_sql("""
        UPDATE {} m SET geom = ST_Multi(
          ST_Union(
            m.geom,
            ST_MakePolygon(
              ST_ExteriorRing(
                (
                  SELECT
                    ST_Buffer(
                      ST_Buffer(
                        ST_MakeValid(
                          ST_Union(s.geom)
                        ),
                        0.01,
                        'join=mitre'
                      ),
                      -0.01,
                      'join=mitre'
                    )
                  FROM {} s
                )
              )
            )
          )
        )
      """.format(
        mask_table_name, source_table_name
      ))

  print("Database {} created with table {}".format(DBNAME, final_table_name))

def clean_sources(sources):
  """Clean any cached data for specified sources"""
  for idx, source_identifier in enumerate(sources):
    path = os.path.join("sources", "{}.py".format(source_identifier))
    work_path = util.make_work_dir(path)
    shutil.rmtree(work_path)

def make_mbtiles():
  """Export rock units into am MBTiles file"""
  path = "./underfoot_rock_units.mbtiles"
  cmd = [
    "node_modules/tl/bin/tl.js",
    "copy",
    "-i",
    "underfoot_rock_units.json",
    "--quiet",
    "-z",
    "7",
    "-Z",
    "14",
    "postgis://underfoot:underfoot@localhost:5432/{}?table={}".format(DBNAME, final_table_name),
    "mbtiles://{}".format(path)
  ]
  util.call_cmd(cmd)
  return os.path.abspath(path)

def make_rocks(sources, clean=False):
  if clean:
    clean_sources(sources)
  load_units(sources)
  mbtiles_path = make_mbtiles()
  return mbtiles_path

if __name__ == "__main__":
  make_rocks(sys.argv)
