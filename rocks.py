"""Methods for generating geologica data for Underfoot

Main export is make_rocks, which should load everything into the database,
export an MBTiles file, and return a list with the files you need.
"""

import argparse
import os
import shutil
import sys
import re
import psycopg2
import time
import json
from multiprocessing import Pool

from sources import util
from database import DBNAME, SRID

NUM_PROCESSES = 4

final_table_name = "rock_units"
mask_table_name = "rock_units_masks"
# TODO if this is going to get reused for other citations it should probably be
# in packs.py
citations_table_name = "citations"

# Painful process of removing polygon overlaps
def remove_polygon_overlaps(source_table_name):
  temp_source_table_name = "temp_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(temp_source_table_name), dbname=DBNAME)
  util.run_sql("ALTER TABLE {} RENAME TO {}".format(source_table_name, temp_source_table_name))
  # First we need to split the table into its constituent polygons so we can
  # sort them by size and use them to cut holes out of the larger polygons
  util.log("\tDumping into constituent polygons...")
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
  polygons = util.run_sql_with_retries("SELECT id, ST_Area(geom) FROM {} ORDER BY ST_Area(geom) ASC".format(dumped_source_table_name))
  for idx, row in enumerate(polygons):
    progress = round(idx / len(polygons) * 100, 2)
    if progress % 10 < 0.01:
      util.log(f"Cutting larger polygons by smaller polygons in {source_table_name}... ({idx} / {len(polygons)}, {progress}%)")
    id = row[0]
    area = row[1]
    # cur2 = con.cursor()
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
    util.run_sql_with_retries(sql, dbname=DBNAME, quiet=True)
  util.log()
  util.log("\tRecreating multipolygons...")
  util.run_sql("CREATE TABLE {} AS SELECT {}, ST_Multi(ST_Union(geom)) AS geom FROM {} GROUP BY {}".format(
    source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES),
    dumped_source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES)
  ))
  util.run_sql("DELETE FROM {} WHERE ST_GeometryType(geom) = 'ST_GeometryCollection'".format(source_table_name))

def load_citation_for_source(source_identifier):
  path = os.path.join("sources", "{}.py".format(source_identifier))
  work_path = util.make_work_dir(path)
  citation_json_path = os.path.join(work_path, "citation.json")
  if os.path.isfile(citation_json_path):
    with open(citation_json_path) as f:
      citation_json = json.loads(f.read())
      c = citation_json[0]
      authorship = None
      if "author" in c:
        authorship = ""
        for idx, author in enumerate(c["author"]):
          if idx != 0:
            if idx == len(c["author"]) - 1:
              authorship += ", & "
            else:
              authorship += ", "
          authorship += ", ".join([piece for piece in [author.get("family"), author.get("given")] if piece is not None])
      pieces = [
        authorship,
        f"({c['issued']['date-parts'][0][0]})",
        c.get("title"),
        c.get("container-title"),
        c.get("publisher"),
        c.get("URL")
      ]
      citation = ". ".join([piece for piece in pieces if piece])
      citation = re.sub(r"\.+", ".", citation)
      existing = util.run_sql(f"DELETE FROM {citations_table_name} WHERE source = '{source_identifier}'")
      util.log(f"Loading citation for {source_identifier}: {citation}")
      util.run_sql(f"INSERT INTO {citations_table_name} VALUES (%s, %s)",
        interpolations=(source_identifier, citation))

# Run the source scripts and load their data into the database
def process_source(source_identifier, clean=False):
  source_table_name = re.sub(r"\W", "_", source_identifier)
  try:
    num_rows = util.run_sql(f"SELECT COUNT(*) FROM {source_table_name}")[0][0]
    if num_rows > 0 and not clean:
      util.log(f"{source_table_name} exists and has data, skipping the source build...")
      load_citation_for_source(source_identifier)
      return
  except psycopg2.errors.UndefinedTable:
    # If the table doesn't exist we need to proceed
    pass
  util.call_cmd(["python", os.path.join("sources", "{}.py".format(source_identifier))])
  path = os.path.join("sources", "{}.py".format(source_identifier))
  work_path = util.make_work_dir(path)
  units_path = os.path.join(work_path, "units.geojson")
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(source_table_name), dbname=DBNAME)
  time.sleep(5) # stupid hack to make sure the table is dropped before we start loading into it
  util.log("Loading {} into {} table...".format(units_path, source_table_name))
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
  util.log("Deleting empty units...")
  util.run_sql("DELETE FROM {} WHERE code IS NULL OR code = ''".format(work_source_table_name), dbname=DBNAME)
  util.log("Repairing invalid geometries...")
  util.run_sql("UPDATE {} SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom)".format(work_source_table_name))
  util.log("Removing polygon overlaps...")
  remove_polygon_overlaps(work_source_table_name)
  util.run_sql("DELETE FROM {} WHERE ST_GeometryType(geom) = 'ST_GeometryCollection'".format(work_source_table_name))
  util.run_sql("UPDATE {} SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom)".format(work_source_table_name))
  load_citation_for_source(source_identifier)

def clip_source_polygons_by_mask(source_table_name):
  util.log("Clipping source polygons by the mask...")
  util.log("\tDumping into constituent polygons...")
  dumped_source_table_name = "dumped_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(dumped_source_table_name), dbname=DBNAME)
  util.run_sql("""
    CREATE TABLE {} AS SELECT {}, (ST_Dump(geom)).geom AS geom FROM {}
  """.format(
    dumped_source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES),
    source_table_name
  ))
  # Pretty sure this clips any polygons that would overlap the existing units,
  # since we don't want any overlaps
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
  util.log("\tRecreating multipolygons...")
  temp_source_table_name = "temp_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(source_table_name), dbname=DBNAME)
  util.run_sql(f"""
    CREATE TABLE {source_table_name} AS
    SELECT
      {", ".join(util.METADATA_COLUMN_NAMES)},
      ST_Multi(ST_Union(geom)) AS geom
    FROM {dumped_source_table_name}
    GROUP BY {", ".join(util.METADATA_COLUMN_NAMES)}
  """)
  util.run_sql(f"""
    DELETE FROM {source_table_name}
    WHERE
      ST_GeometryType(geom) = 'ST_GeometryCollection'
      OR ST_NPoints(geom) = 0
  """)

def load_units(sources, clean=False, procs=NUM_PROCESSES):
  """Load geological units into the database from the specified sources
  
  Parameters
  ----------
  sources : list
    Names of sources to load
  """
  # Drop existing units and masks tables
  for table_name in [final_table_name, mask_table_name, citations_table_name]:
    util.run_sql("DROP TABLE IF EXISTS {} CASCADE".format(table_name), dbname=DBNAME)

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

  # Create the citations table
  util.run_sql(f"""
    CREATE TABLE IF NOT EXISTS {citations_table_name} (
      source VARCHAR(255),
      citation TEXT)
  """)

  # Creaate a processing pool to max out 4 processors
  pool = Pool(processes=procs)
  # Since I'm almost certainly going to forget how this works,
  # pool.map(process_source, sources) would run process_source() on each item in
  # sources, so if sources is ['foo', 'bar'], it would run process_source('foo')
  # and process_source('bar'). pool.starmap does the same thing except the
  # second arg is an iterable of iterables, if it's ['foo', true], it will run
  # process_source('foo', true)
  pool.starmap(process_source, [[src, clean] for src in sources])

  for idx, source_identifier in enumerate(sources):
    source_table_name = re.sub(r"\W", "_", source_identifier)
    work_source_table_name = "work_{}".format(source_table_name)
    if idx == 0:
      util.log("Creating {} and inserting...".format(final_table_name))
      util.run_sql("INSERT INTO {} ({}, source, geom) SELECT {}, '{}', geom FROM {}".format(
        final_table_name,
        ", ".join(util.METADATA_COLUMN_NAMES),
        ", ".join(util.METADATA_COLUMN_NAMES),
        source_identifier,
        work_source_table_name
      ))
    else:
      clip_source_polygons_by_mask(work_source_table_name)
      util.log("Inserting into {}...".format(final_table_name))
      util.run_sql("""
        INSERT INTO {} ({})
        SELECT {}, '{}', s.geom
        FROM {} s
      """.format(
        final_table_name, ", ".join(column_names[1:]),
        ", ".join(util.METADATA_COLUMN_NAMES), source_identifier,
        work_source_table_name
      ))
    util.log("Updating {}...".format(mask_table_name))
    # Remove slivers and make it valid
    if idx == 0:
      util.initialize_masks_table(mask_table_name, source_table_name)
    else:
      util.update_masks_table(mask_table_name, source_table_name)

  util.log("Database {} created with table {}".format(DBNAME, final_table_name))

def clean_sources(sources):
  """Clean any cached data for specified sources"""
  for idx, source_identifier in enumerate(sources):
    path = os.path.join("sources", "{}.py".format(source_identifier))
    work_path = util.make_work_dir(path)
    shutil.rmtree(work_path)

def add_table_from_query_to_mbtiles(table_name, query, path, index_column=None):
  csv_path = f"{os.path.basename(path)}.csv"
  # columns = ["id"] + util.METADATA_COLUMN_NAMES + ["source"]
  # sql = "SELECT {} FROM {}".format(", ".join(columns), final_table_name)
  util.call_cmd(f"psql {DBNAME} -c \"COPY ({query}) TO STDOUT WITH CSV HEADER\" > {csv_path}", shell=True, check=True)
  shutil.rmtree(csv_path, ignore_errors=True)
  util.call_cmd([
    "sqlite3",
    "-csv",
    path,
    f".import {csv_path} {table_name}"
  ])
  if index_column:
    util.call_cmd([
      "sqlite3",
      path,
      f"CREATE INDEX {table_name}_{index_column} ON {table_name}({index_column})"
    ], check=True)

def make_mbtiles(path="./rocks.mbtiles"):
  """Export rock units into am MBTiles file"""
  mbtiles_cmd = [
    "ogr2ogr",
    "-f", "MBTILES",
    path,
    f"PG:dbname={DBNAME}",
    "-sql", "SELECT id::text AS id, lithology, min_age, controlled_span, geom FROM rock_units",
    "-nln", final_table_name,
    "-dsco", "MAX_SIZE=5000000",
    "-dsco", "MINZOOM=7",
    "-dsco", "MAXZOOM=14",
    "-dsco", "DESCRIPTION=\"Geological units\""
  ]
  util.call_cmd(mbtiles_cmd)
  columns = ["id"] + util.METADATA_COLUMN_NAMES + ["source"]
  add_table_from_query_to_mbtiles(
    table_name=f"{final_table_name}_attrs",
    query=f"SELECT {', '.join(columns)} FROM {final_table_name}",
    path=path,
    index_column="id")
  add_table_from_query_to_mbtiles(
    table_name=citations_table_name,
    query=f"SELECT * FROM {citations_table_name}",
    path=path,
    index_column="source")
  return os.path.abspath(path)

def make_rocks(sources, clean=False, path="./rocks.mbtiles", procs=NUM_PROCESSES):
  if clean:
    clean_sources(sources)
  load_units(sources, clean=clean, procs=procs)
  mbtiles_path = make_mbtiles(path=path)
  return mbtiles_path

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Make an MBTiles of geologic units from given source(s)")
  parser.add_argument("source", type=str, nargs="+", help="Source(s)")
  parser.add_argument("--clean", action="store_true", help="Clean cached data before running")
  args = parser.parse_args()
  make_rocks(args.source, clean=args.clean)
