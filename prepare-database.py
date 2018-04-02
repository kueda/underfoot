from sources import util
from subprocess import call, Popen, PIPE
import glob
import os
import re
import psycopg2
import time

final_table_name = "units"
mask_table_name = "masks"
dbname = "underfoot"
srid = "4326"
sources = [
  "mf2342c",      # Oakland, CA
  "mf2337c",      # SF, parts of Marin County, CA
  "of94_622",     # Contra Costa County, CA
  "of97_489",     # Santa Cruz County, CA
  "of98_354",     # South SF
  "mf2403c",      # Napa and Lake Counties, in part
  "of98_137",     # San Mateo County, CA
  "mf2402",       # Western Sonoma County
  "sim2858",      # Mark West Springs, Sonoma County (Pepperwood)
  "of96_252",     # Alameda County, CA
  "of97_456",     # Point Reyes, Marin County, CA
  "of2005_1305",  # all of California, coarse
]

util.call_cmd(["createdb", dbname])
util.call_cmd(["psql", "-d", dbname, "-c", "CREATE EXTENSION postgis;"])

for table_name in [final_table_name, mask_table_name]:
  util.run_sql("DROP TABLE IF EXISTS {}".format(table_name), dbname=dbname)

column_names = ['id'] + util.METADATA_COLUMN_NAMES + ['source', 'geom']
column_defs = [
  "id BIGSERIAL PRIMARY KEY"
] + [
  "{} text".format(c) for c in util.METADATA_COLUMN_NAMES
] + [
  "source text",
  "geom geometry(MULTIPOLYGON, {})".format(srid)
]
util.run_sql("""
  CREATE TABLE {} (
    {}
  )
""".format(
  final_table_name,
  ", ".join(column_defs)
))

util.run_sql("""
  CREATE TABLE {} (
    source varchar(255),
    geom geometry(MULTIPOLYGON, {})
  )
""".format(mask_table_name, srid))

def remove_polygon_overlaps(source_table_name, skip_mask=False):
  temp_source_table_name = "temp_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(temp_source_table_name), dbname=dbname)
  util.run_sql("ALTER TABLE {} RENAME TO {}".format(source_table_name, temp_source_table_name))
  # First we need to split the table into its constituent polygons so we can
  # sort them by size and use them to cut holes out of the larger polygons
  print("\tDumping into constituent polygons...")
  dumped_source_table_name = "dumped_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(dumped_source_table_name), dbname=dbname)
  util.run_sql("""
    CREATE TABLE {} AS SELECT {}, (ST_Dump(geom)).geom AS geom FROM {}
  """.format(
    dumped_source_table_name,
    ", ".join(util.METADATA_COLUMN_NAMES),
    temp_source_table_name
  ))
  # Next we'll cut all the polygons that intersect the existing mask
  if not skip_mask:
    print("\tCutting out polygons that overlap previous sources...")
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
  util.run_sql("ALTER TABLE {} ADD COLUMN id SERIAL PRIMARY KEY".format(dumped_source_table_name))
  # Now we iterate over each polygon order by size, and use it to cut a hole
  # out of all the other polygons that intersect it
  print("\tCutting larger polygons by smaller polygons...")
  con = psycopg2.connect("dbname=underfoot")
  cur1 = con.cursor()
  cur1.execute("SELECT id, ST_Area(geom) FROM {} ORDER BY ST_Area(geom) ASC".format(dumped_source_table_name))
  for row in cur1:
    print('.', end="", flush=True)
    id = row[0]
    area = row[1]
    cur2 = con.cursor()
    sql = """
      UPDATE {}
      SET geom = ST_Multi(ST_Difference(geom, (SELECT geom FROM {} WHERE id = {})))
      WHERE
        ST_Intersects(geom, (SELECT geom FROM {} WHERE id = {}))
        AND id != {}
        AND ST_Area(geom) >= {}
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
  # util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(temp_source_table_name), dbname=dbname)
  # util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(dumped_source_table_name), dbname=dbname)

for idx, source_identifier in enumerate(sources):
  path = os.path.join("sources", "{}.py".format(source_identifier))
  work_path = util.make_work_dir(path)
  util.call_cmd(["python", path])
  units_path = os.path.join(work_path, "units.geojson")
  source_table_name = re.sub(r"\W", "_", source_identifier)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(source_table_name), dbname=dbname)
  time.sleep(5) # stupid hack to make sure the table is dropped before we start loading into it
  print("Loading {} into {} table...".format(units_path, source_table_name))
  util.call_cmd([
    "ogr2ogr",
      "-f", "PostgreSQL",
      "PG:dbname=underfoot",
      units_path,
      "-nln", source_table_name,
      "-nlt", "MULTIPOLYGON",
      "-lco", "GEOMETRY_NAME=geom",
      "-skipfailures",
      "-a_srs", "EPSG:{}".format(srid)
  ])
  work_source_table_name = "work_{}".format(source_table_name)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(work_source_table_name), dbname=dbname)
  util.run_sql("CREATE TABLE {} AS SELECT * FROM {}".format(work_source_table_name, source_table_name), dbname=dbname)
  print("Deleting water units...")
  util.run_sql("DELETE FROM {} WHERE LOWER(code) IN ('h2o', 'water')".format(work_source_table_name), dbname=dbname)
  print("Deleting empty units...")
  util.run_sql("DELETE FROM {} WHERE code IS NULL OR code = ''".format(work_source_table_name), dbname=dbname)
  print("Repairing invalid geometries...")
  util.run_sql("UPDATE {} SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom)".format(work_source_table_name))
  print("Removing polygon overlaps...")
  remove_polygon_overlaps(work_source_table_name, skip_mask=(idx == 0))
  util.run_sql("UPDATE {} SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom)".format(source_table_name))
  print()
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
              1,
              'join=mitre'
            ),
            -1,
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
                      1,
                      'join=mitre'
                    ),
                    -1,
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

print("Database {} created with table {}".format(dbname, final_table_name))
