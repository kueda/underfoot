from sources import util
from subprocess import call, Popen, PIPE
import glob
import os
import re
import psycopg2

work_table_name = "units"
mask_table_name = "masks"
final_table_name = "units"
dbname = "underfoot"
srid = "3857"
sources = [
  "mf2342c",
  "mf2337c",
  "of94-622",
  "of97-489",
  "of98-354",
  "mf2403c"
]

util.call_cmd(["createdb", dbname])
util.call_cmd(["psql", "-d", dbname, "-c", "CREATE EXTENSION postgis;"])

for table_name in [work_table_name, mask_table_name, final_table_name]:
  util.run_sql("DROP TABLE IF EXISTS {}".format(table_name), dbname=dbname)

column_names = util.METADATA_COLUMN_NAMES + ['source', 'geom']
column_defs = ["{} varchar(1024)".format(c) for c in util.METADATA_COLUMN_NAMES] + [
  "source varchar(255)",
  "geom geometry(MULTIPOLYGON, {})".format(srid)
]
util.run_sql("""
  CREATE TABLE {} (
    {}
  )
""".format(
  work_table_name,
  ", ".join(column_defs)
))

util.run_sql("""
  CREATE TABLE {} (
    source varchar(255),
    geom geometry(MULTIPOLYGON, {})
  )
""".format(mask_table_name, srid))

for idx, source_identifier in enumerate(sources):
  path = os.path.join("sources", "{}.py".format(source_identifier))
  work_path = util.make_work_dir(path)
  util.call_cmd(["python", path])
  units_path = os.path.join(work_path, "units.shp")
  source_table_name = re.sub(r"\W", "_", source_identifier)
  util.run_sql("DROP TABLE IF EXISTS \"{}\"".format(source_table_name), dbname=dbname)
  shp2pgsql_cmd = [
    "shp2pgsql", "-s", srid, "-I", units_path, source_table_name
  ]
  psql_cmd = [
    "psql", dbname
  ]
  print(shp2pgsql_cmd)
  p1 = Popen(shp2pgsql_cmd, stdout=PIPE)
  p2 = Popen(psql_cmd, stdin=p1.stdout, stdout=PIPE)
  p1.stdout.close()
  print("Loading {} into {} table...".format(units_path, source_table_name))
  output = p2.communicate()[0]
  print("Repairing invalid geometries...")
  util.run_sql("UPDATE {} SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom)".format(source_table_name))
  print("Removing polygon overlaps...")
  con = psycopg2.connect("dbname=underfoot")
  cur1 = con.cursor()
  cur1.execute("SELECT gid, label_code FROM {} ORDER BY ST_Area(geom) ASC".format(source_table_name))
  for row in cur1:
    print('.', end="", flush=True)
    gid = row[0]
    label_code = row[1]
    cur2 = con.cursor()
    sql = """
      UPDATE {}
      SET geom = ST_Multi(ST_Difference(geom, (SELECT geom FROM {} WHERE gid = {})))
      WHERE ST_Intersects(geom, (SELECT geom FROM {} WHERE gid = {})) AND gid != {}
    """.format(
      source_table_name,
      source_table_name, gid,
      source_table_name, gid, gid
    )
    cur2.execute(sql)
    cur2.close()
  print()
  con.commit()
  if idx == 0:
    print("Creating {} and inserting...".format(work_table_name))
    util.run_sql("INSERT INTO {} ({}, source, geom) SELECT {}, '{}', geom FROM {}".format(
      work_table_name,
      ", ".join(util.METADATA_COLUMN_NAMES),
      ", ".join(util.METADATA_COLUMN_NAMES),
      source_identifier,
      source_table_name
    ))
  else:
    print("Inserting into {}...".format(work_table_name))
    util.run_sql("""
      INSERT INTO {} ({})
      SELECT {}, '{}', ST_Multi(ST_Difference(s.geom, (SELECT ST_Union(m.geom) FROM {} m)))
      FROM {} s
      WHERE ST_GeometryType(ST_Difference(s.geom, (SELECT ST_Union(m.geom) FROM {} m))) != 'ST_GeometryCollection'
    """.format(
      work_table_name, ", ".join(column_names),
      ", ".join(util.METADATA_COLUMN_NAMES), source_identifier, mask_table_name,
      source_table_name,
      mask_table_name
    ))
  print("Updating {}...".format(mask_table_name))
  util.run_sql("INSERT INTO {} (source, geom) SELECT '{}', ST_Multi(ST_Union(geom)) FROM {}".format(mask_table_name, source_identifier, source_table_name))

print("Deleting water units...")
util.run_sql("DELETE FROM {} WHERE LOWER(label_code) IN ('h2o', 'water')".format(work_table_name), dbname=dbname)
print("Database {} created with table {}".format(dbname, final_table_name))
