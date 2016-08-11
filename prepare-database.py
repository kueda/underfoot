from sources import util
from subprocess import call, Popen, PIPE
import glob
import os
import re

work_table_name = "work"
mask_table_name = "masks"
final_table_name = "units"
dbname = "underfoot"
srid = "900913"
sources = [
  "mf2342c",
  "mf2337c",
  "of94-622",
  "of97-489"
]

util.call_cmd(["createdb", dbname])
util.call_cmd(["psql", "-d", dbname, "-c", "CREATE EXTENSION postgis;"])

for table_name in [work_table_name, mask_table_name, final_table_name]:
  util.run_sql("DROP TABLE IF EXISTS {}".format(table_name), dbname=dbname)

util.run_sql("""
  CREATE TABLE {} (
    PTYPE varchar(255),
    source varchar(255),
    geom geometry(MULTIPOLYGON, {})
  )
""".format(work_table_name, srid))

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
  print("Inserting into {}...".format(work_table_name))
  if idx == 0:
    util.run_sql("INSERT INTO {} (PTYPE, geom, source) SELECT PTYPE, geom, '{}' FROM {}".format(work_table_name, source_identifier, source_table_name))
  else:
    util.run_sql("""
      INSERT INTO {} (PTYPE, source, geom)
      SELECT PTYPE, '{}', ST_Multi(ST_Difference(s.geom, (SELECT ST_Union(m.geom) FROM {} m)))
      FROM {} s
      WHERE ST_GeometryType(ST_Difference(s.geom, (SELECT ST_Union(m.geom) FROM {} m))) != 'ST_GeometryCollection'
    """.format(
      work_table_name,
      source_identifier, mask_table_name,
      source_table_name,
      mask_table_name
    ))
  print("Updating {}...".format(mask_table_name))
  util.run_sql("INSERT INTO {} (source, geom) SELECT '{}', ST_Multi(ST_Union(geom)) FROM {}".format(mask_table_name, source_identifier, source_table_name))

print("Deleting water units...")
util.run_sql("DELETE FROM {} WHERE LOWER(PTYPE) IN ('h2o', 'water')".format(work_table_name), dbname=dbname)
print("Dissolving units by PTYPE...")
util.run_sql("CREATE TABLE {} AS SELECT PTYPE, ST_Union(geom) AS geom FROM {} GROUP BY PTYPE".format(final_table_name, work_table_name), dbname=dbname)
util.run_sql("DROP TABLE {}".format(work_table_name), dbname=dbname)
print("Database {} created with table {}".format(dbname, final_table_name))
