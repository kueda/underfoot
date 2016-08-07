from sources import util
from subprocess import call, Popen, PIPE
import glob
import os

work_table_name = "work"
final_table_name = "units"
dbname = "underfoot"

util.call_cmd(["createdb", dbname])
util.call_cmd(["psql", "-d", dbname, "-c", "CREATE EXTENSION postgis;"])

util.run_sql("DROP TABLE IF EXISTS {}".format(work_table_name), dbname=dbname)
util.run_sql("DROP TABLE IF EXISTS {}".format(final_table_name), dbname=dbname)

for idx, path in enumerate(glob.glob("sources/*.py")):
  work_path = util.make_work_dir(path)
  util.call_cmd(["python", path])
  units_path = os.path.join(work_path, "units.shp")
  shp2pgsql_cmd = [
    "shp2pgsql", ("-c" if idx == 0 else "-a"), "-s", "900913", "-I", units_path, work_table_name
  ]
  psql_cmd = [
    "psql", dbname
  ]
  print(shp2pgsql_cmd)
  p1 = Popen(shp2pgsql_cmd, stdout=PIPE)
  p2 = Popen(psql_cmd, stdin=p1.stdout, stdout=PIPE)
  p1.stdout.close()
  print("Loading {} into {} table...".format(units_path, work_table_name))
  output = p2.communicate()[0]

print("Deleting water units...")
util.run_sql("DELETE FROM {} WHERE LOWER(PTYPE) IN ('h2o', 'water')".format(work_table_name), dbname=dbname)
print("Dissolving units by PTYPE...")
util.run_sql("CREATE TABLE {} AS SELECT PTYPE, ST_Union(geom) AS geom FROM {} GROUP BY PTYPE".format(final_table_name, work_table_name), dbname=dbname)
util.run_sql("DROP TABLE {}".format(work_table_name), dbname=dbname)
print("Database {} created with table {}".format(dbname, final_table_name))
