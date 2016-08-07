# consolidate all data sources into a single postgis table
from subprocess import call, Popen, PIPE
import os
import shutil

sources = [
  {
    'path': 'ma-geol-polygons-by-ptype.shp',
    'proj': '+proj=lcc +lat_1=37.06666666666 +lat_2=38.43333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=90 +y_0=10 +ellps=clrk66 +units=m +no_defs'
  },
  {
    'path': 'sfs-geol-uncompressed-polygons-by-ptype.shp',
    'proj': '+proj=lcc +lat_1=37.066667 +lat_2=38.433333 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.21920 +y_0=-6 +datum=NAD27 +units=m +no_defs'
  },
  {
    'path': 'bv-geol-polygons-by-ptype.shp',
    'proj': '+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs'
  },
  {
    'path': 'ha-geol-polygons-by-ptype.shp',
    'proj': '+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs'
  },
  {
    'path': 'lt-geol-polygons-by-ptype.shp',
    'proj': '+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs'
  },
  {
    'path': 'oe-geol-polygons-by-ptype.shp',
    'proj': '+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs'
  },
  {
    'path': 'ow-geol-polygons-by-ptype.shp',
    'proj': '+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs'
  },
  {
    'path': 'ri-geol-polygons-by-ptype.shp',
    'proj': '+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs'
  },
  {
    'path': 'sl-geol-polygons-by-ptype.shp',
    'proj': '+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs'
  }
]
work_table_name = "work"
final_table_name = "merged_units"
dbname = "underfoot"

def extless_basename(path):
  return os.path.splitext(os.path.basename(path))[0]
def work_dir(path):
  return "work-{}".format(extless_basename(path))
def call_cmd(cmd):
  print("Calling {}".format(cmd))
  call(cmd)
def run_sql(sql):
  call_cmd([
    "psql", dbname, "-c", sql
  ])

run_sql("DROP TABLE IF EXISTS {}".format(work_table_name))
run_sql("DROP TABLE IF EXISTS {}".format(final_table_name))

for idx, source in enumerate(sources):
  print("\n")
  print(source['path'])
  print("\n")
  shutil.rmtree(work_dir(source['path']), ignore_errors=True)
  os.makedirs(work_dir(source['path']))
  projected_fname = "%s.shp" % extless_basename(source['path'])
  projected_path = os.path.join(work_dir(source['path']), projected_fname)
  project_cmd = [
    "ogr2ogr",
      "-s_srs", source['proj'],
      "-t_srs", "EPSG:900913",
      projected_path,
      source['path']
  ]
  call_cmd(project_cmd)
  shp2pgsql_cmd = [
    "shp2pgsql", ("-c" if idx == 0 else "-a"), "-s", "900913", "-I", projected_path, work_table_name
  ]
  psql_cmd = [
    "psql", dbname
  ]
  print(shp2pgsql_cmd)
  p1 = Popen(shp2pgsql_cmd, stdout=PIPE)
  p2 = Popen(psql_cmd, stdin=p1.stdout, stdout=PIPE)
  p1.stdout.close()
  print("Loading {} into {} table...".format(projected_path, work_table_name))
  output = p2.communicate()[0]

run_sql("DELETE FROM {} WHERE LOWER(PTYPE) IN ('h2o', 'water')".format(work_table_name))
run_sql("CREATE TABLE {} AS SELECT PTYPE, ST_Union(geom) AS geom FROM {} GROUP BY PTYPE".format(final_table_name, work_table_name))

for idx, source in enumerate(sources):
  print(source['url'])
  download_path = download(source)
  for path in source['paths']:
    if source['extract']:
      extracted_path = extract_e00(path)
    if source['polygonize']:
      poly_path = polygonize(path)
