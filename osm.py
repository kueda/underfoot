from sources import util
import json
import os
import psycopg2
import re
import sys
import time

DBNAME = "underfoot_ways"
DB_USER = "underfoot"
DB_PASSWORD = "underfoot"
TABLE_NAME = "underfoot_ways"

def database_connection(recreate=False):
  if recreate:
    util.call_cmd(["dropdb", "--if-exists", DBNAME], check=True)
  # Check to see if db exists
  try:
    return psycopg2.connect("dbname={}".format(DBNAME))
  except psycopg2.OperationalError:
    osmosis_path = "/usr/share/doc/osmosis/examples"
    if not os.path.isdir(osmosis_path):
      osmosis_path = "/usr/local/Cellar/osmosis/0.45/libexec/script"
      if not os.path.isdir(osmosis_path):
        raise ValueError("Couldn't find osmosis path to osmosis files")
    util.call_cmd(["createdb", DBNAME])
    util.call_cmd(["psql", "-d", DBNAME, "-c", "CREATE EXTENSION postgis; CREATE EXTENSION hstore;"])
    util.call_cmd(["psql", "-d", DBNAME, "-f", f"{osmosis_path}/pgsnapshot_schema_0.6.sql"])
    util.call_cmd(["psql", "-d", DBNAME, "-f", f"{osmosis_path}/pgsnapshot_schema_0.6_bbox.sql"])
    util.call_cmd(["psql", "-d", DBNAME, "-f", f"{osmosis_path}/pgsnapshot_schema_0.6_linestring.sql"])
    return psycopg2.connect("dbname={}".format(DBNAME))

def fetch_data(url, clean=False):
  filename = os.path.basename(url)
  if os.path.isfile(filename) and not clean:
    pass
  else:
    util.call_cmd(["curl", "-o", filename, url], check=True)
  return filename

def load_ways_data(data_path, recreate=False, bbox=None):
  con = database_connection(recreate=recreate)
  # Check to see if table exists and has rows
  cur1 = con.cursor()
  ways_table_missing = False
  try:
    cur1.execute("SELECT count(*) FROM ways")
    # Bail if it has rows unless we're forcing it
    row = cur1.fetchone()
    if row[0] > 0:
      print("ways table has rows. Use --clean to force a new import")
    else:
      ways_table_missing = True
  except psycopg2.errors.UndefinedTable:
    ways_table_missing = True
  con.close()
  if ways_table_missing:
    util.call_cmd([
      "osmosis",
      "--truncate-pgsql",
      "database={}".format(DBNAME),
      "user={}".format(DB_USER),
      "password={}".format(DB_PASSWORD)
    ])
    read_args = [
      "--read-pbf", data_path,
      "--tf", "accept-ways", "highway=*",
      "--tf", "reject-ways", "service=*",
      "--tf", "reject-ways", "footway=sidewalk",
      "--tf", "reject-ways", "highway=proposed",
      "--tf", "reject-ways", "golf=*",
      "--tf", "reject-ways", "golf-cart=*",
      "--tf", "reject-ways", "highway=pedestrian",
      "--tf", "reject-ways", "highway=steps"
    ]
    # Get bounding box coordinates from the database... or maybe the source
    if bbox:
      read_args += [
        "--bounding-box",
        "top={}".format(bbox["top"]),
        "left={}".format(bbox["left"]),
        "bottom={}".format(bbox["bottom"]),
        "right={}".format(bbox["right"])
      ]
    write_args = [
      "--write-pgsql",
        "database={}".format(DBNAME),
        "user={}".format(DB_USER),
        "password={}".format(DB_PASSWORD)
    ]
    cmd = ["osmosis"] + read_args + write_args
    # Load data from PBF into the database with osmosis
    util.call_cmd(cmd)
  util.run_sql(f"DROP TABLE IF EXISTS {TABLE_NAME}", dbname=DBNAME)
  util.run_sql(f"""
    CREATE TABLE {TABLE_NAME} AS
    SELECT
      id,
      COALESCE(tags -> 'ref', tags -> 'tiger:name_base', tags -> 'name') AS name,
      tags -> 'highway' AS highway,
      linestring
    FROM ways
  """, dbname=DBNAME)

def make_ways_mbtiles(path):
  # Export ways into the MBTiles using different zoom levels for different types
  if os.path.exists(path):
    os.remove(path)
  gpkg_path = f"{util.basename_for_path(path)}.gpkg"
  zooms = (
    (3,6, ('motorway',)),
    (7,10, ('motorway', 'primary', 'trunk')),
    (11,12, ('motorway', 'primary', 'trunk', 'secondary', 'tertiary', 'motorway_link')),
    (13,13, ())
  )
  for idx, row in enumerate(zooms):
    minzoom, maxzoom, types = row
    where = None
    cmd = ["ogr2ogr"]
    if idx != 0:
      cmd += ["-update"]
    cmd += [
      gpkg_path,
      f"PG:dbname={DBNAME}",
      TABLE_NAME,
      "-nln", f"underfoot_ways_{minzoom}_{maxzoom}"
    ]
    if len(types):
      types_list = ", ".join([f"'{t}'" for t in types])
      cmd += ["-where", f"highway IN ({types_list})"]
    util.call_cmd(cmd)
  conf = {
    f"underfoot_ways_{minzoom}_{maxzoom}": {
      "target_name": TABLE_NAME,
      "minzoom": minzoom,
      "maxzoom": maxzoom
    } for minzoom, maxzoom, types in zooms}
  cmd = f"""
    ogr2ogr {path} {gpkg_path}
      -dsco MAX_SIZE=5000000
      -dsco MINZOOM={min([row[0] for row in zooms])}
      -dsco MAXZOOM={max([row[0] for row in zooms])}
      -dsco CONF='{json.dumps(conf)}'
  """
  # Some weirdness: if you calll subprocess.run with a list like usually do, it
  # fails because it will pass the CONF arg like -dsco "CONF=blah" and ogr2ogr
  # does not like that. It just silently ignores that dsco. Instead, I'm
  # constructing a string version of the command and pass that with shell=True
  # some it doesn't try to do any fance string escaping
  util.call_cmd(re.sub(r'\s+', " ", cmd).strip(), shell=True)

def make_ways(pbf_url, clean=False, bbox=None, path="./ways.mbtiles"):
  r"""Make an MBTiles files for OSM ways data given an OSM PBF export URL

  Parameters
  ----------
  pbf_url : str
    The URL of a PBF export of OSM data
  bbox : dict
    Bounding box to import from the PBF export of the form
    {"top": 1, "bottom": 0, "left": 0, "right": 1}
  clean : bool
    Force
  """
  # Bail if no PBF URL
  if not pbf_url or len(pbf_url) == 0:
    raise ValueError("You must specify a PBF URL")
  filename = fetch_data(pbf_url, clean=clean)
  load_ways_data(filename, recreate=clean, bbox=bbox)
  make_ways_mbtiles(path)
  return path

if __name__ == "__main__":
  path = make_ways(sys.argv[0])
  print("Created mbtiles at {}".format(path))
