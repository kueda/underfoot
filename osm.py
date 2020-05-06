import os
import psycopg2
import sys
from sources import util

DBNAME = "underfoot_ways"
DB_USER = "underfoot"
DB_PASSWORD = "underfoot"
TABLE_NAME = "underfoot_ways"

def make_ways(pbf_url, clean=False, bbox=None):
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
  filename = os.path.basename(pbf_url)
  if os.path.isfile(filename) and not clean:
    pass
  else:
    util.call_cmd(["curl", "-o", filename, pbf_url], check=True)
  util.call_cmd(["dropdb", DBNAME], check=True)
  # Check to see if db exists
  try:
    con = psycopg2.connect("dbname={}".format(DBNAME))
  except psycopg2.OperationalError:
    util.call_cmd(["createdb", DBNAME])
    util.call_cmd(["psql", "-d", DBNAME, "-c", "CREATE EXTENSION postgis; CREATE EXTENSION hstore;"])
    util.call_cmd(["psql", "-d", DBNAME, "-f", "/usr/share/doc/osmosis/examples/pgsnapshot_schema_0.6.sql"])
    util.call_cmd(["psql", "-d", DBNAME, "-f", "/usr/share/doc/osmosis/examples/pgsnapshot_schema_0.6_bbox.sql"])
    util.call_cmd(["psql", "-d", DBNAME, "-f", "/usr/share/doc/osmosis/examples/pgsnapshot_schema_0.6_linestring.sql"])
    con = psycopg2.connect("dbname={}".format(DBNAME))
  # con.close()
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
    read_args = [
      "--read-pbf", filename,
      "--tf", "accept-ways", "highway=*",
      "--tf", "reject-ways", "service=*",
      "--tf", "reject-ways", "footway=sidewalk",
      "--tf", "reject-ways", "highway=proposed",
      "--tf", "reject-ways", "highway=footway",
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
  util.call_cmd(["psql", DBNAME, "-c", "DROP TABLE {}".format(TABLE_NAME)])
  util.call_cmd([
    "psql",
    DBNAME,
    "-c",
    "CREATE TABLE {} AS SELECT id, version, tags -> 'name' AS name, tags -> 'highway' AS highway, linestring FROM ways".format(TABLE_NAME)
  ])
  # Export ways into the MBTiles using different zoom levels for different types
  mbtiles_path = "./underfoot_ways.mbtiles"
  if os.path.exists(mbtiles_path):
    os.remove(mbtiles_path)
  util.call_cmd([
    "./node_modules/tl/bin/tl.js", "copy", "-i", "underfoot_ways.json", "--quiet", "-z", "3", "-Z", "13",
    "postgis://{}:{}@localhost:5432/{}?table={}&query=(SELECT%20*%20from%20underfoot_ways%20WHERE%20highway%20in%20('motorway'))%20AS%20foo".format(
      DB_USER,
      DB_PASSWORD,
      DBNAME,
      TABLE_NAME
    ),
    "mbtiles://{}".format(mbtiles_path)
  ])
  util.call_cmd([
    "./node_modules/tl/bin/tl.js", "copy", "-i", "underfoot_ways.json", "--quiet", "-z", "7", "-Z", "13",
    "postgis://{}:{}@localhost:5432/{}?table={}&query=(SELECT%20*%20from%20underfoot_ways%20WHERE%20highway%20in%20('motorway','primary','trunk'))%20AS%20foo".format(
      DB_USER,
      DB_PASSWORD,
      DBNAME,
      TABLE_NAME
    ),
    "mbtiles://{}".format(mbtiles_path)
  ])
  util.call_cmd([
    "./node_modules/tl/bin/tl.js", "copy", "-i", "underfoot_ways.json", "--quiet", "-z", "11", "-Z", "13",
    "postgis://{}:{}@localhost:5432/{}?table={}&query=(SELECT%20*%20from%20underfoot_ways%20WHERE%20highway%20in%20('motorway','primary','trunk','secondary','tertiary','motorway_link'))%20AS%20foo".format(
      DB_USER,
      DB_PASSWORD,
      DBNAME,
      TABLE_NAME
    ),
    "mbtiles://{}".format(mbtiles_path)
  ])
  util.call_cmd([
    "./node_modules/tl/bin/tl.js", "copy", "-i", "underfoot_ways.json", "--quiet", "-z", "13", "-Z", "13",
    "postgis://{}:{}@localhost:5432/{}?table={}".format(
      DB_USER,
      DB_PASSWORD,
      DBNAME,
      TABLE_NAME
    ),
    "mbtiles://{}".format(mbtiles_path)
  ])
  return mbtiles_path

if __name__ == "__main__":
  mbtiles_path = make_ways(sys.argv[0])
  print("Created mbtiles at {}".format(mbtiles_path))
