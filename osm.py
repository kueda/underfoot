"""Functions for working with OpenStreetMap data"""

import json
import os
import re
import sys
import psycopg2

from sources import util

DBNAME = "underfoot_osm"
DB_USER = "underfoot"
DB_PASSWORD = "underfoot"
WAYS_TABLE_NAME = "underfoot_ways"
NATURAL_WAYS_TABLE_NAME = "underfoot_natural_ways"
NATURAL_NODES_TABLE_NAME = "underfoot_natural_nodes"


def create_database():
    """Create the OSM database"""
    osmosis_path = "/usr/share/doc/osmosis/examples"
    if not os.path.isdir(osmosis_path):
        osmosis_path = "/usr/local/Cellar/osmosis/0.45/libexec/script"
        if not os.path.isdir(osmosis_path):
            raise ValueError("Couldn't find osmosis path to osmosis files")
    util.call_cmd(["dropdb", "--if-exists", DBNAME], check=True)
    util.call_cmd(["createdb", DBNAME])
    util.call_cmd([
      "psql", "-d", DBNAME,
      "-c", "CREATE EXTENSION postgis; CREATE EXTENSION hstore;"])
    util.call_cmd([
      "psql", "-d", DBNAME,
      "-f", f"{osmosis_path}/pgsnapshot_schema_0.6.sql"])
    util.call_cmd([
      "psql", "-d", DBNAME,
      "-f", f"{osmosis_path}/pgsnapshot_schema_0.6_bbox.sql"])
    util.call_cmd([
      "psql", "-d", DBNAME,
      "-f", f"{osmosis_path}/pgsnapshot_schema_0.6_linestring.sql"])
    return psycopg2.connect(f"dbname={DBNAME}")


def database_connection(recreate=False):
    """Return a psycopg database connection or make the database"""
    if recreate:
        return create_database()
    # Check to see if db exists
    try:
        return psycopg2.connect(f"dbname={DBNAME}")
    except psycopg2.OperationalError:
        return create_database()


def fetch_data(url, clean=False):
    """Fetch data from URL"""
    filename = os.path.basename(url)
    if os.path.isfile(filename) and not clean:
        pass
    else:
        util.call_cmd(["curl", "-o", filename, url], check=True)
    return filename

def load_osm_from_pbf(data_path, bbox=None):
    """Load OSM data from PBF export into a PostgreSQL database using osmosis"""
    util.call_cmd([
        "osmosis",
        "--truncate-pgsql",
        f"database={DBNAME}",
        f"user={DB_USER}",
        f"password={DB_PASSWORD}"
    ])
    read_args = [
        "--read-pbf", data_path,
        "--tf", "accept-ways", "highway=*", "natural=mountain_range,ridge,valley",
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
            f"top={bbox['top']}",
            f"left={bbox['left']}",
            f"bottom={bbox['bottom']}",
            f"right={bbox['right']}"
        ]
    write_args = [
        "--write-pgsql",
        f"database={DBNAME}",
        f"user={DB_USER}",
        f"password={DB_PASSWORD}"
    ]
    cmd = ["osmosis"] + read_args + write_args
    # Load data from PBF into the database with osmosis
    util.call_cmd(cmd)


def is_osm_loaded():
    """Check is OSM data has been loaded into the database"""
    con = database_connection()
    # Check to see if table exists and has rows
    cur1 = con.cursor()
    osm_loaded = False
    try:
        cur1.execute("SELECT count(*) FROM ways")
        # Bail if it has rows unless we're forcing it
        row = cur1.fetchone()
        if row[0] > 0:
            print("ways table has rows. Use --clean to force a new import")
            osm_loaded = True
    except psycopg2.errors.UndefinedTable:
        osm_loaded = False
    con.close()
    return osm_loaded

def load_ways_data(data_path, bbox=None):
    """Load ways from OSM data"""
    if not is_osm_loaded():
        load_osm_from_pbf(data_path, bbox)
    util.run_sql(f"DROP TABLE IF EXISTS {WAYS_TABLE_NAME}", dbname=DBNAME)
    util.run_sql(
        f"""
            CREATE TABLE {WAYS_TABLE_NAME} AS
            SELECT
              id,
              COALESCE(
                  tags -> 'ref', tags -> 'tiger:name_base', tags -> 'name'
              ) AS name,
              tags -> 'highway' AS highway,
              linestring
            FROM ways
            WHERE
              tags -> 'highway' IS NOT NULL
        """,
        dbname=DBNAME
    )


def load_natural_ways_data(data_path, bbox=None):
    """Load natural ways from OSM data"""
    if not is_osm_loaded():
        load_osm_from_pbf(data_path, bbox)
    util.run_sql(f"DROP TABLE IF EXISTS {NATURAL_WAYS_TABLE_NAME}", dbname=DBNAME)
    # TODO think about how to add a column to control zoom level, maybe using
    # the length of the diagonal of the bounding box
    # ROUND(ST_Length(ST_Simplify(ST_ChaikinSmoothing(linestring), 0.001)::geography)) AS length_m,
    util.run_sql(
        f"""
            CREATE TABLE {NATURAL_WAYS_TABLE_NAME} AS
            SELECT
                id,
                COALESCE(
                    tags -> 'ref', tags -> 'tiger:name_base', tags -> 'name'
                ) AS name,
                tags -> 'natural' AS "natural",
                ROUND(st_length(st_boundingdiagonal(linestring))::numeric, 2) AS diag_deg,
                ST_ChaikinSmoothing(ST_Simplify(linestring, 0.005), 3) AS linestring
            FROM ways
            WHERE
                tags -> 'natural' IS NOT NULL
                AND tags -> 'highway' IS NULL
                AND COALESCE(
                    tags -> 'ref', tags -> 'tiger:name_base', tags -> 'name'
                ) IS NOT NULL
        """,
        dbname=DBNAME
    )
    # Delete the natural lines that were intended to be polygons (or are empty)
    util.run_sql(
        f"""
            DELETE FROM {NATURAL_WAYS_TABLE_NAME}
            WHERE
                "natural" IS NOT NULL
                AND ST_StartPoint(linestring) = ST_EndPoint(linestring)
        """,
        dbname=DBNAME
    )


def load_natural_nodes_data(data_path, bbox=None):
    """Load natural nodes from OSM data"""
    if not is_osm_loaded():
        load_osm_from_pbf(data_path, bbox)
    util.run_sql(f"DROP TABLE IF EXISTS {NATURAL_NODES_TABLE_NAME}", dbname=DBNAME)
    util.run_sql(
        f"""
            CREATE TABLE {NATURAL_NODES_TABLE_NAME} AS
            SELECT
              id,
              COALESCE(
                  tags -> 'ref', tags -> 'tiger:name_base', tags -> 'name'
              ) AS name,
              tags -> 'natural' AS natural,
              tags -> 'ele' AS elevation_m,
              tags -> 'intermittent' AS intermittent,
              geom
            FROM nodes
            WHERE
              tags -> 'natural' IN ('peak', 'saddle', 'spring')
        """,
        dbname=DBNAME
    )


def make_ways_mbtiles(path):
    """Export ways into the MBTiles using different zoom levels for different types"""
    if os.path.exists(path):
        os.remove(path)
    gpkg_path = f"{util.basename_for_path(path)}.gpkg"
    zooms = (
      (3, 6, ('motorway',)),
      (7, 10, ('motorway', 'primary', 'trunk')),
      (11, 12, (
          'motorway',
          'primary',
          'trunk',
          'secondary',
          'tertiary',
          'motorway_link'
      )),
      (13, 13, ())
    )
    for idx, row in enumerate(zooms):
        minzoom, maxzoom, types = row
        cmd = ["ogr2ogr"]
        if idx != 0:
            cmd += ["-update"]
        cmd += [
          gpkg_path,
          f"PG:dbname={DBNAME}",
          WAYS_TABLE_NAME,
          "-nln", f"{WAYS_TABLE_NAME}_{minzoom}_{maxzoom}"
        ]
        if len(types):
            types_list = ", ".join([f"'{t}'" for t in types])
            cmd += ["-where", f"highway IN ({types_list})"]
        util.call_cmd(cmd)
    conf = {
        f"{WAYS_TABLE_NAME}_{minzoom}_{maxzoom}": {
          "target_name": WAYS_TABLE_NAME,
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
    # Some weirdness: if you calll subprocess.run with a list like I usually
    # do, it fails because it will pass the CONF arg like -dsco "CONF=blah"
    # and ogr2ogr does not like that. It just silently ignores that dsco.
    # Instead, I'm constructing a string version of the command and passing
    # that with shell=True so it doesn't try to do any fancy string escaping
    util.call_cmd(re.sub(r'\s+', " ", cmd).strip(), shell=True)


def make_context_mbtiles(path):
    """Make context mbtiles"""
    if os.path.exists(path):
        os.remove(path)
    gpkg_path = f"{util.basename_for_path(path)}.gpkg"
    cmds = [
        [
            "ogr2ogr",
            gpkg_path,
            f"PG:dbname={DBNAME}",
            NATURAL_WAYS_TABLE_NAME,
            "-nln", NATURAL_WAYS_TABLE_NAME
        ],
        [
            "ogr2ogr",
            gpkg_path,
            f"PG:dbname={DBNAME}",
            NATURAL_NODES_TABLE_NAME,
            "-nln", NATURAL_NODES_TABLE_NAME
        ]
    ]
    for idx, cmd in enumerate(cmds):
        if idx > 0:
            cmd += ["-update"]
        util.call_cmd(cmd)
    conf = {
        NATURAL_WAYS_TABLE_NAME: {
            "target_name": NATURAL_WAYS_TABLE_NAME
        },
        NATURAL_NODES_TABLE_NAME: {
            "target_name": NATURAL_NODES_TABLE_NAME
        }
    }
    cmd = f"""
      ogr2ogr {path} {gpkg_path}
        -dsco MAX_SIZE=5000000
        -dsco MINZOOM=7
        -dsco MAXZOOM=14
        -dsco CONF='{json.dumps(conf)}'
    """
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
    if clean:
        con = database_connection(recreate=True)
        con.close()
    load_ways_data(filename, bbox=bbox)
    make_ways_mbtiles(path)
    return path


def make_context(pbf_url, clean=False, bbox=None, path="./context.mbtiles"):
    """Makes an mbtiles with contextual geographic info from OSM"""
    if not pbf_url or len(pbf_url) == 0:
        raise ValueError("You must specify a PBF URL")
    filename = fetch_data(pbf_url, clean=clean)
    if clean:
        con = database_connection(recreate=True)
        con.close()
    load_natural_ways_data(filename, bbox=bbox)
    load_natural_nodes_data(filename, bbox=bbox)
    make_context_mbtiles(path)

if __name__ == "__main__":
    PATH = make_ways(sys.argv[0])
    print(f"Created mbtiles at {PATH}")
