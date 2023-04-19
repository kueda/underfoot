"""
  Colleciton of methods and scripts for processing geologic unit data, mostly
  from the USGS.
"""

from subprocess import call, run
import json
import os
import re
import shutil
import time
from glob import glob
import xml.etree.ElementTree as ET
import csv
from datetime import datetime as dt
import psycopg2

from .proj import *


def log(msg="", **kwargs):
    """Logging utility"""
    print(f"[{dt.now().isoformat()}] {msg}", **kwargs)


def extless_basename(path):
    """Like basename but without the need to explicitly list an extension"""
    if os.path.isfile(path):
        return os.path.splitext(os.path.basename(path))[0]
    return os.path.split(path)[-1]


def call_cmd(*args, **kwargs):
    """Calls shell command with logging"""
    # pylint: disable=subprocess-run-check
    if 'check' not in kwargs or kwargs['check'] is None:
        kwargs['check'] = True
    if isinstance(args[0], str) and len(args) == 1:
        print(f"Calling `{args[0]}` with kwargs: {kwargs}")
        return run(args[0], **kwargs)
    print(f"Calling `{' '.join(args[0])}` with kwargs: {kwargs}")
    return run(*args, **kwargs)
    # pylint: enable=subprocess-run-check


def run_sql(sql, dbname="underfoot", quiet=False, interpolations=None):
    """Run a SQL statement in the database"""
    con = psycopg2.connect(f"dbname={dbname}")
    cur = con.cursor()
    if interpolations:
        if not quiet:
            log(f"Running {sql} ({interpolations})")
        cur.execute(sql, interpolations)
    else:
        if not quiet:
            log(f"Running {sql}")
        cur.execute(sql)
    results = None
    try:
        results = cur.fetchall()
    except psycopg2.ProgrammingError:
        results = None
    con.commit()
    cur.close()
    con.close()
    return results


def run_sql_with_retries(
    sql,
    max_retries=3,
    retry=1,
    dbname="underfoot",
    quiet=False
):
    """Run a SQL statement with backoff retries"""
    try:
        return run_sql(sql, dbname=dbname, quiet=quiet)
    except (psycopg2.OperationalError, psycopg2.IntegrityError) as pg_err:
        if retry > max_retries:
            raise pg_err
        sleep = retry ** 3
        print(
            f"Failed to execute `{sql}`: {pg_err}. Trying again in {sleep}s"
        )
        time.sleep(sleep)
        return run_sql_with_retries(sql, retry + 1)


def basename_for_path(path):
    """Extensionless basename for path, defaulting to dir name if it's a module"""
    basename = extless_basename(path)
    if basename == "__init__":
        dirpath = os.path.dirname(os.path.realpath(path))
        basename = extless_basename(dirpath)
    return basename


def make_work_dir(path):
    """Makes the default work directory for a source"""
    dirpath = os.path.dirname(os.path.realpath(__file__))
    basename = basename_for_path(path)
    work_path = os.path.join(dirpath, "..", f"work-{basename}")
    if not os.path.isdir(work_path):
        os.makedirs(work_path)
    return work_path


def underscore(str_to_underscore):
    """Converts a string with spaces to underscores"""
    return "_".join([piece.lower() for piece in re.split(r'\s', str_to_underscore)])


def extract_e00(path):
    """Extracts data in an ArcInfo Interchange File Format (e00)"""
    dirpath = os.path.join(
        os.path.realpath(os.path.dirname(path)),
        f"{extless_basename(path)}-shapefiles".format()
    )
    if os.path.isfile(os.path.join(dirpath, "PAL.shp")):
        return dirpath
    shutil.rmtree(dirpath, ignore_errors=True)
    os.makedirs(dirpath)
    call(["ogr2ogr", "-f", "ESRI Shapefile", dirpath, path])
    return dirpath


def polygonize_arcs(
    shapefiles_path,
    polygon_pattern=".+-ID?$",
    force=False,
    outfile_path=None
):
    """Convert shapefile arcs from an extracted ArcINFO coverage and convert
    them to polygons.

    More often than not ArcINFO coverages seem to include arcs but not polygons
    when converted to shapefiles using ogr. A PAL.shp file gets created, but it
    only has polygon IDs, not geometries. This method will walk through all the
    arcs and combine them into their relevant polygons.

    Why is it in its own python script and not in this library? For reasons as
    mysterious as they are maddening, when you import fiona into this module,
    it causes subproccess calls to ogr2ogr to ignore the "+nadgrids=@null" proj
    flag, which results in datum shifts when attempting to project to web
    mercator. Yes, seriously. I have no idea why or how it does this, but that
    was the only explanation I could find for the datum shift.
    """
    if not outfile_path:
        outfile_path = os.path.join(shapefiles_path, "polygons.shp")
    if force:
        shutil.rmtree(outfile_path, ignore_errors=True)
    elif os.path.isfile(outfile_path):
        return outfile_path
    pal_path = os.path.join(shapefiles_path, "PAL.shp")
    arc_path = os.path.join(shapefiles_path, "ARC.shp")
    polygonize_arcs_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "polygonize_arcs.py"
    )
    call_cmd([
        "python",
        polygonize_arcs_path,
        outfile_path,
        pal_path,
        arc_path,
        "--polygon-column-pattern",
        polygon_pattern
    ])
    return outfile_path


def met2xml(path):
    """Converts a USGS metadata text file to XML"""
    output_path = os.path.join(
        os.path.realpath(os.path.dirname(path)),
        f"{extless_basename(path)}.xml"
    )
    met2xml_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "met2xml.py"
    )
    call_cmd(["python", met2xml_path, path, output_path])
    return output_path


def initialize_masks_table(mask_table_name, source_table_name, buff=0.01):
    """
      Creates the first row in a table intended to hold a single MULTIPOLYGON
      geometry that acts as a mask. This assumes that both tables have
      MULTIPOLYGON geom columns themselves.
    """
    run_sql(f"""
      INSERT INTO {mask_table_name} (geom)
      SELECT
        ST_MakeValid(
          ST_Multi(
            ST_Buffer(
              ST_Buffer(
                ST_MakeValid(
                  ST_Union(geom)
                ),
                {buff},
                'join=mitre'
              ),
              -{buff},
              'join=mitre'
            )
          )
        )
      FROM {source_table_name}
    """)


def update_masks_table(mask_table_name, source_table_name, buff=0.01):
    """
      Updates an existing masks table with geometries from another table.
      Again, the assumption is that all tables have geom columns. This also
      assumes no holes: it just takes the exterior ring from all the geoms in
      the source.
    """
    run_sql(f"""
        UPDATE {mask_table_name} m SET geom = ST_Multi(
          ST_Union(
            ST_MakeValid(m.geom),
            ST_MakeValid(
              ST_Multi(
                (
                  SELECT
                    ST_Buffer(
                      ST_Buffer(
                        ST_MakeValid(
                          ST_Union(ST_MakeValid(s.geom))
                        ),
                        {buff},
                        'join=mitre'
                      ),
                      -{buff},
                      'join=mitre'
                    )
                  FROM {source_table_name} s
                )
              )
            )
          )
        )
    """)


def add_table_from_query_to_mbtiles(
        table_name, dbname, query, mbtiles_path, index_columns=None):
    """Add a table to an MBTiles from a query to the Postgres db"""
    if index_columns is None:
        index_columns = []
    csv_path = f"{os.path.basename(mbtiles_path)}.csv"
    call_cmd(
        f"psql {dbname} -c \"COPY ({query}) TO STDOUT WITH CSV HEADER\" > {csv_path}",  # noqa: E501
        shell=True, check=True)
    shutil.rmtree(csv_path, ignore_errors=True)
    call_cmd([
        "sqlite3",
        "-csv",
        mbtiles_path,
        f".import {csv_path} {table_name}"
    ])
    for index_column in index_columns:
        sql = f"""
            CREATE INDEX {table_name}_{index_column}
            ON {table_name}({index_column})
        """
        call_cmd([
            "sqlite3",
            mbtiles_path,
            sql
        ], check=True)

def unzip(path):
    """Unzip a zip file at a path, updating if necessary"""
    return call_cmd(["unzip", "-u", path])
