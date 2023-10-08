"""Methods for generating geologic data for Underfoot

Main export is make_rocks, which should load everything into the database,
export an MBTiles file, and return a list with the files you need.
"""

import argparse
import os
import shutil
import re
import time
from multiprocessing import Pool
import subprocess
import traceback

import psycopg2

from sources import util
from sources.util import rocks
from sources.util.citations import load_citation_for_source, CITATIONS_TABLE_NAME
from database import DBNAME, SRID, make_database

NUM_PROCESSES = 4

FINAL_TABLE_NAME = "rock_units"
MASK_TABLE_NAME = "rock_units_masks"


def remove_polygon_overlaps(source_table_name):
    """Painful process of removing polygon overlaps"""
    temp_source_table_name = f"temp_{source_table_name}"
    util.run_sql(
        f"DROP TABLE IF EXISTS \"{temp_source_table_name}\"",
        dbname=DBNAME
    )
    util.run_sql(
        f"ALTER TABLE {source_table_name} RENAME TO {temp_source_table_name}"
    )
    # First we need to split the table into its constituent polygons so we can
    # sort them by size and use them to cut holes out of the larger polygons
    util.log("\tDumping into constituent polygons...")
    dumped_source_table_name = f"dumped_{source_table_name}"
    util.run_sql(
        f"DROP TABLE IF EXISTS \"{dumped_source_table_name}\"",
        dbname=DBNAME
    )
    util.run_sql(f"""
      CREATE TABLE {dumped_source_table_name} AS
      SELECT {', '.join(rocks.METADATA_COLUMN_NAMES)}, (ST_Dump(geom)).geom AS geom
      FROM {temp_source_table_name}
    """)
    util.run_sql(f"""
        ALTER TABLE {dumped_source_table_name}
        ADD COLUMN id SERIAL PRIMARY KEY,
        ADD COLUMN area float
    """)
    util.run_sql(f"UPDATE {dumped_source_table_name} SET area = ST_Area(geom)")
    # Now we iterate over each polygon order by size, and use it to cut a hole
    # out of all the other polygons that intersect it
    polygons = util.run_sql_with_retries(
        f"SELECT id, ST_Area(geom) FROM {dumped_source_table_name} ORDER BY ST_Area(geom) ASC"
    )
    for idx, row in enumerate(polygons):
        progress = round(idx / len(polygons) * 100, 2)
        if progress % 10 < 0.01:
            util.log(
                f"Cutting larger polygons by smaller polygons in {source_table_name}... "
                f"({idx} / {len(polygons)}, {progress}%)"
            )
        poly_id = row[0]
        area = row[1]
        sql = f"""
          UPDATE {dumped_source_table_name}
          SET geom = ST_Multi(
              ST_Difference(geom, (SELECT geom FROM {dumped_source_table_name} WHERE id = {poly_id})))
          WHERE
            ST_Intersects(geom, (SELECT geom FROM {dumped_source_table_name} WHERE id = {poly_id}))
            AND id != {poly_id}
            AND area >= {area}
        """
        util.run_sql_with_retries(sql, dbname=DBNAME, quiet=True)
    util.log()
    util.log("\tRecreating multipolygons...")
    util.run_sql(f"""
        CREATE TABLE {source_table_name} AS
        SELECT
            {', '.join(rocks.METADATA_COLUMN_NAMES)},
            ST_Multi(ST_Union(geom)) AS geom
        FROM {dumped_source_table_name}
        GROUP BY {', '.join(rocks.METADATA_COLUMN_NAMES)}
    """)
    util.run_sql(
        f"DELETE FROM {source_table_name} WHERE ST_GeometryType(geom) = 'ST_GeometryCollection'"
    )


def process_source(source_identifier, clean=False):
    """Run the source scripts and load their data into the database"""
    util.log(f"Starting to process source: {source_identifier}")
    source_table_name = re.sub(r"\W", "_", source_identifier)
    try:
        num_rows = util.run_sql(
            f"SELECT COUNT(*) FROM {source_table_name}")[0][0]
        if num_rows > 0 and not clean:
            util.log(
                f"{source_table_name} exists and has data, skipping the source build..."
            )
            load_citation_for_source(source_identifier)
            return
    except psycopg2.errors.UndefinedTable:
        # If the table doesn't exist we need to proceed
        pass
    try:
        util.call_cmd([
            "python",
            os.path.join("sources", f"{source_identifier}.py")
        ])
        path = os.path.join("sources", f"{source_identifier}.py")
        work_path = util.make_work_dir(path)
        units_path = os.path.join(work_path, "units.geojson")
        util.run_sql(
            f"DROP TABLE IF EXISTS \"{source_table_name}\"",
            dbname=DBNAME
        )
        # stupid hack to make sure the table is dropped before we start loading into
        # it
        time.sleep(5)
        util.log(f"Loading {units_path} into {source_table_name} table...")
        util.call_cmd([
            "ogr2ogr",
            "-f", "PostgreSQL",
            f"PG:dbname={DBNAME}",
            units_path,
            "-nln", source_table_name,
            "-nlt", "MULTIPOLYGON",
            "-lco", "GEOMETRY_NAME=geom",
            "-skipfailures",
            "-a_srs", f"EPSG:{SRID}"
        ])
        work_source_table_name = f"work_{source_table_name}"
        util.run_sql(
            f"DROP TABLE IF EXISTS \"{work_source_table_name}\"",
            dbname=DBNAME
        )
        util.run_sql(
            f"CREATE TABLE {work_source_table_name} AS SELECT * FROM {source_table_name}",
            dbname=DBNAME
        )
        util.log("Deleting empty units...")
        util.run_sql(
            f"DELETE FROM {work_source_table_name} WHERE code IS NULL OR code = ''",
            dbname=DBNAME
        )
        util.log("Repairing invalid geometries...")
        util.run_sql(f"""
            UPDATE {work_source_table_name}
            SET geom = ST_MakeValid(geom)
            WHERE NOT ST_IsValid(geom)
        """)
        util.log("Removing polygon overlaps...")
        remove_polygon_overlaps(work_source_table_name)
        util.run_sql(
            f"DELETE FROM {work_source_table_name} "
            "WHERE ST_GeometryType(geom) = 'ST_GeometryCollection'"
        )
        util.run_sql(f"""
            UPDATE {work_source_table_name}
            SET geom = ST_MakeValid(geom)
            WHERE NOT ST_IsValid(geom)
        """)
        load_citation_for_source(source_identifier)
        util.log(f"Finished processing source: {source_identifier}")
    except subprocess.CalledProcessError as process_error:
        # If you don't do this, exceptions in subprocesses may not print stack
        # traces
        print(f"Failed to process {source_identifier}. Error: {process_error}")
        traceback.print_exc()
        print()
        raise process_error


def clip_source_polygons_by_mask(source_table_name):
    """Clip polygons in a source table by the mask table"""
    util.log("Clipping source polygons by the mask...")
    util.log("\tDumping into constituent polygons...")
    dumped_source_table_name = f"dumped_{source_table_name}"
    util.run_sql(
        f"DROP TABLE IF EXISTS \"{dumped_source_table_name}\"",
        dbname=DBNAME
    )
    util.run_sql(f"""
      CREATE TABLE {dumped_source_table_name} AS
      SELECT {', '.join(rocks.METADATA_COLUMN_NAMES)}, (ST_Dump(geom)).geom AS geom
      FROM {source_table_name}
    """)
    # Pretty sure this clips any polygons that would overlap the existing
    # units, since we don't want any overlaps
    util.run_sql(f"""
      UPDATE {dumped_source_table_name}
      SET geom = ST_Difference({dumped_source_table_name}.geom, {MASK_TABLE_NAME}.geom)
      FROM {MASK_TABLE_NAME}
      WHERE ST_Intersects({dumped_source_table_name}.geom, {MASK_TABLE_NAME}.geom)
    """)
    util.log("\tRecreating multipolygons...")
    util.run_sql(
        f"DROP TABLE IF EXISTS \"{source_table_name}\"",
        dbname=DBNAME
    )
    util.run_sql(f"""
      CREATE TABLE {source_table_name} AS
      SELECT
        {", ".join(rocks.METADATA_COLUMN_NAMES)},
        ST_Multi(ST_Union(geom)) AS geom
      FROM {dumped_source_table_name}
      GROUP BY {", ".join(rocks.METADATA_COLUMN_NAMES)}
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
    tables = [FINAL_TABLE_NAME, MASK_TABLE_NAME]
    for table_name in tables:
        util.run_sql(
            f"DROP TABLE IF EXISTS {table_name} CASCADE",
            dbname=DBNAME
        )

    # Create the units table
    column_names = ['id'] + rocks.METADATA_COLUMN_NAMES + ['source', 'geom']
    column_defs = [
      "id BIGSERIAL PRIMARY KEY"
    ] + [
      f"{c} text" for c in rocks.METADATA_COLUMN_NAMES
    ] + [
      "source text",
      f"geom geometry(MULTIPOLYGON, {SRID})"
    ]
    util.run_sql(f"""
      CREATE TABLE {FINAL_TABLE_NAME} (
        {', '.join(column_defs)}
      )
    """)

    # Create the masks table
    util.run_sql(f"""
      CREATE TABLE {MASK_TABLE_NAME} (
        source varchar(255),
        geom geometry(MULTIPOLYGON, {SRID})
      )
    """)

    # Since I'm almost certainly going to forget how this works, pool.map
    # (process_source, sources) would run process_source() on each item in
    # sources, so if sources is ['foo', 'bar'], it would run process_source
    # ('foo') and process_source('bar'). pool.starmap does the same thing
    # except the second arg is an iterable of iterables, if it's ['foo', true],
    # it will run process_source('foo', true)
    with Pool(processes=procs) as pool:
        # TODO how can I make this terminate the parent process if a child
        # process raises an exception
        pool.starmap(process_source, [[src, clean] for src in sources])
    col_names = ", ".join(rocks.METADATA_COLUMN_NAMES)

    for idx, source_identifier in enumerate(sources):
        source_table_name = re.sub(r"\W", "_", source_identifier)
        work_source_table_name = f"work_{source_table_name}"
        if idx == 0:
            util.log(f"Creating {FINAL_TABLE_NAME} and inserting...")
            insert_q = f"""
                INSERT INTO {FINAL_TABLE_NAME} ({col_names}, source, geom)
                SELECT {col_names}, '{source_identifier}', geom FROM {work_source_table_name}
            """
            util.run_sql(insert_q)
        else:
            clip_source_polygons_by_mask(work_source_table_name)
            util.log(f"Inserting into {FINAL_TABLE_NAME}...")
            util.run_sql(f"""
              INSERT INTO {FINAL_TABLE_NAME} ({', '.join(column_names[1:])})
              SELECT {col_names}, '{source_identifier}', s.geom
              FROM {work_source_table_name} s
            """)
        util.log(f"Updating {MASK_TABLE_NAME}...")
        # Remove slivers and make it valid
        if idx == 0:
            util.initialize_masks_table(MASK_TABLE_NAME, source_table_name)
        else:
            util.update_masks_table(MASK_TABLE_NAME, source_table_name)
    util.log(f"Database {DBNAME} created with table {FINAL_TABLE_NAME}")


def clean_sources(sources):
    """Clean any cached data for specified sources"""
    for source_identifier in sources:
        path = os.path.join("sources", f"{source_identifier}.py")
        work_path = util.make_work_dir(path)
        shutil.rmtree(work_path)


def make_mbtiles(sources, path="./rocks.mbtiles", bbox=None, geojson_path=None):
    """Export rock units into am MBTiles file"""
    mbtiles_cmd = [
        "ogr2ogr",
        "-f", "MBTILES",
        path,
        f"PG:dbname={DBNAME}",
        "-sql", "SELECT id::text AS id, lithology, min_age, controlled_span, geom FROM rock_units",
        "-nln", FINAL_TABLE_NAME,
        "-dsco", "MAX_SIZE=5000000",
        "-dsco", "MINZOOM=7",
        "-dsco", "MAXZOOM=14",
        "-dsco", "DESCRIPTION=\"Geological units\""
    ]
    if geojson_path:
        mbtiles_cmd += ["-clipdst", geojson_path]
    elif bbox:
        mbtiles_cmd += [
            "-clipdst",
            str(bbox["left"]),
            str(bbox["bottom"]),
            str(bbox["right"]),
            str(bbox["top"])
        ]
    util.call_cmd(mbtiles_cmd)
    columns = ["id"] + rocks.METADATA_COLUMN_NAMES + ["source"]
    util.add_table_from_query_to_mbtiles(
        table_name=f"{FINAL_TABLE_NAME}_attrs",
        dbname=DBNAME,
        query=f"SELECT {', '.join(columns)} FROM {FINAL_TABLE_NAME}",
        mbtiles_path=path,
        index_columns=["id"])
    sources_sql = ",".join([f"'{s}'" for s in sources])
    util.add_table_from_query_to_mbtiles(
        table_name=CITATIONS_TABLE_NAME,
        dbname=DBNAME,
        query=f"""
            SELECT * FROM {CITATIONS_TABLE_NAME}
            WHERE source IN ({sources_sql})
        """,
        mbtiles_path=path,
        index_columns=["source"])
    return os.path.abspath(path)


def make_rocks(
    sources,
    clean=False,
    path="./rocks.mbtiles",
    procs=NUM_PROCESSES,
    bbox=None,
    geojson_path=None
):
    """Make rocks MBTiles from a collection of sources"""
    make_database()
    if clean:
        clean_sources(sources)
    load_units(sources, clean=clean, procs=procs)
    mbtiles_path = make_mbtiles(sources, path=path, bbox=bbox, geojson_path=geojson_path)
    return mbtiles_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Make an MBTiles of geologic units from given source(s)")
    parser.add_argument(
        "source",
        type=str,
        nargs="+",
        help="Source(s). If the first source is a pack ID, the sources from the pack will be used "
             "and the other source arguments will be ignored"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean cached data before running"
    )
    parser.add_argument(
        "--path",
        type=str,
        help="Path to write the MBTiles file"
    )
    parser.add_argument(
        "--procs",
        type=int,
        help="Number of processes to use in parallel"
    )
    args = parser.parse_args()
    kwargs = { k: args[k] for k in args if args[k] and k in ("clean", "path", "procs")}
    make_rocks(args.source, **kwargs)
