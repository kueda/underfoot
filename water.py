"""Methods for generating hydrologic data for Underfoot"""

import argparse
import json
import os
import re
import shutil
from glob import glob
from multiprocessing import Pool

import psycopg2

from database import DBNAME, SRID, make_database
from sources import util
from sources.util.citations import load_citation_for_source, CITATIONS_TABLE_NAME
from sources.util.water import process_nhdplus_hr_source
from sources.util.tiger_water import process_tiger_water_for_fips


NUM_PROCESSES = 4
WATERWAYS_TABLE_NAME = "waterways"
WATERWAYS_MASK_TABLE_NAME = "waterways_mask"
WATERBODIES_TABLE_NAME = "waterbodies"
WATERBODIES_MASK_TABLE_NAME = "waterbodies_mask"
WATERSHEDS_TABLE_NAME = "watersheds"
WATERSHEDS_MASK_TABLE_NAME = "watersheds_mask"
WATERWAYS_NETWORK_TABLE_NAME = "waterways_network"


def clean_sources(sources, debug=False):
    """Clean any cached data for specified sources"""
    for source_identifier in sources:
        path = os.path.join("sources", f"{source_identifier}.py")
        work_path = util.make_work_dir(path)
        if debug:
            util.log(f"water: removing work dir: {work_path}")
        shutil.rmtree(work_path)


def process_source(source, clean=False, cleandb=False, cleanfiles=False, debug=False):
    """Process water source"""
    if debug:
        util.log(f"water: processing source: {source}")
    path = os.path.join("sources", f"{source}.py")
    source_script_path = os.path.join("sources", f"{source}.py")
    work_path = util.make_work_dir(source_script_path)
    if cleanfiles:
        gpkgs_path = os.path.join(work_path, "*.gpkg")
        for file_to_delete in glob(gpkgs_path):
            util.log(f"Deleting {file_to_delete}...")
            os.remove(file_to_delete)
    if os.path.isfile(path):
        util.call_cmd(["python", path], check=True)
    elif source.startswith("nhdplus_"):
        process_nhdplus_hr_source(
          os.path.join(os.path.realpath(__file__), "sources", source.upper()),
          url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/"
              f"{source.upper()}_GDB.zip",
          gdb_name=f"{source.upper()}_GDB.gdb"
        )
    elif source.startswith("tiger_water_"):
        fips_code = source.replace("tiger_water_", "")
        process_tiger_water_for_fips(
            [fips_code],
            source=os.path.join(os.path.realpath(__file__), "sources", source.upper())
        )
    else:
        raise ValueError(f"{source} has no file and no way to process it")
    load_citation_for_source(source)
    for layer in ["waterways", "waterbodies", "watersheds"]:
        gpkg_path = os.path.join(work_path, f"{layer}.gpkg")
        source_table_name = f"{source}_{layer}"
        if not os.path.isfile(gpkg_path):
            util.log(f"{gpkg_path} doesn't exist, skipping...")
            continue
        if cleandb:
            util.run_sql(f"DROP TABLE IF EXISTS {source_table_name}")
        try:
            num_rows = util.run_sql(
                f"SELECT COUNT(*) FROM {source_table_name}"
            )[0][0]
            if num_rows > 0 and not clean:
                util.log(
                    f"{source_table_name} exists and has data, skipping data "
                    "loading..."
                )
                # load_citation_for_source(source_identifier)
                continue
        except psycopg2.errors.UndefinedTable:
            # If the table doesn't exist we need to proceed
            pass
        # util.call_cmd(f"ogr2ogr {} {waterways_path}", shell=True, check=True)
        cmd = f"""ogr2ogr \
            -f PostgreSQL \
            PG:dbname={DBNAME} \
            {gpkg_path} \
            -nln {source_table_name} \
            -skipfailures \
            -a_srs EPSG:{SRID}
        """
        util.call_cmd(cmd, shell=True, check=True)
        if layer in ("watersheds", "waterbodies"):
            util.run_sql(f"""
                UPDATE {source_table_name}
                SET geom = ST_MakeValid(geom)
                WHERE NOT ST_IsValid(geom)
                """)
    network_path = os.path.join(work_path, "waterways-network.csv")
    if os.path.isfile(network_path):
        network_table_name = f"{source}_waterways_network"
        util.run_sql(f"DROP TABLE IF EXISTS {network_table_name}")
        util.run_sql(
            f"""
                CREATE TABLE {network_table_name} (
                    source_id VARCHAR(32),
                    to_source_id VARCHAR(32),
                    from_source_id VARCHAR(32)
                )
            """,
            dbname=DBNAME
        )
        util.run_sql(f"DELETE FROM {network_table_name}")
        util.call_cmd(f"""
            psql {DBNAME} -c "\\copy {network_table_name} FROM '{network_path}' WITH CSV HEADER"
        """, shell=True)


def process_sources(sources, clean=False, cleandb=False, cleanfiles=False, procs=NUM_PROCESSES,
                    debug=False):
    """Process multiple sources in parallel processes"""
    with Pool(processes=procs) as pool:
        pool.starmap(
            process_source,
            [[src, clean, cleandb, cleanfiles] for src in sources])


def load_waterways(sources, debug=False):
    """Load waterways into the database"""
    if debug:
        util.log(f"water: loading waterways for sources: {sources}")
    util.run_sql(f"DROP TABLE IF EXISTS {WATERWAYS_TABLE_NAME}", dbname=DBNAME)
    util.run_sql(
        f"""
            CREATE TABLE {WATERWAYS_TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                name TEXT,
                source VARCHAR(32),
                source_id VARCHAR(32),
                source_id_attr VARCHAR(32),
                type VARCHAR(128),
                is_natural INTEGER DEFAULT 1,
                permanence VARCHAR(64) DEFAULT 'permanent',
                surface VARCHAR(64) DEFAULT 'surface',
                geom geometry(MultiLineString, {SRID})
            )
        """,
        dbname=DBNAME
    )
    for source in sources:
        source_table_name = f"{source}_waterways"
        sql = f"""
            INSERT INTO {WATERWAYS_TABLE_NAME} (
                name,
                source,
                source_id_attr,
                source_id,
                type,
                is_natural,
                permanence,
                surface,
                geom
            )
            SELECT
                max(name),
                '{source}',
                max(source_id_attr),
                source_id,
                max(type),
                max(is_natural),
                max(permanence),
                max(surface),
                ST_Collect(ST_SimplifyPreserveTopology(geom, 0.00001)) AS geom
            FROM {source_table_name}
            GROUP BY source_id
        """
        try:
            util.run_sql(sql)
        except psycopg2.errors.UndefinedTable:
            util.log(f"{source_table_name} doesn't exist, skipping...")


def load_waterbodies(sources, debug=False):
    """Load waterbodies into the database"""
    if debug:
        util.log(f"water: loading waterbodies for sources: {sources}")
    util.run_sql(
        f"DROP TABLE IF EXISTS {WATERBODIES_TABLE_NAME}", dbname=DBNAME)
    util.run_sql(
        f"""
            CREATE TABLE {WATERBODIES_TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                name TEXT,
                source VARCHAR(32),
                source_id VARCHAR(32),
                source_id_attr VARCHAR(32),
                type VARCHAR(128),
                is_natural INTEGER DEFAULT 1,
                permanence VARCHAR(64) DEFAULT 'permanent',
                geom geometry(MultiPolygon, {SRID})
            )
        """,
        dbname=DBNAME
    )
    for source in sources:
        source_table_name = f"{source}_waterbodies"
        util.run_sql(f"""
            INSERT INTO {WATERBODIES_TABLE_NAME} (
                name,
                source,
                source_id_attr,
                source_id,
                type,
                is_natural,
                permanence,
                geom
            )
            SELECT
                name,
                '{source}',
                source_id_attr,
                source_id,
                type,
                is_natural::int,
                permanence,
                geom
            FROM {source_table_name}
        """)


def load_watersheds(sources, debug=False):
    """Load watersheds into the database"""
    if debug:
        util.log(f"water: loading watersheds for sources: {sources}")
    util.run_sql(
        f"DROP TABLE IF EXISTS \"{WATERSHEDS_TABLE_NAME}\"",
        dbname=DBNAME
    )
    util.run_sql(
        f"""
            CREATE TABLE {WATERSHEDS_TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                name TEXT,
                source VARCHAR(32),
                source_id VARCHAR(32),
                source_id_attr VARCHAR(32),
                geom geometry(MULTIPOLYGON, {SRID})
            )
        """,
        dbname=DBNAME
    )
    util.run_sql(
        f"DROP TABLE IF EXISTS \"{WATERSHEDS_MASK_TABLE_NAME}\"",
        dbname=DBNAME
    )
    util.run_sql(
        f"""
            CREATE TABLE {WATERSHEDS_MASK_TABLE_NAME} (
                geom geometry(MULTIPOLYGON, {SRID})
            )
        """,
        dbname=DBNAME
    )
    for source in sources:
        source_table_name = f"{source}_watersheds"
        num_mask_rows = util.run_sql(
            f"SELECT COUNT(*) FROM {WATERSHEDS_MASK_TABLE_NAME}"
        )[0][0]
        if num_mask_rows == 0:
            try:
                # Insert the first watersheds
                util.run_sql(f"""
                    INSERT INTO {WATERSHEDS_TABLE_NAME} (
                        name,
                        source,
                        source_id_attr,
                        source_id,
                        geom
                    )
                    SELECT
                        name,
                        '{source}',
                        source_id_attr,
                        source_id,
                        geom
                    FROM {source_table_name}
                """)
                # Build the mask
                util.initialize_masks_table(
                    WATERSHEDS_MASK_TABLE_NAME, source_table_name, buff=0.0001)
            except psycopg2.errors.UndefinedTable:
                util.log(f"{source_table_name} doesn't exist, skipping...")
                continue
        else:
            try:
                source_dump_table_name = f"{source_table_name}_dump"
                util.run_sql(f"DROP TABLE IF EXISTS {source_dump_table_name}")
                util.run_sql(f"""
                    CREATE TABLE {source_dump_table_name} AS
                    SELECT
                        name,
                        source_id_attr,
                        source_id,
                        (ST_Dump(
                            ST_Difference(
                                geom,
                                (SELECT geom FROM {WATERSHEDS_MASK_TABLE_NAME})
                            )
                        )).geom AS geom
                    FROM
                        {source_table_name}
                """)
                # Remove the polygons that are entirely within a small buffer of
                # the existing mask, i.e. the slivers that might have resulted
                # from diffing a complex coastline
                util.run_sql(f"""
                    DELETE FROM {source_dump_table_name}
                    WHERE ST_Contains(
                        (
                            SELECT st_buffer(geom, 0.01)
                            FROM {WATERSHEDS_MASK_TABLE_NAME}
                        ),
                        geom
                    )
                """)
                # Insert the massaged polygons as multipolygons
                util.run_sql(f"""
                    INSERT INTO {WATERSHEDS_TABLE_NAME} (
                        name,
                        source,
                        source_id_attr,
                        source_id,
                        geom
                    )
                    SELECT
                        name,
                        '{source}',
                        source_id_attr,
                        source_id,
                        ST_Collect(geom)
                    FROM {source_dump_table_name}
                    GROUP BY name, source_id_attr, source_id
                """)
                util.run_sql(f"DROP TABLE {source_table_name}_dump")
                # Update the mask
                util.update_masks_table(
                    WATERSHEDS_MASK_TABLE_NAME, source_table_name, buff=0.0001)
            except psycopg2.errors.UndefinedTable:
                util.log(f"{source_table_name} doesn't exist, skipping...")
                continue


def load_networks(sources, debug=False):
    """Combine waterways networks from multiple sources into a single network
    """
    if debug:
        util.log(f"water: loading waterway networks for sources: {sources}")
    util.run_sql(f"DROP TABLE IF EXISTS {WATERWAYS_NETWORK_TABLE_NAME}", dbname=DBNAME)
    util.run_sql(
        f"""
            CREATE TABLE {WATERWAYS_NETWORK_TABLE_NAME} (
                source VARCHAR(32),
                source_id VARCHAR(32),
                to_source_id VARCHAR(32),
                from_source_id VARCHAR(32)
            )
        """,
        dbname=DBNAME
    )
    for source in sources:
        source_table_name = f"{source}_waterways_network"
        # just merge in the source table and use the original source_ids
        try:
            util.run_sql(f"""
                INSERT INTO {WATERWAYS_NETWORK_TABLE_NAME}
                SELECT
                    '{source}' AS source,
                    source_id,
                    to_source_id,
                    from_source_id
                FROM {source_table_name}
                """)
        except psycopg2.errors.UndefinedTable:
            util.log(f"{source_table_name} doesn't exist, skipping...")


def make_mbtiles(sources, path="./water.mbtiles", bbox=None, debug=False):
    """Export water into am MBTiles file"""
    if debug:
        util.log(f"water: making mbtiles for sources: {sources}")
    if os.path.exists(path):
        os.remove(path)
    # 1. Write ways, bodies, and sheds to separate layers of a single GeoPackage file
    gpkg_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        f"{util.extless_basename(path)}.gpkg"
    )
    if os.path.exists(gpkg_path):
        os.remove(gpkg_path)
    table_names = [
        WATERWAYS_TABLE_NAME,
        WATERBODIES_TABLE_NAME,
        WATERSHEDS_TABLE_NAME
    ]
    for idx, table_name in enumerate(table_names):
        cmd = ["ogr2ogr"]
        if idx > 0:
            cmd += ["-update"]
        cmd += [
            gpkg_path,
            f"PG:dbname={DBNAME}",
            table_name,
            "-a_srs", f"EPSG:{SRID}",
        ]
        if bbox:
            cmd += [
                "-clipdst",
                str(bbox["left"]),
                str(bbox["bottom"]),
                str(bbox["right"]),
                str(bbox["top"])
            ]
        util.call_cmd(cmd, check=True)
    # 1. Write additional overview layers of perennial ways and large bodies
    waterways_overview_table_name = f"{WATERWAYS_TABLE_NAME}_overview"
    waterbodies_overview_table_name = f"{WATERBODIES_TABLE_NAME}_overview"
    cmd = [
        "ogr2ogr",
        "-update",
        gpkg_path,
        f"PG:dbname={DBNAME}",
        "-sql", f"""
            SELECT * FROM {WATERWAYS_TABLE_NAME}
            WHERE
                name IS NOT NULL
                AND is_natural = 1 AND permanence = 'perennial'
        """,
        "-nln", waterways_overview_table_name,
        "-a_srs", f"EPSG:{SRID}"
    ]
    if bbox:
        cmd += [
            "-clipdst",
            str(bbox["left"]),
            str(bbox["bottom"]),
            str(bbox["right"]),
            str(bbox["top"])
        ]
    util.call_cmd(cmd, check=True)
    cmd = [
        "ogr2ogr",
        "-update",
        gpkg_path,
        f"PG:dbname={DBNAME}",
        "-sql", f"""
            SELECT * FROM {WATERBODIES_TABLE_NAME}
            WHERE name IS NOT NULL AND ST_Area(geom) > 0.00001
        """,
        "-nln", waterbodies_overview_table_name,
        "-a_srs", f"EPSG:{SRID}"
    ]
    if bbox:
        cmd += [
            "-clipdst",
            str(bbox["left"]),
            str(bbox["bottom"]),
            str(bbox["right"]),
            str(bbox["top"])
        ]
    util.call_cmd(cmd, check=True)
    # 1. Use `-dsco CONF` to write all these layers to the mbtiles in one fell
    # swoop
    conf = {
        WATERWAYS_TABLE_NAME: {
            "target_name": WATERWAYS_TABLE_NAME,
            "minzoom": 9,
            "maxzoom": 14
        },
        WATERBODIES_TABLE_NAME: {
            "target_name": WATERBODIES_TABLE_NAME,
            "minzoom": 9,
            "maxzoom": 14
        },
        WATERSHEDS_TABLE_NAME: {
            "target_name": WATERSHEDS_TABLE_NAME,
            "minzoom": 7,
            "maxzoom": 14
        },
        waterways_overview_table_name: {
            "target_name": waterways_overview_table_name,
            "minzoom": 7,
            "maxzoom": 8
        },
        waterbodies_overview_table_name: {
            "target_name": waterbodies_overview_table_name,
            "minzoom": 7,
            "maxzoom": 8
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
    util.add_table_from_query_to_mbtiles(
        table_name=WATERWAYS_NETWORK_TABLE_NAME,
        dbname=DBNAME,
        query=f"SELECT * FROM {WATERWAYS_NETWORK_TABLE_NAME}",
        mbtiles_path=path,
        index_columns=["source_id", "to_source_id", "from_source_id"])
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
    return path


def make_water(
        sources, clean=False, cleandb=False, cleanfiles=False, bbox=None,
        path="./water.mbtiles", procs=NUM_PROCESSES, debug=False):
    """Process and load all water sources and write them to a MBTiles file"""
    if debug:
        util.log("water: making database")
    make_database()
    if clean:
        clean_sources(sources, debug=debug)
    process_sources(sources, cleandb=cleandb, cleanfiles=cleanfiles, procs=procs, debug=debug)
    load_waterways(sources, debug=debug)
    load_waterbodies(sources, debug=debug)
    load_watersheds(sources, debug=debug)
    load_networks(sources, debug=debug)
    return make_mbtiles(sources, path=path, bbox=bbox, debug=debug)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Make an MBTiles of hydrological data from given source(s)"
    )
    parser.add_argument("source", type=str, nargs="+", help="Source(s)")
    parser.add_argument(
        "--clean",
        action="store_true",
        help="""
            Clean all cached data before running, include downloads, files, and
            database tables
        """
    )
    parser.add_argument(
        "--cleandb",
        action="store_true",
        help="Just clean the database before running"
    )
    parser.add_argument(
        "--cleanfiles",
        action="store_true",
        help="Just clean the files extracted from the download"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug statements")
    args = parser.parse_args()
    util.log(f"cleandb: {args.cleandb}")
    make_water(
        args.source,
        clean=args.clean,
        cleandb=args.cleandb,
        cleanfiles=args.cleanfiles,
        debug=args.debug)
