"""Methods for generating hydrologic data for Underfoot"""

import argparse
import io
import json
import os
import re
import shutil
import tempfile
from collections import defaultdict
from glob import glob
from multiprocessing import Pool

import psycopg2

from database import DBNAME, SRID, make_database
from sources import util
from sources.util.citations import load_citation_for_source, CITATIONS_TABLE_NAME
from sources.util.water import process_nhdplus_hr_source, WATERWAYS_FLOW_FNAME
from sources.util.tiger_water import process_tiger_water_for_fips


NUM_PROCESSES = 4
WATERWAYS_TABLE_NAME = "waterways"
WATERWAYS_MASK_TABLE_NAME = "waterways_mask"
WATERBODIES_TABLE_NAME = "waterbodies"
WATERBODIES_MASK_TABLE_NAME = "waterbodies_mask"
WATERSHEDS_TABLE_NAME = "watersheds"
WATERSHEDS_MASK_TABLE_NAME = "watersheds_mask"
WATERWAYS_FLOW_TABLE_NAME = "waterways_flow"
# Attributes to include in tiles. source_id and source_id_attr stay in the
# database but not the tiles, since the app doesn't use them and, for
# waterways, a unique ID on every feature makes the tiles much bigger.
WATERWAYS_TILE_FIELDS = [
    "name",
    "source",
    "type",
    "is_natural",
    "is_imaginary",
    "permanence",
    "surface",
    "flow_pre",
    "flow_upstream",
]
WATERBODIES_TILE_FIELDS = [
    "name",
    "source",
    "type",
    "is_natural",
    "permanence",
]
WATERSHEDS_TILE_FIELDS = [
    "name",
    "source",
]
TILE_FIELDS = {
    WATERWAYS_TABLE_NAME: WATERWAYS_TILE_FIELDS,
    WATERBODIES_TABLE_NAME: WATERBODIES_TILE_FIELDS,
    WATERSHEDS_TABLE_NAME: WATERSHEDS_TILE_FIELDS,
}


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
          os.path.join(os.path.realpath(__file__), "sources", source),
          url="https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/"
              f"{source.upper()}_GDB.zip",
          gdb_name=f"{source.upper()}_GDB.gdb"
        )
    elif source.startswith("tiger_water_"):
        fips_code = source.replace("tiger_water_", "")
        process_tiger_water_for_fips(
            [fips_code],
            source=os.path.join(os.path.realpath(__file__), "sources", source)
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
    flow_path = os.path.join(work_path, WATERWAYS_FLOW_FNAME)
    if os.path.isfile(flow_path):
        flow_table_name = f"{source}_{WATERWAYS_FLOW_TABLE_NAME}"
        util.run_sql(f"DROP TABLE IF EXISTS {flow_table_name}")
        util.run_sql(
            f"""
                CREATE TABLE {flow_table_name} (
                    source_id VARCHAR(32),
                    hydroseq BIGINT,
                    dnhydroseq BIGINT
                )
            """,
            dbname=DBNAME
        )
        util.run_sql(f"DELETE FROM {flow_table_name}")
        util.call_cmd(f"""
            psql {DBNAME} -c "\\copy {flow_table_name} FROM '{flow_path}' WITH CSV HEADER"
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
                is_imaginary INTEGER DEFAULT 0,
                permanence VARCHAR(64) DEFAULT 'permanent',
                surface VARCHAR(64) DEFAULT 'surface',
                flow_pre INTEGER,
                flow_upstream INTEGER,
                geom geometry(MultiLineString, {SRID})
            )
        """,
        dbname=DBNAME
    )
    util.run_sql(f"""
        CREATE INDEX {WATERWAYS_TABLE_NAME}_geom_idx ON {WATERWAYS_TABLE_NAME} USING GIST(geom)
    """)
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
    util.run_sql(f"""
        CREATE INDEX {WATERBODIES_TABLE_NAME}_geom_idx ON {WATERBODIES_TABLE_NAME} USING GIST(geom)
    """)
    for source in sources:
        source_table_name = f"{source}_waterbodies"
        try:
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
        except psycopg2.errors.UndefinedTable:
            util.log(f"{source_table_name} doesn't exist, skipping...")


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


def load_flow(sources, debug=False):
    """Combine waterways flow from multiple sources into a single table
    """
    if debug:
        util.log(f"water: loading waterway flow for sources: {sources}")
    util.run_sql(f"DROP TABLE IF EXISTS {WATERWAYS_FLOW_TABLE_NAME}", dbname=DBNAME)
    util.run_sql(
        f"""
            CREATE TABLE {WATERWAYS_FLOW_TABLE_NAME} (
                source VARCHAR(32),
                source_id VARCHAR(32),
                hydroseq BIGINT,
                dnhydroseq BIGINT
            )
        """,
        dbname=DBNAME
    )
    for source in sources:
        source_table_name = f"{source}_{WATERWAYS_FLOW_TABLE_NAME}"
        # just merge in the source table and use the original source_ids
        try:
            util.run_sql(f"""
                INSERT INTO {WATERWAYS_FLOW_TABLE_NAME}
                SELECT
                    '{source}' AS source,
                    source_id,
                    hydroseq,
                    dnhydroseq
                FROM {source_table_name}
                """)
        except psycopg2.errors.UndefinedTable:
            util.log(f"{source_table_name} doesn't exist, skipping...")


def label_flow_tree(segments):
    """Label waterway segments so flow traces become simple range checks

    Takes (source_id, hydroseq, dnhydroseq) tuples, where each segment flows
    into the segment whose hydroseq matches its dnhydroseq. That's NHDPlus's
    main flow path, so it doesn't follow minor divergences like a canal
    leaving a river, and it makes every river a tree rooted at its outlet.
    Numbering each tree depth-first from its outlet gives each segment a
    pre-order number (pre), and the segments upstream of it get the next
    (upstream) numbers, so

    * segments downstream of X have pre <= X.pre <= pre + upstream
    * segments upstream of X have X.pre <= pre <= X.pre + X.upstream

    Storing the count of segments upstream instead of the last number
    upstream makes tiles smaller, since the count is small for most segments
    and vector tiles store each distinct value once per tile.

    Returns a dict of source_id => (pre, upstream)
    """
    segments = list(segments)
    source_ids_by_hydroseq = {hydroseq: source_id for source_id, hydroseq, _ in segments}
    upstream_ids = defaultdict(list)
    outlet_ids = []
    for source_id, _, dnhydroseq in segments:
        downstream_id = source_ids_by_hydroseq.get(dnhydroseq)
        if downstream_id is None:
            outlet_ids.append(source_id)
        else:
            upstream_ids[downstream_id].append(source_id)
    labels = {}
    pre = 0
    for outlet_id in outlet_ids:
        # Walk with a stack instead of recursion because big rivers can be
        # thousands of segments long
        stack = [(outlet_id, False)]
        while stack:
            source_id, visited_upstream = stack.pop()
            if visited_upstream:
                source_pre = labels[source_id][0]
                labels[source_id] = (source_pre, pre - 1 - source_pre)
                continue
            labels[source_id] = (pre, None)
            pre += 1
            stack.append((source_id, True))
            stack.extend((upstream_id, False) for upstream_id in upstream_ids[source_id])
    return labels


def label_waterways(debug=False):
    """Set flow_pre and flow_upstream on waterways (see label_flow_tree)"""
    if debug:
        util.log("water: labeling waterways flow")
    segments = util.run_sql(
        f"SELECT source_id, hydroseq, dnhydroseq FROM {WATERWAYS_FLOW_TABLE_NAME}",
        dbname=DBNAME
    )
    labels = label_flow_tree(segments)
    rows = io.StringIO("".join(
        f"{source_id}\t{pre}\t{upstream}\n" for source_id, (pre, upstream) in labels.items()
    ))
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE flow_labels (
                source_id VARCHAR(32),
                flow_pre INTEGER,
                flow_upstream INTEGER
            )
        """)
        cur.copy_expert("COPY flow_labels FROM STDIN", rows)
        # Match on source too so an ID from a source without flow data can't
        # collide with an NHDPlusID
        cur.execute(f"""
            UPDATE {WATERWAYS_TABLE_NAME} w
            SET flow_pre = l.flow_pre, flow_upstream = l.flow_upstream
            FROM flow_labels l
                JOIN {WATERWAYS_FLOW_TABLE_NAME} f ON f.source_id = l.source_id
            WHERE w.source = f.source AND w.source_id = l.source_id
        """)
    con.close()


def make_pmtiles(sources, path="./water.pmtiles", bbox=None, geojson_path=None, debug=False):
    """Export water into am PMTiles file"""
    if debug:
        util.log(f"water: making pmtiles for sources: {sources}")
    if os.path.exists(path):
        os.remove(path)
    table_names = [
        WATERWAYS_TABLE_NAME,
        WATERBODIES_TABLE_NAME,
        WATERSHEDS_TABLE_NAME
    ]
    # Write ways, bodies, and sheds to separate layers of a single GeoPackage
    # file in a temporary dir, since it's just an intermediate step towards
    # the PMTiles file and doesn't need to stick around after this returns
    with tempfile.TemporaryDirectory() as tmpdir:
        gpkg_path = os.path.join(tmpdir, f"{util.extless_basename(path)}.gpkg")
        for idx, table_name in enumerate(table_names):
            cmd = ["ogr2ogr"]
            if idx > 0:
                cmd += ["-update"]
            cmd += [
                gpkg_path,
                f"PG:dbname={DBNAME}",
                table_name,
                "-a_srs", f"EPSG:{SRID}",
                "-select", ",".join(TILE_FIELDS[table_name]),
            ]
            # Don't clip the waterways, useful to see connectivity across the
            # entire watershed
            if table_name != WATERWAYS_TABLE_NAME:
                if geojson_path:
                    cmd += ["-clipdst", geojson_path]
                elif bbox:
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
                SELECT {", ".join(WATERWAYS_TILE_FIELDS)}, geom
                FROM {WATERWAYS_TABLE_NAME}
                WHERE
                    name IS NOT NULL
                    AND is_natural = 1 AND permanence = 'perennial'
            """,
            "-nln", waterways_overview_table_name,
            "-a_srs", f"EPSG:{SRID}"
        ]
        if geojson_path:
            cmd += ["-clipdst", geojson_path]
        elif bbox:
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
                SELECT {", ".join(WATERBODIES_TILE_FIELDS)}, geom
                FROM {WATERBODIES_TABLE_NAME}
                WHERE name IS NOT NULL AND ST_Area(geom) > 0.00001
            """,
            "-nln", waterbodies_overview_table_name,
            "-a_srs", f"EPSG:{SRID}"
        ]
        if geojson_path:
            cmd += ["-clipdst", geojson_path]
        elif bbox:
            cmd += [
                "-clipdst",
                str(bbox["left"]),
                str(bbox["bottom"]),
                str(bbox["right"]),
                str(bbox["top"])
            ]
        util.call_cmd(cmd, check=True)
        # 1. Use `-dsco CONF` to write all these layers to the pmtiles in one fell
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
    sources_sql = ",".join([f"'{s}'" for s in sources])
    util.add_table_from_query_to_pmtiles(
        table_name=CITATIONS_TABLE_NAME,
        dbname=DBNAME,
        query=f"""
            SELECT * FROM {CITATIONS_TABLE_NAME}
            WHERE source IN ({sources_sql})
        """,
        pmtiles_path=path)
    return path

def update_imaginary_waterways():
    """
    Set the imaginary column in the waterways table for all ways that are
    effectively imaginary, i.e. they depict the path water might take through
    a waterbody. NHD lumps these in the "artificial" type, even though the
    artificer in these cases are mapmapkers, not people making physical
    changes on the ground.
    """
    util.run_sql(f"""
        UPDATE {WATERWAYS_TABLE_NAME} SET is_imaginary = 1
        FROM {WATERBODIES_TABLE_NAME}
        WHERE
            ST_CONTAINS({WATERBODIES_TABLE_NAME}.geom, {WATERWAYS_TABLE_NAME}.geom)
            AND {WATERWAYS_TABLE_NAME}.type = 'artificial'
    """)

def make_water(
        sources, clean=False, cleandb=False, cleanfiles=False, bbox=None,
        path="./water.pmtiles", procs=NUM_PROCESSES, debug=False, geojson_path=None):
    """Process and load all water sources and write them to a PMTiles file"""
    if debug:
        util.log("water: making database")
    make_database()
    if clean:
        clean_sources(sources, debug=debug)
    # Cleaning the work dirs without dropping the per-source tables would leave
    # process_source skipping the reload because the old rows are still there
    process_sources(
        sources, cleandb=(clean or cleandb), cleanfiles=cleanfiles, procs=procs, debug=debug)
    load_waterways(sources, debug=debug)
    load_waterbodies(sources, debug=debug)
    update_imaginary_waterways()
    load_watersheds(sources, debug=debug)
    load_flow(sources, debug=debug)
    label_waterways(debug=debug)
    return make_pmtiles(sources, path=path, bbox=bbox, geojson_path=geojson_path, debug=debug)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Make an PMTiles of hydrological data from given source(s)"
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
