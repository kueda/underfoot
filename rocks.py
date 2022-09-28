"""Methods for generating geologica data for Underfoot

Main export is make_rocks, which should load everything into the database,
export an MBTiles file, and return a list with the files you need.
"""

import argparse
import os
import shutil
import re
import psycopg2
import time
from multiprocessing import Pool

from sources import util
from sources.util.citations import load_citation_for_source, CITATIONS_TABLE_NAME
from database import DBNAME, SRID

NUM_PROCESSES = 4

final_table_name = "rock_units"
mask_table_name = "rock_units_masks"


# Painful process of removing polygon overlaps
def remove_polygon_overlaps(source_table_name):
    temp_source_table_name = "temp_{}".format(source_table_name)
    util.run_sql(
        "DROP TABLE IF EXISTS \"{}\"".format(temp_source_table_name),
        dbname=DBNAME
    )
    util.run_sql(
        "ALTER TABLE {} RENAME TO {}".format(
            source_table_name,
            temp_source_table_name
        )
    )
    # First we need to split the table into its constituent polygons so we can
    # sort them by size and use them to cut holes out of the larger polygons
    util.log("\tDumping into constituent polygons...")
    dumped_source_table_name = "dumped_{}".format(source_table_name)
    util.run_sql(
        "DROP TABLE IF EXISTS \"{}\"".format(dumped_source_table_name),
        dbname=DBNAME
    )
    util.run_sql("""
      CREATE TABLE {} AS SELECT {}, (ST_Dump(geom)).geom AS geom FROM {}
    """.format(
      dumped_source_table_name,
      ", ".join(util.METADATA_COLUMN_NAMES),
      temp_source_table_name
    ))
    util.run_sql(
        "ALTER TABLE {} ADD COLUMN id SERIAL PRIMARY KEY, ADD COLUMN area float".format(  # noqa: E501
            dumped_source_table_name
        )
    )
    util.run_sql(
        "UPDATE {} SET area = ST_Area(geom)".format(dumped_source_table_name))
    # Now we iterate over each polygon order by size, and use it to cut a hole
    # out of all the other polygons that intersect it
    polygons = util.run_sql_with_retries(
        "SELECT id, ST_Area(geom) FROM {} ORDER BY ST_Area(geom) ASC".format(
            dumped_source_table_name
        )
    )
    for idx, row in enumerate(polygons):
        progress = round(idx / len(polygons) * 100, 2)
        if progress % 10 < 0.01:
            util.log(
                f"Cutting larger polygons by smaller polygons in {source_table_name}... ({idx} / {len(polygons)}, {progress}%)"  # noqa: E501
            )
        id = row[0]
        area = row[1]
        # cur2 = con.cursor()
        sql = """
          UPDATE {}
          SET geom = ST_Multi(
              ST_Difference(geom, (SELECT geom FROM {} WHERE id = {})))
          WHERE
            ST_Intersects(geom, (SELECT geom FROM {} WHERE id = {}))
            AND id != {}
            AND area >= {}
        """.format(
          dumped_source_table_name,
          dumped_source_table_name, id,
          dumped_source_table_name, id, id,
          area
        )
        util.run_sql_with_retries(sql, dbname=DBNAME, quiet=True)
    util.log()
    util.log("\tRecreating multipolygons...")
    util.run_sql(
        "CREATE TABLE {} AS SELECT {}, ST_Multi(ST_Union(geom)) AS geom FROM {} GROUP BY {}".format(  # noqa: E501
          source_table_name,
          ", ".join(util.METADATA_COLUMN_NAMES),
          dumped_source_table_name,
          ", ".join(util.METADATA_COLUMN_NAMES)
        )
    )
    util.run_sql(
        "DELETE FROM {} WHERE ST_GeometryType(geom) = 'ST_GeometryCollection'".format(  # noqa: E501
            source_table_name
        )
    )


# Run the source scripts and load their data into the database
def process_source(source_identifier, clean=False):
    source_table_name = re.sub(r"\W", "_", source_identifier)
    try:
        num_rows = util.run_sql(
            f"SELECT COUNT(*) FROM {source_table_name}")[0][0]
        if num_rows > 0 and not clean:
            util.log(
                f"{source_table_name} exists and has data, skipping the source build..."  # noqa: E501
            )
            load_citation_for_source(source_identifier)
            return
    except psycopg2.errors.UndefinedTable:
        # If the table doesn't exist we need to proceed
        pass
    util.call_cmd([
        "python",
        os.path.join("sources", "{}.py".format(source_identifier))
    ])
    path = os.path.join("sources", "{}.py".format(source_identifier))
    work_path = util.make_work_dir(path)
    units_path = os.path.join(work_path, "units.geojson")
    util.run_sql(
        "DROP TABLE IF EXISTS \"{}\"".format(source_table_name),
        dbname=DBNAME
    )
    # stupid hack to make sure the table is dropped before we start loading into
    # it
    time.sleep(5)
    util.log(f"Loading {units_path} into {source_table_name} table...")
    util.call_cmd([
        "ogr2ogr",
        "-f", "PostgreSQL",
        "PG:dbname={}".format(DBNAME),
        units_path,
        "-nln", source_table_name,
        "-nlt", "MULTIPOLYGON",
        "-lco", "GEOMETRY_NAME=geom",
        "-skipfailures",
        "-a_srs", "EPSG:{}".format(SRID)
    ])
    work_source_table_name = "work_{}".format(source_table_name)
    util.run_sql(
        "DROP TABLE IF EXISTS \"{}\"".format(work_source_table_name),
        dbname=DBNAME
    )
    util.run_sql(
        f"CREATE TABLE {work_source_table_name} AS SELECT * FROM {source_table_name}",  # noqa: E501
        dbname=DBNAME
    )
    util.log("Deleting empty units...")
    util.run_sql(
        f"DELETE FROM {work_source_table_name} WHERE code IS NULL OR code = ''",  # noqa: E501
        dbname=DBNAME
    )
    util.log("Repairing invalid geometries...")
    util.run_sql(
        f"UPDATE {work_source_table_name} SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom)"  # noqa: E501
    )
    util.log("Removing polygon overlaps...")
    remove_polygon_overlaps(work_source_table_name)
    util.run_sql(
        f"DELETE FROM {work_source_table_name} WHERE ST_GeometryType(geom) = 'ST_GeometryCollection'"  # noqa: E501
    )
    util.run_sql(
        f"UPDATE {work_source_table_name} SET geom = ST_MakeValid(geom) WHERE NOT ST_IsValid(geom)"  # noqa: E501
    )
    load_citation_for_source(source_identifier)


def clip_source_polygons_by_mask(source_table_name):
    util.log("Clipping source polygons by the mask...")
    util.log("\tDumping into constituent polygons...")
    dumped_source_table_name = "dumped_{}".format(source_table_name)
    util.run_sql(
        "DROP TABLE IF EXISTS \"{}\"".format(dumped_source_table_name),
        dbname=DBNAME
    )
    util.run_sql("""
      CREATE TABLE {} AS SELECT {}, (ST_Dump(geom)).geom AS geom FROM {}
    """.format(
        dumped_source_table_name,
        ", ".join(util.METADATA_COLUMN_NAMES),
        source_table_name
    ))
    # Pretty sure this clips any polygons that would overlap the existing
    # units, since we don't want any overlaps
    util.run_sql("""
      UPDATE {}
      SET geom = ST_Difference({}.geom, {}.geom)
      FROM {}
      WHERE ST_Intersects({}.geom, {}.geom)
    """.format(
        dumped_source_table_name,
        dumped_source_table_name, mask_table_name,
        mask_table_name,
        dumped_source_table_name, mask_table_name
      ))
    util.log("\tRecreating multipolygons...")
    util.run_sql(
        "DROP TABLE IF EXISTS \"{}\"".format(source_table_name),
        dbname=DBNAME
    )
    util.run_sql(f"""
      CREATE TABLE {source_table_name} AS
      SELECT
        {", ".join(util.METADATA_COLUMN_NAMES)},
        ST_Multi(ST_Union(geom)) AS geom
      FROM {dumped_source_table_name}
      GROUP BY {", ".join(util.METADATA_COLUMN_NAMES)}
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
    tables = [final_table_name, mask_table_name]
    for table_name in tables:
        util.run_sql(
            "DROP TABLE IF EXISTS {} CASCADE".format(table_name),
            dbname=DBNAME
        )

    # Create the units table
    column_names = ['id'] + util.METADATA_COLUMN_NAMES + ['source', 'geom']
    column_defs = [
      "id BIGSERIAL PRIMARY KEY"
    ] + [
      "{} text".format(c) for c in util.METADATA_COLUMN_NAMES
    ] + [
      "source text",
      "geom geometry(MULTIPOLYGON, {})".format(SRID)
    ]
    util.run_sql("""
      CREATE TABLE {} (
        {}
      )
    """.format(
      final_table_name,
      ", ".join(column_defs)
    ))

    # Create the masks table
    util.run_sql("""
      CREATE TABLE {} (
        source varchar(255),
        geom geometry(MULTIPOLYGON, {})
      )
    """.format(mask_table_name, SRID))

    # Creaate a processing pool to max out 4 processors
    pool = Pool(processes=procs)
    # Since I'm almost certainly going to forget how this works, pool.map
    # (process_source, sources) would run process_source() on each item in
    # sources, so if sources is ['foo', 'bar'], it would run process_source
    # ('foo') and process_source('bar'). pool.starmap does the same thing
    # except the second arg is an iterable of iterables, if it's ['foo', true],
    # it will run process_source('foo', true)
    pool.starmap(process_source, [[src, clean] for src in sources])

    for idx, source_identifier in enumerate(sources):
        source_table_name = re.sub(r"\W", "_", source_identifier)
        work_source_table_name = "work_{}".format(source_table_name)
        if idx == 0:
            util.log("Creating {} and inserting...".format(final_table_name))
            insert_q = "INSERT INTO {} ({}, source, geom) SELECT {}, '{}', geom FROM {}"  # noqa: E501
            util.run_sql(insert_q.format(
                final_table_name,
                ", ".join(util.METADATA_COLUMN_NAMES),
                ", ".join(util.METADATA_COLUMN_NAMES),
                source_identifier,
                work_source_table_name
            ))
        else:
            clip_source_polygons_by_mask(work_source_table_name)
            util.log("Inserting into {}...".format(final_table_name))
            util.run_sql("""
              INSERT INTO {} ({})
              SELECT {}, '{}', s.geom
              FROM {} s
            """.format(
              final_table_name, ", ".join(column_names[1:]),
              ", ".join(util.METADATA_COLUMN_NAMES), source_identifier,
              work_source_table_name
            ))
        util.log("Updating {}...".format(mask_table_name))
        # Remove slivers and make it valid
        if idx == 0:
            util.initialize_masks_table(mask_table_name, source_table_name)
        else:
            util.update_masks_table(mask_table_name, source_table_name)  
    util.log(
        "Database {} created with table {}".format(DBNAME, final_table_name)
    )


def clean_sources(sources):
    """Clean any cached data for specified sources"""
    for idx, source_identifier in enumerate(sources):
        path = os.path.join("sources", "{}.py".format(source_identifier))
        work_path = util.make_work_dir(path)
        shutil.rmtree(work_path)


def make_mbtiles(sources, path="./rocks.mbtiles", bbox=None):
    """Export rock units into am MBTiles file"""
    mbtiles_cmd = [
        "ogr2ogr",
        "-f", "MBTILES",
        path,
        f"PG:dbname={DBNAME}",
        "-sql", "SELECT id::text AS id, lithology, min_age, controlled_span, geom FROM rock_units",  # noqa: E501
        "-nln", final_table_name,
        "-dsco", "MAX_SIZE=5000000",
        "-dsco", "MINZOOM=7",
        "-dsco", "MAXZOOM=14",
        "-dsco", "DESCRIPTION=\"Geological units\""
    ]
    if bbox:
        mbtiles_cmd += [
            "-clipdst",
            str(bbox["left"]),
            str(bbox["bottom"]),
            str(bbox["right"]),
            str(bbox["top"])
        ]
    util.call_cmd(mbtiles_cmd)
    columns = ["id"] + util.METADATA_COLUMN_NAMES + ["source"]
    util.add_table_from_query_to_mbtiles(
        table_name=f"{final_table_name}_attrs",
        dbname=DBNAME,
        query=f"SELECT {', '.join(columns)} FROM {final_table_name}",
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
    bbox=None
):
    if clean:
        clean_sources(sources)
    load_units(sources, clean=clean, procs=procs)
    mbtiles_path = make_mbtiles(sources, path=path, bbox=bbox)
    return mbtiles_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Make an MBTiles of geologic units from given source(s)")
    parser.add_argument("source", type=str, nargs="+", help="Source(s)")
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean cached data before running"
    )
    args = parser.parse_args()
    make_rocks(args.source, clean=args.clean)
