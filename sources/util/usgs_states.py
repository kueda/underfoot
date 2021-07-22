import csv
import os
import pandas as pd
from . import (
    ages_from_span,
    call_cmd,
    controlled_span_from_span,
    join_polygons_and_metadata,
    make_work_dir,
    METADATA_COLUMN_NAMES,
    rock_type_from_lithology,
    span_from_lithology,
    SRS as DEST_SRS
)

SRS = "+proj=longlat +datum=NAD27 +no_defs"


def download_shapes(state, base_url):
    print("DOWNLOADING SHAPEFILES FOR {}...".format(state))
    url = os.path.join(base_url, f"{state}geol_dd.zip")
    download_path = os.path.basename(url)
    if not os.path.isfile(download_path):
        print("DOWNLOADING {}".format(url))
        call_cmd(["curl", "-OL", url])
    shp_path = "{}geol_poly_dd.shp".format(state.lower())
    if not os.path.isfile(shp_path):
        print("EXTRACTING ARCHIVE...")
        call_cmd(["unzip", download_path])
    return os.path.realpath(shp_path)


def download_attributes(state, base_url):
    print("DOWNLOADING ATTRIBUTES FOR {}...".format(state))
    url = os.path.join(base_url, f"{state}csv.zip")
    download_path = os.path.basename(url)
    if not os.path.isfile(download_path):
        print("DOWNLOADING {}".format(url))
        call_cmd(["curl", "-OL", url])
    csv_path = "{}units.csv".format(state)
    if not os.path.isfile(csv_path):
        print("EXTRACTING ARCHIVE...")
        call_cmd(["unzip", download_path])
    return os.path.realpath(csv_path)


def schemify_attributes(attributes_path):
    print("SCHEMIFYING ATTRIBUTES for {}...".format(attributes_path))
    outfile_path = "units.csv"
    with open(attributes_path) as f:
        reader = csv.DictReader(f)
        with open(outfile_path, 'w') as outfile:
            columns = METADATA_COLUMN_NAMES + ['UNIT_LINK']
            writer = csv.DictWriter(
                outfile,
                fieldnames=columns,
                extrasaction='ignore'
            )
            writer.writeheader()
            for row in reader:
                row['code'] = row['ORIG_LABEL']
                row['title'] = row['UNIT_NAME']
                row['description'] = ". ".join(
                    [row['UNITDESC'], row['UNIT_COM']]
                )
                row['lithology'] = row['ROCKTYPE1']
                row['rock_type'] = rock_type_from_lithology(
                    row['ROCKTYPE1']
                )
                row['span'] = row['UNIT_AGE']
                if not row['span']:
                    row['span'] = span_from_lithology(row['lithology'])
                row['controlled_span'] = controlled_span_from_span(
                    row['span']
                )
                row['min_age'], row['max_age'], row['est_age'] = ages_from_span(row['span'])  # noqa: E501
                writer.writerow(row)
    return os.path.realpath(outfile_path)


def merge_shapes(paths):
    print("MERGING SHAPEFILES...")
    merged_path = "merged_units.shp"
    call_cmd(["ogr2ogr", "-overwrite", merged_path, paths.pop()])
    for path in paths:
        call_cmd(["ogr2ogr", "-update", "-append", merged_path, path])
    print("DISSOLVING SHAPES AND REPROJECTING...")
    dissolved_path = "dissolved_units.shp"
    call_cmd([
        "ogr2ogr",
        "-s_srs", SRS,
        "-t_srs", DEST_SRS,
        dissolved_path, merged_path,
        "-overwrite",
        "-dialect", "sqlite",
        "-sql", "SELECT UNIT_LINK, ST_Union(geometry) as geometry FROM 'merged_units' GROUP BY UNIT_LINK"  # noqa: E501
    ])
    return dissolved_path


def merge_attributes(paths):
    print("MERGING SHAPEFILES...")
    merged_path = "merged_units.csv"
    merged = pd.concat([
        pd.read_csv(path, encoding="ISO-8859-1") for path in paths
    ])
    merged.loc[:, [
      'ORIG_LABEL',
      'UNIT_LINK',
      'UNIT_NAME',
      'UNIT_AGE',
      'UNITDESC',
      'UNIT_COM',
      'ROCKTYPE1'
    ]].to_csv(merged_path, encoding='utf-8')
    return merged_path


def copy_citation(base_path, work_path):
    print("COPYING CITATION")
    call_cmd([
      "cp",
      os.path.join(os.path.dirname(base_path), "citation.json"),
      os.path.join(work_path, "citation.json")
    ])


def process_usgs_states(
    # List of strings of two-letter state codes
    states,
    # Path to the source script that has relevant data files as peers
    base_path,
    # URL where remote data files lives
    base_url,
    # Path to the source script that output files are being made for
    source_path
):
    work_path = make_work_dir(base_path)
    os.chdir(work_path)
    shape_paths = [download_shapes(state, base_url) for state in states]
    single_shapefile_path = merge_shapes(shape_paths)
    attribute_paths = [download_attributes(state, base_url) for state in states]
    single_attributes_path = merge_attributes(attribute_paths)
    schemified_attributes_path = schemify_attributes(single_attributes_path)
    join_polygons_and_metadata(
      single_shapefile_path,
      schemified_attributes_path,
      polygons_join_col="UNIT_LINK",
      metadata_join_col="UNIT_LINK",
      output_path="units.geojson")
    copy_citation(base_path, work_path)
    dest_work_path = make_work_dir(source_path)
    for fname in ["units.csv", "units.geojson", "citation.json"]:
        call_cmd([
            "cp",
            os.path.join(work_path, fname),
            os.path.join(dest_work_path, fname)
        ])
