"""
    Geology of Joshua Tree National Park geodatabase
    Open-File Report 2015-1175
    National Park Service
"""

import os
import csv
import json
import time

import util
from util import rocks

work_path = util.make_work_dir(os.path.realpath(__file__))
os.chdir(work_path)
SRS = "+proj=utm +zone=11 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"


def get_archive(url):
    """Download and extract the archive"""
    download_path = os.path.basename(url)
    if not os.path.isfile(download_path):
        print(f"DOWNLOADING {url}")
        util.call_cmd(["curl", "-OL", url])
    gdb_path = "JOTR_OFR_v10-2.gdb"
    if not os.path.isdir(gdb_path):
        print("EXTRACTING ARCHIVE...")
        util.unzip(download_path)
    return os.path.realpath(gdb_path)


def extract_units(gdb_path):
    """Convert GeoDatabase shapes to GeoJSON"""
    # ogr2ogr -progress -overwrite -skipfailures jotr-units.geojson
    # ~/Downloads/JOTR_OFR_v10-2.gdb GeologicUnits -nlt MULTIPOLYGON
    units_path = "shapes.geojson"
    if not os.path.isfile(units_path):
        util.call_cmd([
            "ogr2ogr",
            "-progress",
            "-overwrite",
            "-skipfailures",
            "-s_srs", SRS,
            "-t_srs", util.SRS,
            "-f", "GeoJSON",
            units_path,
            gdb_path,
            "GeologicUnits",
            "-nlt",
            "MULTIPOLYGON"
        ])
    return os.path.realpath(units_path)


def extract_attributes(gdb_path):
    """Extract GeoDatabase attributes to CSV"""
    # ogr2ogr -progress -overwrite -skipfailures -f "CSV" test.csv
    # ~/Downloads/JOTR_OFR_v10-2.gdb TblUnitDescriptionSummary
    csv_path = "attributes.csv"
    if not os.path.isfile(csv_path):
        util.call_cmd([
            "ogr2ogr",
            "-progress",
            "-overwrite",
            "-skipfailures",
            "-f", "CSV",
            csv_path,
            gdb_path,
            "TblUnitDescriptionSummary"
        ])
    return os.path.realpath(csv_path)


def schemify_attributes(attributes_path):
    """Convert attributes to the Underfoot schema"""
    util.log(f"SCHEMIFYING ATTRIBUTES for {attributes_path}...")
    outfile_path = "units.csv"
    with open(attributes_path, encoding="utf-8") as attributes_file:
        reader = csv.DictReader(attributes_file)
        with open(outfile_path, "w", encoding="utf-8") as outfile:
            columns = rocks.METADATA_COLUMN_NAMES
            writer = csv.DictWriter(
                outfile,
                fieldnames=columns,
                extrasaction="ignore"
            )
            writer.writeheader()
            for row in reader:
                row["code"] = row["MapUnitLabel"]
                row["title"] = row["UnitName"]
                row["description"] = row["UnitDescription"]
                row["lithology"] = rocks.lithology_from_text(
                    row["GeologicMaterialClassification"]
                )
                if not row["lithology"]:
                    row["lithology"] = rocks.lithology_from_text(
                        row["UnitDescription"]
                    )
                row["rock_type"] = rocks.rock_type_from_lithology(
                    row["lithology"]
                )
                split_unit_age = row["UnitAge"].split(',')
                if len(split_unit_age) > 1:
                    row["span"] = rocks.span_from_text(split_unit_age[1])
                if 'span' not in row.keys() or not row["span"]:
                    row["span"] = rocks.span_from_text(row["UnitAge"])
                row["controlled_span"] = rocks.controlled_span_from_span(
                    row["span"]
                )
                row["min_age"], row["max_age"], row["est_age"] = rocks.ages_from_span(row["span"])
                writer.writerow(row)
    return os.path.realpath(outfile_path)


def copy_citation():
    """Write citation to file"""
    now = time.localtime()
    data = [
      {
        "id": "http://dx.doi.org/10.3133/ofr20151175",
        "type": "GIS database",
        "title": "Geology of the Joshua Tree National Park geodatabase: U.S. Geological Survey "
                 "Open-File Report 2015-1175",
        "container-title": "United States Geological Survey",
        "URL": "https://pubs.er.usgs.gov/publication/ofr20151175",
        "language": "English",
        "author": [
          {
            "family": "Powell",
            "given": "Robert E."
          },
          {
            "family": "Matti",
            "given": "Jonathan C."
          },
          {
            "family": "Cossette",
            "given": "Pamela M."
          }
        ],
        "issued": {
          "date-parts": [
            [
              "2015"
            ]
          ]
        },
        "accessed": {
          "date-parts": [
            [
              now.tm_year,
              now.tm_mon,
              now.tm_mday
            ]
          ]
        }
      }
    ]
    with open(os.path.join(work_path, "citation.json"), "w", encoding="utf-8") as outfile:
        json.dump(data, outfile)


# download https://pubs.usgs.gov/of/2015/1175/ofr20151175_geodatabase.zip
gdb_path = get_archive(
    "https://pubs.usgs.gov/of/2015/1175/ofr20151175_geodatabase.zip"
)

# extract units into geojson
shapes_path = extract_units(gdb_path)

# extract data into csv
attributes_path = extract_attributes(gdb_path)

schemified_attributes_path = schemify_attributes(attributes_path)

rocks.join_polygons_and_metadata(
    shapes_path,
    schemified_attributes_path,
    polygons_join_col="MapUnitLabel",
    polygons_table_name="GeologicUnits",
    output_path="units.geojson"
)

copy_citation()
