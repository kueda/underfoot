"""Import geologic units for Hawaii"""
import csv
import locale
import json
import os
import re
import time
from datetime import date
import fiona
import util

work_path = util.make_work_dir(os.path.realpath(__file__))
os.chdir(work_path)
# NAD83 / UTM zone 4N, EPSG:26904
SRS = "+proj=utm +zone=4 +datum=NAD83 +units=m +no_defs"  # noqa: E501
TABLE_NAME = "GeologicUnits"
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )


def fetch(url):
    """Fetch the data and extract it"""
    download_path = "sim3143.zip"
    if not os.path.isfile(download_path):
        util.log(f"DOWNLOADING {url}")
        util.call_cmd(["curl", "-L", url, "--output", download_path])
    shp_dir_path = "HawaiiStateGeologicMap_GeMS-open"
    if not os.path.isdir(shp_dir_path):
        util.log("EXTRACTING ARCHIVE...")
        util.call_cmd(["unzip", download_path])
    return os.path.realpath(shp_dir_path)


def reproject(shp_dir_path):
    """Reproject shapefile into standard SRS"""
    shp_path = os.path.join(shp_dir_path, "GM_MapUnitPolys.shp")
    units_path = "shapes.geojson"
    util.call_cmd([
        "ogr2ogr",
        "-progress",
        "-overwrite",
        "-skipfailures",
        "-s_srs", SRS,
        "-t_srs", util.SRS,
        "-f", "GeoJSON",
        units_path,
        shp_path,
        "-nlt", "MULTIPOLYGON",
        "-nln", TABLE_NAME
    ])
    return units_path


def schemify_attributes(attributes_path):
    """Create an attributes CSV with standard attributes"""
    util.log(f"SCHEMIFYING ATTRIBUTES for {attributes_path}...")
    units_from_shp = {}
    shp_path = os.path.join(os.path.dirname(attributes_path), "GM_MapUnitPolys.shp")
    with fiona.open(shp_path) as units:
        for unit in units:
            if "properties" not in unit:
                continue
            code = unit["properties"]["MapUnit"]
            if code in units_from_shp:
                continue
            min_age = None
            max_age = None
            est_age = None
            if age_range := unit["properties"].get("AgeRange", None):
                if match := re.match(r"([\d\.\,]+)\s*?(to|-)\s*?([\d\.\,]+)\s*(.+)", age_range):
                    age1_s, _btwn, age2_s, age_unit = match.groups()
                    age1 = locale.atof(age1_s)
                    age2 = locale.atof(age2_s)
                    year_multiplier = 1
                    if "Ma" in age_unit:
                        year_multiplier = 1_000_000
                    min_age, max_age = sorted([age * year_multiplier for age in [age1, age2]])
                    est_age = (min_age + max_age) / 2
                elif match := re.match(r"A\.D\.\s*(\d{4})\s*?-?\s*?(\d{4})?", age_range):
                    year1, year2 = match.groups()
                    years = [int(year1)]
                    if year2:
                        years.append(int(year2))
                    else:
                        years.append(int(year1))
                    current_year = date.today().year
                    min_age, max_age = sorted([current_year - year for year in years])
                    est_age = (min_age + max_age) / 2
            units_from_shp[code] = {
                "code": code,
                "lithology": util.lithology_from_text(unit["properties"]["Com"]),
                "min_age": min_age,
                "max_age": max_age,
                "est_age": est_age,
                "span": util.span_from_usgs_code(code),
                "controlled_span": util.controlled_span_from_span(util.span_from_usgs_code(code)),
                "formation": unit["properties"]["Formation"],
                "title": f"{unit['properties']['Formation']}: {unit['properties']['RockType']}",
                "description": f"{unit['properties']['Com']}. {unit['properties']['Lithology']}"
            }
    outfile_path = "units.csv"
    with open(outfile_path, 'w', encoding="utf-8") as outfile:
        columns = util.METADATA_COLUMN_NAMES
        writer = csv.DictWriter(
            outfile,
            fieldnames=columns,
            extrasaction='ignore'
        )
        writer.writeheader()
        codes_encoutered = {}
        with open(attributes_path, encoding="utf-8") as attributes_file:
            reader = csv.DictReader(attributes_file)
            for row in reader:
                row["code"] = row["MapUnit"]
                codes_encoutered[row["code"]] = True
                shp_unit = units_from_shp.get(row["code"], {})
                row["title"] = row["FullName"]
                row["description"] = row["Descr"]
                if not row["description"] or len(row["description"]) == 0:
                    row["description"] = shp_unit.get("description")
                row["lithology"] = shp_unit.get("lithology")
                if not row["lithology"]:
                    row["lithology"] = util.lithology_from_text(
                        row["FullName"]
                    )
                if not row["lithology"]:
                    row["lithology"] = util.lithology_from_text(
                        row["Descr"]
                    )
                row["rock_type"] = util.rock_type_from_lithology(
                    row["lithology"]
                )
                row["span"] = row["Age"]
                row["controlled_span"] = util.controlled_span_from_span(
                    row["span"]
                )
                ages_from_span = util.ages_from_span(row["span"])
                row["min_age"] = shp_unit.get("min_age") or ages_from_span[0]
                row["max_age"] = shp_unit.get("max_age") or ages_from_span[1]
                row["est_age"] = shp_unit.get("est_age") or ages_from_span[2]
                writer.writerow(row)
        for code, unit in units_from_shp.items():
            if code not in codes_encoutered:
                writer.writerow(unit)
    return os.path.realpath(outfile_path)


def copy_citation():
    """Write citation.json"""
    now = time.localtime()
    data = [
      {
        "id": "https://doi.org/10.5066/P9YWXT41",
        "type": "GIS database",
        "title": "Geologic map database to accompany geologic map of the State of Hawaii: U.S. Geological Survey data release",  # pylint: disable=line-too-long
        "container-title": "United States Geological Survey",
        "URL": "https://www.sciencebase.gov/catalog/item/60df56d5d34ed15aa3b8a39c",
        "language": "English",
        "author": [
          {
            "family": "Sherrod",
            "given": "D.R."
          },
          {
            "family": "Robinson",
            "given": "J.E."
          },
          {
            "family": "Sinton",
            "given": "J.M."
          },
          {
            "family": "Brunt",
            "given": "K.M."
          }
        ],
        "issued": {
          "date-parts": [
            [
              "2021"
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
    with open(os.path.join(work_path, "citation.json"), 'w', encoding="utf-8") as outfile:
        json.dump(data, outfile)


DIR_PATH = fetch(
    "https://www.sciencebase.gov/catalog/file/get/60df56d5d34ed15aa3b8a39c?f=__disk__68%2Fb9%2F0f%2F68b90f223e4d4bbcd04c7b725b5757a2e1cb46bb"  # pylint: disable=line-too-long
)
PROJECTED_PATH = reproject(DIR_PATH)
SCHEMIFIED_ATTRIBUTES_PATH = schemify_attributes(
    os.path.join(DIR_PATH, "DescriptionOfMapUnits.csv")
)
util.join_polygons_and_metadata(
    PROJECTED_PATH,
    SCHEMIFIED_ATTRIBUTES_PATH,
    polygons_join_col="MapUnit",
    polygons_table_name=TABLE_NAME,
    output_path="units.geojson"
)

copy_citation()
