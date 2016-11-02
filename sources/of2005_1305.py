import util
import os
import csv
import pandas as pd
import json

work_path = util.make_work_dir(os.path.realpath(__file__))
os.chdir(work_path)
srs = "+proj=longlat +datum=NAD27 +no_defs"

def download_shapes(state):
  print("DOWNLOADING SHAPEFILES FOR {}...".format(state))
  url = "http://pubs.usgs.gov/of/2005/1305/data/{}geol_dd.zip".format(state)
  download_path = os.path.basename(url)
  if not os.path.isfile(download_path):
    print("DOWNLOADING {}".format(url))
    util.call_cmd(["curl", "-OL", url])
  shp_path = "{}geol_poly_dd.shp".format(state.lower())
  if not os.path.isfile(shp_path):
    print("EXTRACTING ARCHIVE...")
    util.call_cmd(["tar", "xzvf", download_path])
  return os.path.realpath(shp_path)

def download_attributes(state):
  print("DOWNLOADING ATTRIBUTES FOR {}...".format(state))
  url = "http://pubs.usgs.gov/of/2005/1305/data/{}csv.zip".format(state)
  download_path = os.path.basename(url)
  if not os.path.isfile(download_path):
    print("DOWNLOADING {}".format(url))
    util.call_cmd(["curl", "-OL", url])
  csv_path = "{}units.csv".format(state)
  if not os.path.isfile(csv_path):
    print("EXTRACTING ARCHIVE...")
    util.call_cmd(["tar", "xzvf", download_path])
  return os.path.realpath(csv_path)

def schemify_attributes(attributes_path):
  print("SCHEMIFYING ATTRIBUTES for {}...".format(attributes_path))
  outfile_path = "units.csv"
  with open(attributes_path) as f:
    reader = csv.DictReader(f)
    with open(outfile_path, 'w') as outfile:
      columns = util.METADATA_COLUMN_NAMES + ['UNIT_LINK']
      writer = csv.DictWriter(outfile, fieldnames=columns, extrasaction='ignore')
      writer.writeheader()
      for row in reader:
        row['code'] = row['ORIG_LABEL']
        row['title'] = row['UNIT_NAME']
        row['description'] = ". ".join([row['UNITDESC'], row['UNIT_COM']])
        row['lithology'] = row['ROCKTYPE1']
        row['rock_type'] = util.rock_type_from_lithology(row['ROCKTYPE1'])
        row['span'] = row['UNIT_AGE']
        row['min_age'], row['max_age'], row['est_age'] = util.ages_from_span(row['UNIT_AGE'])
        writer.writerow(row)
  return os.path.realpath(outfile_path)

def merge_shapes(paths):
  print("MERGING SHAPEFILES...")
  merged_path = "merged_units.shp"
  util.call_cmd(["ogr2ogr", "-overwrite", merged_path, paths.pop()])
  for path in paths:
    util.call_cmd(["ogr2ogr", "-update", "-append", merged_path, path])
  print("DISSOLVING SHAPES AND REPROJECTING...")
  dissolved_path = "dissolved_units.shp"
  util.call_cmd([
    "ogr2ogr",
      "-s_srs", srs,
      "-t_srs", util.WEB_MERCATOR_PROJ4,
      dissolved_path, merged_path,
      "-overwrite",
      "-dialect", "sqlite",
      "-sql", "SELECT UNIT_LINK, ST_Union(geometry) as geometry FROM 'merged_units' GROUP BY UNIT_LINK"
  ])
  return dissolved_path

def merge_attributes(paths):
  print("MERGING SHAPEFILES...")
  merged_path = "merged_units.csv"
  merged = pd.concat([pd.read_csv(path) for path in paths])
  merged.to_csv(merged_path)
  return merged_path

def copy_citation():
  data = [
    {
      "id": "http://zotero.org/users/2632539/items/4CWCNIFE",
      "type": "webpage",
      "title": "Preliminary Integrated Geologic Map Databases of the United States: The Western States: California, Nevada, Arizona, Washington, Idaho, Utah (OFR 2005-1305)",
      "container-title": "United States Geological Survey",
      "URL": "http://pubs.usgs.gov/of/2005/1305/",
      "note": "Version 1.3",
      "language": "English",
      "author": [
        {
          "family": "Ludington",
          "given": "Steve"
        },
        {
          "family": "Moring",
          "given": "Barry C."
        },
        {
          "family": "Miller",
          "given": "Robert J."
        },
        {
          "family": "Stone",
          "given": "Paul A."
        },
        {
          "family": "Bookstrom",
          "given": "Arthur A."
        },
        {
          "family": "Bedford",
          "given": "David R."
        },
        {
          "family": "Evans",
          "given": "James G."
        },
        {
          "family": "Haxel",
          "given": "Gordon A."
        },
        {
          "family": "Nutt",
          "given": "Constance J."
        },
        {
          "family": "Flyn",
          "given": "Kathryn S."
        },
        {
          "family": "Hopkins",
          "given": "Melanie J."
        }
      ],
      "issued": {
        "date-parts": [
          [
            "2005"
          ]
        ]
      },
      "accessed": {
        "date-parts": [
          [
            "2016",
            10,
            27
          ]
        ]
      }
    }
  ]
  with open(os.path.join(work_path, "citation.json"), 'w') as outfile:
    json.dump(data, outfile)

states = ['CA']
shape_paths = [download_shapes(state) for state in states]
single_shapefile_path = merge_shapes(shape_paths)
attribute_paths = [download_attributes(state) for state in states]
single_attributes_path = merge_attributes(attribute_paths)
schemified_attributes_path = schemify_attributes(single_attributes_path)
joined_path = util.join_polygons_and_metadata(
  single_shapefile_path,
  schemified_attributes_path,
  polygons_join_col="UNIT_LINK",
  metadata_join_col="UNIT_LINK",
  output_path="units.geojson")
copy_citation()
