import util
import os
import glob
import subprocess
import csv
import sys

def run():
  debug = False
  work_path = util.make_work_dir(__file__)
  os.chdir(work_path)

  # download the file if necessary
  if not os.path.isfile("mf2342c.tgz"):
    url = "http://pubs.usgs.gov/mf/2000/2342/mf2342c.tgz"
    print("Downloading {}\n".format(url))
    util.call_cmd(["curl", "-OL", url])

  # extract the archive if necessary
  if not os.path.isdir("oakdb"):
    print("\nExtracting archive...\n")
    util.call_cmd(["tar", "xzvf", "mf2342c.tgz"])

  # convert the Arc Info coverages to shapefiles
  print("CONVERTING E00 TO SHAPEFILES...")
  polygon_paths = []
  for path in glob.glob("oakdb/*-geol.e00"):
    shapefiles_path = util.extract_e00(path)
    polygon_paths.append(util.polygonize_arcs(shapefiles_path))

  # merge all the shapefiles into a single file
  print("MERGING SHAPEFILES...")
  merged_polygons_path = "merged-polygons.shp"
  util.call_cmd(["ogr2ogr", "-overwrite", merged_polygons_path, polygon_paths.pop()])
  for path in polygon_paths:
    util.call_cmd(["ogr2ogr", "-update", "-append", merged_polygons_path, path])

  # dissolve all the shapes by PTYPE and project them into Google Mercator
  print("DISSOLVING SHAPES AND REPROJECTING...")
  final_polygons_path = "polygons.shp"
  util.call_cmd([
    "ogr2ogr",
      "-s_srs", "+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs",
      "-t_srs", util.WEB_MERCATOR_PROJ4,
      final_polygons_path, merged_polygons_path,
      "-overwrite",
      "-dialect", "sqlite",
      "-sql", "SELECT PTYPE,ST_Union(geometry) AS geometry FROM 'merged-polygons' GROUP BY PTYPE"
  ])

  print("EXTRACTING METADATA...")
  metadata_path = "data.csv"
  formation_spans = {
    'briones formation': 'Late Miocene',
    'franciscan complex': 'Late Jurassic to Miocene',
    'knoxville formation': 'Late Jurassic to Early Cretaceous',
    'redwood canyon formation': 'Late Cretaceous',
    'joaquin miller formation': 'Late Cretaceous',
    'oakland formation': 'Late Cretaceous',
    'pinehurst shale': 'Paleocene',
    'shephard creek formation': 'Late Cretaceous'
  }
  data = util.metadata_from_usgs_met("oakdb/mf2342e.met")
  span_i        = util.METADATA_COLUMN_NAMES.index('span')
  formation_i   = util.METADATA_COLUMN_NAMES.index('formation')
  min_age_i     = util.METADATA_COLUMN_NAMES.index('min_age')
  max_age_i     = util.METADATA_COLUMN_NAMES.index('max_age')
  est_age_i     = util.METADATA_COLUMN_NAMES.index('est_age')
  code_i  = util.METADATA_COLUMN_NAMES.index('code')
  title_i  = util.METADATA_COLUMN_NAMES.index('title')
  desc_i  = util.METADATA_COLUMN_NAMES.index('description')
  lithology_i   = util.METADATA_COLUMN_NAMES.index('lithology')
  rock_type_i   = util.METADATA_COLUMN_NAMES.index('rock_type')
  curated_data = {}
  with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "units.csv")) as infile:
    reader = csv.DictReader(infile)
    for row in reader:
      curated_data[row['code']] = row
  for idx, row in enumerate(data):
    if idx == 0:
      continue
    curated_row = curated_data.get(row[code_i], None)
    if curated_row:
      data[idx][title_i] = curated_row['title']
      data[idx][desc_i] = curated_row['description']
    span = formation_spans.get(
      row[formation_i].lower(),
      util.span_from_text(data[idx][title_i])
    )
    if span:
      min_age, max_age, est_age = util.ages_from_span(span)
      data[idx][span_i] = span
      data[idx][min_age_i] = min_age
      data[idx][max_age_i] = max_age
      data[idx][est_age_i] = est_age
    data[idx][lithology_i] = util.lithology_from_text(data[idx][title_i])
    if not data[idx][lithology_i]:
      data[idx][lithology_i] = util.lithology_from_text(data[idx][desc_i])
    data[idx][rock_type_i] = util.rock_type_from_lithology(data[idx][lithology_i])
  with open(metadata_path, 'w') as outfile:
    writer = csv.writer(outfile)
    writer.writerows(data)
    outfile.truncate()

  print("JOINING METADATA...")
  util.join_polygons_and_metadata(final_polygons_path, metadata_path)
