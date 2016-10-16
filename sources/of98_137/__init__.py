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
  if not os.path.isfile("sm_g1.tar.gz"):
    url = "http://pubs.usgs.gov/of/1998/of98-137/sm_g1.tar.gz"
    print("Downloading {}\n".format(url))
    util.call_cmd(["curl", "-OL", url])

  # extract the archive if necessary
  if not os.path.isdir("smgeo"):
    print("\nExtracting archive...\n")
    util.call_cmd(["tar", "xzvf", "sm_g1.tar.gz"])

  # convert the Arc Info coverages to shapefiles
  print("CONVERTING E00 TO SHAPEFILES...")
  shapefiles_path = util.extract_e00("smgeo/sm_um-py.e00")
  polygons_path = util.polygonize_arcs(shapefiles_path, polygon_pattern="SM3_UM-PY")

  # dissolve all the shapes by PTYPE and project them into Google Mercator
  print("DISSOLVING SHAPES AND REPROJECTING...")
  final_polygons_path = "final_polygons.shp"
  util.call_cmd([
    "ogr2ogr",
      "-s_srs", util.NAD27_UTM10_PROJ4,
      "-t_srs", util.WEB_MERCATOR_PROJ4,
      final_polygons_path, polygons_path,
      "-overwrite",
      "-dialect", "sqlite",
      "-sql", "SELECT PTYPE,ST_Union(geometry) AS geometry FROM 'polygons' GROUP BY PTYPE"
  ])

  print("EXTRACTING METADATA...")
  metadata_path = "data.csv"
  with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "units.csv")) as infile:
    reader = csv.DictReader(infile)
    with open(metadata_path, 'w') as outfile:
      writer = csv.DictWriter(outfile, fieldnames=util.METADATA_COLUMN_NAMES, extrasaction='ignore')
      writer.writeheader()
      for row in reader:
        # print(row)
        row['span'] = util.span_from_text(row['title'])
        row['lithology'] = util.lithology_from_text(row['title'])
        row['formation'] = util.formation_from_text(row['title'])
        if row['lithology']:
          row['rock_type'] = util.rock_type_from_lithology(row['lithology'])
        if row['span']:
          min_age, max_age, est_age = util.ages_from_span(row['span'])
          row['min_age'] = min_age
          row['max_age'] = max_age
          row['est_age'] = est_age
        writer.writerow(row)
  print("JOINING METADATA...")
  util.join_polygons_and_metadata(final_polygons_path, metadata_path)
