import util
import os
import glob
import subprocess
import re

def run():
  work_path = util.make_work_dir(__file__)
  os.chdir(work_path)

  # download the file if necessary
  if not os.path.isfile("cc_g1.tar.Z"):
    url = "http://pubs.usgs.gov/of/1994/of94-622/cc_g1.tar.Z"
    print("Downloading {}\n".format(url))
    util.call_cmd(["curl", "-OL", url])

  # extract the archive if necessary
  if not os.path.isdir("ccgeo/cc_utm/"):
    print("\nExtracting archive...\n")
    util.call_cmd(["tar", "xzvf", "cc_g1.tar.Z"])

  # convert the Arc Info coverages to shapefiles
  polygons_path = "ccgeo/cc_utm-shapefiles/PAL.shp"
  if not os.path.isfile(polygons_path):
    print("\nConverting e00 to shapefiles...\n")
    shapefiles_path = util.extract_e00("ccgeo/cc_utm")

  # dissolve all the shapes by PTYPE and project them into Google Mercator
  print("\nDissolving shapes and reprojecting...\n")
  final_polygons_path = "polygons.shp"
  util.call_cmd([
    "ogr2ogr",
      "-s_srs", "+proj=utm +zone=10 +datum=NAD27 +units=m +no_defs",
      "-t_srs", util.WEB_MERCATOR_PROJ4,
      final_polygons_path, polygons_path,
      "-overwrite",
      "-dialect", "sqlite",
      "-sql", "SELECT PTYPE,ST_Union(geometry) as geometry FROM 'PAL' GROUP BY PTYPE"
  ])

  print("EXTRACTING METADATA...")
  metadata_path = util.infer_metadata_from_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), "units.csv"))

  print("JOINING METADATA...")
  util.join_polygons_and_metadata(final_polygons_path, metadata_path)

  print("COPYING CITATION")
  util.call_cmd([
    "cp",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "citation.json"),
    os.path.join(work_path, "citation.json")
  ])
