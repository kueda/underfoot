import util
import os
import glob
import subprocess
import csv

def run():
  work_path = util.make_work_dir(__file__)
  os.chdir(work_path)

  # download the file if necessary
  if not os.path.isfile("mf2337c.tgz"):
    url = "http://pubs.usgs.gov/mf/2000/2337/mf2337c.tgz"
    print("DOWNLOADING {}".format(url))
    util.call_cmd(["curl", "-OL", url])

  # extract the archive if necessary
  if not os.path.isdir("mageo"):
    print("EXTRACTING ARCHIVE...")
    util.call_cmd(["tar", "xzvf", "mf2337c.tgz"])

  # convert the Arc Info coverages to shapefiles
  polygons_path = "mageo/ma-geol-shapefiles/polygons.shp"
  if not os.path.isfile(polygons_path):
    print("CONVERTING E00 TO SHAPEFILES...")
    shapefiles_path = util.extract_e00("mageo/ma-geol.e00")
    polygons_path = util.polygonize_arcs(shapefiles_path)

  # dissolve all the shapes by PTYPE and project them into Google Mercator
  print("DISSOLVING SHAPES AND REPROJECTING...")
  final_polygons_path = "polygons.shp"
  util.call_cmd([
    "ogr2ogr",
      "-s_srs", "+proj=lcc +lat_1=37.06666666666 +lat_2=38.43333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=90 +y_0=10 +ellps=clrk66 +units=m +no_defs",
      "-t_srs", util.WEB_MERCATOR_PROJ4,
      final_polygons_path, polygons_path,
      "-overwrite",
      "-dialect", "sqlite",
      "-sql", "SELECT PTYPE,ST_Union(geometry) as geometry FROM 'polygons' GROUP BY PTYPE"
  ])

  print("EXTRACTING METADATA...")
  metadata_path = "data.csv"
  data = util.metadata_from_usgs_met("mageo/mf2337d.met")
  # TODO custom processing on this array
  with open(metadata_path, 'w') as f:
    csv.writer(f).writerows(data)

  print("JOINING METADATA...")
  util.join_polygons_and_metadata(final_polygons_path, metadata_path)
