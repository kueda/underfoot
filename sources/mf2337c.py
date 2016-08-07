import util
import os
import glob
import subprocess

work_path = util.make_work_dir(__file__)
os.chdir(work_path)

# download the file if necessary
if not os.path.isfile("mf2337c.tgz"):
  url = "http://pubs.usgs.gov/mf/2000/2337/mf2337c.tgz"
  print("Downloading {}\n".format(url))
  util.call_cmd(["curl", "-OL", url])

# extract the archive if necessary
if not os.path.isdir("mageo"):
  print("\nExtracting archive...\n")
  util.call_cmd(["tar", "xzvf", "mf2337c.tgz"])

# convert the Arc Info coverages to shapefiles
polygons_path = "mageo/ma-geol-shapefiles/polygons.shp"
if not os.path.isfile(polygons_path):
  print("\nConverting e00 to shapefiles...\n")
  shapefiles_path = util.extract_e00("mageo/ma-geol.e00")
  polygons_path = util.polygonize_arcs(shapefiles_path)

# dissolve all the shapes by PTYPE and project them into Google Mercator
print("\nDissolving shapes and reprojecting...\n")
util.call_cmd([
  "ogr2ogr",
    "-s_srs", "+proj=lcc +lat_1=37.06666666666 +lat_2=38.43333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=90 +y_0=10 +ellps=clrk66 +units=m +no_defs",
    "-t_srs", util.WEB_MERCATOR_PROJ4,
    "units.shp", polygons_path,
    "-overwrite",
    "-dialect", "sqlite",
    "-sql", "SELECT PTYPE,ST_Union(geometry) as geometry FROM 'polygons' GROUP BY PTYPE"
])

# TODO generate  units.csv... or load that into the db?
