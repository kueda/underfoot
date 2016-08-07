import util
import os
import glob
import subprocess

work_path = util.make_work_dir(__file__)
os.chdir(work_path)

# download the file if necessary
if not os.path.isfile("sfs_data.tar.gz"):
  url = "http://pubs.usgs.gov/of/1998/of98-354/sfs_data.tar.gz"
  print("Downloading {}\n".format(url))
  util.call_cmd(["curl", "-OL", url])

# extract the archive if necessary
if not os.path.isfile("sfs-geol.e00"):
  print("\nExtracting archive...\n")
  util.call_cmd(["tar", "xzvf", "sfs_data.tar.gz"])

# uncompress the e00 itself
if not os.path.isfile("sfs-geol-uncompressed.e00"):
  util.call_cmd(["../../bin/e00compr/e00conv", "sfs-geol.e00", "sfs-geol-uncompressed.e00"])

# convert the Arc Info coverages to shapefiles
polygons_path = "sfs-geol-uncompressed-shapefiles/polygons.shp"
if not os.path.isfile(polygons_path):
  print("\nConverting e00 to shapefiles...\n")
  shapefiles_path = util.extract_e00("sfs-geol-uncompressed.e00")
  polygons_path = util.polygonize_arcs(shapefiles_path)

# dissolve all the shapes by PTYPE and project them into Google Mercator
print("\nDissolving shapes and reprojecting...\n")
util.call_cmd([
  "ogr2ogr",
    "-s_srs", "+proj=lcc +lat_1=37.066667 +lat_2=38.433333 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.21920 +y_0=-6 +datum=NAD27 +units=m +no_defs",
    "-t_srs", util.WEB_MERCATOR_PROJ4,
    "units.shp", polygons_path,
    "-overwrite",
    "-dialect", "sqlite",
    "-sql", "SELECT PTYPE,ST_Union(geometry) as geometry FROM 'polygons' GROUP BY PTYPE"
])

# TODO generate  units.csv... or load that into the db?
