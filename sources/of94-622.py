import util
import os
import glob
import subprocess

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
polygons_path = "ccgeo/cc_utm-shapefiles/polygons.shp"
if not os.path.isfile(polygons_path):
  print("\nConverting e00 to shapefiles...\n")
  shapefiles_path = util.extract_e00("ccgeo/cc_utm")
  polygons_path = util.polygonize_arcs(shapefiles_path)

# This dataset has one unit, Ku, that underlies all the other polygons,
# creating some annoying overlap, so first we need to get all the shapes that
# aren't that...
final_polygons_path = "polygons-separated.shp"
util.call_cmd([
  "ogr2ogr",
    "-overwrite",
    "-where", "POLY_ID != '1'",
    final_polygons_path, polygons_path
])

# ...and then append shapes cut out of the rest
util.call_cmd([
  "ogr2ogr",
    "-append",
    "-dialect", "sqlite",
    "-sql", "SELECT p1.PTYPE, ST_Difference(p1.geometry, ST_Union(p2.geometry)) AS geometry FROM polygons AS p1, polygons AS p2 WHERE p1.POLY_ID = '1' AND p2.POLY_ID != '1'",
     final_polygons_path, polygons_path
])

# dissolve all the shapes by PTYPE and project them into Google Mercator
print("\nDissolving shapes and reprojecting...\n")
util.call_cmd([
  "ogr2ogr",
    "-s_srs", "+proj=utm +zone=10 +datum=NAD27 +units=m +no_defs",
    "-t_srs", util.WEB_MERCATOR_PROJ4,
    "units.shp", final_polygons_path,
    "-overwrite",
    "-dialect", "sqlite",
    "-sql", "SELECT PTYPE,ST_Union(geometry) as geometry FROM 'polygons-separated' GROUP BY PTYPE"
])

# # TODO generate  units.csv... or load that into the db?
