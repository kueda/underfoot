import util
import os
import glob
import subprocess

work_path = util.make_work_dir(__file__)
os.chdir(work_path)

# download the file if necessary
if not os.path.isfile("mf2403c.tgz"):
  url = "http://pubs.usgs.gov/mf/2002/2403/mf2403c.tgz"
  print("Downloading {}\n".format(url))
  util.call_cmd(["curl", "-OL", url])

# extract the archive if necessary
if not os.path.isdir("nesfgeo"):
  print("\nExtracting archive...\n")
  util.call_cmd(["tar", "xzvf", "mf2403c.tgz"])

# convert the Arc Info coverages to shapefiles
print("\nConverting e00 to shapefiles...\n")
polygon_paths = []
for path in glob.glob("nesfgeo/*-geol.e00"):
  print("\tExtracting e00...")
  shapefiles_path = util.extract_e00(path)
  print("\tPolygonizing arcs...")
  polygon_paths.append(util.polygonize_arcs(shapefiles_path, polygon_pattern="GEOL"))

# merge all the shapefiles into a single file
print("\nMerging shapefiles...\n")
merged_polygons_path = "merged-polygons.shp"
util.call_cmd(["ogr2ogr", "-overwrite", merged_polygons_path, polygon_paths.pop()])
for path in polygon_paths:
  util.call_cmd(["ogr2ogr", "-update", "-append", merged_polygons_path, path])

# dissolve all the shapes by PTYPE and project them into Google Mercator
print("\nDissolving shapes and reprojecting...\n")
util.call_cmd([
  "ogr2ogr",
    "-s_srs", "+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs",
    "-t_srs", util.WEB_MERCATOR_PROJ4,
    "units.shp", merged_polygons_path,
    "-overwrite",
    "-dialect", "sqlite",
    "-sql", "SELECT PTYPE,ST_Union(geometry) as geometry FROM 'merged-polygons' GROUP BY PTYPE"
])

# TODO generate  units.csv... or load that into the db?
