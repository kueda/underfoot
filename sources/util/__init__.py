"""
  Colleciton of methods and scripts for processing geologic unit data, mostly
  from the USGS.
"""

from subprocess import call, run, Popen, PIPE
import os
import re
import shutil
import glob

WEB_MERCATOR_PROJ4="+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +over +no_defs"

def extless_basename(path):
  if os.path.isfile(path):
    return os.path.splitext(os.path.basename(path))[0]
  return os.path.split(path)[-1]

def call_cmd(*args, **kwargs):
  # print("Calling `{}` with kwargs: {}".format(args[0], kwargs))
  run(*args, **kwargs)

def run_sql(sql, dbname="underfoot"):
  call_cmd([
    "psql", dbname, "-c", sql
  ])

def make_work_dir(path):
  work_path = os.path.join(os.path.dirname(os.path.realpath(path)), "work-{}".format(extless_basename(path)))
  if not os.path.isdir(work_path):
    os.makedirs(work_path)
  return work_path

def extract_e00(path):
  dirpath = os.path.join(os.path.realpath(os.path.dirname(path)), "{}-shapefiles".format(extless_basename(path)))
  if os.path.isfile(os.path.join(dirpath, "PAL.shp")):
    return dirpath
  shutil.rmtree(dirpath, ignore_errors=True)
  os.makedirs(dirpath)
  call(["ogr2ogr", "-f", "ESRI Shapefile", dirpath, path])
  return dirpath

def polygonize_arcs(shapefiles_path, polygon_pattern=".+-ID?$", force=False):
  """Convert shapefile arcs from an extracted ArcINFO coverage and convert them to polygons.

  More often than not ArcINFO coverages seem to include arcs but not polygons
  when converted to shapefiles using ogr. A PAL.shp file gets created, but it
  only has polygon IDs, not geometries. This method will walk through all the arcs and combine them into their relevant polygons.

  Why is it in its own python script and not in this library? For reasons as
  mysterious as they are maddening, when you import fiona into this module, it
  causes subproccess calls to ogr2ogr to ignore the "+nadgrids=@null" proj
  flag, which results in datum shifts when attempting to project to web
  mercator. Yes, seriously. I have no idea why or how it does this, but that
  was the only explanation I could find for the datum shift.
  """
  polygons_path = os.path.join(shapefiles_path, "polygons.shp")
  if force:
    shutil.rmtree(polygons_path, ignore_errors=True)
  elif os.path.isfile(polygons_path):
    return polygons_path
  pal_path = os.path.join(shapefiles_path, "PAL.shp")
  arc_path = os.path.join(shapefiles_path, "ARC.shp")
  polygonize_arcs_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "polygonize_arcs.py")
  call_cmd(["python", polygonize_arcs_path, polygons_path, pal_path, arc_path, "--polygon-column-pattern", polygon_pattern])
  return polygons_path
