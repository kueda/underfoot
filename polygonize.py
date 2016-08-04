from __future__ import print_function
from pprint import pprint
from shapely.geometry import shape, mapping
from shapely.ops import polygonize
from subprocess import call
from sys import argv
import fiona
import os
import re
import shapely
import shutil
import argparse

parser = argparse.ArgumentParser(description='Create usable data from metadata XML file')
parser.add_argument('input_path', help="Path to metadata XML file")
parser.add_argument('polygon_column',
  help="Column specifying the ID of the polygons in PAL.shp")
parser.add_argument('--debug', '-d', 
  help="Pretty print results instead of printing CSV", 
  action="store_true")
args = parser.parse_args()
  
def extract_e00(path):
  call(["ogr2ogr", "-f", "ESRI Shapefile", shp_dir(path), path])
def shp_dir(path):
  return "work-{}".format(extless_basename(path))
def make_polygons(path):
  schema = { 
    'geometry': 'Polygon', 
    'properties': { 
      'POLY_ID': 'str',
      'PTYPE': 'str'
    }
  }
  polygons_path = os.path.join(shp_dir(path), "{}-polygons.shp".format(extless_basename(path)))
  pal_path = os.path.join(shp_dir(path), "PAL.shp")
  arc_path = os.path.join(shp_dir(path), "ARC.shp")
  with fiona.collection(polygons_path, "w", "ESRI Shapefile", schema) as output:
    with fiona.open(pal_path) as pal:
      with fiona.open(arc_path) as arc:
        for pf in pal:
          if args.debug:
            print('.', end="")
          lines = []
          polygon_id = pf['properties'][args.polygon_column]
          ptype = pf['properties']['PTYPE']
          for af in arc:
            if (af['properties']['LPOLY_'] == polygon_id or af['properties']['RPOLY_'] == polygon_id):
              lines.append(shape(af['geometry']))
          for polygon in polygonize(lines):
            output.write({
              'properties': {
                'POLY_ID': polygon_id,
                'PTYPE': ptype
              },
              'geometry': mapping(polygon)
            })
  return polygons_path
def cleanup(path):
  shutil.rmtree(shp_dir(path))
def extless_basename(path):
  return os.path.splitext(os.path.basename(path))[0]

if __name__ == '__main__':
  print("Working on {}...".format(args.input_path))
  extract_e00(args.input_path)
  print("Making polygons for {}...".format(args.input_path))
  polygons_path = make_polygons(args.input_path)
  # print "Cleaning up work files for for %s..." % args.input_path
  # cleanup(args.input_path)
  output_path = "{}-polygons-by-ptype.shp".format(extless_basename(args.input_path))
  print("Dissolving polgyons...")
  call([
    "ogr2ogr", output_path, polygons_path, "-dialect", "sqlite", "-sql", 
    "SELECT PTYPE,ST_Union(geometry) as geometry FROM '{}' GROUP BY PTYPE".format(extless_basename(polygons_path))
  ])
  print("You should now have a nice set of polys at {}".format(output_path))

