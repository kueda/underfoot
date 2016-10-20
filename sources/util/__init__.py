"""
  Colleciton of methods and scripts for processing geologic unit data, mostly
  from the USGS.
"""

from subprocess import call, run, Popen, PIPE
import os
import re
import shutil
import glob
import xml.etree.ElementTree as ET
from collections import OrderedDict
import csv

WEB_MERCATOR_PROJ4 = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +over +no_defs"
NAD27_UTM10_PROJ4 = "+proj=utm +zone=10 +datum=NAD27 +units=m +no_defs"
METADATA_COLUMN_NAMES = [
  'code',
  'title',
  'description',
  'lithology',
  'rock_type',
  'formation',
  'grouping',
  'span',
  'min_age',
  'max_age',
  'est_age'
]

lithology_PATTERN = re.compile(r'''(
  basalt|
  chert|
  conglomerate|
  diabase|
  gabbro|
  granite|
  greenstone|
  keratophyre|
  limestone|
  mudstone|
  quartz\sarenite|
  quartz\sdiorite|
  quartz\skeratophyre|
  sandstone|
  sand|
  schist|
  serpentinite|
  shale|
  siltstone|
  tuff|
  volcanoclastic\sbreccia
)(?:\W|$)''', re.VERBOSE|re.I)

IGNEUS_ROCKS = [
  'basalt',
  'diabase',
  'gabbro',
  'granite',
  'keratophyre',
  'quartz diorite',
  'quartz keratophyre',
  'tuff',
  'volcanoclastic breccia'
]

METAMORPHIC_ROCKS = [
  'greenstone',
  'schist',
  'serpentinite'
]

SEDIMENTARY_ROCKS = [
  'chert',
  'conglomerate',
  'limestone',
  'mudstone',
  'quartz arenite',
  'sandstone',
  'schist',
  'shale',
  'siltstone'
]

GROUP_PATTERN = re.compile(r'(Franciscan complex|Great Valley Sequence)')
FORMATION_PATTERN = re.compile(r'(Franciscan complex|([A-Z]\w+ )+[A-Z]\w+)')

spans = {
  'precambrian': [4600e6, 570e6],
  'paleozoic': [570e6, 245e6],
  'mesozoic': [245e6, 65e6],
    'triassic': [252.17e6, 201.3e6],  # https://en.wikipedia.org/wiki/Triassic, accessed 2016-09-17
    'jurassic': [201.3e6, 145e6],     # https://en.wikipedia.org/wiki/Jurassic, accessed 2016-09-17
    'cretaceous': [145e6, 65e6],
  'cenozoic': [65e6, 0],
    'paleocene': [66e6, 56e6],
    'eocene': [56e6, 33.9e6],
    'oligocene': [35.4e6, 23.3e6],
    'miocene': [23.03e6, 5.332e6],
    'pliocene': [5.333e6, 2.58e6],
    'pleistocene': [2.588e6, 11700],
    'holocene': [11700, 0]
}
split_spans = {}
for span, dates in spans.items():
  third = ((dates[0] - dates[1]) / 3.0)
  split_spans['late {}'.format(span).lower()] = [
    dates[1] + third,
    dates[1]
  ]
  split_spans['upper {}'.format(span).lower()] = [
    dates[1] + third,
    dates[1]
  ]
  split_spans['early %s'.format(span).lower()] = [
    dates[0],
    dates[0] - third,
  ]
  split_spans['lower %s'.format(span).lower()] = [
    dates[0],
    dates[0] - third,
  ]
  split_spans['middle %s'.format(span).lower()] = [
    dates[0] - third,
    dates[1] + third
  ]
SPANS = {**spans, **split_spans}
SPAN_PATTERN = re.compile(r'('+('|').join(SPANS.keys())+')', re.I)

def extless_basename(path):
  if os.path.isfile(path):
    return os.path.splitext(os.path.basename(path))[0]
  return os.path.split(path)[-1]

def call_cmd(*args, **kwargs):
  # print("Calling `{}` with kwargs: {}".format(" ".join(args[0]), kwargs))
  run(*args, **kwargs)

def run_sql(sql, dbname="underfoot"):
  # print("running {}".format(sql))
  call_cmd([
    "psql", dbname, "-c", sql
  ])

def basename_for_path(path):
  basename = extless_basename(path)
  if basename == "__init__":
    dirpath = os.path.dirname(os.path.realpath(path))
    basename = extless_basename(dirpath)
  return basename

def make_work_dir(path):
  dirpath = os.path.dirname(os.path.realpath(__file__))
  basename = basename_for_path(path)
  work_path = os.path.join(dirpath, "..", "work-{}".format(basename))
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

def met2xml(path):
  output_path = os.path.join(os.path.realpath(os.path.dirname(path)), "{}.xml".format(extless_basename(path)))
  met2xml_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "met2xml.py")
  call_cmd(["python", met2xml_path, path, output_path])
  return output_path

def lithology_from_text(text):
  if not text:
    return
  lithology_matches = lithology_PATTERN.search(text)
  return (lithology_matches.group(1) if lithology_matches else '').lower()

def formation_from_text(text):
  formation_matches = FORMATION_PATTERN.search(text) # basically any proper nouns
  return formation_matches.group(1) if formation_matches else ''

def rock_type_from_lithology(lithology):
  rock_type = ''
  if lithology in IGNEUS_ROCKS:
    rock_type = 'igneus'
  elif lithology in METAMORPHIC_ROCKS:
    rock_type = 'metamorphic'
  elif lithology in SEDIMENTARY_ROCKS:
    rock_type = 'sedimentary'
  return rock_type

def span_from_text(text):
  span_matches = SPAN_PATTERN.search(text)
  return span_matches.group(1).lower() if span_matches else ''

def ages_from_span(span):
  min_age = None
  max_age = None
  est_age = None
  if span and len(span) > 0:
    span = span.lower()
    ages = SPANS.get(span)
    if ages:
      max_age = ages[0]
      min_age = ages[1]
    else:
      matches = re.findall(r'(\w+)\s+?(to|\-)\s+?(\w+)', span)
      if len(matches) > 0:
        min_ages = SPANS.get(matches[0][0])
        max_ages = SPANS.get(matches[0][2])
        if min_ages:
          min_age = min_ages[0]
        if max_ages:
          max_age = max_ages[0]
  if min_age and max_age:
    est_age = int((min_age + max_age) / 2.0)
  return (min_age, max_age, est_age)

def metadata_from_usgs_met(path):
  """Parse metadata from a USGS met file

  USGS geologic databases seem to come with a somewhat machine-readable
  document with a .met extension that contains information about the
  geological units in the database. This method tries to convert that document
  into an array of arrays suitable for use in underfoot. It's not going to do
  a perfect job for every such document, but it will get you part of the way
  there.
  """
  data = [[col for col in METADATA_COLUMN_NAMES]]
  xml_path = met2xml(path)
  tree = ET.parse(xml_path)
  for ed in tree.iterfind('.//Attribute[Attribute_Label="PTYPE"]//Enumerated_Domain'):
    row = dict([[col, None] for col in METADATA_COLUMN_NAMES])
    edv = ed.find('Enumerated_Domain_Value')
    edvd = ed.find('Enumerated_Domain_Value_Definition')
    if edv == None or edvd == None:
      continue
    row['code'] = edv.text
    row['title'] = edvd.text
    if row['code'] == None or row['title'] == None:
      continue
    row['title'] = re.sub(r"\n", " ", row['title'])
    row['lithology'] = lithology_from_text(row['title'])
    row['formation'] = formation_from_text(row['title'])
    row['rock_type'] = rock_type_from_lithology(row['lithology'])
    row['span'] = span_from_text(row['title'])
    row['min_age'], row['max_age'], row['est_age'] = ages_from_span(row['span'])
    data.append([row[col] for col in METADATA_COLUMN_NAMES])
  return data

def join_polygons_and_metadata(polygons_path, metadata_path, output_path="units.geojson"):
  # def join_polygons_and_metadata(polygons_path, metadata_path, output_path="units.shp"):
  polygons_table_name = extless_basename(polygons_path)
  column_names = [col for col in METADATA_COLUMN_NAMES]
  column_names[column_names.index('code')] = "PTYPE AS code"
  sql = """
    SELECT
      {}
    FROM {}
      LEFT JOIN '{}'.data ON {}.PTYPE = data.code
  """.format(", ".join(column_names), polygons_table_name, metadata_path, polygons_table_name)
  call_cmd(["rm", output_path])
  call_cmd([
    "ogr2ogr",
    "-sql", sql.replace("\n", " "),
    "-f", "GeoJSON",
    output_path,
    polygons_path
  ])
  return output_path

def infer_metadata_from_csv(infile_path):
  outfile_path = "data.csv"
  with open(infile_path) as infile:
    reader = csv.DictReader(infile)
    with open(outfile_path, 'w') as outfile:
      writer = csv.DictWriter(outfile, fieldnames=METADATA_COLUMN_NAMES, extrasaction='ignore')
      writer.writeheader()
      for row in reader:
        row['span'] = span_from_text(row['title'])
        row['lithology'] = lithology_from_text(row['title'])
        if not row['lithology'] or len(row['lithology']) == 0:
          row['lithology'] = lithology_from_text(row['description'])
        row['formation'] = formation_from_text(row['title'])
        if row['lithology']:
          row['rock_type'] = rock_type_from_lithology(row['lithology'])
        if row['span']:
          min_age, max_age, est_age = ages_from_span(row['span'])
          row['min_age'] = min_age
          row['max_age'] = max_age
          row['est_age'] = est_age
        writer.writerow(row)
  return outfile_path

