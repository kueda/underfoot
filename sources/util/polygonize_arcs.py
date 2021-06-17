from shapely.geometry import shape, mapping
from shapely.ops import polygonize
import fiona
import re
import argparse

parser = argparse.ArgumentParser(
  description='Convert Arc INFO arcs into polgyons')
parser.add_argument('output_path', help="Path to output file")
parser.add_argument('pal_path', help="Path to PAL.shp")
parser.add_argument('arc_path', help="Path to ARC.shp")
parser.add_argument(
    '--polygon-column-pattern',
    default=".+_SP-PY-I$",
    help="Column specifying the ID of the polygons in PAL.shp")
parser.add_argument(
    '--debug', '-d', 
    help="Pretty print results instead of printing CSV", 
    action="store_true")
args = parser.parse_args()
polygon_pattern = re.compile(args.polygon_column_pattern)
schema = { 
  'geometry': 'Polygon', 
  'properties': { 
    'POLY_ID': 'str',
    'PTYPE': 'str'
  }
}

with fiona.collection(args.output_path, "w", "ESRI Shapefile", schema) as output:  # noqa: E501
    with fiona.open(args.pal_path) as pal:
        with fiona.open(args.arc_path) as arc:
            for pf in pal:
                print(".", end="")
                lines = []
                polygon_column = next(
                    x for x in pf["properties"].keys()
                    if re.search(polygon_pattern, x)
                )
                polygon_id = pf["properties"][polygon_column]
                ptype = pf["properties"]["PTYPE"]
                if args.debug:
                    print("polygon_column: ", polygon_column)
                    print("polygon_id: ", polygon_id)
                    print("ptype: ", ptype)
                for af in arc:
                    if (
                        af["properties"]["LPOLY_"] == polygon_id
                        or af["properties"]["RPOLY_"] == polygon_id
                    ):
                        if args.debug:
                            print("adding line for polygon ", polygon_id)
                        lines.append(shape(af['geometry']))
                for polygon in polygonize(lines):
                    output.write({
                        'properties': {
                          'POLY_ID': polygon_id,
                          'PTYPE': ptype
                        },
                        'geometry': mapping(polygon)
                    })
