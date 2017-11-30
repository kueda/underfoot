import util
import os

def run():
  util.process_usgs_source(
    base_path=os.path.realpath(__file__),
    url="http://pubs.usgs.gov/of/1994/of94-622/cc_g1.tar.Z",
    e00_path="ccgeo/cc_utm",
    polygon_pattern="CC_UTM#",
    srs=util.NAD27_UTM10_PROJ4,
    metadata_csv_path=os.path.join(os.path.dirname(os.path.realpath(__file__)), "units.csv")
  )
