import util
import os

def run():
  util.process_usgs_source(
    base_path=os.path.realpath(__file__),
    url="http://pubs.usgs.gov/of/1997/of97-456/pr-data.tar.gz",
    e00_path="pr-data/pr-geol.e00",
    srs=util.NAD27_UTM10_PROJ4,
    metadata_csv_path=os.path.join(os.path.dirname(os.path.realpath(__file__)), "units.csv")
  )
