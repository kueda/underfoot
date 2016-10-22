import util
import os

def run():
  util.process_usgs_source(
    base_path=os.path.realpath(__file__),
    url="http://pubs.usgs.gov/mf/2000/2342/mf2342c.tgz",
    extract_path="oakdb",
    e00_path="oakdb/*-geol.e00",
    srs="+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 +lat_0=36.5 +lon_0=-120.5 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs",
    metadata_csv_path=os.path.join(os.path.dirname(os.path.realpath(__file__)), "units.csv")
  )
