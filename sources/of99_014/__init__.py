import util
import os

def run():
  util.process_usgs_source(
    base_path=os.path.realpath(__file__),
    url="https://pubs.usgs.gov/of/1999/of99-014/of99-14_3a.e00.gz",
    e00_path="of99-14_3a.e00",
    polygon_pattern="CARR-GEOL#",
    # Supposed to be California State Plane Zone 5 (https://spatialreference.org/ref/epsg/2229/), but this works instead
    srs="+proj=lcc +lat_1=35.46666666666667 +lat_2=34.03333333333333 +lat_0=33.5 +lon_0=-118 +x_0=609601.2192024384 +y_0=0 +datum=NAD27 +units=m +no_defs",
    metadata_csv_path=os.path.join(os.path.dirname(os.path.realpath(__file__)), "units.csv")
  )
