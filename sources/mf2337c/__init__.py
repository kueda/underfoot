import util
import os

def run():
  util.process_usgs_source(base_path=os.path.realpath(__file__),
      url="http://pubs.usgs.gov/mf/2000/2337/mf2337c.tgz",
      extract_path="mageo", e00_path="mageo/ma-geol.e00",
      polygon_pattern="GEOL5-I",
      srs="+proj=lcc +lat_1=37.06666666666 +lat_2=38.43333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=90 +y_0=10 +ellps=clrk66 +units=m +no_defs")
