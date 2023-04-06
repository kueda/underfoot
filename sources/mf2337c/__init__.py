"""
    Geologic Map and Map Database of Parts of Marin, San Francisco, Alameda,
    Contra Costa, and Sonoma Counties, California
"""

import os

from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/mf/2000/2337/mf2337c.tgz",
        extracted_file_path="mageo/ma-geol.e00",
        polygon_pattern="GEOL5-I",
        srs="+proj=lcc +lat_1=37.06666666666 +lat_2=38.43333333333 "
            "+lat_0=36.5 +lon_0=-120.5 +x_0=90 +y_0=10 +ellps=clrk66 +units=m "
            "+no_defs",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
