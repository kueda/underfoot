"""
    Geologic Map of Yosemite National Park and Vicinity, California: a digital
    database
"""

import os

from util.proj import NAD27_UTM11_PROJ4
from util.rocks import process_usgs_source

def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/imap/i1874/yosenp.e00.zip",
        use_unzip=True,
        extracted_file_path="yosenp.e00",
        skip_polygonize_arcs=True,
        srs=NAD27_UTM11_PROJ4,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
