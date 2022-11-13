"""
    Geologic Map and Map Database of Western Sonoma, Northernmost Marin, and
    Southernmost Mendocino Counties, California
"""

import os

from util.proj import NAD27_UTM10_PROJ4
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/mf/2002/2402/mf2402c.tgz",
        e00_path="wsogeo/wso-geol.e00",
        srs=NAD27_UTM10_PROJ4,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        polygon_pattern="SO-GEOL7_U"
    )
