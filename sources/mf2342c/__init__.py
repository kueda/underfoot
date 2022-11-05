"""
    Geologic Map and Map Database of the Oakland Metropolitan Area, Alameda,
    Contra Costa, and San Francisco Counties, California
"""

import os

from util.proj import STATE_PLANE_CA_ZONE_3
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/mf/2000/2342/mf2342c.tgz",
        e00_path="oakdb/*-geol.e00",
        polygon_pattern="PY#",
        srs=STATE_PLANE_CA_ZONE_3,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
