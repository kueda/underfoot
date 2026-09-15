"""
    Geologic Map and Map Database of Northeastern San Francisco Bay Region,
    California
"""
import os

from util.proj import STATE_PLANE_CA_ZONE_3
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/mf/2002/2403/mf2403c.tgz",
        extracted_file_path="nesfgeo/*-geol.e00",
        srs=STATE_PLANE_CA_ZONE_3,
        polygon_pattern="GEOL",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
