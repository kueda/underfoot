"""Source for geologic units in and around Los Angeles, California"""

import os

from util.proj import NAD27_UTM11_PROJ4
from util.rocks import process_usgs_source


def run():
    """Process of2005_1019 (Los Angeles)"""
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/of/2005/1019/los_angeles.tar.gz",
        e00_path="los_angeles/la1_geo.e00",
        polygons_join_col="LABL",
        skip_polygonize_arcs=True,
        srs=NAD27_UTM11_PROJ4,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
