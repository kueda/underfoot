"""
    Geologic Map of the Hayward fault zone, Contra Costa, Alameda, and Santa
    Clara Counties, California: A digital database
"""

import os

from util.proj import NAD27_UTM10_PROJ4
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/of/1995/of95-597/hf_g1.tar.Z",
        extracted_file_path="hfg/hf*_um",
        srs=NAD27_UTM10_PROJ4,
        skip_polygonize_arcs=True
    )
