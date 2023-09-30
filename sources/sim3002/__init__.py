"""
Geologic Map of the Eastern Three-Quarters of the Cuyama 30’ x 60’ Quadrangle, California
"""

import os
import re

from util.rocks import process_usgs_source
from util.proj import NAD27_UTM11_PROJ4


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/sim/3002/downloads/cuya_shapefiles.zip",
        extracted_file_path="cuya_shapefiles/geo1.shp",
        srs=NAD27_UTM11_PROJ4,
        use_unzip=True,
        polygons_join_col="PLABL",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
