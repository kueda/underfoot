"""
Preliminary Surficial Geologic Map of the Newberry Springs 30’ x 60’
Quadrangle, California
"""

import os
import re

from util.rocks import process_usgs_source
from util.proj import NAD27_UTM11_PROJ4


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/of/2011/1044/OFR2011-1044_database.zip",
        extracted_file_path="OFR2011-1044_database/OFR2011-1044.e00",
        skip_polygonize_arcs=True,
        srs=NAD27_UTM11_PROJ4,
        use_unzip=True,
        # This dataset specifies units using codes for surficial rocks AND
        # basement rocks. Here we're just using the stuff at the surface.
        join_col_modifier=lambda ptype: re.split(r'[/+-]', ptype)[0],
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
