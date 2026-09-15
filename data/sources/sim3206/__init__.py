"""
Surficial Geologic Map of the Ivanpah 30’ x 60’ Quadrangle, San Bernardino
County, California, and Clark County, Nevada
"""

import os
import re

from util.rocks import process_usgs_source
from util.proj import NAD83_UTM11_PROJ4

def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/sim/3206/sim3206_database.zip",
        extracted_file_path="sim3206_database/SIM3206_gdb.mdb",
        srs=NAD83_UTM11_PROJ4,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        use_unzip=True,
        layer_name="ivanpah_polygon",
        # This dataset specifies units using codes for surficial rocks AND
        # basement rocks. Here we're just using the stuff at the surface.
        join_col_modifier=lambda ptype: re.split(r'[/+-]', ptype)[0]
    )
