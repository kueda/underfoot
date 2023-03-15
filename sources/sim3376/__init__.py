"""
Geologic map of the Providence Mountains in parts of the Fountain Peak and
adjacent 7.5' quadrangles, San Bernardino County, California
"""

import os
import re

from util.rocks import process_usgs_source
from util.proj import NAD83_UTM11_PROJ4

def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/sim/3376/sim3376_database.zip",
        extracted_file_path="sim3376_database/sim3376_shapefiles/provglg_poly.shp",
        srs=NAD83_UTM11_PROJ4,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        use_unzip=True,
    )
