"""
Data release of geologic and geophysical maps of the onshore parts of the
Santa Maria and Point Conception 30' x 60' quadrangles, California
"""

import os
import re

from util.rocks import process_usgs_source
from util.proj import NAD83_UTM10_PROJ4


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://www.sciencebase.gov/catalog/file/get/5d76b9a7e4b0c4f70d01ff80?f=__disk__0b%2Fa9%2F29%2F0ba9299b45d9dbecf4c3cf66aa25bcf5e3b44cbb",
        extracted_file_path="SMPtC_Shapefiles/MapUnitPolys.shp",
        srs=NAD83_UTM10_PROJ4,
        use_unzip=True,
        # layer_name="amb_geo_polygon",
        # This dataset specifies units using codes for surficial rocks AND
        # basement rocks. Here we're just using the stuff at the surface.
        # join_col_modifier=lambda ptype: re.split(r'[/+-]', ptype)[0],
        polygons_join_col="LABEL"
        # metadata_csv_path=os.path.join(
        #     os.path.dirname(os.path.realpath(__file__)),
        #     "units.csv"
        # )
    )
