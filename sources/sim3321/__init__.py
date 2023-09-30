"""
Geologic Map of the Southern White Ledge Peak and Matilija Quadrangles, Santa
Barbara and Ventura Counties, California
"""

import os
import re

from util.rocks import process_usgs_source
from util.proj import NAD27_UTM11_PROJ4


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/sim/3321/downloads/sim3321_GIS.zip",
        extracted_file_path="Shapefiles/Shapefiles/wlmageo.shp",
        srs=NAD27_UTM11_PROJ4,
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
