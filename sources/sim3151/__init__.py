"""
Geologic Map of the southern Funeral Mountains including nearby Groundwater
Discharge Sites in Death Valley National Park, California and Nevada
"""

import os

from util.rocks import process_usgs_source
from util.proj import NAD27_UTM11_PROJ4


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/sim/3151/downloads/SIM3151.zip",
        extracted_file_path="shapefiles/sfuneralmtns_poly.shp",
        srs=NAD27_UTM11_PROJ4,
        use_unzip=True,
        polygons_join_col="UNIT",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        # lithology_from_description=True
    )
