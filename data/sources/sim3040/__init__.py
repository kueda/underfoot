"""
Surficial Geologic Map of the Darwin Hills 30' x 60' Quadrangle,
Inyo County, California
"""

import os

from util.rocks import process_usgs_source
from util.proj import NAD27_UTM11_PROJ4


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/sim/3040/sim3040_data.zip",
        extracted_file_path="sim3040_data/DHpolygons.shp",
        srs=NAD27_UTM11_PROJ4,
        use_unzip=True,
        polygons_join_col="PTYPE",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        lithology_from_description=True,
    )
