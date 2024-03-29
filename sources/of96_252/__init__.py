"""
    Preliminary Geologic Map Emphasizing Bedrock Formations in Alameda County,
    California: A Digital Database
"""

import os

from util.proj import NAD27_UTM10_PROJ4
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/of/1996/of96-252/al_g1.tar.Z",
        extracted_file_path="algeo/al_um-py.e00",
        srs=NAD27_UTM10_PROJ4,
        polygon_pattern="AL_UM-PY#",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
