"""
Bedrock Geologic Map of the Essex Junction Quadrangle, Vermont

units.csv is transcribed from the Description of Map Units on the plate in
the archive. A few of the data's codes differ from the plate's: the data's
CZfpqs, CZpbc, CZpc, and CZpq are the plate's CZfpq, CZpcg, CZpq, and CZpqm
"""

import os

from util.proj import NAD83_VT_STATE_PLANE
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://anrmaps.vermont.gov/websites/geology/EssexJunction_Quad_Rock.zip",
        use_unzip=True,
        extracted_file_path="EssexJunction_Quad_Rock/Contacts.shp",
        srs=NAD83_VT_STATE_PLANE,
        polygons_join_col="LithCode",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        lithology_from_description=True
    )
