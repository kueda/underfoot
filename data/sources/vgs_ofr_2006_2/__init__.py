"""
Bedrock Geologic Map of the Southern Worcester Mountains Watershed, Middlesex
and Stowe 7.5 Minute Quadrangles, Vermont

units.csv is transcribed from the Description of Map Units on the scanned
plate in the archive, with titles and spans from its formation headings
"""

import os

from util.proj import NAD83_VT_STATE_PLANE
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://anrmaps.vermont.gov/websites/geology/WorcesterSouth_Rock.zip",
        use_unzip=True,
        # Despite the name, these are the unit polygons
        extracted_file_path="WorcesterSouth_Rock/Contacts.shp",
        srs=NAD83_VT_STATE_PLANE,
        polygons_join_col="LithCode",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        lithology_from_description=True
    )
