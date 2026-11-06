"""
Bedrock Geologic Map of Charlotte, Vermont

units.csv is transcribed from the Description of Map Units on the scanned
plate in the archive
"""

import os

from util.proj import NAD83_VT_STATE_PLANE
from util.rocks import process_usgs_source


def cambrian_symbol(code):
    """The data writes the Cambrian symbol in codes like _cs as _"""
    return f"C{code[1:]}" if code.startswith("_") else code


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://anrmaps.vermont.gov/websites/geology/Charlotte_Town_Rock.zip",
        use_unzip=True,
        extracted_file_path="Charlotte_Town_Rock/contacts.shp",
        srs=NAD83_VT_STATE_PLANE,
        polygons_join_col="LithCode",
        join_col_modifier=cambrian_symbol,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        lithology_from_description=True
    )
