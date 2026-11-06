"""
Bedrock Geologic Map of Woodstock, Vermont

The report in the archive has no description of map units, so units.csv
draws on its discussion of stratigraphy, quoting it where possible
"""

import os

from util.proj import NAD83_VT_STATE_PLANE
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://anrmaps.vermont.gov/websites/geology/Woodstock_Town_Rock.zip",
        use_unzip=True,
        extracted_file_path="Woodstock_Town_Rock/contacts.shp",
        srs=NAD83_VT_STATE_PLANE,
        polygons_join_col="Lithology",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        lithology_from_description=True
    )
