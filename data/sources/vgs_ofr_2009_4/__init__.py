"""
Bedrock geologic map of parts of the Eden, Albany, Lowell, and Irasburg quadrangles,
Vermont

units.csv is transcribed from the Description of Map Units on the scanned
plate in the archive. Many of the data's codes differ from the plate's,
and the units were matched by description and by where they're mapped,
e.g. the data's Omu is the plate's COmbh1, the Umbrella Hill Conglomerate
"""

import os

from util.proj import NAD83_VT_STATE_PLANE
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://anrmaps.vermont.gov/websites/geology/Lowell_Mtns_Rock.zip",
        use_unzip=True,
        extracted_file_path="Lowell_Mtns_Rock/contacts.shp",
        srs=NAD83_VT_STATE_PLANE,
        polygons_join_col="lithcode",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        lithology_from_description=True
    )
