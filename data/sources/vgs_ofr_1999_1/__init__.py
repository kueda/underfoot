"""
Compilation bedrock geologic map of the Jay Peak quadrangle, Vermont

units.csv is transcribed from the Description of Map Units on the plate in
the archive. The plate gives no ages, so spans come from the age symbols
that begin the unit codes. The Lithology column has the codes with the
Cambrian symbol as C, where Lith_Code has it as _
"""

import os

from util.proj import NAD83_VT_STATE_PLANE
from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://anrmaps.vermont.gov/websites/geology/JayPeak_Quad_Bedrock.zip",
        use_unzip=True,
        extracted_file_path="JayPeak_Quad_Bedrock/JayPeak_Bedrock_Contacts.shp",
        srs=NAD83_VT_STATE_PLANE,
        polygons_join_col="Lithology",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        lithology_from_description=True
    )
