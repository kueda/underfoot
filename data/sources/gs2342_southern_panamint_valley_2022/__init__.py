"""
Geologic map of southern Panamint Valley, southern Panamint Range, and central
Slate Range, California, USA
"""

import os

from util.rocks import process_usgs_source
from util.proj import NAD83_UTM11_PROJ4


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://ndownloader.figstatic.com/files/31508711",
        extracted_file_path="GS2342_DigitalGISMapData/GS2342_JEA-SPV-Geopoly.shp",
        srs=NAD83_UTM11_PROJ4,
        use_unzip=True,
        polygons_join_col="UNITLABEL",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "units.csv"
        ),
    )
