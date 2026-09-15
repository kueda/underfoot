"""
Geologic map of central Panamint Range, California
"""

import os

from util.rocks import process_usgs_source
from util.proj import NAD83_UTM11_PROJ4


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://ndownloader.figstatic.com/files/31508747",
        extracted_file_path="GS2344_JEA-CPR-Geopoly.shp",
        srs=NAD83_UTM11_PROJ4,
        use_unzip=True,
        polygons_join_col="UNITLABEL",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "units.csv"
        ),
    )
