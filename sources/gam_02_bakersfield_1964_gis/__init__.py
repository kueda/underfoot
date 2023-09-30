"""
Geologic map of California : Bakersfield sheet
"""

import os
import re

from util.rocks import process_usgs_source
from util.proj import NAD83_CA_ALBERS


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://www.conservation.ca.gov/cgs/Documents/Publications/Geologic-Atlas-Maps/"
            "GAM_02-Bakersfield-1964-GIS.zip",
        extracted_file_path="GAM_02-Bakersfield-1964-GIS/GAM_02_Bakersfield-open/"
                            "GM_MapUnitPolys.shp",
        srs=NAD83_CA_ALBERS,
        use_unzip=True,
        polygons_join_col="MapUnit",
        mappable_metadata_csv_path="GAM_02-Bakersfield-1964-GIS/GAM_02_Bakersfield-open/"
                                   "DescriptionOfMapUnits.csv",
        mappable_metadata_mapping={
            "code": "MapUnit",
            "title": "FullName",
            "span": "Age",
            "description": "Descr"
        }
    )
