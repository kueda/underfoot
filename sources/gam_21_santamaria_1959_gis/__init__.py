"""
Geologic map of California : Santa Maria Sheet
"""

import os
import re

from util.rocks import process_usgs_source
from util.proj import NAD83_CA_ALBERS


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://www.conservation.ca.gov/cgs/Documents/Publications/Geologic-Atlas-Maps/GAM_21-SantaMaria-1959-GIS.zip",
        extracted_file_path="GAM_21-SantaMaria-1959-GIS/GAM_21_SantaMaria-open/GM_MapUnitPolys.shp",
        srs=NAD83_CA_ALBERS,
        use_unzip=True,
        polygons_join_col="MapUnit",
        mappable_metadata_csv_path="GAM_21-SantaMaria-1959-GIS/GAM_21_SantaMaria-open/DescriptionOfMapUnits.csv",
        mappable_metadata_mapping={
            "code": "MapUnit",
            "title": "FullName",
            "span": "Age",
            "description": "Descr"
        }
    )
