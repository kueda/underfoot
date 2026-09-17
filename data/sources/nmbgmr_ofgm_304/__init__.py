"""
Geologic Map Database of New Mexico, adapted from Geologic Map of New Mexico
"""

import os

from util.rocks import process_usgs_source
from util.proj import NAD83_NM_LAMBERT


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://geoinfo.nmt.edu/publications/maps/geologic/ofgm/downloads/304/GISdata/"
            "NewMexicoGeologicMap.mpk",
        extracted_file_path="v108/newmexicogeologicmap.gdb",
        srs=NAD83_NM_LAMBERT,
        layer_name="MapUnitPolys",
        polygons_join_col="MapUnit",
        mappable_metadata_layer_name="DescriptionOfMapUnits",
        mappable_metadata_mapping={
            "code": "MapUnit",
            "title": "FullName",
            "span": "Age",
            "description": "Description",
            "geomaterial": "GeoMaterial"
        },
        # Lithologies the text gets wrong, e.g. "sand" from "Sandia
        # Formation", or that can't be inferred at all
        metadata_overrides_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "overrides.csv"
        )
    )
