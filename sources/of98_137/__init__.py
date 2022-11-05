import util
import os

from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/of/1998/of98-137/sm_g1.tar.gz",
        e00_path="smgeo/sm_um-py.e00",
        polygon_pattern="SM3_UM-PY",
        srs=util.NAD27_UTM10_PROJ4,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
