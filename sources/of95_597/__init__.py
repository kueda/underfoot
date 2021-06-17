import util
import os


def run():
    util.process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/of/1995/of95-597/hf_g1.tar.Z",
        e00_path="hfg/hf*_um",
        srs=util.NAD27_UTM10_PROJ4,
        skip_polygonize_arcs=True
    )
