import util
import os

from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/sim/2004/2858/SIM2858.tar.gz",
        extracted_file_path="mws/mws-geo.e00",
        srs=util.NAD27_UTM10_PROJ4,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
