import util
import os


def run():
    util.process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/imap/i1874/yosenp.e00.zip",
        use_unzip=True,
        e00_path="yosenp.e00",
        skip_polygonize_arcs=True,
        srs="+proj=utm +zone=11 +datum=NAD27 +units=m +no_defs",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
