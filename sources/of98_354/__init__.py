import util
import os

from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="http://pubs.usgs.gov/of/1998/of98-354/sfs_data.tar.gz",
        uncompress_e00=True,
        e00_path="sfs-geol.e00",
        srs="+proj=lcc +lat_1=37.066667 +lat_2=38.433333 +lat_0=36.5 "
            "+lon_0=-120.5 +x_0=609601.21920 +y_0=-6 +datum=NAD27 +units=m "
            "+no_defs",
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
