import util
import os

from util.rocks import process_usgs_source


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/sim/3318/downloads/SIM3318_Geodatabase_Shapefiles.zip",
        extracted_file_path="SIM3318_Geodatabase_Shapefiles/SIM3318_shapefiles/Polygons.shp",
        use_unzip=True,
        srs=util.WGS84_UTM11_PROJ4,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        )
    )
