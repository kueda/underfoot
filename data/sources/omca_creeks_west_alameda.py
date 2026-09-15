"""OMCA creeks source for western Alameda County, CA, USA"""
# WIP
import os
import util
from util.water import process_omca_creeks_source

# NAD83 / California zone 3 (ftUS) (EPSG 2227)
SRS = (
    "+proj=lcc +lat_1=38.43333333333333 +lat_2=37.06666666666667 "
    "+lat_0=36.5 +lon_0=-120.5 +x_0=2000000.0001016 +y_0=500000.0001016001 "
    "+ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=us-ft +no_defs"
)
# Make the work dir
work_path = util.make_work_dir(os.path.realpath(__file__))
os.chdir(work_path)
process_omca_creeks_source(
    url="http://explore.museumca.org/creeks/GIS/WesternAlamedaCoCreeksGIS-1.0.zip",  # noqa: E501
    dir_name="WAC_Creek_Map_GIS_ver_1.0",
    waterways_shp_path=os.path.join(
        "WAC_Creek_Map_GIS_ver_1.0", "Shapefiles",
        "WAC_Flow_Network.shp"
    ),
    watersheds_shp_path=os.path.join(
        "WAC_Creek_Map_GIS_ver_1.0", "Shapefiles",
        "WAC_Watershed.shp"
    ),
    waterways_name_col="NAME",
    watersheds_name_col="NAME1",
    srs=SRS
)
