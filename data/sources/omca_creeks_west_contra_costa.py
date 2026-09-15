"""OMCA creeks source for western Contra Costa County, CA, USA"""
# WIP
import os
import util
from util.proj import STATE_PLANE_CA_ZONE_3
from util.water import process_omca_creeks_source


work_path = util.make_work_dir(os.path.realpath(__file__))
os.chdir(work_path)


regions = {
    "WestContraCosta": {
        "url": "http://explore.museumca.org/creeks/GIS/WestContraCostaCreeksGIS.zip",  # noqa: E501
        "dir_name": "WestContraCostaCreeksGIS",
        "waterways_shp_path": os.path.join(
            "WestContraCostaCreeksGIS", "RICH_Creek_Map_ver1.0", "Shapefiles",
            "RICH_flownetwork.shp"
        ),
        "watersheds_shp_path": os.path.join(
            "WestContraCostaCreeksGIS", "RICH_Creek_Map_ver1.0", "Shapefiles",
            "RICH_Watershed.shp"
        ),
        "waterways_name_col": "NAME1",
        "watersheds_name_col": "NAME1"
    },
    "WestAlameda": {
        "url": "http://explore.museumca.org/creeks/GIS/WesternAlamedaCoCreeksGIS-1.0.zip",
        "dir_name": "WAC_Creek_Map_GIS_ver_1.0",
        "waterways_shp_path": os.path.join(
            "WAC_Creek_Map_GIS_ver_1.0", "Shapefiles",
            "WAC_Flow_Network.shp"
        ),
        "watersheds_shp_path": os.path.join(
            "WAC_Creek_Map_GIS_ver_1.0", "Shapefiles",
            "WAC_Watershed.shp"
        ),
        "waterways_name_col": "NAME",
        "watersheds_name_col": "NAME1"
    }
}

process_omca_creeks_source(
    url="http://explore.museumca.org/creeks/GIS/WestContraCostaCreeksGIS.zip",
    dir_name="WestContraCostaCreeksGIS",
    waterways_shp_path=os.path.join(
        "WestContraCostaCreeksGIS", "RICH_Creek_Map_ver1.0", "Shapefiles",
        "RICH_flownetwork.shp"
    ),
    watersheds_shp_path=os.path.join(
        "WestContraCostaCreeksGIS", "RICH_Creek_Map_ver1.0", "Shapefiles",
        "RICH_Watershed.shp"
    ),
    waterways_name_col="NAME1",
    watersheds_name_col="NAME1",
    srs=STATE_PLANE_CA_ZONE_3
)
