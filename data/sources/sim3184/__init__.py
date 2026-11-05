"""
Bedrock Geologic Map of Vermont

units.csv comes from shape/VTcontax_attrib.txt in the source archive: titles
from FORMATION and MEMBER, descriptions from ROCKTYPES (often simplified from
the pamphlet), spans from AGE, and lithologies from ROCKTYPE1. Owbl is listed
twice there, once for the Vermont Valley sequence and once for the Taconic
Allochthon, but the polygons can't tell them apart, so its descriptions are
combined.
"""

import os

from util.proj import NAD83_VT_STATE_PLANE
from util.rocks import process_usgs_source


def distinguish_ocw(code):
    """
    Rename Ocw, the Weybridge Member of the Chipman Formation, which the
    case-insensitive metadata join would otherwise confuse with OCw, the
    Whitehall Formation
    """
    return "Ocw-Weybridge" if code == "Ocw" else code


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="https://pubs.usgs.gov/sim/3184/Downloads/SIM3184_nobase.zip",
        use_unzip=True,
        extracted_file_path="shape/VTcontax_poly.shp",
        srs=NAD83_VT_STATE_PLANE,
        polygons_join_col="LITH",
        join_col_modifier=distinguish_ocw,
        metadata_csv_path=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "units.csv"
        ),
        lithology_from_description=True
    )
