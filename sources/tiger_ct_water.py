"""Generates water data from TIGER for New Hampshire"""

from util import tiger_water

FIPS_CODES = [
    # Fairfield County
    "09001",
    # Hartford County
    "09003",
    # Litchfield County
    "09005",
    # Middlesex County
    "09007",
    # New Haven County
    "09009",
    # New London County
    "09011",
    # Tolland County
    "09013",
    # Windham County
    "09015",
]

tiger_water.process_tiger_water_for_fips(FIPS_CODES, source=__file__)
