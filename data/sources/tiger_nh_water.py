"""Generates water data from TIGER for New Hampshire"""

from util import tiger_water

FIPS_CODES = [
    # Belknap County
    "33001",
    # Carroll County
    "33003",
    # Cheshire County
    "33005",
    # Coos County
    "33007",
    # Grafton County
    "33009",
    # Hillsborough County
    "33011",
    # Merrimack County
    "33013",
    # Rockingham County
    "33015",
    # Strafford County
    "33017",
    # Sullivan County
    "33019",
]

tiger_water.process_tiger_water_for_fips(FIPS_CODES, source=__file__)
