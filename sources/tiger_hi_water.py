"""Generates water data from TIGER for Hawaii"""

from util import tiger_water

FIPS_CODES = [
 # Hawaii
  "15001",
 # Honolulu
  "15003",
 # Kauai
  "15007",
 # Maui
  "15009"
]

tiger_water.process_tiger_water_for_fips(FIPS_CODES, __file__)
