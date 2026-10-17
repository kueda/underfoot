"""
    Preliminary integrated geologic map databases for the United States:
    Alabama, Florida, Georgia, Mississippi, North Carolina, and South Carolina
"""

import os
from util import usgs_states


def process_usgs_states(
        states=None,
        base_path=os.path.realpath(__file__),
        source_path=os.path.realpath(__file__)):
    """Process some or all of AL, FL, GA, MS, NC, or SC"""
    if states is None:
        states = ["AL", "FL", "GA", "MS", "NC", "SC"]
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2005/1323/data",
        source_path=source_path
    )
