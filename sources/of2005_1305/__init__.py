"""
    Preliminary integrated geologic map databases for the United States
    Western States: California, Nevada, Arizona, Washington, Oregon, Idaho, and Utah
"""

import os
from util import usgs_states


def process_usgs_states(
        states=None,
        base_path=os.path.realpath(__file__),
        source_path=os.path.realpath(__file__)
):
    """Process some or all of AZ, CA, ID, NV, OR, or WA"""
    if states is None:
        states = ["AZ", "CA", "ID", "NV", "OR", "UT", "WA"]
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2005/1305/data",
        source_path=source_path
    )
