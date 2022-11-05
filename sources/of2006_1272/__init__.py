"""
    Preliminary integrated geologic map databases for the United States:
    Connecticut, Maine, Massachusetts, New Hampshire, New Jersey, Rhode Island and Vermont
"""

import os
from util import usgs_states


def process_usgs_states(
        states=None,
        base_path=os.path.realpath(__file__),
        source_path=os.path.realpath(__file__)):
    """Process some or all of ME, NH, VT, MA, CT, RI, NJ"""
    if states is None:
        states = ["ME", "NH", "VT", "MA", "CT", "RI", "NJ"]
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2006/1272/data",
        source_path=source_path
    )
