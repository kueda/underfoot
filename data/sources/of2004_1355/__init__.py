"""
    Preliminary integrated geologic map databases for the United States:
    The Upper Midwest States: Minnesota, Wisconsin, Michigan, Illinois, and Indiana
"""

import os
from util import usgs_states


def process_usgs_states(
        states=None,
        base_path=os.path.realpath(__file__),
        source_path=os.path.realpath(__file__)):
    """Process some or all of IL, IN, MI, MN, or WI"""
    if states is None:
        states = ["IL", "IN", "MI", "MN", "WI"]
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2004/1355/data",
        source_path=source_path
    )
