"""
    Preliminary integrated geologic map databases for the United States:
    Delaware, Maryland, New York, Pennsylvania, and Virginia
"""

import os
from util import usgs_states


def process_usgs_states(
        states=None,
        base_path=os.path.realpath(__file__),
        source_path=os.path.realpath(__file__)):
    """Process some or all of DE, MD, NY, PA, VA"""
    if states is None:
        states = ["DE", "MD", "NY", "PA", "VA"]
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2005/1325/data",
        source_path=source_path
    )
