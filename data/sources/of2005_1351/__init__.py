"""
    Preliminary integrated geologic map databases for the United States:
    Central States: Montana, Wyoming, Colorado, New Mexico, North Dakota,
    South Dakota, Nebraska, Kansas, Oklahoma, Texas, Iowa, Missouri, Arkansas,
    and Louisiana
"""

import os
from util import usgs_states


def process_usgs_states(
        states=None,
        base_path=os.path.realpath(__file__),
        source_path=os.path.realpath(__file__)):
    """Process some or all of AR, CO, IA, KS, LA, MO, MT, ND, NE, NM, OK, SD, TX, or WY"""
    if states is None:
        states = [
            "AR", "CO", "IA", "KS", "LA", "MO", "MT", "ND", "NE", "NM", "OK", "SD", "TX", "WY"
        ]
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2005/1351/data",
        source_path=source_path
    )
