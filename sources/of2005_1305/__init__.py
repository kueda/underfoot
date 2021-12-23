import os
import util.usgs_states as usgs_states


def process_usgs_states(
        states=["AZ", "CA", "ID", "NV", "OR", "UT", "WA"],
        base_path=os.path.realpath(__file__)
):
    """Process some or all of AZ, CA, ID, NV, OR, or WA"""
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2005/1305/data",
        source_path=os.path.realpath(__file__)
    )
