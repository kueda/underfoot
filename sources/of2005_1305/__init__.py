import os
import util.usgs_states as usgs_states


def process_usgs_states(
        states=["AZ", "CA", "ID", "NV", "OR", "UT", "WA"],
        base_path=os.path.realpath(__file__)):
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2005/1305/data"
    )
