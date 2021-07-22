import os
import util.usgs_states as usgs_states


def process_usgs_states(
        states=["ME", "NH", "VT", "MA", "CT", "RI", "NJ"],
        base_path=os.path.realpath(__file__),
        source_path=os.path.realpath(__file__)):
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2006/1272/data",
        source_path=source_path
    )
