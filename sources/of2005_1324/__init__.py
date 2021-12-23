import os
import util.usgs_states as usgs_states


def process_usgs_states(
        states=["KY", "OH", "TN", "WV"],
        base_path=os.path.realpath(__file__),
        source_path=os.path.realpath(__file__)):
    """Process some or all of KY, OH, TN, WV"""
    usgs_states.process_usgs_states(
        states=states,
        base_path=base_path,
        base_url="http://pubs.usgs.gov/of/2005/1324/data",
        source_path=source_path
    )
