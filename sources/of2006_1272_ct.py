import os
from of2006_1272 import process_usgs_states

process_usgs_states(
    states=["CT"],
    source_path=os.path.realpath(__file__))
