import os
from of2005_1325 import process_usgs_states

process_usgs_states(states=["NY"], source_path=os.path.realpath(__file__))
