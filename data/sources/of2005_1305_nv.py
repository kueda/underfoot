"""Source for state-wide geologic units from Nevada"""
import os
from of2005_1305 import process_usgs_states

process_usgs_states(states=["NV"], source_path=os.path.realpath(__file__))
