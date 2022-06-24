"""Source for state-wide geologic units from California"""
import os
from of2005_1305 import process_usgs_states

process_usgs_states(states=["CA"], source_path=os.path.realpath(__file__))
