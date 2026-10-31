import os
from of2005_1351 import process_usgs_states

process_usgs_states(
    states=["LA"],
    source_path=os.path.realpath(__file__),
    metadata_overrides_csv_path=os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "of2005_1351", "la", "overrides.csv"
    )
)
