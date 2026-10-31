import os
from of2006_1272 import process_usgs_states

process_usgs_states(
    states=["MA"],
    source_path=os.path.realpath(__file__),
    metadata_overrides_csv_path=os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "of2006_1272", "ma", "overrides.csv"
    )
)
