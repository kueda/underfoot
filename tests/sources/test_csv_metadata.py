# pylint: disable=missing-function-docstring
"""Tests unit data in sources"""

import csv
from pathlib import Path

from sources.util.rocks import infer_metadata_from_csv_row

def test_lithology():
    for path in Path("sources").rglob("units.csv"):
        with open(path, encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                try:
                    parsed_row = infer_metadata_from_csv_row(row)
                    assert parsed_row.get("lithology") is not None
                except Exception as parsing_exception:
                    print(f"Exception in {path} parsing row: {row}")
                    raise parsing_exception
