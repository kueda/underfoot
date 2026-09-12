# pylint: disable=missing-function-docstring
"""Tests unit data in sources"""

import csv
from pathlib import Path

from sources.util.rocks import infer_metadata_from_csv_row, LITHOLOGIES, LITHOLOGY_SYNONYMS

def test_lithology():
    for path in Path("sources").rglob("units.csv"):
        # Only the hand-authored source CSVs, not generated copies in work dirs
        if "work-" in str(path):
            continue
        with open(path, encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                try:
                    parsed_row = infer_metadata_from_csv_row(row)
                    assert parsed_row.get("lithology") is not None
                except Exception as parsing_exception:
                    print(f"Exception in {path} parsing row: {row}")
                    raise parsing_exception


def test_override_lithologies_are_known():
    for path in Path("sources").rglob("overrides.csv"):
        if "work-" in str(path):
            continue
        with open(path, encoding="utf-8") as infile:
            for row in csv.DictReader(infile):
                if lithology := row.get("lithology"):
                    assert lithology in LITHOLOGIES or lithology in LITHOLOGY_SYNONYMS, (
                        f"Unknown lithology in {path}: {row}"
                    )
