# pylint: disable=missing-function-docstring
"""Tests citation parsing"""

from pathlib import Path

from sources.util.citations import citation_txt_from_csl_json_path

def test_csl_json_files():
    """Test parsability of CSL JSON citations checked in to the repo"""
    for path in Path("sources").rglob("citation.json"):
        if "work-" in str(path):
            continue
        try:
            if citation_txt := citation_txt_from_csl_json_path(path):
                assert citation_txt != ""
        except Exception as parsing_exception:
            print(f"Exception parsing citation for {path}")
            raise parsing_exception
