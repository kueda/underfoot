"""Tests for sources.util.water"""

import os
import sqlite3
import subprocess

import pytest

from sources.util import water


def test_process_nhdplus_hr_source_waterways_flow_rebuilds_after_failure(tmp_path):
    """Regression test: the CSV used to be written with a shell redirect,
    which creates it before sqlite3 runs, so a failed run left an empty CSV
    behind. Later runs saw it and skipped regenerating it, and the source's
    waterways never got flow labels."""
    sqlite_path = tmp_path / water.WATERWAYS_NETWORK_FNAME
    csv_path = tmp_path / water.WATERWAYS_FLOW_FNAME
    con = sqlite3.connect(sqlite_path)

    # No NHDPlusFlowlineVAA table yet, so the query fails
    with pytest.raises(subprocess.CalledProcessError):
        water.process_nhdplus_hr_source_waterways_flow(str(tmp_path))
    assert not os.path.exists(csv_path)

    con.execute("CREATE TABLE NHDPlusFlowlineVAA (NHDPlusID, HydroSeq, DnHydroSeq)")
    con.execute("INSERT INTO NHDPlusFlowlineVAA VALUES (55000100000001.0, 20.0, 10.0)")
    con.commit()
    con.close()
    water.process_nhdplus_hr_source_waterways_flow(str(tmp_path))
    assert csv_path.read_text().splitlines() == [
        "source_id,hydroseq,dnhydroseq",
        "55000100000001,20,10",
    ]
