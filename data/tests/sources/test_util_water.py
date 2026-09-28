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


@pytest.mark.parametrize("fcode", [46600, 46601, 46602])
def test_waterbodies_sql_classifies_all_swamp_marsh_fcodes_as_swamps(fcode):
    """NHD splits swamps/marshes into intermittent (46601) and perennial
    (46602) FCodes, and those used to fall through to lake/pond."""
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE NHDWaterbody (GNIS_ID, GNIS_Name, FCode, FType, Shape)")
    con.execute("CREATE TABLE NHDFcode (FCode, Description, HydrographicCategory)")
    con.execute("INSERT INTO NHDWaterbody VALUES ('1', 'Some Swamp', ?, 466, NULL)", (fcode,))
    con.execute("INSERT INTO NHDFcode VALUES (?, 'Swamp/Marsh', ' ')", (fcode,))
    cursor = con.execute(water.waterbodies_sql("NHDWaterbody"))
    columns = [d[0] for d in cursor.description]
    assert dict(zip(columns, cursor.fetchone()))["type"] == "swamp/marsh"
