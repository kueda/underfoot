"""Tests for water.py source dispatch."""

import os

import water
from sources import util


def test_process_source_nhdplus_fallback_uses_matching_work_dir(monkeypatch):
    """The nhdplus_* fallback branch (for sources with no dedicated
    sources/<source>.py file, added in d979678b) must hand
    process_nhdplus_hr_source a base_path whose work dir matches the one
    process_source itself checks for finished gpkg files afterward.

    Regression test: the fallback used to uppercase the source identifier in
    that base_path (to match NHD's GDB naming, e.g. NHDPLUS_H_1307_HU4_GDB.zip),
    which also uppercased the work dir it resolved to. On a case-sensitive
    filesystem that's a different directory than the lowercase one
    process_source looks in, so the source's gpkg files were always "missing"
    and its data silently never made it into the pack.
    """
    source = "nhdplus_h_1307_hu4"
    expected_work_path = util.make_work_dir(os.path.join("sources", f"{source}.py"))

    captured = {}

    def fake_process_nhdplus_hr_source(base_path, url, gdb_name):
        captured["work_path"] = util.make_work_dir(os.path.realpath(base_path))
        captured["url"] = url
        captured["gdb_name"] = gdb_name

    monkeypatch.setattr(water, "process_nhdplus_hr_source", fake_process_nhdplus_hr_source)
    monkeypatch.setattr(water, "load_citation_for_source", lambda source: None)
    monkeypatch.setattr(water.os.path, "isfile", lambda path: False)

    water.process_source(source)

    assert captured["work_path"] == expected_work_path
    # The GDB naming convention on S3 is still uppercase.
    assert captured["url"].endswith("NHDPLUS_H_1307_HU4_GDB.zip")
    assert captured["gdb_name"] == "NHDPLUS_H_1307_HU4_GDB.gdb"
