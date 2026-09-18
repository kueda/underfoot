"""Tests for water.py source dispatch."""

import os

import water
from sources import util


def test_make_pmtiles_cleans_up_intermediate_gpkg(monkeypatch, tmp_path):
    """Regression test: make_pmtiles used to write its intermediate
    GeoPackage next to water.py (i.e. data/water.gpkg) and never delete it,
    leaving a dangling multi-megabyte file after every pack build. It should
    write that intermediate file to a temporary directory that's removed
    once the PMTiles file has been built.
    """
    captured_gpkg_dirs = []

    def fake_call_cmd(cmd, **kwargs):
        args = cmd.split() if isinstance(cmd, str) else cmd
        for arg in args:
            if isinstance(arg, str) and arg.endswith(".gpkg"):
                captured_gpkg_dirs.append(os.path.dirname(os.path.realpath(arg)))

    monkeypatch.setattr(water.util, "call_cmd", fake_call_cmd)
    monkeypatch.setattr(water.util, "add_table_from_query_to_pmtiles", lambda **kwargs: None)

    water.make_pmtiles(["fake_source"], path=str(tmp_path / "water.pmtiles"))

    water_py_dir = os.path.dirname(os.path.realpath(water.__file__))
    assert captured_gpkg_dirs, "expected ogr2ogr calls referencing a .gpkg path"
    for gpkg_dir in captured_gpkg_dirs:
        assert gpkg_dir != water_py_dir
        assert not os.path.isdir(gpkg_dir)


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


def _stub_make_water_steps(monkeypatch):
    """Stub everything make_water does except the wiring between its own
    arguments and process_sources. Returns the kwargs process_sources got."""
    captured = {}

    def fake_process_sources(sources, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(water, "make_database", lambda: None)
    monkeypatch.setattr(water, "clean_sources", lambda sources, debug=False: None)
    monkeypatch.setattr(water, "process_sources", fake_process_sources)
    for step in (
        "load_waterways",
        "load_waterbodies",
        "update_imaginary_waterways",
        "load_watersheds",
        "load_networks",
        "make_pmtiles",
    ):
        monkeypatch.setattr(water, step, lambda *args, **kwargs: None)
    return captured


def test_make_water_clean_also_cleans_the_database(monkeypatch):
    """Regression test for #22: --clean deleted each source's work dir but
    left the per-source tables in Postgres, so process_source skipped
    reloading any table that still had rows and the pack was built from stale
    data. Cleaning must imply dropping the per-source tables too.
    """
    captured = _stub_make_water_steps(monkeypatch)

    water.make_water(["fake_source"], clean=True)

    assert captured["cleandb"] is True


def test_make_water_without_clean_leaves_the_database_alone(monkeypatch):
    captured = _stub_make_water_steps(monkeypatch)

    water.make_water(["fake_source"])

    assert not captured["cleandb"]


def test_make_water_cleandb_alone_still_cleans_the_database(monkeypatch):
    captured = _stub_make_water_steps(monkeypatch)

    water.make_water(["fake_source"], cleandb=True)

    assert captured["cleandb"] is True
