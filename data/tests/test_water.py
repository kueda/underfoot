"""Tests for water.py source dispatch."""

import os
import sys

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


def exported_fields(cmd):
    """Fields an ogr2ogr command copies, from its -select or -sql, or None if
    it copies all of them"""
    if "-select" in cmd:
        return cmd[cmd.index("-select") + 1].split(",")
    if "-sql" in cmd:
        sql = cmd[cmd.index("-sql") + 1]
        select_list = sql.split("SELECT", 1)[1].split("FROM", 1)[0]
        return [field.strip() for field in select_list.split(",")]
    return None


def test_make_pmtiles_leaves_waterway_source_id_out_of_tiles(monkeypatch, tmp_path):
    """The app doesn't use a waterway's source_id, and a unique string on
    every waterway makes the tiles much bigger"""
    cmds = []

    def fake_call_cmd(cmd, **kwargs):
        if isinstance(cmd, list):
            cmds.append(cmd)

    monkeypatch.setattr(water.util, "call_cmd", fake_call_cmd)
    monkeypatch.setattr(water.util, "add_table_from_query_to_pmtiles", lambda **kwargs: None)

    water.make_pmtiles(["fake_source"], path=str(tmp_path / "water.pmtiles"))

    waterways_cmds = [
        cmd for cmd in cmds
        if water.WATERWAYS_TABLE_NAME in cmd or f"{water.WATERWAYS_TABLE_NAME}_overview" in cmd
    ]
    assert len(waterways_cmds) == 2
    for cmd in waterways_cmds:
        fields = exported_fields(cmd)
        assert fields is not None, f"expected {cmd} to select fields"
        assert "source_id" not in fields
        assert "flow_pre" in fields
        assert "flow_upstream" in fields


def test_make_pmtiles_leaves_waterbody_and_watershed_source_id_out_of_tiles(monkeypatch, tmp_path):
    """The app doesn't use a waterbody's or watershed's source_id or
    source_id_attr, and they stay in the database, so they're just dead weight
    in the tiles"""
    cmds = []

    def fake_call_cmd(cmd, **kwargs):
        if isinstance(cmd, list):
            cmds.append(cmd)

    monkeypatch.setattr(water.util, "call_cmd", fake_call_cmd)
    monkeypatch.setattr(water.util, "add_table_from_query_to_pmtiles", lambda **kwargs: None)

    water.make_pmtiles(["fake_source"], path=str(tmp_path / "water.pmtiles"))

    layers = [
        water.WATERBODIES_TABLE_NAME,
        f"{water.WATERBODIES_TABLE_NAME}_overview",
        water.WATERSHEDS_TABLE_NAME,
    ]
    for layer in layers:
        layer_cmds = [cmd for cmd in cmds if layer in cmd]
        assert len(layer_cmds) == 1, f"expected one ogr2ogr command for {layer}"
        fields = exported_fields(layer_cmds[0])
        assert fields is not None, f"expected {layer_cmds[0]} to select fields"
        assert "*" not in fields
        assert "source_id" not in fields
        assert "source_id_attr" not in fields
        assert "name" in fields
        assert "source" in fields


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
        "load_flow",
        "label_waterways",
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


def downstream_of(labels, source_id):
    """Segments a downstream trace from source_id would highlight, using the
    same comparison the web app's map filter makes on flow_pre and
    flow_upstream"""
    pre, _ = labels[source_id]
    return {
        other for other, (other_pre, other_upstream) in labels.items()
        if other_pre <= pre <= other_pre + other_upstream
    }


def upstream_of(labels, source_id):
    """Segments an upstream trace from source_id would highlight"""
    pre, upstream = labels[source_id]
    return {
        other for other, (other_pre, _) in labels.items()
        if pre <= other_pre <= pre + upstream
    }


# A river with two tributaries: the headwater segment "c" and the tributary
# "d" both flow into "b", "e" flows into "d", and "b" flows into the outlet
# "a". Tuples are (source_id, hydroseq, dnhydroseq); NHDPlus uses a
# dnhydroseq of 0 for segments with nothing downstream.
RIVER = [
    ("a", 10, 0),
    ("b", 20, 10),
    ("c", 30, 20),
    ("d", 40, 20),
    ("e", 50, 40),
]


def test_label_flow_tree_downstream_follows_main_path_to_outlet():
    labels = water.label_flow_tree(RIVER)
    assert downstream_of(labels, "e") == {"e", "d", "b", "a"}
    assert downstream_of(labels, "c") == {"c", "b", "a"}
    assert downstream_of(labels, "a") == {"a"}


def test_label_flow_tree_upstream_includes_all_tributaries():
    labels = water.label_flow_tree(RIVER)
    assert upstream_of(labels, "b") == {"b", "c", "d", "e"}
    assert upstream_of(labels, "d") == {"d", "e"}
    assert upstream_of(labels, "c") == {"c"}


def test_label_flow_tree_counts_segments_upstream():
    """The count is small for most segments, which keeps it cheap in tiles"""
    labels = water.label_flow_tree(RIVER)
    assert {source_id: upstream for source_id, (_, upstream) in labels.items()} == {
        "a": 4,
        "b": 3,
        "c": 0,
        "d": 1,
        "e": 0,
    }


def test_label_flow_tree_keeps_separate_rivers_separate():
    other_river = [("x", 100, 0), ("y", 110, 100)]
    labels = water.label_flow_tree(RIVER + other_river)
    assert downstream_of(labels, "y") == {"y", "x"}
    assert upstream_of(labels, "a") == {"a", "b", "c", "d", "e"}


def test_label_flow_tree_treats_segment_flowing_out_of_the_data_as_outlet():
    """A segment whose downstream neighbor isn't in the pack, e.g. where a
    river leaves the pack's last NHD source, is where traces end"""
    labels = water.label_flow_tree([("a", 10, 999), ("b", 20, 10)])
    assert downstream_of(labels, "b") == {"b", "a"}


def test_label_flow_tree_links_segments_across_sources():
    """NHDPlus HR hydroseqs are unique across HU4s, so the segment where a
    river leaves one source links to the segment where it enters the next,
    regardless of the order the sources were loaded in"""
    source_1303 = [("inlet", 35000500041934, 35000500001465)]
    source_1302 = [("outlet", 35000600001526, 35000500041934)]
    labels = water.label_flow_tree(source_1303 + source_1302)
    assert downstream_of(labels, "outlet") == {"outlet", "inlet"}


def test_label_flow_tree_handles_rivers_longer_than_the_recursion_limit():
    length = sys.getrecursionlimit() * 2
    segments = [(str(i), i + 1, i) for i in range(length)]
    labels = water.label_flow_tree(segments)
    assert len(downstream_of(labels, str(length - 1))) == length
