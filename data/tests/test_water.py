"""Tests for water.py source dispatch."""

import json
import os
import sys

import pytest

import water
from sources import util

BBOX = {"left": -156.1, "bottom": 18.9, "right": -154.8, "top": 20.3}
OVERTURE_PATH = "s3://overturemaps-us-west-2/release/2026-10-21.0/theme=base"
SOURCES_PATH = os.path.join(os.path.dirname(os.path.realpath(water.__file__)), "sources")


def water_pmtiles_commands(monkeypatch, tmp_path, **kwargs):
    """Run make_pmtiles with stubbed commands, writing the files they would.
    Returns the export commands for each layer of the tiles, by layer name,
    and the tippecanoe commands."""
    exports = {}
    tile_cmds = []

    def fake_call_cmd(cmd, **_kwargs):
        if cmd[0] == "ogr2ogr":
            export_path = cmd[cmd.index("GeoJSONSeq") + 1]
            exports[export_path] = cmd
            with open(export_path, "w", encoding="utf-8") as outfile:
                outfile.write("{}\n")
        elif cmd[0] == "tippecanoe":
            tile_cmds.append(cmd)

    monkeypatch.setattr(water.util, "call_cmd", fake_call_cmd)
    monkeypatch.setattr(water.util, "add_table_from_query_to_pmtiles", lambda **kwargs: None)
    water.make_pmtiles(["fake_source"], path=str(tmp_path / "water.pmtiles"), **kwargs)
    layers = {}
    for cmd in tile_cmds:
        for arg in cmd:
            if arg.startswith("--named-layer="):
                layer, export_path = arg.split("=", 1)[1].split(":", 1)
                layers.setdefault(layer, []).append(exports[export_path])
    return layers, tile_cmds


def sql_of(cmd):
    return cmd[cmd.index("-sql") + 1]


def is_sea_export(cmd):
    return "type = 'ocean/sea'" in sql_of(cmd)


def test_make_pmtiles_cleans_up_intermediate_files(monkeypatch, tmp_path):
    """Regression test: make_pmtiles used to write its intermediate
    GeoPackage next to water.py (i.e. data/water.gpkg) and never delete it,
    leaving a dangling multi-megabyte file after every pack build. It should
    write its intermediate files to a temporary directory that's removed
    once the PMTiles file has been built.
    """
    layers, _ = water_pmtiles_commands(monkeypatch, tmp_path)

    water_py_dir = os.path.dirname(os.path.realpath(water.__file__))
    assert layers
    for cmd in [cmd for cmds in layers.values() for cmd in cmds]:
        export_path = cmd[cmd.index("GeoJSONSeq") + 1]
        assert os.path.dirname(os.path.realpath(export_path)) != water_py_dir
        assert not os.path.exists(export_path)


def test_make_pmtiles_tiles_every_water_layer_with_tippecanoe(monkeypatch, tmp_path):
    layers, tile_cmds = water_pmtiles_commands(monkeypatch, tmp_path)

    assert set(layers) == {
        water.WATERWAYS_TABLE_NAME,
        water.WATERBODIES_TABLE_NAME,
        water.WATERSHEDS_TABLE_NAME,
        f"{water.WATERWAYS_TABLE_NAME}_overview",
        f"{water.WATERBODIES_TABLE_NAME}_overview",
    }
    [tile_cmd] = tile_cmds
    assert f"--output={tmp_path / 'water.pmtiles'}" in tile_cmd
    assert "--minimum-zoom=7" in tile_cmd
    assert "--maximum-zoom=14" in tile_cmd
    # Watersheds cover the pack like rock units, so simplify their shared
    # borders the same way on both sides
    assert "--detect-shared-borders" in tile_cmd


def test_make_pmtiles_shows_overview_layers_below_the_detailed_ones(monkeypatch, tmp_path):
    _, [tile_cmd] = water_pmtiles_commands(monkeypatch, tmp_path)

    [filter_arg] = [arg for arg in tile_cmd if arg.startswith("--feature-filter=")]
    assert json.loads(filter_arg.split("=", 1)[1]) == {
        water.WATERWAYS_TABLE_NAME: [">=", "$zoom", 9],
        # The sea shows at every zoom, so the rocks map can draw it under the units
        water.WATERBODIES_TABLE_NAME: [
            "any", ["==", "type", "ocean/sea"], [">=", "$zoom", 9]],
        f"{water.WATERWAYS_TABLE_NAME}_overview": ["<=", "$zoom", 8],
        f"{water.WATERBODIES_TABLE_NAME}_overview": ["<=", "$zoom", 8],
    }


def test_make_pmtiles_clips_everything_but_the_waterways_and_the_sea(monkeypatch, tmp_path):
    """Unclipped waterways show connectivity across the whole watershed"""
    layers, _ = water_pmtiles_commands(
        monkeypatch, tmp_path, geojson_path="/packs/pack.geojson", bbox=BBOX)

    for layer, cmds in layers.items():
        for cmd in cmds:
            if layer == water.WATERWAYS_TABLE_NAME:
                assert "-clipdst" not in cmd
            elif not is_sea_export(cmd):
                assert cmd[cmd.index("-clipdst") + 1] == "/packs/pack.geojson"


def test_make_pmtiles_clips_the_sea_to_the_bbox(monkeypatch, tmp_path):
    """Pack boundaries often follow the shore, like Hawaii's, which would leave the sea
    between islands blank. The sea only covers water, so filling the bbox with it doesn't
    cover land outside the boundary."""
    layers, _ = water_pmtiles_commands(
        monkeypatch, tmp_path, geojson_path="/packs/pack.geojson", bbox=BBOX)

    [sea] = [cmd for cmd in layers[water.WATERBODIES_TABLE_NAME] if is_sea_export(cmd)]
    clip = sea.index("-clipdst")
    assert sea[clip + 1:clip + 5] == [
        str(BBOX["left"]), str(BBOX["bottom"]), str(BBOX["right"]), str(BBOX["top"])]


def test_make_pmtiles_puts_the_sea_in_the_waterbodies_layer_once(monkeypatch, tmp_path):
    layers, _ = water_pmtiles_commands(
        monkeypatch, tmp_path, geojson_path="/packs/pack.geojson", bbox=BBOX)

    sea, others = [], []
    for cmd in layers[water.WATERBODIES_TABLE_NAME]:
        (sea if is_sea_export(cmd) else others).append(cmd)
    assert len(sea) == 1
    [other] = others
    assert "type IS DISTINCT FROM 'ocean/sea'" in sql_of(other)


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
    layers, _ = water_pmtiles_commands(monkeypatch, tmp_path)

    for layer in [water.WATERWAYS_TABLE_NAME, f"{water.WATERWAYS_TABLE_NAME}_overview"]:
        [cmd] = layers[layer]
        fields = exported_fields(cmd)
        assert fields is not None, f"expected {cmd} to select fields"
        assert "source_id" not in fields
        assert "flow_pre" in fields
        assert "flow_upstream" in fields


def test_make_pmtiles_leaves_waterbody_and_watershed_source_id_out_of_tiles(monkeypatch, tmp_path):
    """The app doesn't use a waterbody's or watershed's source_id or
    source_id_attr, and they stay in the database, so they're just dead weight
    in the tiles"""
    layers, _ = water_pmtiles_commands(monkeypatch, tmp_path)

    for layer in [
        water.WATERBODIES_TABLE_NAME,
        f"{water.WATERBODIES_TABLE_NAME}_overview",
        water.WATERSHEDS_TABLE_NAME,
    ]:
        for cmd in layers[layer]:
            fields = exported_fields(cmd)
            assert fields is not None, f"expected {cmd} to select fields"
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
        captured["base_path"] = base_path
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
    # The path a sources/<source>.py script would pass as __file__
    assert captured["base_path"] == os.path.join(SOURCES_PATH, f"{source}.py")


def test_process_source_tiger_water_fallback_uses_fips_and_matching_work_dir(monkeypatch):
    """create_pack.py writes tiger_water_<GEOID> identifiers with no
    sources/<source>.py file, so process_source has to handle them itself."""
    source = "tiger_water_06001"
    expected_work_path = util.make_work_dir(os.path.join("sources", f"{source}.py"))

    captured = {}

    def fake_process_tiger_water_for_fips(fips_codes, source):
        captured["fips_codes"] = fips_codes
        captured["work_path"] = util.make_work_dir(source)
        captured["source_path"] = source

    monkeypatch.setattr(water, "process_tiger_water_for_fips", fake_process_tiger_water_for_fips)
    monkeypatch.setattr(water, "load_citation_for_source", lambda source: None)
    monkeypatch.setattr(water.os.path, "isfile", lambda path: False)

    water.process_source(source)

    assert captured["fips_codes"] == ["06001"]
    assert captured["work_path"] == expected_work_path
    assert captured["source_path"] == os.path.join(SOURCES_PATH, f"{source}.py")


def test_process_source_finds_source_file_from_another_cwd(monkeypatch, tmp_path):
    """Regression test: process_source looked for sources/<source>.py relative
    to the cwd, but process_nhdplus_hr_source chdirs into its work dir and
    doesn't change back. A pool worker that processed an uncached NHDPlus
    source then failed on the next scripted source, e.g. "tiger_ny_water has
    no file and no way to process it" when building us-ny.
    """
    commands = []
    monkeypatch.setattr(
        water.util, "call_cmd", lambda cmd, **kwargs: commands.append(cmd))
    monkeypatch.setattr(water, "load_citation_for_source", lambda source: None)
    monkeypatch.chdir(tmp_path)

    water.process_source("tiger_ny_water")

    source_path = os.path.join(
        os.path.dirname(os.path.realpath(water.__file__)), "sources", "tiger_ny_water.py")
    assert commands[0] == ["python", source_path]


def _stub_make_water_steps(monkeypatch):
    """Stub everything make_water does except the wiring between its own
    arguments and process_sources. Returns the kwargs process_sources got,
    plus any SQL make_water ran directly, under "sql"."""
    captured = {"sql": []}

    def fake_process_sources(sources, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(water, "make_database", lambda: None)
    monkeypatch.setattr(water, "clean_sources", lambda sources, debug=False: None)
    monkeypatch.setattr(water, "process_sources", fake_process_sources)
    monkeypatch.setattr(water, "fetch_data", lambda url, **kwargs: os.path.basename(url))
    monkeypatch.setattr(water, "overture_base_path", lambda: OVERTURE_PATH)
    monkeypatch.setattr(
        water.util, "run_sql", lambda sql, **kwargs: captured["sql"].append(sql)
    )
    for step in (
        "create_tables",
        "load_waterways",
        "load_waterbodies",
        "inherit_waterbody_permanence",
        "load_watersheds",
        "load_flow",
        "label_waterways",
        "make_pmtiles",
        "read_osm_waterways",
        "apply_osm_names",
        "apply_osm_gap_fills",
        "apply_osm_network",
        "apply_osm_waterbodies",
        "apply_osm_sea",
        "cite_osm",
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


def test_make_water_does_not_mark_waterways_imaginary(monkeypatch):
    """The web app draws NHD's artificial waterways, which run through lakes
    and wide rivers, like natural water based on their type, so make_water
    doesn't need a spatial join to find the ones inside waterbodies. That join
    took an hour on big packs.
    """
    captured = _stub_make_water_steps(monkeypatch)

    water.make_water(["fake_source"])

    assert not [sql for sql in captured["sql"] if "is_imaginary" in sql]


def test_make_water_gives_artificial_paths_permanence_after_loading_waterbodies(monkeypatch):
    """Artificial paths inherit permanence from the waterbodies they run
    through, so that has to happen after both are loaded and before the
    tiles are written"""
    _stub_make_water_steps(monkeypatch)
    steps = []
    for step in ("load_waterways", "load_waterbodies", "inherit_waterbody_permanence",
                 "make_pmtiles"):
        monkeypatch.setattr(
            water, step, lambda *args, step=step, **kwargs: steps.append(step))

    water.make_water(["fake_source"])

    assert steps == [
        "load_waterways",
        "load_waterbodies",
        "inherit_waterbody_permanence",
        "make_pmtiles",
    ]


def test_make_water_adds_the_sea_from_osm_to_packs_with_hydrography_from_the_sources(
        monkeypatch):
    """NHD and TIGER don't map the open sea, which left it blank between islands, like
    in Hawaii"""
    _stub_make_water_steps(monkeypatch)
    calls = []
    monkeypatch.setattr(
        water, "apply_osm_sea",
        lambda overture_path, bbox, **kwargs: calls.append((overture_path, bbox)))

    water.make_water(["fake_source"], bbox=BBOX)

    assert calls == [(OVERTURE_PATH, BBOX)]


def test_make_water_adds_the_sea_before_filling_gaps_so_creeks_can_reach_it(monkeypatch):
    _stub_make_water_steps(monkeypatch)
    steps = []
    for step in ("inherit_waterbody_permanence", "apply_osm_sea", "apply_osm_gap_fills"):
        monkeypatch.setattr(
            water, step, lambda *args, step=step, **kwargs: steps.append(step))

    water.make_water(["fake_source"], bbox=BBOX, osm_pbf_url="https://x/hawaii.osm.pbf")

    assert steps == ["inherit_waterbody_permanence", "apply_osm_sea", "apply_osm_gap_fills"]


def test_make_water_cites_osm_for_the_sea(monkeypatch):
    _stub_make_water_steps(monkeypatch)
    cited = []
    monkeypatch.setattr(water, "apply_osm_sea", lambda *args, **kwargs: 1)
    monkeypatch.setattr(water, "cite_osm", lambda: cited.append(True))
    tiled = {}
    monkeypatch.setattr(
        water, "make_pmtiles", lambda sources, **kwargs: tiled.update(sources=sources))

    water.make_water(["fake_source"], bbox=BBOX)

    assert cited
    assert water.OSM_SOURCE in tiled["sources"]


def test_make_water_without_a_bbox_does_not_add_the_sea(monkeypatch):
    _stub_make_water_steps(monkeypatch)
    calls = []
    monkeypatch.setattr(water, "apply_osm_sea", lambda *args, **kwargs: calls.append(args))

    water.make_water(["fake_source"])

    assert not calls


def test_inherit_waterbody_permanence_only_joins_artificial_paths(monkeypatch):
    """A spatial join of every waterway to the waterbodies took an hour on big
    packs, so only artificial paths, which NHD draws through waterbodies,
    look for a waterbody"""
    statements = []
    monkeypatch.setattr(water.util, "run_sql", lambda sql, **kwargs: statements.append(sql))

    water.inherit_waterbody_permanence()

    assert len(statements) == 1
    assert "type = 'artificial'" in statements[0]


def test_create_tables_has_no_is_imaginary_column(monkeypatch):
    statements = []
    monkeypatch.setattr(water.util, "run_sql", lambda sql, **kwargs: statements.append(sql))

    water.create_tables()

    assert statements
    assert not [sql for sql in statements if "is_imaginary" in sql]


def test_create_tables_creates_the_tables_make_water_loads(monkeypatch):
    statements = []
    monkeypatch.setattr(water.util, "run_sql", lambda sql, **kwargs: statements.append(sql))

    water.create_tables()

    for table in (water.WATERWAYS_TABLE_NAME, water.WATERBODIES_TABLE_NAME,
                  water.WATERSHEDS_TABLE_NAME, water.WATERSHEDS_MASK_TABLE_NAME,
                  water.WATERWAYS_FLOW_TABLE_NAME):
        assert [sql for sql in statements if f"CREATE TABLE {table} (" in sql], table


@pytest.mark.parametrize("load", [
    "load_waterways", "load_waterbodies", "load_watersheds", "load_flow"])
def test_loading_sources_leaves_the_tables_to_create_tables(monkeypatch, load):
    """So loading nothing from the sources doesn't have to be called just to make
    the tables"""
    statements = []
    monkeypatch.setattr(
        water.util, "run_sql", lambda sql, **kwargs: statements.append(sql) or [[0]])

    getattr(water, load)(["fake_source"])

    assert not [sql for sql in statements if "DROP TABLE IF EXISTS" in sql
                and "_dump" not in sql]
    assert not [sql for sql in statements if "CREATE TABLE" in sql and "_dump" not in sql]


def test_make_pmtiles_leaves_is_imaginary_out_of_the_tiles(monkeypatch, tmp_path):
    commands = []
    monkeypatch.setattr(water.util, "call_cmd", lambda cmd, **kwargs: commands.append(cmd))
    monkeypatch.setattr(water.util, "add_table_from_query_to_pmtiles", lambda **kwargs: None)

    water.make_pmtiles(["fake_source"], path=str(tmp_path / "water.pmtiles"))

    assert commands
    assert not [cmd for cmd in commands if "is_imaginary" in str(cmd)]


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


def test_make_water_uses_osm_waterways_for_names_and_gaps(monkeypatch):
    """OSM names replace the sources' names, so they're applied after the waterways are
    loaded. Dead ends are continued once the flow is loaded, since that's how they're
    found, and before it's labeled, so the continuations are part of the flow. Both use
    the same OSM waterways, read once from a little past the bbox."""
    _stub_make_water_steps(monkeypatch)
    steps = []
    bbox = {"left": -122.4, "bottom": 37.6, "right": -122.0, "top": 38.0}
    osm_ways = [{"id": 1}]
    monkeypatch.setattr(water, "fetch_data", lambda url: f"{url}.pbf")
    for step in ("create_tables", "load_waterways", "load_flow", "label_waterways"):
        monkeypatch.setattr(
            water, step, lambda *args, step=step, **kwargs: steps.append(step))
    monkeypatch.setattr(
        water, "make_pmtiles",
        lambda sources, **kwargs: steps.append(("make_pmtiles", sources)))
    monkeypatch.setattr(
        water, "read_osm_waterways",
        lambda pbf_path, read_bbox, **kwargs: steps.append(
            ("read", pbf_path, read_bbox)) or osm_ways)
    for step in ("apply_osm_names", "apply_osm_gap_fills"):
        monkeypatch.setattr(
            water, step,
            lambda ways, b, step=step, **kwargs: steps.append((step, ways, b)))

    water.make_water(["fake_source"], bbox=bbox, osm_pbf_url="http://example.com/x.osm")

    assert steps == [
        "create_tables",
        "load_waterways",
        "load_flow",
        ("read", "http://example.com/x.osm.pbf",
         water.widen_bbox(bbox, water.OSM_BBOX_MARGIN_DEG)),
        ("apply_osm_names", osm_ways, bbox),
        ("apply_osm_gap_fills", osm_ways, bbox),
        "label_waterways",
        ("make_pmtiles", ["fake_source", "osm"]),
    ]


def test_make_water_without_an_osm_url_leaves_waterways_alone(monkeypatch):
    _stub_make_water_steps(monkeypatch)
    calls = []
    for step in ("read_osm_waterways", "apply_osm_names", "apply_osm_gap_fills"):
        monkeypatch.setattr(water, step, lambda *args, step=step, **kwargs: calls.append(step))
    pmtiles_sources = []
    monkeypatch.setattr(
        water, "make_pmtiles", lambda sources, **kwargs: pmtiles_sources.append(sources))

    water.make_water(["fake_source"], bbox={"left": 0, "bottom": 0, "right": 1, "top": 1})

    assert not calls
    assert pmtiles_sources == [["fake_source"]]


def test_make_water_can_take_waterways_and_flow_from_osm_instead_of_the_sources(monkeypatch):
    """The sources still provide watersheds, and OSM provides the waterbodies, which line up
    with its waterways, with the sea from the water polygons made from its coastline"""
    captured = _stub_make_water_steps(monkeypatch)
    steps = []
    bbox = {"left": -122.4, "bottom": 37.6, "right": -122.0, "top": 38.0}
    osm_ways = [{"id": 1}]
    monkeypatch.setattr(water, "fetch_data", lambda url: f"{url}.pbf")
    monkeypatch.setattr(water, "create_tables", lambda **kwargs: steps.append("create_tables"))
    for step in ("load_waterways", "load_waterbodies", "load_watersheds", "load_flow"):
        monkeypatch.setattr(
            water, step, lambda sources, step=step, **kwargs: steps.append((step, sources)))
    monkeypatch.setattr(
        water, "apply_osm_waterbodies",
        lambda pbf_path, overture_path, b, **kwargs: steps.append(
            ("apply_osm_waterbodies", pbf_path, overture_path, b)))
    monkeypatch.setattr(
        water, "read_osm_waterways",
        lambda pbf_path, read_bbox, kinds: steps.append(("read", kinds)) or osm_ways)
    for step in ("apply_osm_names", "apply_osm_gap_fills", "apply_osm_network"):
        monkeypatch.setattr(
            water, step,
            lambda ways, b, step=step, **kwargs: steps.append((step, ways, b)))
    monkeypatch.setattr(water, "label_waterways", lambda **kwargs: steps.append("label"))
    monkeypatch.setattr(
        water, "make_pmtiles",
        lambda sources, **kwargs: steps.append(("make_pmtiles", sources)))

    water.make_water(["fake_source"], bbox=bbox, osm_pbf_url="http://example.com/x.osm",
                     osm_hydrography=True)

    assert steps == [
        "create_tables",
        ("load_watersheds", ["fake_source"]),
        ("read", water.HYDROGRAPHY_WATERWAY_KINDS),
        ("apply_osm_waterbodies", "http://example.com/x.osm.pbf", OVERTURE_PATH, bbox),
        ("apply_osm_network", osm_ways, bbox),
        "label",
        ("make_pmtiles", ["fake_source", "osm"]),
    ]
    assert "cleandb" in captured


def test_make_water_cites_osm_even_if_it_only_names_waterways(monkeypatch):
    """OSM names waterways even where no dead ends continue along it"""
    _stub_make_water_steps(monkeypatch)
    cited = []
    monkeypatch.setattr(water, "fetch_data", lambda url: f"{url}.pbf")
    monkeypatch.setattr(water, "cite_osm", lambda: cited.append("osm"))

    water.make_water(["fake_source"], bbox={"left": 0, "bottom": 0, "right": 1, "top": 1},
                     osm_pbf_url="http://example.com/x.osm")

    assert cited == ["osm"]


def test_make_water_needs_an_osm_extract_to_take_hydrography_from_osm(monkeypatch):
    """Without one it would make a pack with no waterways at all"""
    captured = _stub_make_water_steps(monkeypatch)

    with pytest.raises(ValueError):
        water.make_water(["fake_source"], bbox={"left": 0, "bottom": 0, "right": 1, "top": 1},
                         osm_hydrography=True)
    assert "cleandb" not in captured
