"""Tests for changed_packs.py, which lists the packs a set of changed files affects."""

import io
import json

import pytest

import changed_packs


def write(path, content=""):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_pack(data_dir, pack_id, rock=(), water=()):
    write(
        data_dir / "packs" / f"{pack_id}.json",
        json.dumps({"id": pack_id, "rock": list(rock), "water": list(water)})
    )


@pytest.fixture(name="data_dir")
def fixture_data_dir(tmp_path):
    """A data dir in a repo, with a few packs and sources"""
    data_dir = tmp_path / "data"
    sources = data_dir / "sources"
    write(sources / "util" / "__init__.py")
    write(sources / "util" / "proj.py")
    write(sources / "util" / "rocks" / "constants.py")
    write(sources / "base" / "__init__.py", "from util import log\n")
    write(sources / "shared" / "__init__.py", "from base import thing\nfrom .helpers import x\n")
    write(sources / "shared" / "helpers.py")
    write(sources / "shared_ca.py", "import os\nfrom shared import process\n")
    write(sources / "shared_ca" / "overrides.csv")
    write(sources / "shared_nv.py", "import shared.helpers\n")
    write(sources / "solo.py", "from util import log\n")
    write(sources / "solo" / "units.csv")
    write_pack(data_dir, "ca", rock=["solo", "shared_ca"], water=["tiger_water_06001"])
    write_pack(data_dir, "nv", rock=["shared_nv"])
    write_pack(data_dir, "solo-only", rock=["solo"])
    write_pack(data_dir, "unrelated", rock=["other"])
    return data_dir


def test_includes_packs_that_use_a_changed_source_module(data_dir):
    assert changed_packs.changed_packs(
        ["data/sources/solo.py"], data_dir
    ) == [
        {"pack": "ca", "layers": ["rocks"]},
        {"pack": "solo-only", "layers": ["rocks"]}
    ]


def test_includes_packs_that_use_a_source_with_changed_files(data_dir):
    assert changed_packs.changed_packs(
        ["data/sources/shared_ca/overrides.csv"], data_dir
    ) == [{"pack": "ca", "layers": ["rocks"]}]


def test_includes_packs_whose_sources_import_a_changed_module(data_dir):
    assert changed_packs.changed_packs(
        ["data/sources/shared/helpers.py"], data_dir
    ) == [{"pack": "ca", "layers": ["rocks"]}, {"pack": "nv", "layers": ["rocks"]}]


def test_includes_packs_whose_sources_import_a_changed_module_indirectly(data_dir):
    assert changed_packs.changed_packs(
        ["data/sources/base/__init__.py"], data_dir
    ) == [{"pack": "ca", "layers": ["rocks"]}, {"pack": "nv", "layers": ["rocks"]}]


def test_includes_packs_that_use_a_changed_water_source(data_dir):
    write(data_dir / "sources" / "tiger_water_06001.py")
    assert changed_packs.changed_packs(
        ["data/sources/tiger_water_06001.py"], data_dir
    ) == [{"pack": "ca", "layers": ["water"]}]


def test_includes_rocks_and_water_for_packs_with_changed_rock_and_water_sources(data_dir):
    write(data_dir / "sources" / "tiger_water_06001.py")
    assert changed_packs.changed_packs(
        ["data/sources/tiger_water_06001.py", "data/sources/shared_ca.py"], data_dir
    ) == [{"pack": "ca", "layers": ["rocks", "water"]}]


def test_includes_every_layer_of_changed_packs(data_dir):
    assert changed_packs.changed_packs(
        ["data/packs/nv.json", "data/packs/unrelated.json", "data/sources/solo.py"], data_dir
    ) == [
        {"pack": "ca", "layers": ["rocks"]},
        {"pack": "nv", "layers": changed_packs.LAYERS},
        {"pack": "solo-only", "layers": ["rocks"]},
        {"pack": "unrelated", "layers": changed_packs.LAYERS}
    ]


def test_excludes_deleted_packs(data_dir):
    assert changed_packs.changed_packs(["data/packs/deleted.json"], data_dir) == []


def test_excludes_packs_for_unrelated_changes(data_dir):
    assert changed_packs.changed_packs(
        ["web/src/App.tsx", "data/tests/test_rocks.py", "data/rocks.py", "README.md"],
        data_dir
    ) == []


def test_lists_no_packs_when_util_changes_because_build_all_packs_builds_them(data_dir):
    assert changed_packs.changed_packs(
        ["data/sources/solo.py", "data/sources/util/rocks/constants.py"], data_dir
    ) == []


def test_ignores_util_init_like_build_all_packs_does(data_dir):
    assert changed_packs.changed_packs(
        ["data/sources/solo.py", "data/sources/util/__init__.py"], data_dir
    ) == [
        {"pack": "ca", "layers": ["rocks"]},
        {"pack": "solo-only", "layers": ["rocks"]}
    ]


def test_main_prints_pack_ids_for_paths_on_stdin_as_json(data_dir, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO("data/sources/solo.py\n"))
    changed_packs.main([], data_dir)
    assert json.loads(capsys.readouterr().out) == [
        {"pack": "ca", "layers": ["rocks"]},
        {"pack": "solo-only", "layers": ["rocks"]}
    ]


ALL_PACKS = ["ca", "nv", "solo-only", "unrelated"]


def layers_by_pack_id(matrix):
    return {item["pack"]: item["layers"] for item in matrix}


@pytest.mark.parametrize("path, layers", [
    ("data/sources/util/water.py", ["water"]),
    ("data/sources/util/tiger_water.py", ["water"]),
    ("data/sources/util/rocks/constants.py", ["rocks"]),
    ("data/sources/util/usgs_states.py", ["rocks"]),
    ("data/sources/util/met2xml.py", ["rocks"]),
    ("data/sources/util/polygonize_arcs.py", ["rocks"]),
    ("data/sources/util/proj.py", changed_packs.LAYERS),
    ("data/sources/util/citations.py", ["rocks", "water"]),
])
def test_build_all_packs_maps_util_changes_to_layers_of_every_pack(data_dir, path, layers):
    assert changed_packs.build_all_packs([path], data_dir) == [
        {"pack": pack_id, "layers": layers} for pack_id in ALL_PACKS
    ]


def test_build_all_packs_combines_layers_of_util_changes(data_dir):
    assert layers_by_pack_id(changed_packs.build_all_packs(
        ["data/sources/util/water.py", "data/sources/util/rocks/constants.py"], data_dir
    )) == {pack_id: ["rocks", "water"] for pack_id in ALL_PACKS}


def test_build_all_packs_ignores_util_init_like_build_all_packs_does(data_dir):
    assert layers_by_pack_id(changed_packs.build_all_packs(
        ["data/sources/util/water.py", "data/sources/util/__init__.py"], data_dir
    )) == {pack_id: ["water"] for pack_id in ALL_PACKS}


def test_build_all_packs_adds_the_layers_of_other_changes_to_the_packs_they_affect(data_dir):
    # Build changed packs leaves these to Build all packs when util changes too
    assert layers_by_pack_id(changed_packs.build_all_packs(
        ["data/sources/util/water.py", "data/sources/solo.py"], data_dir
    )) == {
        "ca": ["rocks", "water"],
        "nv": ["water"],
        "solo-only": ["rocks", "water"],
        "unrelated": ["water"]
    }
    assert layers_by_pack_id(changed_packs.build_all_packs(
        ["data/sources/util/water.py", "data/packs/nv.json"], data_dir
    )) == {
        "ca": ["water"],
        "nv": changed_packs.LAYERS,
        "solo-only": ["water"],
        "unrelated": ["water"]
    }


def test_build_all_packs_can_build_every_layer(data_dir):
    assert changed_packs.build_all_packs([], data_dir, every_layer=True) == [
        {"pack": pack_id, "layers": changed_packs.LAYERS} for pack_id in ALL_PACKS
    ]


def test_main_prints_build_all_packs_matrix_as_json(data_dir, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO("data/sources/util/water.py\n"))
    changed_packs.main(["--build-all-packs"], data_dir)
    assert json.loads(capsys.readouterr().out) == [
        {"pack": pack_id, "layers": ["water"]} for pack_id in ALL_PACKS
    ]


def test_main_prints_every_layer_of_every_pack_without_reading_stdin(
    data_dir, monkeypatch, capsys
):
    monkeypatch.setattr("sys.stdin", io.StringIO("data/sources/util/water.py\n"))
    changed_packs.main(["--build-all-packs", "--every-layer"], data_dir)
    assert json.loads(capsys.readouterr().out) == [
        {"pack": pack_id, "layers": changed_packs.LAYERS} for pack_id in ALL_PACKS
    ]


def test_definition_hash_changes_with_the_pack_definition(data_dir):
    pack_path = data_dir / "packs" / "nv.json"
    before = changed_packs.definition_hash(pack_path)
    write_pack(data_dir, "nv", rock=["shared_nv", "solo"])
    assert changed_packs.definition_hash(pack_path) != before


def test_definition_hash_changes_with_the_referenced_geojson(data_dir):
    pack_path = data_dir / "packs" / "with-boundary.json"
    write(pack_path, json.dumps({
        "id": "with-boundary",
        "geojson": {"$ref": "file://./with-boundary.geojson"}
    }))
    write(data_dir / "packs" / "with-boundary.geojson", '{"type": "Polygon"}')
    before = changed_packs.definition_hash(pack_path)
    assert changed_packs.definition_hash(pack_path) == before
    write(data_dir / "packs" / "with-boundary.geojson", '{"type": "MultiPolygon"}')
    assert changed_packs.definition_hash(pack_path) != before


def test_main_prints_the_definition_hash(data_dir, capsys):
    pack_path = data_dir / "packs" / "nv.json"
    changed_packs.main(["--definition-hash", str(pack_path)], data_dir)
    assert capsys.readouterr().out.strip() == changed_packs.definition_hash(pack_path)
