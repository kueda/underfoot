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
    ) == ["ca", "solo-only"]


def test_includes_packs_that_use_a_source_with_changed_files(data_dir):
    assert changed_packs.changed_packs(
        ["data/sources/shared_ca/overrides.csv"], data_dir
    ) == ["ca"]


def test_includes_packs_whose_sources_import_a_changed_module(data_dir):
    assert changed_packs.changed_packs(
        ["data/sources/shared/helpers.py"], data_dir
    ) == ["ca", "nv"]


def test_includes_packs_whose_sources_import_a_changed_module_indirectly(data_dir):
    assert changed_packs.changed_packs(
        ["data/sources/base/__init__.py"], data_dir
    ) == ["ca", "nv"]


def test_includes_packs_that_use_a_changed_water_source(data_dir):
    write(data_dir / "sources" / "tiger_water_06001.py")
    assert changed_packs.changed_packs(
        ["data/sources/tiger_water_06001.py"], data_dir
    ) == ["ca"]


def test_includes_changed_packs(data_dir):
    assert changed_packs.changed_packs(
        ["data/packs/nv.json", "data/packs/unrelated.json"], data_dir
    ) == ["nv", "unrelated"]


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
    ) == ["ca", "solo-only"]


def test_main_prints_pack_ids_for_paths_on_stdin_as_json(data_dir, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO("data/sources/solo.py\n"))
    changed_packs.main(data_dir)
    assert json.loads(capsys.readouterr().out) == ["ca", "solo-only"]
