"""Tests for packs.py helpers that don't need to build any map data."""

import json
import os
import zipfile

import pytest

import changed_packs
import packs

PACK_ID = "test-pack"

PACK_DEFINITION = {
    "id": PACK_ID,
    "name": "Test Pack",
    "description": "A pack for testing",
    "admin1": "California",
    "admin2": "Alameda County",
    "bbox": {"left": -122.3, "bottom": 37.7, "right": -122.2, "top": 37.8},
    # Build config that has no business in the metadata
    "rock": ["some-source"],
    "geojson_path": "/some/path.geojson"
}


@pytest.fixture(name="build_dir")
def fixture_build_dir(tmp_path, monkeypatch):
    """Point packs at a temp build dir and a test pack definition"""
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    monkeypatch.setattr(packs, "get_build_dir", lambda: str(build_dir))
    definition_path = tmp_path / f"{PACK_ID}.json"
    definition_path.write_text(json.dumps(PACK_DEFINITION), encoding="utf-8")
    monkeypatch.setitem(
        packs.PACKS, PACK_ID, {**PACK_DEFINITION, "definition_path": str(definition_path)}
    )
    return build_dir


def test_write_pack_metadata_writes_descriptive_metadata(build_dir):
    pack_dir = packs.get_pack_dir(PACK_ID)
    packs.write_pack_metadata(PACK_ID, pack_dir)
    with open(os.path.join(pack_dir, "pack.json"), encoding="utf-8") as metadata_f:
        metadata = json.load(metadata_f)
    assert metadata["id"] == PACK_ID
    assert metadata["name"] == "Test Pack"
    assert metadata["description"] == "A pack for testing"
    assert metadata["admin1"] == "California"
    assert metadata["admin2"] == "Alameda County"
    assert metadata["bbox"] == PACK_DEFINITION["bbox"]
    assert metadata["updated_at"]
    assert "rock" not in metadata
    assert "geojson_path" not in metadata
    assert "definition_path" not in metadata


def test_write_pack_metadata_includes_the_definition_hash(build_dir):
    pack_dir = packs.get_pack_dir(PACK_ID)
    packs.write_pack_metadata(PACK_ID, pack_dir)
    with open(os.path.join(pack_dir, "pack.json"), encoding="utf-8") as metadata_f:
        metadata = json.load(metadata_f)
    assert metadata["definition_hash"] == changed_packs.definition_hash(
        packs.PACKS[PACK_ID]["definition_path"]
    )


def write_pack_zip(build_dir, pack_id, metadata):
    with zipfile.ZipFile(build_dir / f"{pack_id}.pmtiles.zip", "w") as pack_zip:
        pack_zip.writestr(f"{pack_id}/pack.json", json.dumps(metadata))


def read_manifest(build_dir):
    with open(build_dir / "manifest.json", encoding="utf-8") as manifest_f:
        return {pack["id"]: pack for pack in json.load(manifest_f)["packs"]}


def test_make_manifest_takes_the_definition_hash_from_the_built_pack(build_dir):
    write_pack_zip(build_dir, PACK_ID, {"id": PACK_ID, "definition_hash": "built"})
    packs.make_manifest()
    assert read_manifest(build_dir)[PACK_ID]["definition_hash"] == "built"


def test_make_manifest_keeps_the_definition_hash_of_packs_it_did_not_build(
    build_dir, monkeypatch
):
    monkeypatch.setattr(packs, "remote_built_packs", lambda url: {
        PACK_ID: {"id": PACK_ID, "updated_at": "2026-01-01", "definition_hash": "old"}
    })
    packs.make_manifest(manifest_url="https://example.com/manifest.json")
    assert read_manifest(build_dir)[PACK_ID]["definition_hash"] == "old"


def test_write_pack_metadata_includes_the_default_view(build_dir, monkeypatch):
    pack = {
        **packs.PACKS[PACK_ID],
        "default_center": {"lon": -122.27, "lat": 37.8},
        "default_zoom": 12
    }
    monkeypatch.setitem(packs.PACKS, PACK_ID, pack)
    pack_dir = packs.get_pack_dir(PACK_ID)
    packs.write_pack_metadata(PACK_ID, pack_dir)
    with open(os.path.join(pack_dir, "pack.json"), encoding="utf-8") as metadata_f:
        metadata = json.load(metadata_f)
    assert metadata["default_center"] == {"lon": -122.27, "lat": 37.8}
    assert metadata["default_zoom"] == 12


def test_make_pack_includes_metadata_in_zip(build_dir, monkeypatch):
    for layer in ["rocks", "water", "ways", "context", "contours"]:
        monkeypatch.setattr(packs, f"make_{layer}_for_pack", lambda *args, **kwargs: None)
    zip_path = packs.make_pack(PACK_ID)
    with zipfile.ZipFile(zip_path) as pack_zip:
        metadata = json.loads(pack_zip.read(f"{PACK_ID}.pmtiles/pack.json"))
    assert metadata["id"] == PACK_ID
    assert metadata["name"] == "Test Pack"


def test_make_water_for_pack_names_waterways_from_the_packs_osm_extract(build_dir, monkeypatch):
    pack = {
        "water": ["fake_source"],
        "bbox": {"left": 0, "bottom": 0, "right": 1, "top": 1},
        "geojson_path": None,
        "osm": "http://example.com/region-latest.osm.pbf",
    }
    captured = {}
    monkeypatch.setitem(packs.PACKS, "fake-pack", pack)
    monkeypatch.setattr(packs, "make_water", lambda *args, **kwargs: captured.update(kwargs))

    packs.make_water_for_pack("fake-pack")

    assert captured["osm_pbf_url"] == "http://example.com/region-latest.osm.pbf"


def test_make_water_for_pack_can_use_osm_hydrography(build_dir, monkeypatch):
    pack = {
        "water": ["fake_source"],
        "bbox": {"left": 0, "bottom": 0, "right": 1, "top": 1},
        "geojson_path": None,
        "osm": "http://example.com/region-latest.osm.pbf",
    }
    captured = {}
    monkeypatch.setitem(packs.PACKS, "fake-pack", pack)
    monkeypatch.setattr(packs, "make_water", lambda *args, **kwargs: captured.update(kwargs))

    packs.make_water_for_pack("fake-pack", osm_hydrography=True)

    assert captured["osm_hydrography"] is True


@pytest.mark.parametrize("hydrography, flag, expected", [
    ("osm", False, True),
    (None, False, False),
    (None, True, True),
])
def test_make_water_for_pack_takes_hydrography_from_osm_if_the_pack_says_to(
        build_dir, monkeypatch, hydrography, flag, expected):
    pack = {
        "water": ["fake_source"],
        "bbox": {"left": 0, "bottom": 0, "right": 1, "top": 1},
        "geojson_path": None,
        "osm": "http://example.com/region-latest.osm.pbf",
    }
    if hydrography:
        pack["hydrography"] = hydrography
    captured = {}
    monkeypatch.setitem(packs.PACKS, "fake-pack", pack)
    monkeypatch.setattr(packs, "make_water", lambda *args, **kwargs: captured.update(kwargs))

    packs.make_water_for_pack("fake-pack", osm_hydrography=flag)

    assert captured["osm_hydrography"] is expected


def test_make_pack_passes_osm_hydrography_to_the_water_build(build_dir, monkeypatch):
    calls = {}
    for layer in ("rocks", "ways", "context", "contours"):
        monkeypatch.setattr(packs, f"make_{layer}_for_pack", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        packs, "make_water_for_pack", lambda pack_id, **kwargs: calls.update(kwargs))

    packs.make_pack(PACK_ID, osm_hydrography=True)

    assert calls["osm_hydrography"] is True


@pytest.mark.parametrize("rock_maxzoom, expected", [(10, 10), (None, 14)])
def test_make_rocks_for_pack_uses_the_packs_rock_maxzoom(
        build_dir, monkeypatch, rock_maxzoom, expected):
    pack = dict(PACK_DEFINITION)
    if rock_maxzoom:
        pack["rock_maxzoom"] = rock_maxzoom
    captured = {}
    monkeypatch.setitem(packs.PACKS, PACK_ID, pack)
    monkeypatch.setattr(packs, "make_rocks", lambda *args, **kwargs: captured.update(kwargs))

    packs.make_rocks_for_pack(PACK_ID)

    assert captured["maxzoom"] == expected


@pytest.mark.parametrize("zoom, expected", [(None, (10, 12)), (10, (8, 10))])
def test_make_contours_for_pack_tiles_contours_to_the_zoom_of_their_dem(
        build_dir, monkeypatch, zoom, expected):
    pack = dict(PACK_DEFINITION)
    if zoom:
        pack["zoom"] = zoom
    captured = {}
    monkeypatch.setitem(packs.PACKS, PACK_ID, pack)
    monkeypatch.setattr(packs, "make_contours", lambda *args, **kwargs: captured.update(kwargs))

    packs.make_contours_for_pack(PACK_ID)

    assert (captured["pmtiles_minzoom"], captured["pmtiles_zoom"]) == expected
