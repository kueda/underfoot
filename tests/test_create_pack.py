"""Tests for create_pack.py helpers that don't need to download anything."""

import json

import create_pack


def box_feature(left, bottom, right, top, properties=None):
    return {
        "type": "Feature",
        "properties": properties or {},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [left, bottom], [right, bottom], [right, top], [left, top], [left, bottom]
            ]]
        }
    }


def write_feature_collection(path, features):
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))


def find_geofabrik_url_for_pack_box(tmp_path, monkeypatch, left, bottom, right, top):
    monkeypatch.chdir(tmp_path)
    write_feature_collection(tmp_path / "geofabrik_index.geojson", [
        box_feature(-10, -10, 10, 10, {"id": "big", "urls": {"pbf": "big.osm.pbf"}}),
        box_feature(0, 0, 1, 1, {"id": "small", "urls": {"pbf": "small.osm.pbf"}})
    ])
    pack_path = tmp_path / "pack.geojson"
    write_feature_collection(pack_path, [box_feature(left, bottom, right, top)])
    return create_pack.find_geofabrik_url(str(pack_path))


def test_find_geofabrik_url_finds_smallest_containing_extract(tmp_path, monkeypatch):
    assert find_geofabrik_url_for_pack_box(
        tmp_path, monkeypatch, 0.1, 0.1, 0.9, 0.9
    ) == "small.osm.pbf"


def test_find_geofabrik_url_tolerates_slivers_outside_smallest_extract(tmp_path, monkeypatch):
    # About 50 m outside the small extract, e.g. where a state boundary in the
    # source data differs slightly from the one Geofabrik used
    assert find_geofabrik_url_for_pack_box(
        tmp_path, monkeypatch, 0.1, 0.1, 1.0005, 0.9
    ) == "small.osm.pbf"


def test_find_geofabrik_url_skips_extracts_that_miss_part_of_the_pack(tmp_path, monkeypatch):
    assert find_geofabrik_url_for_pack_box(
        tmp_path, monkeypatch, 0.1, 0.1, 1.5, 0.9
    ) == "big.osm.pbf"
