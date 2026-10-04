"""Tests for create_pack.py helpers that don't need to download anything."""

import json
import os
import re
import subprocess
import sys

import pytest

import create_pack
from sources.util import tiger_water


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


def test_find_geofabrik_url_tolerates_wider_slivers_outside_smallest_extract(
    tmp_path, monkeypatch
):
    # About 200 m outside the small extract, like the OSM outline of New York
    # overhanging Geofabrik's New York polygon
    assert find_geofabrik_url_for_pack_box(
        tmp_path, monkeypatch, 0.1, 0.1, 1.002, 0.9
    ) == "small.osm.pbf"


def write_candidate_index(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_feature_collection(tmp_path / "geofabrik_index.geojson", [
        box_feature(-10, -10, 10, 10, {"id": "big", "urls": {"pbf": "big.osm.pbf"}}),
        box_feature(0, 0, 1, 1, {"id": "small", "urls": {"pbf": "small.osm.pbf"}}),
        box_feature(5, 5, 6, 6, {"id": "elsewhere", "urls": {"pbf": "elsewhere.osm.pbf"}})
    ])
    pack_path = tmp_path / "pack.geojson"
    # Mostly inside the small extract, all inside the big one
    write_feature_collection(pack_path, [box_feature(0.1, 0.1, 1.05, 0.9)])
    return str(pack_path)


def test_find_geofabrik_candidates_lists_intersecting_extracts_smallest_first(
    tmp_path, monkeypatch
):
    pack_path = write_candidate_index(tmp_path, monkeypatch)
    candidates = create_pack.find_geofabrik_candidates(pack_path)
    assert [c["url"] for c in candidates] == ["small.osm.pbf", "big.osm.pbf"]
    assert round(candidates[0]["coverage"], 3) == round(0.9 / 0.95, 3)
    assert round(candidates[1]["coverage"], 3) == 1


SIZES = {"small.osm.pbf": 100_000_000, "big.osm.pbf": 20_000_000_000}


def mock_sizes(monkeypatch, sizes=None):
    monkeypatch.setattr(
        create_pack, "get_content_length", lambda url: (sizes or SIZES).get(url)
    )


def test_choose_osm_url_returns_extract_under_size_limit_without_prompting(
    tmp_path, monkeypatch
):
    pack_path = write_candidate_index(tmp_path, monkeypatch)
    mock_sizes(monkeypatch, {"big.osm.pbf": 100_000_000})

    def fail_input(prompt):
        raise AssertionError(f"Should not prompt: {prompt}")
    monkeypatch.setattr("builtins.input", fail_input)
    assert create_pack.choose_osm_url(pack_path, interactive=True) == "big.osm.pbf"


def test_choose_osm_url_refuses_huge_extract_when_not_interactive(tmp_path, monkeypatch):
    pack_path = write_candidate_index(tmp_path, monkeypatch)
    mock_sizes(monkeypatch)
    with pytest.raises(ValueError) as err:
        create_pack.choose_osm_url(pack_path, interactive=False)
    assert "--osm" in str(err.value)
    assert "small.osm.pbf" in str(err.value)


def test_choose_osm_url_lets_user_pick_a_candidate_by_number(tmp_path, monkeypatch):
    pack_path = write_candidate_index(tmp_path, monkeypatch)
    mock_sizes(monkeypatch)
    monkeypatch.setattr("builtins.input", lambda prompt: "1")
    assert create_pack.choose_osm_url(pack_path, interactive=True) == "small.osm.pbf"


def test_choose_osm_url_lets_user_paste_a_url(tmp_path, monkeypatch):
    pack_path = write_candidate_index(tmp_path, monkeypatch)
    mock_sizes(monkeypatch)
    url = "https://example.com/custom.osm.pbf"
    monkeypatch.setattr("builtins.input", lambda prompt: url)
    assert create_pack.choose_osm_url(pack_path, interactive=True) == url


def test_choose_osm_url_lets_user_accept_the_huge_extract(tmp_path, monkeypatch):
    pack_path = write_candidate_index(tmp_path, monkeypatch)
    mock_sizes(monkeypatch)
    monkeypatch.setattr("builtins.input", lambda prompt: "")
    assert create_pack.choose_osm_url(pack_path, interactive=True) == "big.osm.pbf"


def test_create_pack_specifies_water_sources_by_identifier_without_source_files(tmp_path):
    """Water sources in the pack are identifiers that water.process_source
    resolves on its own, so create_pack.py shouldn't write any source scripts"""
    (tmp_path / "packs").mkdir()
    rock_work_path = tmp_path / "sources" / "work-rock_source"
    rock_work_path.mkdir(parents=True)
    write_feature_collection(rock_work_path / "units.geojson", [box_feature(0, 0, 1, 1)])
    write_feature_collection(tmp_path / "simplified_wbd_hu4.geojson", [
        box_feature(-1, -1, 0.5, 2, {"huc4": "1801"}),
        box_feature(0.5, -1, 2, 2, {"huc4": "1802"}),
        box_feature(5, 5, 6, 6, {"huc4": "1803"})
    ])
    write_feature_collection(tmp_path / create_pack.TIGER_COUNTIES_GEOJSON_PATH, [
        box_feature(-1, -1, 2, 2, {"GEOID": "06001"}),
        box_feature(5, 5, 6, 6, {"GEOID": "06003"})
    ])
    files_before = {path for path in tmp_path.rglob("*")}

    subprocess.run(
        [
            sys.executable, os.path.abspath(create_pack.__file__), "rock_source",
            "--id", "test-pack", "--osm", "https://example.com/test.osm.pbf"
        ],
        cwd=tmp_path, check=True
    )

    with open(tmp_path / "packs" / "test-pack.json", encoding="utf-8") as pack_file:
        pack = json.load(pack_file)
    assert pack["water"] == [
        "nhdplus_h_1801_hu4", "nhdplus_h_1802_hu4", "tiger_water_06001"
    ]
    assert pack["osm"] == "https://example.com/test.osm.pbf"
    new_files = {path for path in tmp_path.rglob("*")} - files_before
    assert new_files == {
        tmp_path / "packs" / "test-pack.json",
        tmp_path / "packs" / "test-pack.geojson"
    }


class StopDownload(Exception):
    pass


def test_tiger_counties_match_the_tiger_water_vintage(tmp_path, monkeypatch):
    """Regression test: county boundaries came from 2022, when Connecticut's
    counties became planning regions with new GEOIDs, so packs got
    tiger_water_<GEOID> sources that the 2020 TIGER water files don't have"""
    monkeypatch.chdir(tmp_path)
    urls = []

    def fake_download_file(url, path):
        urls.append(url)
        raise StopDownload()

    monkeypatch.setattr(create_pack, "download_file", fake_download_file)
    monkeypatch.setattr(tiger_water, "download_file", fake_download_file)
    monkeypatch.setattr(tiger_water, "make_work_dir", lambda path: str(tmp_path))
    with pytest.raises(StopDownload):
        create_pack.ensure_tiger_counties()
    with pytest.raises(StopDownload):
        tiger_water.download("09001")
    counties_url, water_url = urls
    counties_year = re.search(r"cb_(\d{4})_us_county", counties_url).group(1)
    water_year = re.search(r"tl_(\d{4})_09001_areawater", water_url).group(1)
    assert counties_year == water_year


def test_ensure_tiger_counties_ignores_counties_cached_from_another_vintage(
        tmp_path, monkeypatch):
    """A cached tiger_counties.geojson from 2022 would keep giving packs
    Connecticut planning region GEOIDs"""
    monkeypatch.chdir(tmp_path)
    write_feature_collection(tmp_path / "tiger_counties.geojson", [
        box_feature(0, 0, 1, 1, {"GEOID": "09150"})
    ])

    def fake_download_file(url, path):
        raise StopDownload()

    monkeypatch.setattr(create_pack, "download_file", fake_download_file)
    with pytest.raises(StopDownload):
        create_pack.ensure_tiger_counties()
