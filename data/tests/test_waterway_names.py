"""Tests for naming waterways from OpenStreetMap."""

import math

import osmium
import pytest

import waterway_names
from waterway_names import match_names, read_osm_waterways

LON0, LAT0 = -122.0, 37.9
M_PER_DEG_LAT = 110540
M_PER_DEG_LON = 111320 * math.cos(math.radians(LAT0))


def lonlat(*points_m):
    """Line from (x, y) points in meters east and north of an arbitrary origin"""
    return [(LON0 + x / M_PER_DEG_LON, LAT0 + y / M_PER_DEG_LAT) for x, y in points_m]


def waterway(key, points_m, name=None, kind="stream"):
    return {"key": key, "name": name, "type": kind, "coords": lonlat(*points_m)}


def osm_way(way_id, points_m, name=None, waterway_tag="stream"):
    return {"id": way_id, "name": name, "waterway": waterway_tag, "coords": lonlat(*points_m)}


def test_unnamed_waterway_alongside_a_named_osm_way_gets_its_name():
    waterways = [waterway(1, [(0, 5), (1000, 5)])]
    ways = [osm_way(10, [(0, 0), (1000, 0)], "Ellinwood Creek")]
    assert match_names(waterways, ways) == {1: "Ellinwood Creek"}


def test_osm_name_overrides_the_waterways_name():
    waterways = [waterway(1, [(0, 5), (1000, 5)], name="Walnut Creek")]
    ways = [osm_way(10, [(0, 0), (1000, 0)], "Ellinwood Creek")]
    assert match_names(waterways, ways) == {1: "Ellinwood Creek"}


def test_reversed_osm_way_still_matches():
    waterways = [waterway(1, [(1000, 5), (0, 5)])]
    ways = [osm_way(10, [(0, 0), (1000, 0)], "Ellinwood Creek")]
    assert match_names(waterways, ways) == {1: "Ellinwood Creek"}


def test_name_is_taken_from_several_osm_ways_that_share_it():
    waterways = [waterway(1, [(0, 5), (1000, 5)])]
    ways = [
        osm_way(10, [(0, 0), (500, 0)], "Ellinwood Creek"),
        osm_way(11, [(500, 0), (1000, 0)], "Ellinwood Creek"),
    ]
    assert match_names(waterways, ways) == {1: "Ellinwood Creek"}


def test_waterway_far_from_any_osm_way_is_not_matched():
    waterways = [waterway(1, [(0, 100), (1000, 100)])]
    ways = [osm_way(10, [(0, 0), (1000, 0)], "Ellinwood Creek")]
    assert match_names(waterways, ways) == {}


def test_waterway_only_partly_alongside_an_osm_way_is_not_matched():
    waterways = [waterway(1, [(0, 0), (1000, 0)])]
    ways = [osm_way(10, [(0, 0), (500, 0)], "Ellinwood Creek")]
    assert match_names(waterways, ways) == {}


def test_best_overlap_unnamed_osm_way_beats_a_named_neighbor():
    waterways = [waterway(1, [(0, 0), (1000, 0)])]
    ways = [
        osm_way(10, [(0, 0), (1000, 0)]),
        osm_way(11, [(0, 20), (850, 20)], "Neighbor Creek"),
    ]
    assert match_names(waterways, ways) == {}


def test_two_differently_named_osm_ways_in_the_same_place_are_ambiguous():
    waterways = [waterway(1, [(0, 0), (1000, 0)])]
    ways = [
        osm_way(10, [(0, 15), (1000, 15)], "Creek C"),
        osm_way(11, [(0, -15), (1000, -15)], "Creek D"),
    ]
    assert match_names(waterways, ways) == {}


@pytest.mark.parametrize("kind", ["siphon", "aqueduct", "connector"])
def test_waterway_types_that_cross_streams_are_not_matched(kind):
    waterways = [waterway(1, [(0, 5), (1000, 5)], kind=kind)]
    ways = [osm_way(10, [(0, 0), (1000, 0)], "Ellinwood Creek")]
    assert match_names(waterways, ways) == {}


def test_short_waterway_is_not_matched():
    waterways = [waterway(1, [(500, 5), (530, 5)])]
    ways = [osm_way(10, [(0, 0), (1000, 0)], "Ellinwood Creek")]
    assert match_names(waterways, ways) == {}


def test_waterway_crossing_an_osm_way_at_an_angle_is_not_matched():
    run = 60
    dx, dy = run * math.cos(math.radians(40)), run * math.sin(math.radians(40))
    waterways = [waterway(1, [(100 - dx / 2, -dy / 2), (100 + dx / 2, dy / 2)])]
    ways = [osm_way(10, [(0, 0), (200, 0)], "Ellinwood Creek")]
    assert match_names(waterways, ways) == {}


def test_short_waterway_between_two_matched_ones_takes_their_name():
    waterways = [
        waterway(1, [(0, 2), (500, 2)]),
        waterway(2, [(500, 2), (530, 2)]),
        waterway(3, [(530, 2), (1030, 2)]),
    ]
    ways = [osm_way(10, [(0, 0), (1030, 0)], "Ellinwood Creek")]
    assert match_names(waterways, ways) == {
        1: "Ellinwood Creek", 2: "Ellinwood Creek", 3: "Ellinwood Creek"}


def test_short_waterway_between_differently_named_ones_stays_unnamed():
    waterways = [
        waterway(1, [(0, 2), (500, 2)]),
        waterway(2, [(500, 2), (530, 2)]),
        waterway(3, [(530, 2), (1030, 2)]),
    ]
    ways = [
        osm_way(10, [(0, 0), (500, 0)], "Creek A"),
        osm_way(11, [(530, 0), (1030, 0)], "Creek B"),
    ]
    assert match_names(waterways, ways) == {1: "Creek A", 3: "Creek B"}


def test_short_waterway_is_not_renamed_alone():
    """A short piece of a stream NHD names one thing, whose neighbor OSM names another,
    is more likely a mismatch than a correction"""
    waterways = [
        waterway(1, [(0, 2), (135, 2)], name="Shephard Creek"),
        waterway(2, [(135, 2), (1900, 2)], name="Shephard Creek"),
    ]
    ways = [
        osm_way(10, [(0, 0), (135, 0)], "Sausal Creek"),
        osm_way(11, [(135, 0), (1900, 0)], "Shepherd Creek"),
    ]
    assert match_names(waterways, ways) == {2: "Shepherd Creek"}


def test_short_waterways_are_renamed_when_their_neighbor_is_renamed_too():
    waterways = [
        waterway(1, [(0, 2), (150, 2)], name="Country Club Branch"),
        waterway(2, [(150, 2), (300, 2)], name="Country Club Branch"),
    ]
    ways = [osm_way(10, [(0, 0), (300, 0)], "Arroyo Viejo Country Club Branch")]
    assert match_names(waterways, ways) == {
        1: "Arroyo Viejo Country Club Branch", 2: "Arroyo Viejo Country Club Branch"}


def write_pbf(path, ways, nodes):
    """ways: {id: (tags, [node ids])}, nodes: {id: (lon, lat)}"""
    with osmium.SimpleWriter(str(path)) as writer:
        for node_id, location in nodes.items():
            writer.add_node(osmium.osm.mutable.Node(id=node_id, location=location))
        for way_id, (tags, node_ids) in ways.items():
            writer.add_way(osmium.osm.mutable.Way(id=way_id, nodes=node_ids, tags=tags))


def test_read_osm_waterways_returns_rivers_streams_and_canals_inside_the_bbox(tmp_path):
    pbf = tmp_path / "test.osm.pbf"
    write_pbf(
        pbf,
        ways={
            1: ({"waterway": "stream", "name": "Ellinwood Creek", "intermittent": "yes"},
                [1, 2]),
            2: ({"waterway": "river", "tunnel": "culvert"}, [2, 3]),
            3: ({"waterway": "stream", "name": "Far Creek"}, [4, 5]),
            4: ({"highway": "residential", "name": "Main St"}, [1, 2]),
            5: ({"waterway": "drain", "name": "Storm Drain"}, [1, 3]),
        },
        nodes={
            1: (-122.0, 37.9), 2: (-122.001, 37.901), 3: (-122.002, 37.902),
            4: (-100.0, 40.0), 5: (-100.001, 40.001),
        })
    bbox = {"left": -122.1, "bottom": 37.8, "right": -121.9, "top": 38.0}

    ways = {w["id"]: w for w in read_osm_waterways(str(pbf), bbox)}

    assert set(ways) == {1, 2}
    assert ways[1]["name"] == "Ellinwood Creek"
    assert ways[1]["waterway"] == "stream"
    assert ways[1]["coords"] == [(-122.0, 37.9), (-122.001, 37.901)]
    assert ways[1]["refs"] == [1, 2]
    assert ways[1]["intermittent"] == "yes"
    assert ways[1]["tunnel"] is None
    assert ways[2]["name"] is None
    assert ways[2]["tunnel"] == "culvert"


def test_widen_bbox_adds_the_margin_on_every_side():
    bbox = {"left": -122.1, "bottom": 37.8, "right": -121.9, "top": 38.0}
    assert waterway_names.widen_bbox(bbox, 0.5) == {
        "left": -122.6, "bottom": 37.3, "right": -121.4, "top": 38.5}


def test_apply_osm_names_writes_only_names_that_change(monkeypatch):
    ways = [osm_way(10, [(0, 0), (1000, 0)], "Ellinwood Creek")]
    rows = [
        waterway(1, [(0, 5), (1000, 5)], name="Walnut Creek"),
        waterway(2, [(0, 8), (1000, 8)], name="Ellinwood Creek"),
        waterway(3, [(0, 500), (1000, 500)], name="Elsewhere Creek"),
    ]
    bbox = {"left": -122.1, "bottom": 37.8, "right": -121.9, "top": 38.0}
    read_bboxes, written = [], []
    monkeypatch.setattr(
        waterway_names, "read_waterway_rows", lambda b: read_bboxes.append(b) or rows)
    monkeypatch.setattr(waterway_names, "write_names", written.append)

    changed = waterway_names.apply_osm_names(ways, bbox)

    assert changed == 1
    assert written == [{1: "Ellinwood Creek"}]
    assert read_bboxes == [bbox]
