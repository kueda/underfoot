"""Tests for continuing dead-end waterways along OpenStreetMap waterways."""

from shapely.geometry import Polygon

import waterway_gaps
from tests.test_waterway_names import M_PER_DEG_LAT, M_PER_DEG_LON, LAT0, LON0, lonlat
from waterway_gaps import find_gap_fills

BBOX = {"left": -122.1, "bottom": 37.8, "right": -121.9, "top": 38.0}


def meters(coords):
    """(x, y) meters from the test origin, rounded, for lon/lat coords"""
    return [(round((lon - LON0) * M_PER_DEG_LON), round((lat - LAT0) * M_PER_DEG_LAT))
            for lon, lat in coords]


def waterway(source_id, points_m, hydroseq, dnhydroseq, kind="stream"):
    return {"key": source_id, "source": "nhd", "source_id": source_id, "type": kind,
            "hydroseq": hydroseq, "dnhydroseq": dnhydroseq, "coords": lonlat(*points_m)}


def osm_way(way_id, refs, points_m, name=None, waterway_tag="stream", **tags):
    return {"id": way_id, "name": name, "waterway": waterway_tag,
            "intermittent": tags.get("intermittent"), "tunnel": tags.get("tunnel"),
            "refs": refs, "coords": lonlat(*points_m)}


def box(left, bottom, right, top):
    return Polygon(lonlat((left, bottom), (right, bottom), (right, top), (left, top)))


RESERVOIR = box(-200, 300, 200, 700)
# A stream that stops 300 m short of a reservoir, like the one in #54
DEAD_END = waterway("a", [(0, -500), (0, 0)], hydroseq=10, dnhydroseq=0)
# The path NHD draws through the reservoir
THROUGH_RESERVOIR = waterway("r", [(-150, 500), (300, 500)], 20, 0, kind="artificial")


def downstream_of(*waterways):
    return {w["hydroseq"]: w["dnhydroseq"] for w in waterways}


def fill(waterways, osm_ways, waterbodies=(RESERVOIR,), downstream=None, bbox=BBOX):
    if downstream is None:
        downstream = downstream_of(*waterways)
    return find_gap_fills(waterways, list(waterbodies), osm_ways, downstream, bbox)


def test_dead_end_continues_along_osm_into_the_path_through_a_reservoir():
    ways = [osm_way(7, [1, 2, 3], [(0, -100), (0, 0), (0, 450)])]

    result = fill([DEAD_END, THROUGH_RESERVOIR], ways)

    piece, connector = result["waterways"]
    assert meters(piece["coords"]) == [(0, 0), (0, 300)]
    assert meters(connector["coords"]) == [(0, 300), (0, 500)]
    assert result["flow"] == [
        (piece["source_id"], -1, -2),
        (connector["source_id"], -2, 20),
    ]
    assert result["relinks"] == {("nhd", "a"): -1}


def test_connector_through_a_waterbody_is_like_the_paths_nhd_draws_through_lakes():
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)], name="Culvert Creek", tunnel="culvert")]

    connector = fill([DEAD_END, THROUGH_RESERVOIR], ways)["waterways"][-1]

    assert connector["name"] is None
    assert connector["type"] == "artificial"
    assert connector["is_natural"] == 0
    assert connector["surface"] == "surface"
    assert connector["permanence"] is None


def test_connector_is_not_drawn_across_land():
    """A straight line to the path through a lake with arms could cross land, so the
    continuation still flows into the path but doesn't draw a line to it"""
    lake_with_an_arm = box(-200, 300, 200, 400).union(box(200, 300, 800, 400)).union(
        box(400, 300, 800, 900))
    through_the_arm = waterway("r", [(600, 850), (600, 950)], 20, 0, kind="artificial")
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([DEAD_END, through_the_arm], ways, waterbodies=[lake_with_an_arm])

    [piece] = result["waterways"]
    assert meters(piece["coords"]) == [(0, 0), (0, 300)]
    assert result["flow"] == [(piece["source_id"], -1, 20)]


def test_continuation_keeps_following_osm_past_a_way_that_ends_near_water():
    ways = [
        osm_way(7, [1, 2], [(0, 0), (0, 260)]),
        osm_way(8, [2, 3], [(0, 260), (0, 450)]),
    ]

    result = fill([DEAD_END, THROUGH_RESERVOIR], ways)

    assert [meters(p["coords"]) for p in result["waterways"]] == [
        [(0, 0), (0, 260)], [(0, 260), (0, 300)], [(0, 300), (0, 500)]]


def test_osm_way_that_ends_near_water_flows_into_it():
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 270)])]

    result = fill([DEAD_END, THROUGH_RESERVOIR], ways)

    assert [meters(p["coords"]) for p in result["waterways"]] == [
        [(0, 0), (0, 270)], [(0, 270), (0, 500)]]
    assert result["flow"][-1][2] == 20


def test_continuation_joins_the_first_waterway_it_reaches():
    joined = waterway("b", [(-300, 200), (300, 200)], hydroseq=30, dnhydroseq=99)
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([DEAD_END, joined, THROUGH_RESERVOIR], ways,
                  downstream={**downstream_of(DEAD_END, joined, THROUGH_RESERVOIR), 99: 0})

    [piece] = result["waterways"]
    assert meters(piece["coords"]) == [(0, 0), (0, 190), (0, 200)]
    assert result["flow"] == [(piece["source_id"], -1, 30)]


def test_continuation_starts_where_the_dead_end_ends():
    """The OSM way is a few meters off, so the continuation starts with a short jog
    instead of a gap"""
    ways = [osm_way(7, [1, 2], [(3, -100), (3, 450)])]

    result = fill([DEAD_END, THROUGH_RESERVOIR], ways)

    assert meters(result["waterways"][0]["coords"]) == [(0, 0), (3, 0), (3, 300)]


def test_continuation_crosses_aqueducts_and_siphons_instead_of_flowing_into_them():
    aqueduct = waterway("q", [(-300, 100), (300, 100)], 60, 99, kind="aqueduct")
    siphon = waterway("s", [(-300, 200), (300, 200)], 70, 99, kind="siphon")
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([DEAD_END, aqueduct, siphon, THROUGH_RESERVOIR], ways, downstream={
        **downstream_of(DEAD_END, aqueduct, siphon, THROUGH_RESERVOIR), 99: 0})

    assert result["flow"][-1][2] == 20


def test_continuation_follows_osm_ways_that_share_a_node():
    ways = [
        osm_way(7, [1, 2], [(0, 0), (0, 150)]),
        osm_way(8, [2, 3], [(0, 150), (0, 450)]),
    ]

    result = fill([DEAD_END, THROUGH_RESERVOIR], ways)

    first, second, connector = result["waterways"]
    assert meters(first["coords"]) == [(0, 0), (0, 150)]
    assert meters(second["coords"]) == [(0, 150), (0, 300)]
    assert result["flow"] == [
        (first["source_id"], -1, -2),
        (second["source_id"], -2, -3),
        (connector["source_id"], -3, 20),
    ]
    assert result["relinks"] == {("nhd", "a"): -1}


def test_dead_end_where_an_osm_way_ends_can_join_a_waterway_right_there():
    """All that's left of the OSM way past the dead end is a point, which can still be
    beside a waterway"""
    ways = [
        osm_way(7, [1, 2], [(0, -100), (0, 0)]),
        osm_way(8, [2, 3], [(0, 0), (0, 450)]),
    ]
    beside = waterway("s", [(-100, 8), (100, 8)], hydroseq=30, dnhydroseq=0)

    result = fill([DEAD_END, beside, THROUGH_RESERVOIR], ways)

    [piece] = result["waterways"]
    assert meters(piece["coords"]) == [(0, 0), (0, 8)]
    assert result["flow"] == [(piece["source_id"], -1, 30)]
    assert result["relinks"] == {("nhd", "a"): -1}


def test_continuation_follows_a_way_it_joins_partway_along():
    ways = [
        osm_way(7, [1, 2], [(0, 0), (0, 150)]),
        osm_way(8, [5, 2, 3], [(-100, 150), (0, 150), (0, 450)]),
    ]

    result = fill([DEAD_END, THROUGH_RESERVOIR], ways)

    assert [meters(p["coords"]) for p in result["waterways"]] == [
        [(0, 0), (0, 150)], [(0, 150), (0, 300)], [(0, 300), (0, 500)]]


def test_pieces_carry_the_osm_ways_attributes():
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)], name="Culvert Creek",
                    waterway_tag="canal", tunnel="culvert", intermittent="yes")]

    piece = fill([DEAD_END, THROUGH_RESERVOIR], ways)["waterways"][0]

    assert piece["name"] == "Culvert Creek"
    assert piece["type"] == "canal/ditch"
    assert piece["is_natural"] == 0
    assert piece["surface"] == "underground"
    assert piece["permanence"] == "intermittent"


def test_culverted_streams_are_man_made():
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)], tunnel="culvert")]

    piece = fill([DEAD_END, THROUGH_RESERVOIR], ways)["waterways"][0]

    assert piece["type"] == "stream"
    assert piece["is_natural"] == 0
    assert piece["surface"] == "underground"


def test_natural_streams_are_natural_surface_waterways_of_unknown_permanence():
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    piece = fill([DEAD_END, THROUGH_RESERVOIR], ways)["waterways"][0]

    assert piece["name"] is None
    assert piece["type"] == "stream"
    assert piece["is_natural"] == 1
    assert piece["surface"] == "surface"
    assert piece["permanence"] is None


def test_continuation_into_a_waterbody_without_a_path_through_it_is_an_outlet():
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([DEAD_END], ways)

    [piece] = result["waterways"]
    assert result["flow"] == [(piece["source_id"], -1, 0)]


def test_continuation_into_a_bay_does_not_join_another_creek_that_ends_in_it():
    """NHD draws creeks a little way into the bay they end in, so a waterway in the bay
    isn't a path through it"""
    creek_mouth = waterway("c", [(100, 350), (100, 450)], hydroseq=30, dnhydroseq=0,
                           kind="artificial")
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([DEAD_END, creek_mouth], ways)

    [piece] = result["waterways"]
    assert result["flow"] == [(piece["source_id"], -1, 0)]


def test_continuation_into_a_waterbody_does_not_join_a_creek_that_only_reaches_it():
    """Like a creek whose mouth touches a bay and that NHD continues along the coastline,
    which isn't in the waterways"""
    other_creek = waterway("c", [(300, -500), (100, 320)], hydroseq=30, dnhydroseq=99)
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([DEAD_END, other_creek], ways,
                  downstream={**downstream_of(DEAD_END, other_creek), 99: 0})

    [piece] = result["waterways"]
    assert result["flow"] == [(piece["source_id"], -1, 0)]


def test_continuation_into_a_lake_joins_a_path_that_flows_out_further_downstream():
    in_lake = waterway("r", [(-150, 500), (150, 500)], hydroseq=20, dnhydroseq=21)
    outflow = waterway("o", [(150, 500), (400, 500)], hydroseq=21, dnhydroseq=0)
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([DEAD_END, in_lake, outflow], ways)

    assert result["flow"][-1][2] == 20


def test_continuation_into_a_lake_joins_a_path_that_flows_beyond_the_waterways_read():
    in_lake = waterway("r", [(-150, 500), (150, 500)], hydroseq=20, dnhydroseq=99)
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([DEAD_END, in_lake], ways,
                  downstream={**downstream_of(DEAD_END, in_lake), 99: 0})

    assert result["flow"][-1][2] == 20


def test_dead_end_without_an_osm_waterway_nearby_is_left_alone():
    ways = [osm_way(7, [1, 2], [(100, 0), (100, 450)])]

    assert fill([DEAD_END, THROUGH_RESERVOIR], ways) == {
        "waterways": [], "flow": [], "relinks": {}}


def test_osm_waterway_that_goes_nowhere_is_not_used():
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 200)])]

    assert fill([DEAD_END, THROUGH_RESERVOIR], ways)["relinks"] == {}


def test_waterway_that_ends_near_water_is_not_a_dead_end():
    ends_at_shore = waterway("a", [(0, -500), (0, 270)], hydroseq=10, dnhydroseq=0)
    ways = [osm_way(7, [1, 2], [(0, 270), (0, 450)])]

    assert fill([ends_at_shore, THROUGH_RESERVOIR], ways)["relinks"] == {}


def test_waterway_that_flows_into_another_is_not_a_dead_end():
    flows_on = waterway("a", [(0, -500), (0, 0)], hydroseq=10, dnhydroseq=99)
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([flows_on, THROUGH_RESERVOIR], ways,
                  downstream={**downstream_of(flows_on, THROUGH_RESERVOIR), 99: 0})

    assert result["relinks"] == {}


def test_dead_end_outside_the_bbox_is_left_alone():
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]
    bbox = {"left": -121.5, "bottom": 37.8, "right": -121.4, "top": 38.0}

    assert fill([DEAD_END, THROUGH_RESERVOIR], ways, bbox=bbox)["relinks"] == {}


def test_continuation_that_loops_back_upstream_is_not_used():
    upstream = waterway("u", [(0, -1000), (0, -500)], hydroseq=5, dnhydroseq=10)
    ways = [osm_way(7, [1, 2, 3, 4], [(0, 0), (300, 0), (300, -800), (5, -800)])]

    assert fill([upstream, DEAD_END, THROUGH_RESERVOIR], ways)["relinks"] == {}


def test_continuation_does_not_join_another_dead_end_where_it_ends():
    other_dead_end = waterway("d", [(300, 5), (8, 5)], hydroseq=50, dnhydroseq=0)
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]

    result = fill([DEAD_END, other_dead_end, THROUGH_RESERVOIR], ways)

    assert result["relinks"][("nhd", "a")] == -1
    downstream = {hydroseq: dn for _, hydroseq, dn in result["flow"]}
    assert (downstream[-1], downstream[-2]) == (-2, 20)


def test_second_dead_end_joins_the_first_ones_continuation():
    second = waterway("b", [(100, -500), (100, 100)], hydroseq=40, dnhydroseq=0)
    ways = [
        osm_way(7, [1, 2], [(0, 0), (0, 450)]),
        osm_way(8, [3, 4], [(100, 100), (0, 150)]),
    ]

    result = fill([DEAD_END, second, THROUGH_RESERVOIR], ways)

    first_piece, connector, second_piece = result["waterways"]
    assert meters(second_piece["coords"])[-1] == (0, 145)
    assert result["relinks"] == {("nhd", "a"): -1, ("nhd", "b"): -3}
    assert result["flow"] == [
        (first_piece["source_id"], -1, -2),
        (connector["source_id"], -2, 20),
        (second_piece["source_id"], -3, -1),
    ]


def test_dead_end_that_nearly_touches_a_waterway_joins_it_with_a_short_piece():
    joined = waterway("b", [(-300, 8), (300, 8)], hydroseq=30, dnhydroseq=99)
    ways = [osm_way(7, [1, 2], [(0, -100), (0, 450)])]

    result = fill([DEAD_END, joined, THROUGH_RESERVOIR], ways,
                  downstream={**downstream_of(DEAD_END, joined, THROUGH_RESERVOIR), 99: 0})

    [piece] = result["waterways"]
    assert meters(piece["coords"]) == [(0, 0), (0, 8)]
    assert result["flow"] == [(piece["source_id"], -1, 30)]
    assert result["relinks"] == {("nhd", "a"): -1}


def test_apply_osm_gap_fills_writes_the_fills(monkeypatch):
    ways = [osm_way(7, [1, 2], [(0, 0), (0, 450)])]
    written = []
    monkeypatch.setattr(
        waterway_gaps, "read_network",
        lambda bbox: ([DEAD_END, THROUGH_RESERVOIR], [RESERVOIR],
                      downstream_of(DEAD_END, THROUGH_RESERVOIR)))
    monkeypatch.setattr(waterway_gaps, "write_gap_fills", written.append)

    filled = waterway_gaps.apply_osm_gap_fills(ways, BBOX)

    assert filled == 1
    [result] = written
    assert result["relinks"] == {("nhd", "a"): -1}
