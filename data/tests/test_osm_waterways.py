"""Tests for building waterways and their flow from OpenStreetMap alone."""

from shapely.geometry import Polygon

import osm_waterways
from osm_waterways import build_network, link_lakes
from tests.test_waterway_names import lonlat


def osm_way(way_id, refs, points_m, name=None, waterway="stream", **tags):
    return {"id": way_id, "name": name, "waterway": waterway,
            "intermittent": tags.get("intermittent"), "tunnel": tags.get("tunnel"),
            "refs": refs, "coords": lonlat(*points_m)}


def by_id(network):
    return {w["source_id"]: w for w in network}


def flows_into(network, source_id):
    """source_id of the waterway source_id flows into, or None"""
    waterways = by_id(network)
    by_hydroseq = {w["hydroseq"]: w["source_id"] for w in network}
    return by_hydroseq.get(waterways[source_id]["dnhydroseq"])


def box(left, bottom, right, top):
    return Polygon(lonlat((left, bottom), (right, bottom), (right, top), (left, top)))


def test_ways_are_split_where_other_ways_join_them():
    network = build_network([
        osm_way(1, [1, 2, 3], [(0, 0), (0, 100), (0, 200)]),
        osm_way(2, [4, 2], [(100, 100), (0, 100)]),
    ])

    assert sorted(by_id(network)) == ["1-1", "1-2", "2-1"]
    assert by_id(network)["1-1"]["coords"] == lonlat((0, 0), (0, 100))
    assert flows_into(network, "1-1") == "1-2"
    assert flows_into(network, "2-1") == "1-2"
    assert flows_into(network, "1-2") is None
    assert by_id(network)["1-2"]["dnhydroseq"] == 0


def test_flow_continues_along_the_same_way_where_it_forks():
    network = build_network([
        osm_way(1, [1, 2, 3], [(0, 0), (0, 100), (100, 150)]),
        osm_way(2, [2, 4], [(0, 100), (0, 200)]),
    ])

    assert flows_into(network, "1-1") == "1-2"


def test_flow_continues_along_the_same_name_where_it_forks():
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 100)], name="Baden Creek"),
        osm_way(2, [2, 3], [(0, 100), (100, 200)], name="Baden Creek"),
        osm_way(3, [2, 4], [(0, 100), (0, 200)]),
    ])

    assert flows_into(network, "1-1") == "2-1"


def test_flow_prefers_streams_to_canals_where_it_forks():
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 100)]),
        osm_way(2, [2, 3], [(0, 100), (0, 200)], waterway="canal"),
        osm_way(3, [2, 4], [(0, 100), (100, 200)]),
    ])

    assert flows_into(network, "1-1") == "3-1"


def test_flow_prefers_the_straightest_way_where_it_forks():
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 100)]),
        osm_way(2, [2, 3], [(0, 100), (100, 100)]),
        osm_way(3, [2, 4], [(0, 100), (10, 200)]),
    ])

    assert flows_into(network, "1-1") == "3-1"


def test_osm_tags_map_to_waterway_attributes():
    network = by_id(build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 100)], name="Big River", waterway="river"),
        osm_way(2, [3, 4], [(0, 0), (0, 100)], waterway="ditch", intermittent="yes"),
        osm_way(3, [5, 6], [(0, 0), (0, 100)], tunnel="culvert", intermittent="no"),
        osm_way(4, [7, 8], [(0, 0), (0, 100)], waterway="drain"),
    ]))

    assert {k: network["1-1"][k] for k in ("name", "type", "is_natural", "permanence",
                                           "surface")} == {
        "name": "Big River", "type": "stream", "is_natural": 1, "permanence": None,
        "surface": "surface"}
    assert (network["2-1"]["type"], network["2-1"]["is_natural"],
            network["2-1"]["permanence"]) == ("canal/ditch", 0, "intermittent")
    assert (network["3-1"]["type"], network["3-1"]["is_natural"],
            network["3-1"]["surface"], network["3-1"]["permanence"]) == (
        "stream", 0, "underground", "perennial")
    assert network["4-1"]["type"] == "canal/ditch"


LAKE = ("reservoir", box(-200, 300, 200, 700))


def test_streams_into_a_lake_flow_into_its_outflow():
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(0, 710), (0, 1000)]),
    ])

    link_lakes(network, [LAKE])

    assert flows_into(network, "1-1") == "2-1"


def test_streams_into_a_lake_flow_into_its_outflow_in_a_large_network():
    """Splitting ways where many tributaries join, far from the lake, mustn't change how
    the lake lines up with the waterways"""
    main = [(5000, 100000 + i * 100) for i in range(50)]
    tributaries = [
        osm_way(100 + i, [200 + i, 300 + i], [(5100, point[1]), point])
        for i, point in enumerate(main[1:-1], 1)
    ]
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(0, 710), (0, 1000)]),
        osm_way(3, [300 + i for i in range(len(main))], main),
        *tributaries,
    ])

    link_lakes(network, [LAKE])

    assert flows_into(network, "1-1") == "2-1"


def test_lake_without_an_outflow_is_where_streams_end():
    network = build_network([osm_way(1, [1, 2], [(0, 0), (0, 290)])])

    link_lakes(network, [LAKE])

    assert by_id(network)["1-1"]["dnhydroseq"] == 0


def test_lake_outflow_prefers_streams_to_canals():
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(-100, 710), (-100, 1000)], waterway="canal"),
        osm_way(3, [5, 6], [(100, 710), (100, 1000)]),
    ])

    link_lakes(network, [LAKE])

    assert flows_into(network, "1-1") == "3-1"


def test_lake_outflow_prefers_one_that_reaches_water_to_a_dead_end():
    """Like a dam's outlet works rather than a longer tunnel that ends at its portal"""
    marsh = ("swamp/marsh", box(50, 820, 150, 900))
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(-100, 710), (-100, 2000)]),
        osm_way(3, [5, 6], [(100, 710), (100, 800)]),
    ])

    link_lakes(network, [LAKE, marsh])

    assert flows_into(network, "1-1") == "3-1"


def test_lake_outflow_can_start_closer_to_a_pond_beside_the_lake():
    """Like a creek leaving a dam from a pond at its intake"""
    pond = ("lake/pond", box(80, 705, 120, 740))
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(-100, 710), (-100, 1000)], waterway="canal"),
        osm_way(3, [5, 6], [(100, 710), (100, 1000)]),
    ])

    link_lakes(network, [LAKE, pond])

    assert flows_into(network, "1-1") == "3-1"


def test_lake_outflow_that_leaves_the_bbox_is_not_a_dead_end():
    """Its ways continue beyond the OSM waterways we read"""
    [(left, bottom), (right, top)] = lonlat((-5000, -5000), (200, 5000))
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(-100, 710), (-100, 1000)]),
        osm_way(3, [5, 6], [(100, 710), (250, 760)]),
    ])

    link_lakes(network, [LAKE],
               bbox={"left": left, "bottom": bottom, "right": right, "top": top})

    assert flows_into(network, "1-1") == "3-1"


def test_lake_outflow_prefers_the_name_of_the_stream_flowing_in():
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)], name="Coyote Creek"),
        osm_way(2, [3, 4], [(-100, 710), (-100, 2000)]),
        osm_way(3, [5, 6], [(100, 710), (100, 1000)], name="coyote creek"),
    ])

    link_lakes(network, [LAKE])

    assert flows_into(network, "1-1") == "3-1"


def test_lake_outflow_prefers_the_longest_way_downstream():
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(-100, 710), (-100, 1000)]),
        osm_way(3, [5, 6], [(100, 710), (100, 800)]),
        osm_way(4, [6, 7], [(100, 800), (100, 2000)]),
    ])

    link_lakes(network, [LAKE])

    assert flows_into(network, "1-1") == "3-1"


def test_stream_ending_at_a_lake_is_not_its_outflow_even_if_it_starts_near_it():
    """Like a creek whose last piece starts at a junction near the shore"""
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [5, 6], [(300, 100), (100, 260)]),
        osm_way(3, [6, 7], [(100, 260), (100, 290)]),
        osm_way(4, [8, 9], [(0, 710), (0, 1000)], waterway="canal"),
    ])

    link_lakes(network, [LAKE])

    assert flows_into(network, "1-1") == "4-1"


def test_streams_into_a_bay_do_not_flow_into_sloughs_leaving_it():
    bay = ("lake/pond", box(-5000, 300, 5000, 5000))
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(1000, 290), (1000, 0)]),
    ])

    link_lakes(network, [bay])

    assert by_id(network)["1-1"]["dnhydroseq"] == 0


def test_lake_outflow_that_flows_back_into_the_lake_is_not_linked():
    network = build_network([
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 1], [(0, 710), (400, 710), (400, 0), (0, 0)]),
    ])

    link_lakes(network, [LAKE])

    assert by_id(network)["1-1"]["dnhydroseq"] == 0


def test_apply_osm_network_writes_the_linked_network(monkeypatch):
    ways = [
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(0, 710), (0, 1000)]),
    ]
    bbox = {"left": -122.1, "bottom": 37.8, "right": -121.9, "top": 38.0}
    read, written = [], []
    monkeypatch.setattr(
        osm_waterways, "read_waterbodies", lambda b: read.append(b) or [LAKE])
    monkeypatch.setattr(osm_waterways, "write_network", written.append)

    count = osm_waterways.apply_osm_network(ways, bbox)

    assert count == 2
    [network] = written
    assert flows_into(network, "1-1") == "2-1"
    assert read == [osm_waterways.widen_bbox(bbox, osm_waterways.OSM_BBOX_MARGIN_DEG)]


def test_apply_osm_network_knows_outflows_leaving_the_ways_it_read_go_somewhere(monkeypatch):
    ways = [
        osm_way(1, [1, 2], [(0, 0), (0, 290)]),
        osm_way(2, [3, 4], [(-100, 710), (-100, 1000)]),
        osm_way(3, [5, 6], [(100, 710), (250, 760)]),
    ]
    # Widened to where the ways were read from, the bbox's right edge is at x = 200
    [(edge, _)] = lonlat((200, 0))
    bbox = {"left": -122.1, "bottom": 37.8, "right": edge - osm_waterways.OSM_BBOX_MARGIN_DEG,
            "top": 38.0}
    written = []
    monkeypatch.setattr(osm_waterways, "read_waterbodies", lambda b: [LAKE])
    monkeypatch.setattr(osm_waterways, "write_network", written.append)

    osm_waterways.apply_osm_network(ways, bbox)

    [network] = written
    assert flows_into(network, "1-1") == "3-1"
