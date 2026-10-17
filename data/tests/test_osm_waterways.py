"""Tests for reading waterways from OpenStreetMap."""

import osmium

from osm_waterways import read_osm_waterways


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


def test_read_osm_waterways_can_read_other_kinds_of_waterways(tmp_path):
    pbf = tmp_path / "test.osm.pbf"
    write_pbf(
        pbf,
        ways={
            1: ({"waterway": "stream"}, [1, 2]),
            2: ({"waterway": "drain"}, [1, 2]),
            3: ({"waterway": "dam"}, [1, 2]),
        },
        nodes={1: (-122.0, 37.9), 2: (-122.001, 37.901)})
    bbox = {"left": -122.1, "bottom": 37.8, "right": -121.9, "top": 38.0}

    ways = read_osm_waterways(str(pbf), bbox, kinds=("stream", "drain"))

    assert sorted(w["id"] for w in ways) == [1, 2]
