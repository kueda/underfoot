"""Waterways and their flow from OpenStreetMap alone

Where OSM maps waterways well, as in the San Francisco Bay Area, it has more of them than
NHD, with more names, and it gives the direction of flow: by convention a waterway is
drawn in the direction it flows. So for those packs we can use OSM's waterways instead
of NHD's, which avoids the seams of mixing the two.

Ways are split where other ways join them, and each piece flows into one of the pieces
leaving its end, preferring the same way, then the same name, then rivers and streams
over canals and ditches, then the straightest. That makes the flow a tree, like the
main path NHD's flow data follows.

OSM rarely draws paths through lakes, so streams that end at a lake flow into the
waterway leaving it, if there is one. Huge waterbodies like bays are left alone, since
the creeks that end in them don't flow out through anything.
"""

import math
from collections import defaultdict

import psycopg2
from psycopg2.extras import execute_values
from shapely import wkb
from shapely.geometry import LineString, Point
from shapely.strtree import STRtree

from database import DBNAME, SRID
from sources import util
from sources.util.citations import CITATIONS_TABLE_NAME, create_table as create_citations_table
from waterway_gaps import OSM_BBOX_MARGIN_DEG, OSM_CITATION, OSM_SOURCE
from waterway_names import Projection, widen_bbox

# Same tables water.py loads
WATERWAYS_TABLE_NAME = "waterways"
WATERBODIES_TABLE_NAME = "waterbodies"
WATERWAYS_FLOW_TABLE_NAME = "waterways_flow"
OSM_WATERWAY_KINDS = ("river", "stream", "canal", "ditch", "drain")
# Where flow forks, it follows these before those
KIND_RANK = {"river": 0, "stream": 0, "canal": 1, "ditch": 2, "drain": 3}
LAKE_TYPES = {"lake/pond", "reservoir"}
# A stream ending this close to a lake flows into it, and a waterway starting this close
# to one flows out of it
NEAR_LAKE_M = 50
# Waterbodies bigger than this are bays and seas, not lakes
MAX_LAKE_KM2 = 20


def osm_waterway_attributes(way):
    natural = way["waterway"] in ("river", "stream")
    # Culverts and other tunnels are man-made even when the stream isn't
    tunnel = way["tunnel"] not in (None, "no")
    return {
        "name": way["name"],
        "type": "stream" if natural else "canal/ditch",
        "is_natural": 1 if natural and not tunnel else 0,
        "permanence": {"yes": "intermittent", "no": "perennial"}.get(way["intermittent"]),
        "surface": "underground" if tunnel else "surface",
    }


def turn(line, other):
    """Angle between the end of line and the start of other, in radians"""
    a, b = line.coords[-2], line.coords[-1]
    c, d = other.coords[0], other.coords[1]
    return abs(math.remainder(
        math.atan2(d[1] - c[1], d[0] - c[0]) - math.atan2(b[1] - a[1], b[0] - a[0]),
        math.tau))


def build_network(osm_ways):
    """Waterways from OSM ways, split where other ways join them, each flowing into one
    other waterway or none

    osm_ways: from waterway_names.read_osm_waterways

    Returns dicts of source_id, name, type, is_natural, permanence, surface, coords as
    (lon, lat) tuples, hydroseq, and dnhydroseq (0 if it flows into nothing)
    """
    if not osm_ways:
        return []
    project = Projection(osm_ways).project
    uses = defaultdict(int)
    for way in osm_ways:
        for ref in way["refs"]:
            uses[ref] += 1
    network = []
    for way in osm_ways:
        refs = way["refs"]
        cuts = [0] + [i for i in range(1, len(refs) - 1) if uses[refs[i]] > 1] \
            + [len(refs) - 1]
        for piece, (start, end) in enumerate(zip(cuts, cuts[1:]), 1):
            coords = way["coords"][start:end + 1]
            network.append({
                "source_id": f"{way['id']}-{piece}",
                "coords": coords,
                "hydroseq": len(network) + 1,
                "dnhydroseq": 0,
                **osm_waterway_attributes(way),
                # Used to build the flow, not stored
                "way": way["id"],
                "kind": way["waterway"],
                "from": refs[start],
                "to": refs[end],
                "line": LineString(project(coords)),
            })
    leaving = defaultdict(list)
    for waterway in network:
        leaving[waterway["from"]].append(waterway)
    for waterway in network:
        options = [w for w in leaving[waterway["to"]] if w is not waterway]
        if not options:
            continue
        name = waterway["name"].lower() if waterway["name"] else None
        downstream = min(options, key=lambda w: (
            w["way"] != waterway["way"],
            not name or (w["name"] or "").lower() != name,
            KIND_RANK[w["kind"]],
            turn(waterway["line"], w["line"]),
        ))
        waterway["dnhydroseq"] = downstream["hydroseq"]
    return network


def link_lakes(network, waterbodies):
    """Send the waterways that end at a lake into the waterway flowing out of it

    network: from build_network
    waterbodies: (type, shapely (multi)polygon in lon/lat) tuples
    """
    if not network:
        return network
    # Project the waterways again with the lakes' projection instead of using their
    # lines, since projections from different coordinates scale differently, and
    # scaling about lon 0 turns that into an offset of a few hundred meters
    projection = Projection([{"coords": w["coords"]} for w in network])
    lines = {w["hydroseq"]: LineString(projection.project(w["coords"])) for w in network}
    lakes = []
    for kind, geom in waterbodies:
        polygon = projection.project_geometry(geom)
        if kind in LAKE_TYPES and polygon.area <= MAX_LAKE_KM2 * 1e6:
            lakes.append(polygon)
    if not lakes:
        return network
    lake_tree = STRtree(lakes)
    by_hydroseq = {w["hydroseq"]: w for w in network}

    def lake_near(point):
        near = [i for i in lake_tree.query(point.buffer(NEAR_LAKE_M))
                if lakes[i].distance(point) <= NEAR_LAKE_M]
        return min(near, key=lambda i: lakes[i].distance(point)) if near else None

    def flows_into(start, target):
        seen = set()
        while start in by_hydroseq and start not in seen:
            if start == target:
                return True
            seen.add(start)
            start = by_hydroseq[start]["dnhydroseq"]
        return False

    # Waterways that start at a lake and flow away from it, unlike the last piece of a
    # stream that starts near the shore and ends at it
    outflows = defaultdict(list)
    for waterway in network:
        line = lines[waterway["hydroseq"]]
        start, end = Point(line.coords[0]), Point(line.coords[-1])
        lake = lake_near(start)
        if lake is not None and line.difference(lakes[lake]).length > 0.5 * line.length \
                and lakes[lake].distance(end) > lakes[lake].distance(start):
            outflows[lake].append(waterway)
    for waterway in network:
        if waterway["dnhydroseq"]:
            continue
        lake = lake_near(Point(lines[waterway["hydroseq"]].coords[-1]))
        options = [w for w in outflows.get(lake, [])
                   if not flows_into(w["hydroseq"], waterway["hydroseq"])]
        if options:
            waterway["dnhydroseq"] = min(options, key=lambda w: (
                KIND_RANK[w["kind"]], -lines[w["hydroseq"]].length))["hydroseq"]
    return network


def read_waterbodies(bbox):
    """(type, geometry) of the waterbodies in the database that intersect bbox"""
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        cur.execute(
            f"""
                SELECT type, ST_AsBinary(geom) FROM {WATERBODIES_TABLE_NAME}
                WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, {SRID})
            """,
            (bbox["left"], bbox["bottom"], bbox["right"], bbox["top"]))
        waterbodies = [(kind, wkb.loads(bytes(geom))) for kind, geom in cur.fetchall()]
    con.close()
    return waterbodies


def write_network(network):
    """Add the waterways and their flow to the database, and cite OSM"""
    create_citations_table()
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        execute_values(
            cur,
            f"""
                INSERT INTO {WATERWAYS_TABLE_NAME} (
                    name, source, source_id, source_id_attr, type, is_natural, permanence,
                    surface, geom)
                VALUES %s
            """,
            [
                (w["name"], OSM_SOURCE, w["source_id"], "OSM way", w["type"], w["is_natural"],
                 w["permanence"], w["surface"], LineString(w["coords"]).wkt)
                for w in network
            ],
            template=f"(%s, %s, %s, %s, %s, %s, %s, %s, ST_Multi(ST_GeomFromText(%s, {SRID})))")
        execute_values(
            cur,
            f"INSERT INTO {WATERWAYS_FLOW_TABLE_NAME} (source, source_id, hydroseq, dnhydroseq) "
            "VALUES %s",
            [(OSM_SOURCE, w["source_id"], w["hydroseq"], w["dnhydroseq"]) for w in network])
        cur.execute(f"DELETE FROM {CITATIONS_TABLE_NAME} WHERE source = %s", (OSM_SOURCE,))
        cur.execute(
            f"INSERT INTO {CITATIONS_TABLE_NAME} (source, citation) VALUES (%s, %s)",
            (OSM_SOURCE, OSM_CITATION))
    con.close()


def apply_osm_network(osm_ways, bbox, debug=False):
    """Load waterways and their flow from osm_ways (see read_osm_waterways, with
    OSM_WATERWAY_KINDS), linking them through the lakes near bbox; returns how many
    waterways were loaded"""
    network = build_network(osm_ways)
    link_lakes(network, read_waterbodies(widen_bbox(bbox, OSM_BBOX_MARGIN_DEG)))
    if debug:
        util.log(f"water: {len(network)} waterways from {len(osm_ways)} OSM ways")
    if network:
        write_network(network)
    return len(network)
