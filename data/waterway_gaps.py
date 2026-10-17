"""Continue dead-end waterways along OpenStreetMap waterways

NHD sometimes ends a stream short of where it goes, like a creek that stops above a
culvert instead of reaching the reservoir it drains into (#54), so tracing flow
downstream stops there too. OSM often has the rest of the stream, so we follow it.

A dead end is a waterway with flow data that flows into nothing and doesn't end near
water. From an OSM river, stream, or canal near its end we follow OSM ways downstream,
from way to way through shared nodes, until the trace

* comes near another waterway with flow data, which it then flows into
* comes near the continuation of an earlier dead end, which it then flows into
* enters a waterbody, or ends at its shore, where it flows into the waterway through it
  along a straight connector like the paths NHD draws through lakes, or ends there if no
  waterway goes through it

The OSM ways the trace followed are added as waterways from the "osm" source and linked
into the flow data. A trace that goes nowhere, goes too far, or would flow back into its
own dead end's upstream is dropped.
"""

import psycopg2
from psycopg2.extras import execute_values
from shapely import wkb
from shapely.geometry import LineString, Point
from shapely.ops import substring
from shapely.strtree import STRtree

from database import DBNAME, SRID
from geometry import Projection, longest_line_coords, widen_bbox
from osm_waterways import (
    OSM_BBOX_MARGIN_DEG, WATERWAYS_FLOW_TABLE_NAME, WATERWAYS_TABLE_NAME,
    insert_osm_waterways, osm_attributes)
from sources import util

# Same table water.py loads
WATERBODIES_TABLE_NAME = "waterbodies"
# How close an OSM way has to come to a dead end's end to continue it
START_M = 15
# How close a trace has to come to a waterway to flow into it
JOIN_M = 10
# A waterway ending this close to a waterbody isn't a dead end
NEAR_WATER_M = 50
MAX_TRACE_M = 5000
MAX_TRACE_WAYS = 40
# Pieces of OSM ways shorter than this aren't worth adding
MIN_PIECE_M = 0.5
# Waterway types that cross streams instead of collecting them, so nothing flows into them
UNJOINABLE_TYPES = {"aqueduct", "siphon"}
# A line from where a continuation reaches a waterbody to the waterway through it, like
# the artificial paths NHD draws through lakes
CONNECTOR_ATTRIBUTES = {
    "name": None,
    "type": "artificial",
    "is_natural": 0,
    "permanence": None,
    "surface": "surface",
}


def earliest_distance(line, geom):
    """Distance along line of the first point where it meets geom, or None. line can be
    a point, which is all that's left of a line at its end."""
    if line.geom_type == "Point":
        return 0 if line.intersects(geom) else None
    hit = line.intersection(geom)
    if hit.is_empty:
        return None
    parts = getattr(hit, "geoms", [hit])
    return min(line.project(Point(part.coords[0])) for part in parts if not part.is_empty)


def nearest_point(line, point):
    return line.interpolate(line.project(point))


def point_along(line, distance):
    """Point distance along line, or line itself if it's a point"""
    return line if line.geom_type == "Point" else line.interpolate(distance)


def without_repeats(coords):
    """coords without points within a centimeter of the one before"""
    kept = []
    for coord in coords:
        if not kept or Point(coord).distance(Point(kept[-1])) > 0.01:
            kept.append(coord)
    return kept


class GapFiller:
    """Finds and links continuations for dead ends, in meters"""

    def __init__(self, waterways, waterbodies, osm_ways, downstream):
        self.projection = Projection(waterways)
        project = self.projection.project
        self.network = [{**w, "line": LineString(project(w["coords"]))} for w in waterways]
        # Waterways a trace can flow into
        self.joinable = [w["type"] not in UNJOINABLE_TYPES for w in self.network]
        # Lines of the waterways and the pieces added for continuations, by hydroseq
        self.lines = {w["hydroseq"]: w["line"] for w in self.network}
        self.network_tree = STRtree([w["line"] for w in self.network])
        self.waterbodies = [self.projection.project_geometry(g) for g in waterbodies]
        self.waterbody_tree = STRtree(self.waterbodies)
        self.osm_ways = []
        self.ways_by_node = {}
        for i, way in enumerate(osm_ways):
            coords = project(way["coords"])
            distances = [0]
            for a, b in zip(coords, coords[1:]):
                distances.append(distances[-1] + Point(a).distance(Point(b)))
            self.osm_ways.append({**way, "line": LineString(coords), "distances": distances})
            # A trace arriving at a node continues along every way that leaves it
            for position, ref in enumerate(way["refs"][:-1]):
                self.ways_by_node.setdefault(ref, []).append((i, position))
        self.osm_tree = STRtree([w["line"] for w in self.osm_ways])
        self.downstream = dict(downstream)
        self.added = []
        self.result = {"waterways": [], "flow": [], "relinks": {}}

    def is_outlet(self, waterway):
        return self.downstream.get(waterway["hydroseq"], waterway["dnhydroseq"]) \
            not in self.downstream

    def near_water(self, point):
        return any(self.waterbodies[i].distance(point) <= NEAR_WATER_M
                   for i in self.waterbody_tree.query(point.buffer(NEAR_WATER_M)))

    def dead_ends(self, bbox):
        found = []
        for waterway in self.network:
            lon, lat = waterway["coords"][-1]
            if not (bbox["left"] <= lon <= bbox["right"]
                    and bbox["bottom"] <= lat <= bbox["top"]):
                continue
            if self.is_outlet(waterway) and not self.near_water(
                    Point(waterway["line"].coords[-1])):
                found.append(waterway)
        return sorted(found, key=lambda w: (w["source"], str(w["source_id"])))

    def flows_into(self, hydroseq, waterway):
        """Whether water from hydroseq reaches waterway"""
        seen = set()
        while hydroseq in self.downstream and hydroseq not in seen:
            if hydroseq == waterway["hydroseq"]:
                return True
            seen.add(hydroseq)
            hydroseq = self.downstream[hydroseq]
        return False

    @staticmethod
    def runs_through(waterbody, waterway):
        """Whether most of waterway is in waterbody, like a path through a lake, unlike a
        creek whose mouth just reaches it"""
        line = waterway["line"]
        return line.intersection(waterbody).length >= 0.5 * line.length

    def drains_out_of(self, waterbody, waterway):
        """Whether water in waterway flows out of waterbody, like a path through a lake,
        rather than ending in it, like a creek NHD draws a little way into a bay"""
        hydroseq, last_line, seen = waterway["hydroseq"], waterway["line"], set()
        while self.downstream.get(hydroseq) in self.downstream and hydroseq not in seen:
            seen.add(hydroseq)
            hydroseq = self.downstream[hydroseq]
            if hydroseq not in self.lines:
                # It flows on past the waterways we have
                return True
            last_line = self.lines[hydroseq]
        return not waterbody.buffer(NEAR_WATER_M).contains(Point(last_line.coords[-1]))

    def waterbody_target(self, waterbody, point, dead_end):
        """(hydroseq, connector) for a trace reaching waterbody at point: the waterway
        through waterbody nearest point and a straight line to it, or (0, None) if no
        waterway goes through it. The connector is None if it would cross land."""
        inside = [
            self.network[i] for i in self.network_tree.query(waterbody)
            if self.joinable[i] and self.network[i] is not dead_end
            and self.runs_through(waterbody, self.network[i])
            and self.drains_out_of(waterbody, self.network[i])
        ]
        if not inside:
            return 0, None
        nearest = min(inside, key=lambda w: w["line"].distance(point))
        connector = LineString(
            [point, nearest["line"].interpolate(nearest["line"].project(point))])
        if connector.length < MIN_PIECE_M \
                or not connector.within(waterbody.buffer(NEAR_WATER_M)):
            connector = None
        return nearest["hydroseq"], connector

    def first_event(self, dead_end, piece):
        """(distance along piece, hydroseq it flows into, connector to it or None) for
        where the trace stops in piece, or None"""
        events = []
        for i in self.network_tree.query(piece.buffer(JOIN_M)):
            waterway = self.network[i]
            if waterway is dead_end or not self.joinable[i]:
                continue
            distance = earliest_distance(piece, waterway["line"].buffer(JOIN_M))
            if distance is None:
                continue
            # Another dead end ending here would flow into nothing
            end = Point(waterway["line"].coords[-1])
            if self.is_outlet(waterway) \
                    and end.distance(point_along(piece, distance)) <= 2 * JOIN_M:
                continue
            events.append((distance, waterway["hydroseq"], None,
                           nearest_point(waterway["line"], point_along(piece, distance))))
        for added in self.added:
            if added["line"].distance(piece) <= JOIN_M:
                distance = earliest_distance(piece, added["line"].buffer(JOIN_M))
                events.append((distance, added["hydroseq"], None,
                               nearest_point(added["line"], point_along(piece, distance))))
        for i in self.waterbody_tree.query(piece):
            distance = earliest_distance(piece, self.waterbodies[i])
            if distance is not None:
                point = point_along(piece, distance)
                events.append((
                    distance, *self.waterbody_target(self.waterbodies[i], point, dead_end),
                    None))
        return min(events, key=lambda e: e[0]) if events else None

    def trace(self, dead_end, way_index, start, travelled=0, visited=()):
        """([(way index, start, end)], hydroseq it flows into, connector to it or None,
        point on the waterway it joins or None) following OSM ways from start along
        way_index, or None if it goes nowhere"""
        if len(visited) >= MAX_TRACE_WAYS or travelled > MAX_TRACE_M or way_index in visited:
            return None
        way = self.osm_ways[way_index]
        length = way["line"].length
        piece = substring(way["line"], start, length) if length - start > MIN_PIECE_M \
            else Point(way["line"].interpolate(start))
        event = self.first_event(dead_end, piece)
        if event:
            distance, target, connector, snap = event
            if self.flows_into(target, dead_end):
                return None
            return [(way_index, start, start + distance)], target, connector, snap
        for next_index, position in sorted(self.ways_by_node.get(way["refs"][-1], [])):
            if next_index == way_index:
                continue
            found = self.trace(
                dead_end, next_index, self.osm_ways[next_index]["distances"][position],
                travelled + length - start, visited + (way_index,))
            if found:
                pieces, *rest = found
                return [(way_index, start, length)] + pieces, *rest
        # OSM ways often stop at the shore without entering the water
        end = Point(way["line"].coords[-1])
        if self.near_water(end):
            # The first of the nearest, since STRtree.nearest picks among ties unpredictably
            waterbody = self.waterbodies[min(self.waterbody_tree.query_nearest(end))]
            target, connector = self.waterbody_target(waterbody, end, dead_end)
            if self.flows_into(target, dead_end):
                return None
            return [(way_index, start, length)], target, connector, None
        return None

    def link(self, dead_end, pieces, target, connector, snap):
        """Add the pieces of a trace and its connector to the waterway it flows into, and
        link them into the flow. The pieces start where the dead end ends and end on the
        waterway they join, so there's no gap between them."""
        lines = []
        for way_index, start, end in pieces:
            if end - start < MIN_PIECE_M:
                continue
            way = self.osm_ways[way_index]
            lines.append((way["id"], osm_attributes(way),
                          list(substring(way["line"], start, end).coords)))
        if not lines:
            first_way = self.osm_ways[pieces[0][0]]
            lines.append((first_way["id"], osm_attributes(first_way), []))
        lines[0][2].insert(0, dead_end["line"].coords[-1])
        if snap:
            lines[-1][2].append((snap.x, snap.y))
        lines = [(way_id, attributes, without_repeats(coords))
                 for way_id, attributes, coords in lines]
        lines = [line for line in lines if len(line[2]) > 1]
        if connector:
            lines.append((self.osm_ways[pieces[-1][0]]["id"], CONNECTOR_ATTRIBUTES,
                          list(connector.coords)))
        hydroseqs = [-(len(self.result["flow"]) + i + 1) for i in range(len(lines))]
        downstreams = hydroseqs[1:] + [target]
        for (way_id, attributes, line_coords), hydroseq, downstream in zip(
                lines, hydroseqs, downstreams):
            source_id = f"{way_id}-{-hydroseq}"
            line = LineString(line_coords)
            coords = self.projection.unproject(line_coords)
            self.result["waterways"].append(
                {"source_id": source_id, "coords": coords, **attributes})
            self.result["flow"].append((source_id, hydroseq, downstream))
            self.downstream[hydroseq] = downstream
            self.added.append({"line": line, "hydroseq": hydroseq})
            self.lines[hydroseq] = line
        first = hydroseqs[0] if hydroseqs else target
        self.result["relinks"][(dead_end["source"], dead_end["source_id"])] = first
        self.downstream[dead_end["hydroseq"]] = first

    def fill(self, dead_end):
        end = Point(dead_end["line"].coords[-1])
        nearby = [
            i for i in self.osm_tree.query(end.buffer(START_M))
            if self.osm_ways[i]["line"].distance(end) <= START_M
        ]
        for i in sorted(nearby, key=lambda i: (self.osm_ways[i]["line"].distance(end), i)):
            found = self.trace(dead_end, i, self.osm_ways[i]["line"].project(end))
            if found:
                self.link(dead_end, *found)
                return


def find_gap_fills(waterways, waterbodies, osm_ways, downstream, bbox):
    """Continuations along OSM ways for the waterways that dead-end in bbox

    waterways: dicts of key, source, source_id, type, hydroseq, dnhydroseq, and coords
      as (lon, lat) tuples running downstream, for the waterways with flow data
    waterbodies: shapely (multi)polygons in lon/lat
    osm_ways: from read_osm_waterways
    downstream: {hydroseq: dnhydroseq} for all the flow data, not just these waterways

    Returns a dict of
      waterways: new waterways, dicts of source_id, name, type, is_natural, permanence,
        surface, and coords
      flow: (source_id, hydroseq, dnhydroseq) for the new waterways
      relinks: {(source, source_id): dnhydroseq} for the dead ends that now continue
    """
    if not waterways or not osm_ways:
        return {"waterways": [], "flow": [], "relinks": {}}
    filler = GapFiller(waterways, waterbodies, osm_ways, downstream)
    for dead_end in filler.dead_ends(bbox):
        filler.fill(dead_end)
    return filler.result


def read_network(bbox):
    """Waterways with flow data and waterbodies in the database that intersect bbox,
    and {hydroseq: dnhydroseq} for all the flow data"""
    envelope = f"ST_MakeEnvelope(%s, %s, %s, %s, {SRID})"
    params = (bbox["left"], bbox["bottom"], bbox["right"], bbox["top"])
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        cur.execute(
            f"""
                SELECT w.id, w.source, w.source_id, w.type, f.hydroseq, f.dnhydroseq,
                    ST_AsBinary(w.geom)
                FROM {WATERWAYS_TABLE_NAME} w
                    JOIN {WATERWAYS_FLOW_TABLE_NAME} f
                        ON f.source = w.source AND f.source_id = w.source_id
                WHERE w.geom && {envelope}
            """,
            params)
        waterways = [
            {"key": key, "source": source, "source_id": source_id, "type": kind,
             "hydroseq": hydroseq, "dnhydroseq": dnhydroseq,
             "coords": longest_line_coords(wkb.loads(bytes(geom)))}
            for key, source, source_id, kind, hydroseq, dnhydroseq, geom in cur.fetchall()
        ]
        cur.execute(
            f"SELECT ST_AsBinary(geom) FROM {WATERBODIES_TABLE_NAME} WHERE geom && {envelope}",
            params)
        waterbodies = [wkb.loads(bytes(geom)) for (geom,) in cur.fetchall()]
        cur.execute(f"SELECT hydroseq, dnhydroseq FROM {WATERWAYS_FLOW_TABLE_NAME}")
        downstream = dict(cur.fetchall())
    con.close()
    return waterways, waterbodies, downstream


def write_gap_fills(result):
    """Add the new waterways and their flow, and relink the dead ends"""
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        insert_osm_waterways(cur, result["waterways"], result["flow"])
        execute_values(
            cur,
            f"""
                UPDATE {WATERWAYS_FLOW_TABLE_NAME} f
                SET dnhydroseq = v.dnhydroseq
                FROM (VALUES %s) AS v(source, source_id, dnhydroseq)
                WHERE f.source = v.source AND f.source_id = v.source_id
            """,
            [(source, source_id, dn) for (source, source_id), dn in result["relinks"].items()],
            template="(%s, %s, %s::bigint)")
    con.close()


def apply_osm_gap_fills(osm_ways, bbox, debug=False):
    """Continue the dead ends in bbox along osm_ways (see read_osm_waterways); returns
    how many dead ends now continue"""
    waterways, waterbodies, downstream = read_network(widen_bbox(bbox, OSM_BBOX_MARGIN_DEG))
    result = find_gap_fills(waterways, waterbodies, osm_ways, downstream, bbox)
    if debug:
        util.log(
            f"water: continued {len(result['relinks'])} dead ends with "
            f"{len(result['waterways'])} OSM waterways")
    if result["relinks"]:
        write_gap_fills(result)
    return len(result["relinks"])
