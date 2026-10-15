"""Name waterways from OpenStreetMap

NHD and TIGER waterways are often unnamed, and their names can be out of date. OSM
waterways are usually more current and more complete, and anyone can correct them, so
where an OSM waterway runs along one of ours we use its name, even if ours already has
one.

Matching is geometric. A waterway takes the name of the OSM waterways that cover at least
MIN_OVERLAP of its length within BUFFER_M and run in about the same direction. A few
guards keep wrong names out, since they would replace right ones:

* a waterway that is short, or crosses other streams (siphons, aqueducts, connectors),
  is a likely false match, so it is not matched on its own
* a short waterway between two named ones takes their name if they agree
* an OSM way with no name that matches better than any named one means OSM doesn't name
  it, so we keep ours
* two differently named OSM ways in the same place are ambiguous
* a short waterway is only renamed if a connected waterway with the same name is renamed
  to the same thing, since an isolated rename is likelier a mismatch than a correction
"""

import math
from collections import defaultdict

import osmium
import psycopg2
from psycopg2.extras import execute_values
from shapely import wkb
from shapely.affinity import scale
from shapely.geometry import LineString, Point
from shapely.ops import linemerge, unary_union
from shapely.strtree import STRtree

from database import DBNAME, SRID
from sources import util

# Same table water.py loads waterways into
WATERWAYS_TABLE_NAME = "waterways"
OSM_WATERWAYS = ("river", "stream", "canal")
BUFFER_M = 25
MIN_OVERLAP = 0.8
# How much better the best name has to cover a waterway than any other name
MARGIN = 0.2
MIN_LENGTH_M = 50
MAX_ANGLE_DEG = 30
ISOLATED_LENGTH_M = 200
JOIN_TOLERANCE_M = 5
# Waterway types that cross other streams instead of following them
SKIPPED_TYPES = {"siphon", "aqueduct", "connector"}
METERS_PER_DEGREE_LAT = 110540
METERS_PER_DEGREE_LON_AT_EQUATOR = 111320


def normalize(name):
    return name.lower().strip() if name else None


def widen_bbox(bbox, margin_deg):
    """bbox grown by margin_deg on every side"""
    return {
        "left": bbox["left"] - margin_deg,
        "bottom": bbox["bottom"] - margin_deg,
        "right": bbox["right"] + margin_deg,
        "top": bbox["top"] + margin_deg,
    }


def read_osm_waterways(pbf_path, bbox, kinds=OSM_WATERWAYS):
    """Waterways of kinds (values of the waterway tag; rivers, streams, and canals by
    default) from an OSM PBF with any node in bbox, as dicts of id, name, waterway,
    intermittent, tunnel, refs (node IDs), and coords as (lon, lat) tuples"""
    ways = []
    processor = (
        osmium.FileProcessor(pbf_path, osmium.osm.NODE | osmium.osm.WAY)
        .with_locations()
        .with_filter(osmium.filter.KeyFilter("waterway"))
    )
    for obj in processor:
        if not obj.is_way() or obj.tags.get("waterway") not in kinds:
            continue
        try:
            coords = [(node.lon, node.lat) for node in obj.nodes]
            refs = [node.ref for node in obj.nodes]
        except osmium.InvalidLocationError:
            continue
        if len(coords) < 2 or not any(
            bbox["left"] <= lon <= bbox["right"] and bbox["bottom"] <= lat <= bbox["top"]
            for lon, lat in coords
        ):
            continue
        ways.append({
            "id": obj.id,
            "name": obj.tags.get("name"),
            "waterway": obj.tags["waterway"],
            "intermittent": obj.tags.get("intermittent"),
            "tunnel": obj.tags.get("tunnel"),
            "refs": refs,
            "coords": coords,
        })
    return ways


class Projection:
    """Projects (lon, lat) to meters and back, accurate enough at the scale of a few
    hundred meters anywhere near the waterways' latitude"""

    def __init__(self, waterways):
        latitudes = [lat for w in waterways for _, lat in w["coords"]]
        self.x_scale = METERS_PER_DEGREE_LON_AT_EQUATOR * math.cos(
            math.radians(sum(latitudes) / len(latitudes)))
        self.y_scale = METERS_PER_DEGREE_LAT

    def project(self, coords):
        return [(lon * self.x_scale, lat * self.y_scale) for lon, lat in coords]

    def unproject(self, coords):
        return [(x / self.x_scale, y / self.y_scale) for x, y in coords]

    def project_geometry(self, geom):
        return scale(geom, xfact=self.x_scale, yfact=self.y_scale, origin=(0, 0))


def bearing(line, distance, half_window=5):
    start = line.interpolate(max(distance - half_window, 0))
    end = line.interpolate(min(distance + half_window, line.length))
    return math.atan2(end.y - start.y, end.x - start.x)


def mean_angle_degrees(line, osm_lines):
    """Mean angle between line and the nearest of osm_lines, ignoring which way either
    runs, sampled every 10 m. None if no sample is within BUFFER_M of one."""
    angles = []
    distance = 0
    while distance < line.length:
        point = line.interpolate(distance)
        nearest = min(osm_lines, key=point.distance)
        if point.distance(nearest) <= BUFFER_M:
            difference = bearing(line, distance) - bearing(nearest, nearest.project(point))
            angles.append(math.degrees(math.acos(min(1, abs(math.cos(difference))))))
        distance += 10
    return sum(angles) / len(angles) if angles else None


class Matcher:
    """Matches waterways to OSM ways, projected into meters"""

    def __init__(self, waterways, osm_ways):
        project = Projection(waterways).project
        self.waterways = [
            {**w, "line": LineString(project(w["coords"])), "norm": normalize(w["name"])}
            for w in waterways
        ]
        self.osm_ways = []
        for way in osm_ways:
            line = LineString(project(way["coords"]))
            self.osm_ways.append(
                {**way, "line": line, "buffer": line.buffer(BUFFER_M),
                 "norm": normalize(way["name"])})
        self.osm_tree = STRtree([w["line"] for w in self.osm_ways])
        self.end_points = [
            (i, Point(coord))
            for i, w in enumerate(self.waterways)
            for coord in (w["line"].coords[0], w["line"].coords[-1])
        ]
        self.end_tree = STRtree([point for _, point in self.end_points])

    def overlap(self, waterway, osm_way):
        line = waterway["line"]
        return line.intersection(osm_way["buffer"]).length / line.length

    def name_for(self, waterway):
        """OSM name for one waterway, or None"""
        if waterway["type"] in SKIPPED_TYPES or waterway["line"].length < MIN_LENGTH_M:
            return None
        candidates = [
            self.osm_ways[i]
            for i in self.osm_tree.query(waterway["line"].buffer(BUFFER_M))
        ]
        groups = defaultdict(list)
        for way in candidates:
            groups[way["norm"]].append(way)
        scores = {}
        for name, ways in groups.items():
            cover = unary_union([w["buffer"] for w in ways])
            line = waterway["line"]
            scores[name] = line.intersection(cover).length / line.length
        if not scores:
            return None
        best = max(scores, key=scores.get)
        if best is None or scores[best] < MIN_OVERLAP:
            return None
        if any(name is not None and scores[best] - score < MARGIN
               for name, score in scores.items() if name != best):
            return None
        angle = mean_angle_degrees(waterway["line"], [w["line"] for w in groups[best]])
        if angle is None or angle > MAX_ANGLE_DEG:
            return None
        return max(groups[best], key=lambda w: self.overlap(waterway, w))["name"]

    def neighbors(self, index):
        """(end, index) of waterways joined to either end of waterway index, where end is
        0 for its start and 1 for its end"""
        found = []
        line = self.waterways[index]["line"]
        for end, coord in enumerate((line.coords[0], line.coords[-1])):
            point = Point(coord)
            for hit in self.end_tree.query(point.buffer(JOIN_TOLERANCE_M)):
                other, other_point = self.end_points[hit]
                if other != index and other_point.distance(point) <= JOIN_TOLERANCE_M:
                    found.append((end, other))
        return found

    def inherited_names(self, names):
        """Names for short waterways whose neighbors on both ends have the same name"""
        inherited = {}
        for i, waterway in enumerate(self.waterways):
            if waterway["type"] in SKIPPED_TYPES or waterway["line"].length >= MIN_LENGTH_M:
                continue
            by_end = defaultdict(set)
            for end, other in self.neighbors(i):
                if names[other]:
                    by_end[end].add(names[other])
            all_names = set().union(*by_end.values()) if by_end else set()
            if len(by_end) == 2 and len(all_names) == 1:
                inherited[i] = all_names.pop()
        return inherited

    def isolated_renames(self, names):
        """Short waterways renamed to something that no connected waterway with the same
        name is renamed to"""
        isolated = set()
        for i, waterway in enumerate(self.waterways):
            new_name = names[i]
            if (not new_name or not waterway["norm"]
                    or normalize(new_name) == waterway["norm"]
                    or waterway["line"].length >= ISOLATED_LENGTH_M):
                continue
            backed = any(
                self.waterways[other]["norm"] == waterway["norm"]
                and names[other] and normalize(names[other]) == normalize(new_name)
                for _, other in self.neighbors(i))
            if not backed:
                isolated.add(i)
        return isolated


def match_names(waterways, osm_ways):
    """Names from OSM ways for the waterways they run along

    waterways: dicts of key, name, type, and coords as (lon, lat) tuples
    osm_ways: dicts of name and coords, from read_osm_waterways

    Returns {key: name} for each waterway matched, including ones whose name doesn't
    change.
    """
    if not waterways or not osm_ways:
        return {}
    matcher = Matcher(waterways, osm_ways)
    names = [matcher.name_for(w) for w in matcher.waterways]
    for i, name in matcher.inherited_names(names).items():
        names[i] = name
    for i in matcher.isolated_renames(names):
        names[i] = None
    return {w["key"]: name for w, name in zip(matcher.waterways, names) if name}


def longest_line_coords(geom):
    """Coordinates of the longest line in a (multi)linestring, after joining the pieces
    that connect end to end"""
    if geom.geom_type == "MultiLineString":
        geom = linemerge(geom)
    if geom.geom_type == "MultiLineString":
        geom = max(geom.geoms, key=lambda part: part.length)
    return list(geom.coords)


def read_waterway_rows(bbox):
    """Waterways in the database that intersect bbox, as dicts of key, name, type, and
    coords"""
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        cur.execute(
            f"""
                SELECT id, name, type, ST_AsBinary(geom)
                FROM {WATERWAYS_TABLE_NAME}
                WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, {SRID})
            """,
            (bbox["left"], bbox["bottom"], bbox["right"], bbox["top"]))
        rows = cur.fetchall()
    con.close()
    return [
        {"key": key, "name": name, "type": kind,
         "coords": longest_line_coords(wkb.loads(bytes(geom)))}
        for key, name, kind, geom in rows
    ]


def write_names(names):
    """Set the name of each waterway in {id: name}"""
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        execute_values(
            cur,
            f"""
                UPDATE {WATERWAYS_TABLE_NAME} w
                SET name = v.name
                FROM (VALUES %s) AS v(id, name)
                WHERE w.id = v.id
            """,
            list(names.items()),
            template="(%s::int, %s)")
    con.close()


def apply_osm_names(osm_ways, bbox, debug=False):
    """Name the waterways in the database within bbox from osm_ways (see
    read_osm_waterways); returns how many names changed"""
    rows = read_waterway_rows(bbox)
    matches = match_names(rows, osm_ways)
    current = {row["key"]: row["name"] for row in rows}
    changes = {key: name for key, name in matches.items() if name != current[key]}
    if debug:
        util.log(
            f"water: {len(rows)} waterways, {len(osm_ways)} OSM waterways, "
            f"{len(matches)} matched, {len(changes)} names changed")
    if changes:
        write_names(changes)
    return len(changes)
