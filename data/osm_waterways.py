"""Waterways, their flow, and waterbodies from OpenStreetMap alone

Where OSM maps waterways well, as in the San Francisco Bay Area, it has more of them than
NHD, with more names, and it gives the direction of flow: by convention a waterway is
drawn in the direction it flows. So for those packs we can use OSM's waterways instead
of NHD's, which avoids the seams of mixing the two. Their waterbodies come from OSM too,
since OSM's lakes line up with its waterways and NHD's don't always.

Ways are split where other ways join them, and each piece flows into one of the pieces
leaving its end, preferring the same way, then the same name, then rivers and streams
over canals and ditches, then the straightest. That makes the flow a tree, like the
main path NHD's flow data follows.

OSM rarely draws paths through lakes, so streams that end at a lake flow into the
waterway leaving it, if there is one. Where several leave it, they prefer one that
reaches water or the edge of the map over a dead end like a tunnel that stops at its
portal, then one with the same name, then rivers and streams, then the longest way
downstream. Huge waterbodies like bays are left alone, since the creeks that end in them
don't flow out through anything.

OSM only implies the sea with its coastline, so the sea comes from the water polygons
osmdata.openstreetmap.de makes from it.
"""

import math
from collections import defaultdict

import fiona
import osmium
import psycopg2
from psycopg2.extras import execute_values
from shapely import wkb
from shapely.geometry import LineString, MultiPolygon, Point, box, shape
from shapely.strtree import STRtree

from database import DBNAME, SRID
from sources import util
from waterway_gaps import (
    NEAR_WATER_M, OSM_BBOX_MARGIN_DEG, OSM_SOURCE, insert_osm_waterways, osm_attributes)
from waterway_names import Projection, normalize, widen_bbox

# Same table water.py loads
WATERBODIES_TABLE_NAME = "waterbodies"
OSM_WATERWAY_KINDS = ("river", "stream", "canal", "ditch", "drain")
# OSM only implies the sea with its coastline, so the sea comes from the water polygons
# osmdata.openstreetmap.de makes from it every day
OSM_WATER_POLYGONS_URL = \
    "https://osmdata.openstreetmap.de/download/water-polygons-split-4326.zip"
OSM_WATER_POLYGONS_SHP = "water-polygons-split-4326/water_polygons.shp"
# Where flow forks, it follows these before those
KIND_RANK = {"river": 0, "stream": 0, "canal": 1, "ditch": 2, "drain": 3}
LAKE_TYPES = {"lake/pond", "reservoir"}
# Waterbody type and is_natural of OSM water areas by their water tag, using the
# sources' types. None is a plain natural=water, and values not listed are lakes.
OSM_WATER_TYPES = {
    "reservoir": ("reservoir", 0),
    "basin": ("storage", 0),
    "wastewater": ("treatment", 0),
    "salt_pond": ("evaporator", 0),
    "river": ("stream/river", 1),
    "stream": ("stream/river", 1),
    "tidal_channel": ("stream/river", 1),
    "canal": ("canal/ditch", 0),
    "ditch": ("canal/ditch", 0),
    "drain": ("canal/ditch", 0),
    "lock": ("canal/ditch", 0),
}
# Small artificial water that isn't part of the hydrology
OSM_SKIPPED_WATER = {"fountain", "reflecting_pool", "pool"}
# Waterbody type of OSM wetlands by their wetland tag; others, like wet meadows, are
# left out
OSM_WETLAND_TYPES = {
    None: "swamp/marsh",
    "marsh": "swamp/marsh",
    "swamp": "swamp/marsh",
    "saltmarsh": "swamp/marsh",
    "reedbed": "swamp/marsh",
    "tidalflat": "foreshore",
}
# A stream ending this close to a lake flows into it, and a waterway starting this close
# to one flows out of it
NEAR_LAKE_M = 50
# Waterbodies bigger than this are bays and seas, not lakes
MAX_LAKE_KM2 = 20


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
                **osm_attributes(way),
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
        name = normalize(waterway["name"])
        downstream = min(options, key=lambda w: (
            w["way"] != waterway["way"],
            not name or normalize(w["name"]) != name,
            KIND_RANK[w["kind"]],
            turn(waterway["line"], w["line"]),
        ))
        waterway["dnhydroseq"] = downstream["hydroseq"]
    # Flow around a loop, like a ring of ditches, would have no outlet, leaving the loop
    # and everything upstream of it out of the flow tree, so it ends where the loop
    # would close
    by_hydroseq = {w["hydroseq"]: w for w in network}
    walked = {}
    for waterway in network:
        path, hydroseq = [], waterway["hydroseq"]
        while hydroseq and hydroseq not in walked:
            walked[hydroseq] = waterway["hydroseq"]
            path.append(hydroseq)
            hydroseq = by_hydroseq[hydroseq]["dnhydroseq"]
        if hydroseq and walked[hydroseq] == waterway["hydroseq"]:
            by_hydroseq[path[-1]]["dnhydroseq"] = 0
    return network


def link_lakes(network, waterbodies, bbox=None):
    """Send the waterways that end at a lake into the waterway flowing out of it

    network: from build_network
    waterbodies: (type, shapely (multi)polygon in lon/lat) tuples
    bbox: where the OSM ways were read from, if they were; ways leaving it continue
      beyond it
    """
    if not network:
        return network
    # Project the waterways again with the lakes' projection instead of using their
    # lines, since projections from different coordinates scale differently, and
    # scaling about lon 0 turns that into an offset of a few hundred meters
    projection = Projection([{"coords": w["coords"]} for w in network])
    lines = {w["hydroseq"]: LineString(projection.project(w["coords"])) for w in network}
    water = [projection.project_geometry(geom) for _, geom in waterbodies]
    lakes = [polygon for (kind, _), polygon in zip(waterbodies, water)
             if kind in LAKE_TYPES and polygon.area <= MAX_LAKE_KM2 * 1e6]
    if not lakes:
        return network
    lake_tree = STRtree(lakes)
    water_tree = STRtree(water)
    by_hydroseq = {w["hydroseq"]: w for w in network}

    def lakes_near(point):
        return [i for i in lake_tree.query(point.buffer(NEAR_LAKE_M))
                if lakes[i].distance(point) <= NEAR_LAKE_M]

    def downstream(start):
        """Hydroseqs of start and the waterways it flows into, in order"""
        path = []
        while start in by_hydroseq and start not in path:
            path.append(start)
            start = by_hydroseq[start]["dnhydroseq"]
        return path

    def dead_end(hydroseq):
        """Whether water from hydroseq stops short of any waterbody, like a tunnel that
        ends at its portal, as opposed to reaching one or leaving bbox"""
        lon, lat = by_hydroseq[hydroseq]["coords"][-1]
        if bbox and not (bbox["left"] <= lon <= bbox["right"]
                         and bbox["bottom"] <= lat <= bbox["top"]):
            return False
        end = Point(lines[hydroseq].coords[-1])
        return not any(water[i].distance(end) <= NEAR_WATER_M
                       for i in water_tree.query(end.buffer(NEAR_WATER_M)))

    # Waterways that start at a lake and flow away from it, unlike the last piece of a
    # stream that starts near the shore and ends at it. One can flow out of more than one
    # lake, like a creek leaving a dam from a pond at its intake.
    outflows = defaultdict(list)
    for waterway in network:
        line = lines[waterway["hydroseq"]]
        start, end = Point(line.coords[0]), Point(line.coords[-1])
        for lake in lakes_near(start):
            if line.difference(lakes[lake]).length > 0.5 * line.length \
                    and lakes[lake].distance(end) > lakes[lake].distance(start):
                outflows[lake].append(waterway)
    # Where each outflow goes before any lakes are linked. One that ends at another lake
    # reaches water, so it isn't a dead end even though it will flow on from there.
    ends = {}
    for waterway in (w for options in outflows.values() for w in options):
        path = downstream(waterway["hydroseq"])
        ends[waterway["hydroseq"]] = (
            dead_end(path[-1]), sum(lines[h].length for h in path))
    for waterway in network:
        if waterway["dnhydroseq"]:
            continue
        # Any lake near its end, since a lake drawn by more than one source can end a
        # stream at one drawing and start its outflow at another
        options = [w for lake in lakes_near(Point(lines[waterway["hydroseq"]].coords[-1]))
                   for w in outflows.get(lake, [])
                   if waterway["hydroseq"] not in downstream(w["hydroseq"])]
        if not options:
            continue
        name = normalize(waterway["name"])
        # Water leaves by the outflow that goes somewhere, preferring the same name as
        # it came in by, then streams over canals, then the longest way downstream
        waterway["dnhydroseq"] = min(options, key=lambda w: (
            ends[w["hydroseq"]][0],
            not name or normalize(w["name"]) != name,
            KIND_RANK[w["kind"]],
            -ends[w["hydroseq"]][1],
        ))["hydroseq"]
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


def osm_waterbody_attributes(tags):
    """Name, type, is_natural, and permanence of an OSM area, like the sources'
    waterbodies, or None if it isn't water we show. Bays are left out, since the sea
    comes from the water polygons made from OSM's coastline (see read_osm_sea)."""
    natural, water, landuse = tags.get("natural"), tags.get("water"), tags.get("landuse")
    if natural == "wetland":
        kind, is_natural = OSM_WETLAND_TYPES.get(tags.get("wetland"), None), 1
        if kind is None:
            return None
    elif natural == "water" or landuse in ("reservoir", "basin"):
        water = water or (landuse if landuse in ("reservoir", "basin") else None)
        if water in OSM_SKIPPED_WATER:
            return None
        kind, is_natural = OSM_WATER_TYPES.get(water, ("lake/pond", 1))
    else:
        return None
    if "yes" in (tags.get("intermittent"), tags.get("seasonal")):
        permanence = "intermittent"
    elif tags.get("intermittent") == "no":
        permanence = "perennial"
    else:
        permanence = None
    return {"name": tags.get("name"), "type": kind, "is_natural": is_natural,
            "permanence": permanence}


def read_osm_waterbodies(pbf_path, bbox):
    """Waterbodies from the water areas in an OSM PBF that intersect bbox, as dicts of
    source_id, source_id_attr, geom (shapely (multi)polygon in lon/lat), and
    osm_waterbody_attributes"""
    waterbodies = []
    factory = osmium.geom.WKBFactory()
    keys = ("natural", "water", "landuse")
    processor = (
        osmium.FileProcessor(pbf_path)
        # Only build areas from the relations that could be water, not every one in the
        # extract
        .with_areas(osmium.filter.KeyFilter(*keys))
        .with_filter(osmium.filter.KeyFilter(*keys))
    )
    for obj in processor:
        if not obj.is_area():
            continue
        attributes = osm_waterbody_attributes(obj.tags)
        if attributes is None:
            continue
        try:
            geom = wkb.loads(factory.create_multipolygon(obj), hex=True)
        except RuntimeError:
            # Broken in OSM, like a relation with an open ring
            continue
        left, bottom, right, top = geom.bounds
        if right < bbox["left"] or left > bbox["right"] or top < bbox["bottom"] \
                or bottom > bbox["top"]:
            continue
        waterbodies.append({
            "source_id": str(obj.orig_id()),
            "source_id_attr": "OSM way" if obj.from_way() else "OSM relation",
            "geom": geom,
            **attributes,
        })
    return waterbodies


def read_osm_sea(zip_path, bbox):
    """The sea within bbox, as waterbodies like read_osm_waterbodies's, from the water
    polygons osmdata.openstreetmap.de makes from OSM's coastline (zip_path is a download of
    OSM_WATER_POLYGONS_URL), which are already split into small pieces"""
    clip = box(bbox["left"], bbox["bottom"], bbox["right"], bbox["top"])
    sea = []
    with fiona.open(f"/vsizip/{zip_path}/{OSM_WATER_POLYGONS_SHP}") as polygons:
        for feature in polygons.filter(bbox=clip.bounds):
            geom = shape(feature["geometry"]).intersection(clip)
            # Water that also runs along the bbox's edge outside it adds that edge as a
            # line, which the waterbodies table can't hold
            if geom.geom_type == "GeometryCollection":
                geom = MultiPolygon([part for part in geom.geoms if part.geom_type == "Polygon"])
            if geom.is_empty or geom.area == 0:
                continue
            sea.append({"source_id": None, "source_id_attr": None, "geom": geom,
                        "name": None, "type": "ocean/sea", "is_natural": 1,
                        "permanence": None})
    return sea


def write_waterbodies(waterbodies):
    """Add the waterbodies to the database"""
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        execute_values(
            cur,
            f"""
                INSERT INTO {WATERBODIES_TABLE_NAME} (
                    name, source, source_id, source_id_attr, type, is_natural, permanence,
                    geom)
                VALUES %s
            """,
            [
                (w["name"], OSM_SOURCE, w["source_id"], w["source_id_attr"], w["type"],
                 w["is_natural"], w["permanence"], psycopg2.Binary(w["geom"].wkb))
                for w in waterbodies
            ],
            template=f"(%s, %s, %s, %s, %s, %s, %s, ST_Multi(ST_GeomFromWKB(%s, {SRID})))")
    con.close()


def write_network(network):
    """Add the waterways and their flow to the database"""
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        insert_osm_waterways(
            cur, network, [(w["source_id"], w["hydroseq"], w["dnhydroseq"]) for w in network])
    con.close()


def apply_osm_waterbodies(pbf_path, sea_path, bbox, debug=False):
    """Load waterbodies from the water areas in the OSM PBF at pbf_path (see
    read_osm_waterbodies) and the sea from the water polygons at sea_path (see
    read_osm_sea), from where water.py reads the OSM ways; returns how many waterbodies
    were loaded"""
    read_bbox = widen_bbox(bbox, OSM_BBOX_MARGIN_DEG)
    waterbodies = read_osm_waterbodies(pbf_path, read_bbox) + read_osm_sea(sea_path, read_bbox)
    if debug:
        util.log(f"water: {len(waterbodies)} waterbodies from OSM")
    if waterbodies:
        write_waterbodies(waterbodies)
    return len(waterbodies)


def apply_osm_network(osm_ways, bbox, debug=False):
    """Load waterways and their flow from osm_ways (see read_osm_waterways, with
    OSM_WATERWAY_KINDS), linking them through the lakes near bbox; returns how many
    waterways were loaded"""
    network = build_network(osm_ways)
    # Same as where water.py read the ways from
    read_bbox = widen_bbox(bbox, OSM_BBOX_MARGIN_DEG)
    link_lakes(network, read_waterbodies(read_bbox), bbox=read_bbox)
    if debug:
        util.log(f"water: {len(network)} waterways from {len(osm_ways)} OSM ways")
    if network:
        write_network(network)
    return len(network)
