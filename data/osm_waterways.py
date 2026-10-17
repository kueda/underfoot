"""Shared methods for handling OSM waterways"""

import osmium
import psycopg2
from psycopg2.extras import execute_values
from shapely.geometry import LineString

from database import DBNAME, SRID
from sources.util.citations import CITATIONS_TABLE_NAME, create_table as create_citations_table

# Same tables water.py loads
WATERWAYS_TABLE_NAME = "waterways"
WATERWAYS_FLOW_TABLE_NAME = "waterways_flow"
OSM_SOURCE = "osm"
OSM_CITATION = "OpenStreetMap contributors. OpenStreetMap. https://www.openstreetmap.org/copyright"
# Waterway tag values of the OSM waterways that name and continue the waterways of packs
# that keep their sources' hydrography. Ditches and drains are left out because they're
# mostly small irrigation and storm drain lines, which could give a stream they run along
# the wrong name or lead a trace away from the creek it should follow.
ENHANCING_WATERWAY_KINDS = ("river", "stream", "canal")
# Waterway tag values of the OSM waterways that packs with "hydrography": "osm" take as
# their waterways, so they show and trace everything OSM maps
HYDROGRAPHY_WATERWAY_KINDS = ("river", "stream", "canal", "ditch", "drain")
# How far past the pack bbox to read OSM ways and waterways, so traces can leave it
OSM_BBOX_MARGIN_DEG = 0.05


def read_osm_waterways(pbf_path, bbox, kinds=ENHANCING_WATERWAY_KINDS):
    """Read the waterways in an OSM PBF file that intersect a bbox. Ways with
    nodes missing from the file are skipped, i.e. when the ways are at the
    edge of an extract.

    pbf_path: path to the OSM PBF file
    bbox: dict of left, bottom, right, and top in degrees. A way is read if any of its
      nodes is inside, so ways can extend past it.
    kinds: which values of the waterway tag to read, e.g. "stream" or "ditch"

    Returns a list of dicts, one per way, of
      id: OSM way ID
      name, waterway, intermittent, tunnel: values of those tags, or None
      refs: IDs of the way's nodes, in order, so ways that share a node connect there
      coords: (lon, lat) of each node, in order. OSM draws waterways in the direction
        they flow, so this runs downstream.
    """
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


def osm_attributes(way):
    """Name, type, is_natural, permanence, and surface of a waterway from an OSM way"""
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


def insert_osm_waterways(cur, waterways, flow):
    """Insert waterways from OSM, dicts of source_id, coords, and osm_attributes, and their
    flow, as (source_id, hydroseq, dnhydroseq)"""
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
            for w in waterways
        ],
        template=f"(%s, %s, %s, %s, %s, %s, %s, %s, ST_Multi(ST_GeomFromText(%s, {SRID})))")
    execute_values(
        cur,
        f"INSERT INTO {WATERWAYS_FLOW_TABLE_NAME} (source, source_id, hydroseq, dnhydroseq) "
        "VALUES %s",
        [(OSM_SOURCE, source_id, hydroseq, dn) for source_id, hydroseq, dn in flow])


def cite_osm():
    """Cite OSM for the water taken from it"""
    create_citations_table()
    con = psycopg2.connect(f"dbname={DBNAME}")
    with con, con.cursor() as cur:
        cur.execute(f"DELETE FROM {CITATIONS_TABLE_NAME} WHERE source = %s", (OSM_SOURCE,))
        cur.execute(
            f"INSERT INTO {CITATIONS_TABLE_NAME} (source, citation) VALUES (%s, %s)",
            (OSM_SOURCE, OSM_CITATION))
    con.close()
