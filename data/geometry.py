"""Geometry helpers for matching and tracing waterways in meters"""

import math

from shapely.affinity import scale
from shapely.ops import linemerge

METERS_PER_DEGREE_LAT = 110540
METERS_PER_DEGREE_LON_AT_EQUATOR = 111320


def widen_bbox(bbox, margin_deg):
    """bbox grown by margin_deg on every side"""
    return {
        "left": bbox["left"] - margin_deg,
        "bottom": bbox["bottom"] - margin_deg,
        "right": bbox["right"] + margin_deg,
        "top": bbox["top"] + margin_deg,
    }


class Projection:
    """Projects (lon, lat) to approximate meters and back, for measuring distances, like
    buffers and trace lengths, in Shapely. Results go back to lon/lat with unproject.

    This is an equirectangular approximation, not a real projection: x is longitude
    scaled by the cosine of the waterways' mean latitude, and y is latitude scaled by a
    constant. Its known problems:

    * It's only exact at the mean latitude. East-west distances are off by about 1.4%
      per degree of latitude away from it at 38°N, so a pack spanning 10° of latitude
      is off by about 7% at its edges.
    * METERS_PER_DEGREE_LAT is the value near the equator, so north-south distances are
      about 0.4% short at 38°N.
    * Each instance picks its own scale from the coordinates it's given and scales about
      lon 0, so a tiny difference in scale is a large offset far from it. Geometries
      projected by different instances can't be compared: lakes in us-ca-sfba once came
      out about 176 m off their waterways that way. Use one instance for everything that
      gets compared.

    To fix all three, project with one fixed low-distortion CRS per pack, like its UTM zone
    or NAD83_CA_ALBERS in sources/util/proj.py, used only for these calculations. GDAL,
    through fiona.transform, can do that without a new dependency; pyproj could too, but
    isn't installed. Web Mercator, though fixed, wouldn't help: its meters are stretched
    by 1/cos(latitude), so it would still need this cosine correction.
    """

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


def longest_line_coords(geom):
    """Coordinates of the longest line in a (multi)linestring, after joining the pieces
    that connect end to end"""
    if geom.geom_type == "MultiLineString":
        geom = linemerge(geom)
    if geom.geom_type == "MultiLineString":
        geom = max(geom.geoms, key=lambda part: part.length)
    return list(geom.coords)
