"""Tests for sources.util.tiger_water"""

import json
import sqlite3
import subprocess

from sources.util import tiger_water


def test_make_gpkg_leaves_permanence_unknown(tmp_path, monkeypatch):
    """Regression test for #31: TIGER doesn't say how often water fills its
    areas, but they used to be called perennial"""
    fips = "35001"
    geojson_path = tmp_path / "areawater.geojson"
    geojson_path.write_text(json.dumps({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {"HYDROID": "1", "FULLNAME": "Rio Grande", "MTFCC": "H3010"},
            "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
        }],
    }))
    subprocess.run([
        "ogr2ogr", "-f", "ESRI Shapefile",
        str(tmp_path / f"tl_2020_{fips}_areawater.shp"), str(geojson_path),
    ], check=True)
    monkeypatch.setattr(tiger_water, "make_work_dir", lambda path: str(tmp_path))

    gpkg_path = tiger_water.make_gpkg(fips, dst_path=str(tmp_path))

    con = sqlite3.connect(gpkg_path)
    rows = con.execute("SELECT name, permanence FROM waterbodies").fetchall()
    con.close()
    assert rows == [("Rio Grande", None)]
