"""Tests for sources.util.water"""

import json
import os
import sqlite3
import subprocess

import pytest

from sources.util import water


def test_process_nhdplus_hr_source_waterways_flow_rebuilds_after_failure(tmp_path):
    """Regression test: the CSV used to be written with a shell redirect,
    which creates it before sqlite3 runs, so a failed run left an empty CSV
    behind. Later runs saw it and skipped regenerating it, and the source's
    waterways never got flow labels."""
    sqlite_path = tmp_path / water.WATERWAYS_NETWORK_FNAME
    csv_path = tmp_path / water.WATERWAYS_FLOW_FNAME
    con = sqlite3.connect(sqlite_path)

    # No NHDPlusFlowlineVAA table yet, so the query fails
    with pytest.raises(subprocess.CalledProcessError):
        water.process_nhdplus_hr_source_waterways_flow(str(tmp_path))
    assert not os.path.exists(csv_path)

    con.execute("CREATE TABLE NHDPlusFlowlineVAA (NHDPlusID, HydroSeq, DnHydroSeq)")
    con.execute("INSERT INTO NHDPlusFlowlineVAA VALUES (55000100000001.0, 20.0, 10.0)")
    con.commit()
    con.close()
    water.process_nhdplus_hr_source_waterways_flow(str(tmp_path))
    assert csv_path.read_text().splitlines() == [
        "source_id,hydroseq,dnhydroseq",
        "55000100000001,20,10",
    ]


# Just enough of an NHDPlus HR GDB for the queries that extract waterways and
# waterbodies. NHD leaves HydrographicCategory blank for FCodes that don't say
# how often water flows, like 46000 (Stream/River) and 43600 (Reservoir).
FAKE_NHD_FCODES = [
    {"FCode": 46000, "HydrographicCategory": " ", "RelationshipToSurface": " ",
     "Description": "Stream/River"},
    {"FCode": 46003, "HydrographicCategory": "Intermittent", "RelationshipToSurface": " ",
     "Description": "Stream/River: Hydrographic Category = Intermittent"},
    {"FCode": 46006, "HydrographicCategory": "Perennial", "RelationshipToSurface": " ",
     "Description": "Stream/River: Hydrographic Category = Perennial"},
    {"FCode": 46007, "HydrographicCategory": "Ephemeral", "RelationshipToSurface": " ",
     "Description": "Stream/River: Hydrographic Category = Ephemeral"},
    {"FCode": 39001, "HydrographicCategory": "Intermittent", "RelationshipToSurface": " ",
     "Description": "Lake/Pond: Hydrographic Category = Intermittent"},
    {"FCode": 43600, "HydrographicCategory": " ", "RelationshipToSurface": " ",
     "Description": "Reservoir"},
    {"FCode": 55800, "HydrographicCategory": " ", "RelationshipToSurface": " ",
     "Description": "Artificial Path"},
]


def _write_layer(gdb_path, name, features, geometry_type):
    geojson_path = gdb_path.parent / f"{name}.geojson"
    geojson_path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    cmd = [
        "ogr2ogr", "-f", "GPKG", str(gdb_path), str(geojson_path),
        "-nln", name, "-nlt", geometry_type, "-lco", "GEOMETRY_NAME=Shape",
    ]
    if gdb_path.exists():
        cmd += ["-update"]
    subprocess.run(cmd, check=True)


def _flowline(nhdplus_id, fcode, ftype):
    return {
        "type": "Feature",
        "properties": {
            "GNIS_ID": None, "GNIS_Name": None, "NHDPlusID": nhdplus_id,
            "FCode": fcode, "FTYPE": ftype,
        },
        "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
    }


def _waterbody(gnis_id, fcode, ftype):
    return {
        "type": "Feature",
        "properties": {"GNIS_ID": gnis_id, "GNIS_Name": None, "FCode": fcode, "FType": ftype},
        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
    }


@pytest.fixture(name="fake_nhd_gdb")
def fixture_fake_nhd_gdb(tmp_path, monkeypatch):
    """A GeoPackage standing in for an NHDPlus HR GDB, with the cwd set to
    tmp_path, where the processing functions write their output"""
    gdb_path = tmp_path / "fake-nhd.gpkg"
    _write_layer(gdb_path, "NHDFlowline", [
        _flowline(1, 46000, 460),
        _flowline(2, 46003, 460),
        _flowline(3, 46006, 460),
        _flowline(4, 46007, 460),
        _flowline(5, 55800, 558),
    ], "LINESTRING")
    _write_layer(gdb_path, "NHDWaterbody", [
        _waterbody("lake", 39001, 390),
        _waterbody("reservoir", 43600, 436),
    ], "POLYGON")
    _write_layer(gdb_path, "NHDArea", [_waterbody("river", 46006, 460)], "POLYGON")
    _write_layer(gdb_path, "NHDFCode", [
        {"type": "Feature", "properties": fcode, "geometry": None}
        for fcode in FAKE_NHD_FCODES
    ], "NONE")
    monkeypatch.chdir(tmp_path)
    return gdb_path


def _permanence_by_source_id(gpkg_path, table):
    con = sqlite3.connect(gpkg_path)
    rows = con.execute(f"SELECT source_id, permanence FROM {table}").fetchall()
    con.close()
    return {str(source_id): permanence for source_id, permanence in rows}


def test_process_nhdplus_hr_source_waterways_keeps_nhd_permanence(fake_nhd_gdb):
    water.process_nhdplus_hr_source_waterways(str(fake_nhd_gdb), "EPSG:4326")
    permanence = _permanence_by_source_id(water.WATERWAYS_FNAME, "waterways")
    assert permanence["2"] == "intermittent"
    assert permanence["3"] == "perennial"
    assert permanence["4"] == "ephemeral"


def test_process_nhdplus_hr_source_waterways_leaves_unknown_permanence_empty(fake_nhd_gdb):
    """Regression test for #31: waterways NHD doesn't categorize, like
    FCode 46000 streams and artificial paths, used to be called perennial"""
    water.process_nhdplus_hr_source_waterways(str(fake_nhd_gdb), "EPSG:4326")
    permanence = _permanence_by_source_id(water.WATERWAYS_FNAME, "waterways")
    assert permanence["1"] is None
    assert permanence["5"] is None


def test_process_nhdplus_hr_source_waterbodies_keeps_nhd_permanence(fake_nhd_gdb):
    water.process_nhdplus_hr_source_waterbodies(str(fake_nhd_gdb), "EPSG:4326")
    permanence = _permanence_by_source_id(water.WATERBODIES_FNAME, "waterbodies")
    assert permanence["lake"] == "intermittent"
    assert permanence["river"] == "perennial"


def test_process_nhdplus_hr_source_waterbodies_leaves_unknown_permanence_empty(fake_nhd_gdb):
    """Regression test for #31: many reservoirs in arid places, like stock
    tanks, are dry most of the year, but used to be called perennial"""
    water.process_nhdplus_hr_source_waterbodies(str(fake_nhd_gdb), "EPSG:4326")
    permanence = _permanence_by_source_id(water.WATERBODIES_FNAME, "waterbodies")
    assert permanence["reservoir"] is None
