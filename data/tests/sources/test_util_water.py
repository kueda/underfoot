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


@pytest.mark.parametrize("fcode", [46600, 46601, 46602])
def test_waterbodies_sql_classifies_all_swamp_marsh_fcodes_as_swamps(fcode):
    """NHD splits swamps/marshes into intermittent (46601) and perennial
    (46602) FCodes, and those used to fall through to lake/pond."""
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE NHDWaterbody (GNIS_ID, GNIS_Name, FCode, FType, Shape)")
    con.execute("CREATE TABLE NHDFcode (FCode, Description, HydrographicCategory)")
    con.execute("INSERT INTO NHDWaterbody VALUES ('1', 'Some Swamp', ?, 466, NULL)", (fcode,))
    con.execute("INSERT INTO NHDFcode VALUES (?, 'Swamp/Marsh', ' ')", (fcode,))
    cursor = con.execute(water.waterbodies_sql("NHDWaterbody"))
    columns = [d[0] for d in cursor.description]
    assert dict(zip(columns, cursor.fetchone()))["type"] == "swamp/marsh"


@pytest.mark.parametrize("fcode,ftype,expected", [
    (46000, 460, "stream/river"),
    (46003, 460, "stream/river"),
    (46006, 460, "stream/river"),
    (46007, 460, "stream/river"),
    (44500, 445, "ocean/sea"),
    (31200, 312, "bay/estuary/gulf/sound"),
    (49300, 493, "bay/estuary/gulf/sound"),
])
def test_waterbodies_sql_classifies_nhd_areas_that_are_not_lakes(fcode, ftype, expected):
    """Regression test for #64: NHD areas for streams and rivers that aren't
    perennial, the sea, and bays used to fall through to lake/pond"""
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE NHDArea (GNIS_ID, GNIS_Name, FCode, FType, Shape)")
    con.execute("CREATE TABLE NHDFcode (FCode, Description, HydrographicCategory)")
    con.execute("INSERT INTO NHDArea VALUES ('1', 'Some Water', ?, ?, NULL)", (fcode, ftype))
    con.execute("INSERT INTO NHDFcode VALUES (?, 'Some Water', ' ')", (fcode,))
    cursor = con.execute(water.waterbodies_sql("NHDArea"))
    columns = [d[0] for d in cursor.description]
    assert dict(zip(columns, cursor.fetchone()))["type"] == expected


@pytest.mark.parametrize("fcode", [40300, 40307, 40308, 40309])
def test_waterbodies_sql_classifies_all_inundation_area_fcodes_as_floodplains(fcode):
    """Inundation areas, like the flood pools behind Addicks and Barker dams in
    Houston, are dry most of the time (#83). Only 40309 used to be a
    floodplain; the rest fell through to lake/pond."""
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE NHDArea (GNIS_ID, GNIS_Name, FCode, FType, Shape)")
    con.execute("CREATE TABLE NHDFcode (FCode, Description, HydrographicCategory)")
    con.execute("INSERT INTO NHDArea VALUES (NULL, NULL, ?, 403, NULL)", (fcode,))
    con.execute("INSERT INTO NHDFcode VALUES (?, 'Inundation Area', ' ')", (fcode,))
    cursor = con.execute(water.waterbodies_sql("NHDArea"))
    columns = [d[0] for d in cursor.description]
    assert dict(zip(columns, cursor.fetchone()))["type"] == "floodplain"


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
    {"FCode": 48400, "HydrographicCategory": " ", "RelationshipToSurface": " ",
     "Description": "Wash"},
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


def _waterbody(gnis_id, fcode, ftype, name=None):
    return {
        "type": "Feature",
        "properties": {"GNIS_ID": gnis_id, "GNIS_Name": name, "FCode": fcode, "FType": ftype},
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
    _write_layer(gdb_path, "NHDArea", [
        _waterbody("river", 46006, 460),
        _waterbody("wash", 48400, 484),
        _waterbody("stream-named-wash", 46000, 460, name="Big Wash"),
        _waterbody("perennial-wash", 46006, 460, name="Wet Wash"),
    ], "POLYGON")
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


def test_process_nhdplus_hr_source_waterbodies_calls_washes_ephemeral(fake_nhd_gdb):
    """Regression test for #60: NHD doesn't say how often water fills its
    washes, so they were drawn like lakes, but they're dry except right after
    rain"""
    water.process_nhdplus_hr_source_waterbodies(str(fake_nhd_gdb), "EPSG:4326")
    permanence = _permanence_by_source_id(water.WATERBODIES_FNAME, "waterbodies")
    assert permanence["wash"] == "ephemeral"
    assert permanence["stream-named-wash"] == "ephemeral"


def test_process_nhdplus_hr_source_waterbodies_keeps_nhd_permanence_for_washes(fake_nhd_gdb):
    water.process_nhdplus_hr_source_waterbodies(str(fake_nhd_gdb), "EPSG:4326")
    permanence = _permanence_by_source_id(water.WATERBODIES_FNAME, "waterbodies")
    assert permanence["perennial-wash"] == "perennial"
