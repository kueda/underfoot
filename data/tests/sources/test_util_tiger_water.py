"""Tests for sources.util.tiger_water"""

import json
import pathlib
import sqlite3
import subprocess
import zipfile

import pytest

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


def write_zip(path, fips):
    with zipfile.ZipFile(path, "w") as zip_file:
        zip_file.writestr(f"tl_2020_{fips}_areawater.shp", b"shp")


def fake_downloads(monkeypatch, tmp_path, responses):
    """Make tiger_water download each of responses in turn, where each is
    either a FIPS code to zip up or bytes to write as is"""
    monkeypatch.setattr(tiger_water, "make_work_dir", lambda path: str(tmp_path))
    monkeypatch.setattr(tiger_water.time, "sleep", lambda seconds: None)
    urls = []

    def fake_download_file(url, path):
        urls.append(url)
        response = responses[len(urls) - 1]
        if isinstance(response, bytes):
            pathlib.Path(path).write_bytes(response)
        else:
            write_zip(path, response)
        return path

    monkeypatch.setattr(tiger_water, "download_file", fake_download_file)
    return urls


def test_download_retries_when_server_sends_something_other_than_a_zip(tmp_path, monkeypatch):
    """Regression test: Census servers sometimes answer bursts of requests
    with a short page and a 200 status, which curl --fail lets through, and
    unzip failed on it"""
    urls = fake_downloads(monkeypatch, tmp_path, [b"<html>busy</html>", "35001"])
    tiger_water.download("35001")
    assert len(urls) == 2
    assert (tmp_path / "tl_2020_35001_areawater.shp").read_bytes() == b"shp"


def test_download_replaces_an_existing_file_that_is_not_a_zip(tmp_path, monkeypatch):
    urls = fake_downloads(monkeypatch, tmp_path, ["35001"])
    (tmp_path / "tl_2020_35001_areawater.zip").write_bytes(b"<html>busy</html>")
    tiger_water.download("35001")
    assert len(urls) == 1
    assert (tmp_path / "tl_2020_35001_areawater.shp").read_bytes() == b"shp"


def test_download_skips_an_existing_zip(tmp_path, monkeypatch):
    urls = fake_downloads(monkeypatch, tmp_path, [])
    write_zip(tmp_path / "tl_2020_35001_areawater.zip", "35001")
    tiger_water.download("35001")
    assert not urls
    assert (tmp_path / "tl_2020_35001_areawater.shp").read_bytes() == b"shp"


def test_download_gives_up_and_leaves_no_file_when_server_never_sends_a_zip(
        tmp_path, monkeypatch):
    fake_downloads(monkeypatch, tmp_path, [b"<html>busy</html>"] * tiger_water.DOWNLOAD_ATTEMPTS)
    with pytest.raises(ValueError, match="tl_2020_35001_areawater.zip"):
        tiger_water.download("35001")
    assert not (tmp_path / "tl_2020_35001_areawater.zip").exists()


def test_download_retries_bypass_a_cached_response_that_is_not_a_zip(tmp_path, monkeypatch):
    """Regression test: Census's CDN cached a "Request Rejected" page for a
    TIGER file and served it for every request to that URL, so retries with
    the same URL could never get the file"""
    busy = b"<html>Request Rejected</html>"
    urls = fake_downloads(monkeypatch, tmp_path, [busy, busy, "35001"])
    tiger_water.download("35001")
    url = "https://www2.census.gov/geo/tiger/TIGER2020/AREAWATER/tl_2020_35001_areawater.zip"
    assert urls[0] == url
    assert all(retry_url.startswith(f"{url}?") for retry_url in urls[1:])
    assert len(set(urls)) == len(urls)
    assert (tmp_path / "tl_2020_35001_areawater.shp").read_bytes() == b"shp"
