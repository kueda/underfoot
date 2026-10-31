"""Tests for osm.py helpers that need neither a database nor imposm."""

import osmium
import pytest

import osm


@pytest.fixture(autouse=True)
def _clear_pg_env(monkeypatch):
    """Start every test from a clean libpq environment."""
    for var in ("PGHOST", "PGPORT", "PGUSER", "PGPASSWORD"):
        monkeypatch.delenv(var, raising=False)


def test_imposm_connection_string_defaults():
    # No env vars: byte-for-byte what osm.py hardcoded before, so bare-metal and
    # Vagrant (unix-socket peer auth on localhost) keep working.
    assert osm.imposm_connection_string() == (
        "postgis://underfoot:underfoot@localhost/underfoot_osm"
    )


def test_imposm_connection_string_from_env(monkeypatch):
    monkeypatch.setenv("PGHOST", "db")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGUSER", "u")
    monkeypatch.setenv("PGPASSWORD", "p")
    assert osm.imposm_connection_string() == "postgis://u:p@db:5432/underfoot_osm"


def test_imposm_connection_string_omits_port_when_unset(monkeypatch):
    monkeypatch.setenv("PGHOST", "db")
    assert osm.imposm_connection_string() == (
        "postgis://underfoot:underfoot@db/underfoot_osm"
    )


def test_imposm_connection_string_quotes_credentials(monkeypatch):
    monkeypatch.setenv("PGUSER", "user name")
    monkeypatch.setenv("PGPASSWORD", "p@ss/w:rd")
    assert osm.imposm_connection_string() == (
        "postgis://user%20name:p%40ss%2Fw%3Ard@localhost/underfoot_osm"
    )


def test_imposm_connection_string_respects_dbname_arg():
    assert osm.imposm_connection_string("other_db") == (
        "postgis://underfoot:underfoot@localhost/other_db"
    )


def test_load_osm_from_pbf_uses_env_binary_and_connection(monkeypatch):
    monkeypatch.setenv("PGHOST", "db")
    monkeypatch.setenv("UNDERFOOT_IMPOSM", "/usr/local/bin/imposm")
    captured = {}
    monkeypatch.setattr(
        osm.util, "call_cmd", lambda cmd, **kwargs: captured.setdefault("cmd", cmd)
    )
    osm.load_osm_from_pbf("norcal-latest.osm.pbf")
    cmd = captured["cmd"]
    assert cmd[0] == "/usr/local/bin/imposm"
    assert cmd[cmd.index("-connection") + 1] == (
        "postgis://underfoot:underfoot@db/underfoot_osm"
    )


def test_load_osm_from_pbf_defaults_cachedir_into_the_tree(monkeypatch):
    # imposm's default cachedir is /tmp/imposm3, which is ephemeral and grows to
    # multiple GB for large extracts. Keep it in the working tree instead.
    monkeypatch.delenv("UNDERFOOT_IMPOSM_CACHEDIR", raising=False)
    captured = {}
    monkeypatch.setattr(
        osm.util, "call_cmd", lambda cmd, **kwargs: captured.setdefault("cmd", cmd)
    )
    osm.load_osm_from_pbf("norcal-latest.osm.pbf")
    cmd = captured["cmd"]
    assert cmd[cmd.index("-cachedir") + 1] == "imposm-cache"


def test_load_osm_from_pbf_cachedir_from_env(monkeypatch):
    monkeypatch.setenv("UNDERFOOT_IMPOSM_CACHEDIR", "/app/imposm-cache")
    captured = {}
    monkeypatch.setattr(
        osm.util, "call_cmd", lambda cmd, **kwargs: captured.setdefault("cmd", cmd)
    )
    osm.load_osm_from_pbf("norcal-latest.osm.pbf")
    cmd = captured["cmd"]
    assert cmd[cmd.index("-cachedir") + 1] == "/app/imposm-cache"


def test_load_natural_nodes_data_includes_volcanoes_as_peaks(monkeypatch):
    # natural=volcano nodes should be loaded and treated as peaks (issue #20),
    # alongside the existing natural=peak, saddle, and spring nodes.
    monkeypatch.setattr(osm, "is_osm_loaded", lambda: True)
    queries = []
    monkeypatch.setattr(
        osm.util, "run_sql", lambda sql, **kwargs: queries.append(sql)
    )
    osm.load_natural_nodes_data("norcal-latest.osm.pbf")
    create_table_sql = next(sql for sql in queries if "CREATE TABLE" in sql)
    assert "'volcano'" in create_table_sql
    assert "'peak'" in create_table_sql
    assert "WHEN tags -> 'natural' = 'volcano' THEN 'peak'" in create_table_sql


def write_pbf(path, node_versions):
    """Write a PBF of nodes given their versions by ID"""
    with osmium.SimpleWriter(str(path), overwrite=True) as writer:
        for node_id, version in node_versions.items():
            writer.add_node(osmium.osm.mutable.Node(
                id=node_id, version=version, location=(1.0, 2.0)))


def read_node_versions(path):
    """Read the versions of the nodes in a PBF by ID"""
    return {node.id: node.version for node in osmium.FileProcessor(str(path))}


def fake_download(downloads, node_versions_by_url):
    """Stand-in for util.download_file that writes a PBF and records the URL"""
    def download_file(url, path, curl_args=None):
        downloads.append(url)
        write_pbf(path, node_versions_by_url[url])
    return download_file


URLS = ["https://example.com/a-latest.osm.pbf", "https://example.com/b-latest.osm.pbf"]
NODE_VERSIONS = {URLS[0]: {1: 1, 2: 1}, URLS[1]: {2: 1, 3: 1}}


def test_fetch_data_returns_the_file_for_a_single_url(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    downloads = []
    monkeypatch.setattr(osm.util, "download_file", fake_download(downloads, NODE_VERSIONS))
    assert osm.fetch_data(URLS[0]) == "a-latest.osm.pbf"
    assert downloads == URLS[:1]


def test_fetch_data_merges_a_list_of_urls_into_one_pbf(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    downloads = []
    monkeypatch.setattr(osm.util, "download_file", fake_download(downloads, NODE_VERSIONS))
    path = osm.fetch_data(URLS)
    assert path.endswith(".osm.pbf")
    assert downloads == URLS
    # The node in both extracts only appears once
    assert read_node_versions(tmp_path / path) == {1: 1, 2: 1, 3: 1}


def test_fetch_data_keeps_the_newest_version_of_an_object_in_several_extracts(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    node_versions = {URLS[0]: {1: 2}, URLS[1]: {1: 3}}
    monkeypatch.setattr(osm.util, "download_file", fake_download([], node_versions))
    assert read_node_versions(tmp_path / osm.fetch_data(URLS)) == {1: 3}


def test_fetch_data_reuses_a_merged_pbf(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(osm.util, "download_file", fake_download([], NODE_VERSIONS))
    path = osm.fetch_data(URLS)
    write_pbf(tmp_path / path, {9: 1})
    assert read_node_versions(tmp_path / osm.fetch_data(URLS)) == {9: 1}


def test_fetch_data_remerges_when_cleaning(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    downloads = []
    monkeypatch.setattr(osm.util, "download_file", fake_download(downloads, NODE_VERSIONS))
    path = osm.fetch_data(URLS)
    write_pbf(tmp_path / path, {9: 1})
    osm.fetch_data(URLS, clean=True)
    assert downloads == URLS + URLS
    assert read_node_versions(tmp_path / path) == {1: 1, 2: 1, 3: 1}


def test_fetch_data_gives_different_lists_of_urls_different_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    node_versions = {**NODE_VERSIONS, "https://example.com/c.osm.pbf": {4: 1}}
    monkeypatch.setattr(osm.util, "download_file", fake_download([], node_versions))
    assert osm.fetch_data(URLS) != osm.fetch_data(URLS[:1] + ["https://example.com/c.osm.pbf"])
