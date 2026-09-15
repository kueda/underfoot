"""Tests for osm.py helpers that need neither a database nor imposm."""

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
