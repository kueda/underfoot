"""Tests for elevation.py contour generation helpers (issue #18)."""

from subprocess import CalledProcessError

import mercantile
import pytest

import elevation


@pytest.fixture(autouse=True)
def _stub_dependencies(monkeypatch):
    """Avoid touching the filesystem or doing a real PROJ transform.

    Every DEM tile "exists" except the merge-contours output, so
    make_contours_for_tile always proceeds past its early-return guards.
    """
    monkeypatch.setattr(
        elevation.os.path, "exists",
        lambda path: not path.endswith(".merge-contours.shp")
    )
    monkeypatch.setattr(elevation, "transform", lambda src, dst, xs, ys: (xs, ys))


def _run_calls(monkeypatch, side_effects=None):
    """Patch elevation.run to record every command it's called with.

    If side_effects is given, it's a list of exceptions (or None) to raise/return,
    one per call, in order.
    """
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if side_effects:
            effect = side_effects.pop(0)
            if effect is not None:
                raise effect
        return None

    monkeypatch.setattr(elevation, "run", fake_run)
    return calls


TILE = mercantile.Tile(x=823, y=1611, z=12)


def test_make_contours_for_tile_clips_via_sqlite_dialect_not_clipsrc(monkeypatch):
    """The PG import should clip and keep only line parts inside a
    `-dialect sqlite -sql` query (via ST_CollectionExtract), rather than
    `-clipsrc`, which can hand ogr2ogr a GeometryCollection that fails to COPY
    into the MULTILINESTRING column.
    """
    calls = _run_calls(monkeypatch)
    elevation.make_contours_for_tile(TILE)
    ogr2ogr_call = calls[-1]
    assert ogr2ogr_call[0] == "ogr2ogr"
    assert "-clipsrc" not in ogr2ogr_call
    assert ogr2ogr_call[ogr2ogr_call.index("-dialect") + 1] == "sqlite"
    sql = ogr2ogr_call[ogr2ogr_call.index("-sql") + 1]
    assert "ST_CollectionExtract" in sql


def test_make_contours_for_tile_returns_tile_on_import_failure(monkeypatch):
    """A failed PG import should be reported back to the caller so
    make_contours_table can count and log it, instead of only being logged
    here where Pool worker output is easy to miss.
    """
    _run_calls(monkeypatch, side_effects=[None, None, CalledProcessError(1, "ogr2ogr")])
    assert elevation.make_contours_for_tile(TILE) == TILE


def test_make_contours_for_tile_returns_none_on_success(monkeypatch):
    _run_calls(monkeypatch)
    assert elevation.make_contours_for_tile(TILE) is None


class _FakePool:
    """Synchronous stand-in for multiprocessing.Pool.

    The real Pool pickles its function and arguments into worker processes,
    which can't see anything monkeypatched in this one, so tests use this
    instead to exercise make_contours_table's own logic.
    """

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def imap_unordered(self, func, iterable):
        return (func(item) for item in iterable)


def test_make_contours_table_creates_table_with_geometry_column_before_pool(monkeypatch):
    """The contours table (with its MULTILINESTRING column) must exist before
    any worker starts appending to it, so workers don't race to CREATE TABLE
    themselves.
    """
    sql_statements = []
    monkeypatch.setattr(elevation, "make_database", lambda: None)
    monkeypatch.setattr(
        elevation.util, "run_sql", lambda sql, **kwargs: sql_statements.append(sql)
    )
    monkeypatch.setattr(elevation, "Pool", _FakePool)
    monkeypatch.setattr(elevation, "make_contours_for_tile", lambda tile: None)

    elevation.make_contours_table([TILE])

    create_table_sql = next(sql for sql in sql_statements if "CREATE TABLE" in sql)
    assert "MULTILINESTRING" in create_table_sql
    assert sql_statements.index(create_table_sql) == 1  # right after DROP TABLE


def test_make_contours_table_reports_failed_tiles(monkeypatch, capsys):
    """Failed tiles should be counted and logged once at the end, since Pool
    workers' buffered stdout makes a single failure log line easy to miss.
    """
    monkeypatch.setattr(elevation, "make_database", lambda: None)
    monkeypatch.setattr(elevation.util, "run_sql", lambda sql, **kwargs: None)
    monkeypatch.setattr(elevation, "Pool", _FakePool)
    ok_tile = mercantile.Tile(x=822, y=1611, z=12)
    monkeypatch.setattr(
        elevation, "make_contours_for_tile",
        lambda tile: tile if tile == TILE else None
    )

    elevation.make_contours_table([ok_tile, TILE])

    output = capsys.readouterr().out
    assert "1" in output
    assert "823" in output


def _stub_make_contours_pmtiles_pipeline(monkeypatch):
    """Stub out everything make_contours_pmtiles does before it builds the
    final ogr2ogr export command, so a test can inspect just that command.
    """
    monkeypatch.setattr(elevation, "make_database", lambda: None)
    monkeypatch.setattr(elevation.os.path, "exists", lambda path: False)

    async def fake_cache_tiles(tiles, clean=False):
        return None

    monkeypatch.setattr(elevation, "cache_tiles", fake_cache_tiles)
    monkeypatch.setattr(elevation, "make_contours_table", lambda tiles, procs=2: None)
    calls = []
    monkeypatch.setattr(elevation.util, "call_cmd", lambda cmd: calls.append(cmd))
    return calls


def test_make_contours_pmtiles_honors_separate_minzoom(monkeypatch, tmp_path):
    """A caller can pass a lower pmtiles_minzoom than pmtiles_zoom so the
    exported PMTiles carries a real zoom pyramid (e.g. minzoom 10, maxzoom
    14) instead of a single fixed zoom. This is what lets a lower-zoom style
    layer show a subset of contours (e.g. just the 100m ones) starting well
    before the pack's usual max zoom.
    """
    calls = _stub_make_contours_pmtiles_pipeline(monkeypatch)
    path = str(tmp_path / "contours.pmtiles")

    elevation.make_contours(
        12, swlon=-122.4, swlat=37.6, nelon=-122.0, nelat=38.0,
        pmtiles_zoom=14, pmtiles_minzoom=10, path=path
    )

    export_cmd = calls[-1]
    assert export_cmd[0] == "ogr2ogr"
    assert "MINZOOM=10" in export_cmd
    assert "MAXZOOM=14" in export_cmd


def test_make_contours_pmtiles_defaults_minzoom_to_maxzoom(monkeypatch, tmp_path):
    """Without an explicit pmtiles_minzoom, behavior is unchanged from before
    it existed: a single fixed zoom, since existing callers rely on this.
    """
    calls = _stub_make_contours_pmtiles_pipeline(monkeypatch)
    path = str(tmp_path / "contours.pmtiles")

    elevation.make_contours(
        12, swlon=-122.4, swlat=37.6, nelon=-122.0, nelat=38.0,
        pmtiles_zoom=14, path=path
    )

    export_cmd = calls[-1]
    assert "MINZOOM=14" in export_cmd
    assert "MAXZOOM=14" in export_cmd
