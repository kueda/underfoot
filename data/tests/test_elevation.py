"""Tests for elevation.py contour generation helpers (issue #18)."""

import json
import pathlib
import time
import uuid
from subprocess import CalledProcessError

import mercantile
import psycopg2
import pytest

import elevation
from database import DBNAME
from sources import util


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


@pytest.mark.parametrize("import_error", [None, CalledProcessError(1, "ogr2ogr")])
def test_make_contours_for_tile_removes_its_intermediate_files(
        monkeypatch, tmp_path, import_error):
    """Each tile's merged DEM is about 9 MB, which across tens of thousands of
    tiles filled the disk of a CI runner"""
    monkeypatch.setattr(elevation, "CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(elevation.os.path, "exists", lambda path: pathlib.Path(path).exists())
    for x in range(TILE.x - 1, TILE.x + 2):
        for y in range(TILE.y - 1, TILE.y + 2):
            tile_path = pathlib.Path(elevation.tile_file_path(x, y, TILE.z))
            tile_path.parent.mkdir(parents=True, exist_ok=True)
            tile_path.touch()

    def fake_run(cmd, **_kwargs):
        if cmd[0] == "gdal_merge.py":
            pathlib.Path(cmd[cmd.index("-o") + 1]).touch()
        elif cmd[0] == "gdal_contour":
            shp_path = pathlib.Path(cmd[-1])
            for ext in ["shp", "shx", "dbf", "prj"]:
                shp_path.with_suffix(f".{ext}").touch()
        elif import_error:
            raise import_error

    monkeypatch.setattr(elevation, "run", fake_run)
    elevation.make_contours_for_tile(TILE)

    assert not list(tmp_path.rglob("*.merge*"))
    assert pathlib.Path(elevation.tile_file_path(TILE.x, TILE.y, TILE.z)).exists()


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
    """Stub out everything make_contours_pmtiles does before it tiles the
    contours, so a test can inspect the export and tippecanoe command.
    """
    monkeypatch.setattr(elevation, "make_database", lambda: None)
    monkeypatch.setattr(elevation.os.path, "exists", lambda path: False)

    async def fake_cache_tiles(tiles, clean=False):
        return None

    monkeypatch.setattr(elevation, "cache_tiles", fake_cache_tiles)
    monkeypatch.setattr(elevation, "make_contours_table", lambda tiles, procs=2: None)
    exports = []
    monkeypatch.setattr(
        elevation, "export_contours",
        lambda path, minor_minzoom: exports.append(minor_minzoom))
    calls = []
    monkeypatch.setattr(elevation.util, "call_cmd", lambda cmd: calls.append(cmd))
    return calls, exports


def test_make_contours_pmtiles_honors_separate_minzoom(monkeypatch, tmp_path):
    """A caller can pass a lower pmtiles_minzoom than pmtiles_zoom so the
    exported PMTiles carries a real zoom pyramid (e.g. minzoom 10, maxzoom
    12) instead of a single fixed zoom. This is what lets a lower-zoom style
    layer show a subset of contours (e.g. just the 100m ones) starting well
    before the pack's usual max zoom.
    """
    calls, _exports = _stub_make_contours_pmtiles_pipeline(monkeypatch)
    path = str(tmp_path / "contours.pmtiles")

    elevation.make_contours(
        12, swlon=-122.4, swlat=37.6, nelon=-122.0, nelat=38.0,
        pmtiles_zoom=12, pmtiles_minzoom=10, path=path
    )

    tile_cmd = calls[-1]
    assert tile_cmd[0] == "tippecanoe"
    assert f"--output={path}" in tile_cmd
    assert "--minimum-zoom=10" in tile_cmd
    assert "--maximum-zoom=12" in tile_cmd


def test_make_contours_pmtiles_defaults_minzoom_to_maxzoom(monkeypatch, tmp_path):
    """Without an explicit pmtiles_minzoom, behavior is unchanged from before
    it existed: a single fixed zoom, since existing callers rely on this.
    """
    calls, _exports = _stub_make_contours_pmtiles_pipeline(monkeypatch)
    path = str(tmp_path / "contours.pmtiles")

    elevation.make_contours(
        12, swlon=-122.4, swlat=37.6, nelon=-122.0, nelat=38.0,
        pmtiles_zoom=14, path=path
    )

    tile_cmd = calls[-1]
    assert "--minimum-zoom=14" in tile_cmd
    assert "--maximum-zoom=14" in tile_cmd


@pytest.mark.parametrize("pmtiles_zoom, expected", [(12, 12), (14, 12), (10, 10)])
def test_make_contours_pmtiles_starts_minor_contours_where_the_style_shows_them(
        monkeypatch, tmp_path, pmtiles_zoom, expected):
    """Minor contours start at the zoom the web app's style shows them from,
    or the max zoom if that's lower, since the map overzooms its tiles"""
    _calls, exports = _stub_make_contours_pmtiles_pipeline(monkeypatch)

    elevation.make_contours(
        12, swlon=-122.4, swlat=37.6, nelon=-122.0, nelat=38.0,
        pmtiles_zoom=pmtiles_zoom, pmtiles_minzoom=8,
        path=str(tmp_path / "contours.pmtiles")
    )

    assert exports == [expected]


@pytest.fixture(name="contours_table")
def fixture_contours_table(monkeypatch):
    try:
        psycopg2.connect(f"dbname={DBNAME}").close()
    except psycopg2.OperationalError:
        pytest.skip("needs a PostGIS database")
    name = f"test_contours_{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(elevation, "TABLE_NAME", name)
    util.run_sql(f"""
        CREATE TABLE {name} (
            elevation NUMERIC(23,15),
            wkb_geometry geometry(MULTILINESTRING, 4326)
        )
    """)
    yield name
    util.run_sql(f"DROP TABLE IF EXISTS {name}")


def test_export_contours_starts_minor_contours_at_minor_minzoom(contours_table, tmp_path):
    for elevation_m in (100, 125):
        util.run_sql(
            f"""
                INSERT INTO {contours_table} VALUES (
                    %s, ST_GeomFromText('MULTILINESTRING((0 0, 1 1))', 4326)
                )
            """,
            interpolations=(elevation_m,)
        )
    path = tmp_path / "contours.geojsons"

    elevation.export_contours(str(path), 12)

    features = [json.loads(line) for line in path.read_text().splitlines()]
    by_elevation = {f["properties"]["elevation"]: f for f in features}
    assert set(by_elevation) == {100, 125}
    assert "tippecanoe" not in by_elevation[100]
    assert by_elevation[125]["tippecanoe"] == {"minzoom": 12}
    assert by_elevation[125]["geometry"]["coordinates"] == [[[0, 0], [1, 1]]]


def test_export_contours_skips_contours_without_geometry(contours_table, tmp_path):
    util.run_sql(
        f"""
            INSERT INTO {contours_table} VALUES
                (100, ST_GeomFromText('MULTILINESTRING((0 0, 1 1))', 4326)),
                (200, NULL)
        """
    )
    path = tmp_path / "contours.geojsons"

    elevation.export_contours(str(path), 12)

    features = [json.loads(line) for line in path.read_text().splitlines()]
    assert [f["properties"]["elevation"] for f in features] == [100]


def test_cache_tiles_caches_every_chunk_without_pausing(monkeypatch):
    cached = []

    async def fake_cache_tile(tile, client, clean=False, debug=False):
        cached.append(tile)
        return f"{tile.x}.tif"

    monkeypatch.setattr(elevation, "cache_tile", fake_cache_tile)
    sleeps = []
    monkeypatch.setattr(time, "sleep", sleeps.append)
    tiles = [mercantile.Tile(x=x, y=0, z=12) for x in range(450)]

    elevation.asyncio.run(elevation.cache_tiles(tiles))

    assert sorted(cached) == sorted(tiles)
    assert not sleeps


def _cache_tile_with_responses(monkeypatch, tmp_path, statuses):
    """Run cache_tile against fake S3 responses with these status codes, in
    order, without actually waiting between retries"""
    monkeypatch.setattr(
        elevation, "tile_file_path",
        lambda x, y, z, ext="tif": str(tmp_path / f"{z}-{x}-{y}.{ext}"))
    # Undo the autouse stub that makes every tile look cached
    monkeypatch.setattr(elevation.os.path, "exists", lambda path: pathlib.Path(path).exists())

    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(elevation.asyncio, "sleep", no_wait)
    requests = []

    def respond(request):
        requests.append(request)
        return elevation.httpx.Response(statuses[len(requests) - 1], content=b"tif")

    async def cache():
        transport = elevation.httpx.MockTransport(respond)
        async with elevation.httpx.AsyncClient(transport=transport) as client:
            return await elevation.cache_tile(TILE, client)

    return elevation.asyncio.run(cache()), requests


@pytest.mark.parametrize("status", [429, 500, 503])
def test_cache_tile_retries_when_s3_is_briefly_unavailable(monkeypatch, tmp_path, status):
    path, requests = _cache_tile_with_responses(monkeypatch, tmp_path, [status, 200])
    assert len(requests) == 2
    assert pathlib.Path(path).read_bytes() == b"tif"


def test_cache_tile_does_not_retry_a_missing_tile(monkeypatch, tmp_path):
    path, requests = _cache_tile_with_responses(monkeypatch, tmp_path, [404, 200])
    assert len(requests) == 1
    assert path is None


def test_cache_tile_skips_a_tile_that_keeps_timing_out(monkeypatch, tmp_path):
    monkeypatch.setattr(
        elevation, "tile_file_path",
        lambda x, y, z, ext="tif": str(tmp_path / f"{z}-{x}-{y}.{ext}"))
    monkeypatch.setattr(elevation.os.path, "exists", lambda path: pathlib.Path(path).exists())

    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(elevation.asyncio, "sleep", no_wait)
    requests = []

    def time_out(request):
        requests.append(request)
        raise elevation.httpx.ConnectTimeout("timed out", request=request)

    async def cache():
        transport = elevation.httpx.MockTransport(time_out)
        async with elevation.httpx.AsyncClient(transport=transport) as client:
            return await elevation.cache_tile(TILE, client)

    assert elevation.asyncio.run(cache()) is None
    assert len(requests) == 3


def test_cache_tiles_fails_after_downloading_if_any_tile_failed(monkeypatch):
    tiles = [mercantile.Tile(x=x, y=0, z=12) for x in range(450)]
    failing = {tiles[3], tiles[401]}
    cached = []

    async def fake_cache_tile(tile, client, clean=False, debug=False):
        cached.append(tile)
        return None if tile in failing else f"{tile.x}.tif"

    monkeypatch.setattr(elevation, "cache_tile", fake_cache_tile)

    with pytest.raises(RuntimeError) as error:
        elevation.asyncio.run(elevation.cache_tiles(tiles))

    assert len(cached) == len(tiles)
    assert "2 of 450" in str(error.value)
    for tile in failing:
        assert str(tile) in str(error.value)
