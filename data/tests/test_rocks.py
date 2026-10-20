# pylint: disable=missing-function-docstring
"""Tests for rocks.py against a real PostGIS database"""

import uuid

import psycopg2
import pytest

import rocks
from database import DBNAME
from sources import util
from sources.util.rocks import METADATA_COLUMN_NAMES


@pytest.fixture(name="table_name")
def fixture_table_name():
    try:
        psycopg2.connect(f"dbname={DBNAME}").close()
    except psycopg2.OperationalError:
        pytest.skip("needs a PostGIS database")
    name = f"test_overlaps_{uuid.uuid4().hex[:8]}"
    columns = ", ".join(f"{col} text" for col in METADATA_COLUMN_NAMES)
    util.run_sql(f"CREATE TABLE {name} ({columns}, geom geometry(MultiPolygon, 4326))")
    yield name
    for prefix in ["", "temp_", "dumped_", "pieces_"]:
        util.run_sql(f"DROP TABLE IF EXISTS {prefix}{name}")


def insert_unit(table_name, code, geom_sql):
    util.run_sql(
        f"INSERT INTO {table_name} (code, geom) VALUES (%s, ST_Multi({geom_sql}))",
        interpolations=(code,)
    )


def areas(table_name):
    return dict(util.run_sql(f"SELECT code, ST_Area(geom) FROM {table_name}"))


def test_remove_polygon_overlaps_cuts_smaller_unit_out_of_larger(table_name):
    insert_unit(table_name, "big", "ST_MakeEnvelope(0, 0, 2, 2, 4326)")
    insert_unit(table_name, "small", "ST_MakeEnvelope(1, 1, 1.5, 1.5, 4326)")
    rocks.remove_polygon_overlaps(table_name)
    result = areas(table_name)
    assert result["big"] == pytest.approx(3.75)
    assert result["small"] == pytest.approx(0.25)


def test_remove_polygon_overlaps_leaves_adjacent_units_alone(table_name):
    insert_unit(table_name, "left", "ST_MakeEnvelope(0, 0, 1, 1, 4326)")
    insert_unit(table_name, "right", "ST_MakeEnvelope(1, 0, 3, 1, 4326)")
    rocks.remove_polygon_overlaps(table_name)
    result = areas(table_name)
    assert result["left"] == pytest.approx(1)
    assert result["right"] == pytest.approx(2)


def test_remove_polygon_overlaps_cuts_units_with_many_vertices(table_name):
    # A circle with 800 vertices, more than fit in one subdivided piece
    insert_unit(table_name, "circle", "ST_Buffer(ST_SetSRID(ST_Point(0, 0), 4326), 1, 200)")
    insert_unit(table_name, "square", "ST_MakeEnvelope(-0.25, -0.25, 0.25, 0.25, 4326)")
    circle_area = util.run_sql(
        "SELECT ST_Area(ST_Buffer(ST_SetSRID(ST_Point(0, 0), 4326), 1, 200))"
    )[0][0]
    rocks.remove_polygon_overlaps(table_name)
    result = areas(table_name)
    assert result["circle"] == pytest.approx(circle_area - 0.25)
    assert result["square"] == pytest.approx(0.25)
    assert util.run_sql(f"SELECT count(*) FROM {table_name}")[0][0] == 2


# Cut both by the whole circle and by just the pieces of it each larger piece
# touches
@pytest.mark.parametrize("max_pieces_to_cut_whole", [64, 1])
def test_remove_polygon_overlaps_cuts_with_units_with_many_vertices(
        table_name, monkeypatch, max_pieces_to_cut_whole):
    monkeypatch.setattr(rocks, "MAX_PIECES_TO_CUT_WHOLE", max_pieces_to_cut_whole)
    # The circle is the smaller unit this time, so it's the one split into
    # pieces that does the cutting
    insert_unit(table_name, "square", "ST_MakeEnvelope(-2, -2, 2, 2, 4326)")
    insert_unit(table_name, "circle", "ST_Buffer(ST_SetSRID(ST_Point(0, 0), 4326), 1, 200)")
    circle_area = util.run_sql(
        "SELECT ST_Area(ST_Buffer(ST_SetSRID(ST_Point(0, 0), 4326), 1, 200))"
    )[0][0]
    rocks.remove_polygon_overlaps(table_name)
    result = areas(table_name)
    assert result["square"] == pytest.approx(16 - circle_area)
    assert result["circle"] == pytest.approx(circle_area)


class InlinePool:
    """Stands in for multiprocessing.Pool, which can't pickle stubs"""
    def __init__(self, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def starmap(self, func, iterable):
        return [func(*args) for args in iterable]


def mask_calls(monkeypatch, sources):
    calls = []
    monkeypatch.setattr(rocks, "Pool", InlinePool)
    monkeypatch.setattr(rocks.util, "run_sql", lambda *args, **kwargs: None)
    monkeypatch.setattr(rocks, "process_source", lambda *args: None)
    monkeypatch.setattr(rocks, "clip_source_polygons_by_mask", lambda *args: None)
    monkeypatch.setattr(
        rocks.util, "initialize_masks_table", lambda _mask, src: calls.append(("init", src))
    )
    monkeypatch.setattr(
        rocks.util, "update_masks_table", lambda _mask, src: calls.append(("update", src))
    )
    rocks.load_units(sources, procs=1)
    return calls


def test_load_units_skips_the_mask_for_a_single_source(monkeypatch):
    # The mask only clips later sources, and unioning a large source like
    # sim3340 can run the database out of memory
    assert not mask_calls(monkeypatch, ["only"])


def test_load_units_masks_every_source_but_the_last(monkeypatch):
    assert mask_calls(monkeypatch, ["first", "second", "third"]) == [
        ("init", "first"), ("update", "second")
    ]


def test_make_pmtiles_tiles_to_the_given_maxzoom(monkeypatch, tmp_path):
    commands = []
    monkeypatch.setattr(rocks.util, "call_cmd", lambda cmd, **kwargs: commands.append(cmd))
    monkeypatch.setattr(rocks.util, "add_table_from_query_to_pmtiles", lambda **kwargs: None)

    rocks.make_pmtiles(["fake_source"], path=str(tmp_path / "rocks.pmtiles"), maxzoom=10)

    assert "MAXZOOM=10" in commands[0]
    assert "MAXZOOM=14" not in commands[0]


def test_make_rocks_passes_the_maxzoom_to_make_pmtiles(monkeypatch):
    captured = {}
    monkeypatch.setattr(rocks, "make_database", lambda: None)
    monkeypatch.setattr(rocks, "load_units", lambda *args, **kwargs: None)
    monkeypatch.setattr(rocks, "make_pmtiles", lambda *args, **kwargs: captured.update(kwargs))

    rocks.make_rocks(["fake_source"], maxzoom=10)

    assert captured["maxzoom"] == 10
