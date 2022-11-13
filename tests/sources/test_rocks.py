# pylint: disable=missing-function-docstring
"""Tests for sources.util.rocks"""
from numbers import Number
from sources.util import rocks


def assert_ages_from_span_parses_span(span):
    ages = rocks.ages_from_span(span)
    assert len(ages) == 3
    for age in ages:
        assert isinstance(age, Number)


def test_ages_from_span_parses_cretaceous():
    assert_ages_from_span_parses_span("Cretaceous")


def test_ages_from_span_parses_middle_dash_upper_ordovician():
    assert_ages_from_span_parses_span("Middle - Upper Ordovician")


def test_ages_from_span_parses_dash_separated_spans_with_question_marks():
    assert_ages_from_span_parses_span("Devonian? - Silurian?")
    assert_ages_from_span_parses_span("Ordovician? - Late Proterozoic?")
    assert_ages_from_span_parses_span("Upper Cambrian? - Lower Ordovician?")
    assert_ages_from_span_parses_span("Lower?- Middle? Silurian")


def test_ages_from_span_uses_one_of_semicolon_separated_spans():
    assert_ages_from_span_parses_span("Lower Devonian; Siegenian")


def test_ages_from_span_uses_one_of_semicolon_separated_spans_when_first_is_garbage():  # noqa: E501
    assert_ages_from_span_parses_span("balderdash; Lower Devonian; Siegenian")


def test_ages_from_span_ignores_parentheses():
    assert_ages_from_span_parses_span("Lower Silurian (Llandoverian)")
    assert_ages_from_span_parses_span("Upper Silurian - (Pridolian and Ludlovian)")


def test_ages_from_span_parses_and_separated_spans():
    assert_ages_from_span_parses_span("Pliocene and Miocene")


def test_span_from_usgs_code():
    assert rocks.span_from_usgs_code("Tapl") == "tertiary"
    assert rocks.span_from_usgs_code("Kpaf") == "cretaceous"

def test_lithology_from_text_extracts_alluvial_fan():
    assert rocks.lithology_from_text("an alluvial fan is nice") == "alluvial fan"

def test_lithology_from_text_extracts_alluvial_hyphen_fan():
    assert rocks.lithology_from_text("an alluvial-fan is nice") == "alluvial fan"

def test_lithology_from_text_extracts_plutonic_rock():
    assert rocks.lithology_from_text("some plutonic rock sandwich") == "plutonic rock"

def test_lithology_from_text_extracts_arenaceous_as_sand():
    assert rocks.lithology_from_text("this is so arenaceous") == "sand"

def test_lithology_from_text_extracts_silt():
    assert rocks.lithology_from_text("what a load of silt") == "silt"

def test_lithology_from_text_extracts_siltstone_before_silt():
    assert rocks.lithology_from_text("some silt and some siltstone") == "siltstone"

def test_lithology_from_text_extracts_calcerenite():
    assert rocks.lithology_from_text("some quartz-bearing calcerenites") == "calcerenite"

def test_lithology_from_text_extracts_colluvium():
    assert rocks.lithology_from_text("some very fine colluvium") == "colluvium"

def test_lithology_from_text_extracts_syenite():
    assert rocks.lithology_from_text("some syenite, oh boy") == "syenite"

def test_lithology_from_text_extracts_sedimentary_breccia():
    assert rocks.lithology_from_text("some sedimentary breccia, friend") == "sedimentary breccia"

def test_lithology_from_text_extracts_tectonic_breccia():
    assert rocks.lithology_from_text("some tectonic breccia, friend") == "tectonic breccia"

def test_lithology_from_text_extracts_everything_before_water():
    assert rocks.lithology_from_text("watery sandstone") == "sandstone"
    assert rocks.lithology_from_text("did you know granite needs water to form?") == "granite"

def test_infer_metadata_from_csv_row_uses_lithology_column_over_inferred_lithology():
    row = {"lithology": "plutonic rock", "title": "granite"}
    inferred_row = rocks.infer_metadata_from_csv_row(row)
    assert inferred_row["lithology"] == row["lithology"]
