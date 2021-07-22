from numbers import Number
from sources import util


def assert_ages_from_span_parses_span(span):
    ages = util.ages_from_span(span)
    assert len(ages) == 3
    for age in ages:
        assert isinstance(age, Number)


def test_ages_from_span_parses_cretaceous():
    assert_ages_from_span_parses_span("Cretaceous")


def test_ages_from_span_parses_middle_dash_upper_ordovician():
    assert_ages_from_span_parses_span("Middle - Upper Ordovician")


def test_ages_from_span_parses_dash_separated_spans_with_question_marks():
    assert_ages_from_span_parses_span("Devonian? - Silurian?")


def test_ages_from_span_uses_one_of_semicolon_separated_spans():
    assert_ages_from_span_parses_span("Lower Devonian; Siegenian")


def test_ages_from_span_uses_one_of_semicolon_separated_spans_when_first_is_garbage():
    assert_ages_from_span_parses_span("balderdash; Lower Devonian; Siegenian")


def test_ages_from_span_ignores_parentheses():
    assert_ages_from_span_parses_span("Lower Silurian (Llandoverian)")
