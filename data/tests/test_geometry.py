"""Tests for the geometry helpers."""

from geometry import widen_bbox


def test_widen_bbox_adds_the_margin_on_every_side():
    bbox = {"left": -122.1, "bottom": 37.8, "right": -121.9, "top": 38.0}
    assert widen_bbox(bbox, 0.5) == {
        "left": -122.6, "bottom": 37.3, "right": -121.4, "top": 38.5}
