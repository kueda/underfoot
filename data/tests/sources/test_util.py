# pylint: disable=missing-function-docstring
"""Tests for sources util"""

import pathlib
from sources import util

def test_extless_basename_removes_extension():
    path = str(pathlib.Path(__file__))
    assert path.endswith(".py")
    assert util.extless_basename(path) == "test_util"
