# pylint: disable=missing-function-docstring
"""Tests for statewide USGS geology processing"""

import pandas as pd
from sources.util import usgs_states

HEADER = (
    "STATE,ORIG_LABEL,MAP_SYM1,MAP_SYM2,UNIT_LINK,PROV_NO,PROVINCE,UNIT_NAME,"
    "UNIT_AGE,UNITDESC,STRAT_UNIT,UNIT_COM,MAP_REF,ROCKTYPE1,ROCKTYPE2,ROCKTYPE3,"
    "UNIT_REF"
)
NH_ROW = (
    "NH,-Ch,CAh,CAh;0,NHCAh;0,0,,Hurricane Mountain Formation,Upper Cambrian?,"
    "Slate,,Melange,NH001,slate,schist,,NH002"
)
# Maine's units CSV in OF 2006-1272 has no header row
ME_ROW = (
    "ME,C1,C1,C1;0,MEC1;0,0,,Carboniferous granite undivided,Carboniferous,"
    "Biotite granite,includes: 58 - Biddeford pluton (ME015).,U - Unmetamorphosed,"
    "ME001,granite,,,ME002ME105"
)


def test_merge_attributes_reads_csv_with_header(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "nhunits.csv"
    path.write_text(f"{HEADER}\n{NH_ROW}\n", encoding="ISO-8859-1")
    merged = pd.read_csv(usgs_states.merge_attributes([str(path)]))
    assert list(merged["UNIT_LINK"]) == ["NHCAh;0"]


def test_merge_attributes_reads_csv_without_header(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "meunits.csv"
    path.write_text(f"{ME_ROW}\n", encoding="ISO-8859-1")
    merged = pd.read_csv(usgs_states.merge_attributes([str(path)]))
    assert list(merged["UNIT_LINK"]) == ["MEC1;0"]
    assert list(merged["UNIT_NAME"]) == ["Carboniferous granite undivided"]
    assert list(merged["ROCKTYPE1"]) == ["granite"]


def schemify_row(tmp_path, monkeypatch, **overrides):
    monkeypatch.chdir(tmp_path)
    row = dict(zip(HEADER.split(","), ME_ROW.split(",")))
    row.update(overrides)
    path = tmp_path / "merged_units.csv"
    pd.DataFrame([row]).to_csv(path, index=False)
    return pd.read_csv(usgs_states.schemify_attributes(str(path)), keep_default_na=False).iloc[0]


def test_schemify_attributes_infers_missing_span_from_title(tmp_path, monkeypatch):
    # Maine unit O1b,2 has a blank UNIT_AGE
    row = schemify_row(
        tmp_path, monkeypatch, UNIT_AGE="", UNIT_NAME="Ordovician granite, granodiorite"
    )
    assert row["span"] == "ordovician"
    assert row["min_age"] != ""


def test_schemify_attributes_leaves_ages_blank_without_any_span(tmp_path, monkeypatch):
    row = schemify_row(tmp_path, monkeypatch, UNIT_AGE="", UNIT_NAME="Granite")
    assert row["span"] == ""
    assert row["min_age"] == ""


def test_merge_attributes_merges_csvs_with_and_without_header(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    nh_path = tmp_path / "nhunits.csv"
    nh_path.write_text(f"{HEADER}\n{NH_ROW}\n", encoding="ISO-8859-1")
    me_path = tmp_path / "meunits.csv"
    me_path.write_text(f"{ME_ROW}\n", encoding="ISO-8859-1")
    merged = pd.read_csv(usgs_states.merge_attributes([str(nh_path), str(me_path)]))
    assert sorted(merged["UNIT_LINK"]) == ["MEC1;0", "NHCAh;0"]
