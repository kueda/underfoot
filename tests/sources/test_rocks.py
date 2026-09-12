# pylint: disable=missing-function-docstring
"""Tests for sources.util.rocks"""
import csv
import json
from numbers import Number
import subprocess
import pytest

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


def test_ages_from_span_parses_and_separated_parts_of_a_span():
    min_age, max_age, _ = rocks.ages_from_span("Late and Middle Jurassic")
    assert min_age == rocks.SPANS["late jurassic"][1]
    assert max_age == rocks.SPANS["middle jurassic"][0]


def test_ages_from_span_orders_ages_of_old_to_young_spans():
    min_age, max_age, _ = rocks.ages_from_span("Cenomanian to Campanian")
    assert min_age == rocks.SPANS["campanian"][1]
    assert max_age == rocks.SPANS["cenomanian"][0]


def test_ages_from_span_orders_ages_of_old_to_young_parts_of_a_span():
    min_age, max_age, _ = rocks.ages_from_span("Middle - Upper Ordovician")
    assert min_age == rocks.SPANS["upper ordovician"][1]
    assert max_age == rocks.SPANS["middle ordovician"][0]


def test_ages_from_span_parses_north_american_regional_stages():
    for span in [
        "Atokan",
        "Desmoinesian",
        "Missourian",
        "Virgilian",
        "Wolfcampian",
        "Leonardian"
    ]:
        assert_ages_from_span_parses_span(span)


def test_ages_from_span_parses_ranges_of_north_american_regional_stages():
    min_age, max_age, _ = rocks.ages_from_span("Atokan to Missourian")
    assert min_age == rocks.SPANS["missourian"][1]
    assert max_age == rocks.SPANS["atokan"][0]


def test_ages_from_span_treats_latest_like_late():
    min_age, max_age, _ = rocks.ages_from_span("middle Pleistocene to latest Pliocene")
    assert min_age == rocks.SPANS["middle pleistocene"][1]
    assert max_age == rocks.SPANS["late pliocene"][0]


def test_ages_from_span_treats_earliest_like_early():
    min_age, max_age, _ = rocks.ages_from_span("Earliest Cretaceous? to Late Jurassic")
    assert min_age == rocks.SPANS["early cretaceous"][1]
    assert max_age == rocks.SPANS["late jurassic"][0]


def test_ages_from_span_treats_latest_like_late_in_parts_of_a_span():
    min_age, max_age, _ = rocks.ages_from_span("latest to early Pleistocene")
    assert min_age == rocks.SPANS["late pleistocene"][1]
    assert max_age == rocks.SPANS["early pleistocene"][0]


def test_ages_from_span_uses_recognized_earliest_span():
    min_age, max_age, _ = rocks.ages_from_span("Earliest Cambrian")
    assert min_age == rocks.SPANS["earliest cambrian"][1]
    assert max_age == rocks.SPANS["earliest cambrian"][0]


def test_ages_from_span_uses_ma_range_when_span_is_unrecognized():
    assert rocks.ages_from_span("late Oligocene, 26–29 Ma") == (26000000, 29000000, 27500000)


def test_ages_from_span_uses_ma_range_when_part_of_span_is_unrecognized():
    assert rocks.ages_from_span("early Oligocene to late Eocene, 31–36 Ma") == (
        31000000, 36000000, 33500000
    )


def test_ages_from_span_uses_widest_ages_of_recognized_span_names():
    min_age, max_age, _ = rocks.ages_from_span("Neogene, mostly Miocene")
    assert min_age == rocks.SPANS["neogene"][1]
    assert max_age == rocks.SPANS["neogene"][0]


def test_ages_from_span_uses_widest_ages_of_span_names_among_qualifiers():
    min_age, max_age, _ = rocks.ages_from_span(
        "Guadalupian in south, in part Leonardian to north"
    )
    assert min_age == rocks.SPANS["guadalupian"][1]
    assert max_age == rocks.SPANS["leonardian"][0]


def test_ages_from_span_uses_widest_ages_of_span_names_in_a_sentence():
    min_age, max_age, _ = rocks.ages_from_span(
        "Maastrichtian to Cenomanian for the most part, although Beartooth and "
        "Sarten Formations are in part Albian"
    )
    assert min_age == rocks.SPANS["maastrichtian"][1]
    assert max_age == rocks.SPANS["albian"][0]


def test_ages_from_span_uses_widest_ages_of_span_names_with_a_qualified_range():
    min_age, max_age, _ = rocks.ages_from_span("locally Virgilian to Late Pennsylvanian")
    assert min_age == rocks.SPANS["virgilian"][1]
    assert max_age == rocks.SPANS["late pennsylvanian"][0]


def test_ages_from_span_ignores_span_names_inside_other_words():
    # "now" and "recent" are spans at age zero, but "unknown" is not a span
    min_age, max_age, _ = rocks.ages_from_span("Cretaceous, age unknown")
    assert min_age == rocks.SPANS["cretaceous"][1]
    assert max_age == rocks.SPANS["cretaceous"][0]


def test_ages_from_span_does_not_infer_ages_when_a_span_excludes_part_of_itself():
    assert rocks.ages_from_span("Quaternary except the Holocene") == (None, None, None)
    assert rocks.ages_from_span("Quaternary (excluding Holocene)") == (None, None, None)


def test_span_from_usgs_code():
    assert rocks.span_from_usgs_code("Tapl") == "tertiary"
    assert rocks.span_from_usgs_code("Kpaf") == "cretaceous"

def test_lithology_from_text_extracts_alluvial_fan():
    assert rocks.lithology_from_text("an alluvial fan is nice") == "alluvial fan"

def test_lithology_from_text_extracts_alluvial_hyphen_fan():
    assert rocks.lithology_from_text("an alluvial-fan is nice") == "alluvial fan"

def test_lithology_from_text_extracts_alluvial_terrace():
    assert rocks.lithology_from_text("an alluvial terrace is nice too") == "alluvial terrace"

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

def test_lithology_from_text_extracts_sedimentary_rock():
    assert rocks.lithology_from_text("interflow sedimentary rocks") == "sedimentary rock"

def test_lithology_from_text_extracts_tectonic_breccia():
    assert rocks.lithology_from_text("some tectonic breccia, friend") == "tectonic breccia"

def test_lithology_from_text_extracts_tectonic_graywacke():
    assert rocks.lithology_from_text("some graywacke, friend") == "graywacke"

def test_lithology_from_text_extracts_tectonic_wacke():
    assert rocks.lithology_from_text("some very fine wacke you got there") == "wacke"

def test_lithology_from_text_extracts_terrace():
    assert rocks.lithology_from_text("marine terrace") == "terrace"

def test_lithology_from_text_extracts_lava_flow():
    assert rocks.lithology_from_text("Basaltic to andesitic lava flows") == "lava flow"

def test_lithology_from_text_extracts_everything_before_water():
    assert rocks.lithology_from_text("watery sandstone") == "sandstone"
    assert rocks.lithology_from_text("did you know granite needs water to form?") == "granite"

def test_infer_metadata_from_csv_row_uses_lithology_column_over_inferred_lithology():
    row = {"lithology": "plutonic rock", "title": "granite"}
    inferred_row = rocks.infer_metadata_from_csv_row(row)
    assert inferred_row["lithology"] == row["lithology"]

def test_infer_metadata_from_csv_row_uses_lithology_from_title_over_description():
    # row = {"title": "alluvium", "description": "gravel"}
    row = {
        "code": "Qa",
        "title": "alluvium",
        "description": "Unconsolidated silt and sand and gravel deposited in active stream channels.",
        "span": "Holocene",
        "lithology": ""
    }
    inferred_row = rocks.infer_metadata_from_csv_row(row)
    assert inferred_row["lithology"] == row["title"]

def test_infer_metadata_from_csv_row_allows_known_lithology():
    row = {"lithology": "sandstone", "title": "foo"}
    assert rocks.infer_metadata_from_csv_row(row)["lithology"] == "sandstone"

def test_infer_metadata_from_csv_row_disallows_unknown_lithology():
    row = {"lithology": "totally not a valid lithology", "title": "foo"}
    with pytest.raises(ValueError):
        rocks.infer_metadata_from_csv_row(row)


def test_infer_metadata_from_csv_row_uses_description_for_lithology_when_specified():
    row = {"title": "granite deposit", "description": "contains sandstone layers"}
    inferred_row = rocks.infer_metadata_from_csv_row(row, lithology_from_description=True)
    assert inferred_row["lithology"] == "sandstone"


def test_infer_metadata_from_csv_row_does_not_use_description_for_lithology_by_default():
    row = {"title": "granite deposit", "description": "contains sandstone layers"}
    inferred_row = rocks.infer_metadata_from_csv_row(row)
    assert inferred_row["lithology"] == "granite"


def test_infer_metadata_from_csv_row_infers_ages_from_span():
    row = {"title": "Some unit", "span": "Cretaceous"}
    inferred_row = rocks.infer_metadata_from_csv_row(row)
    assert inferred_row["min_age"] == rocks.SPANS["cretaceous"][1]
    assert inferred_row["max_age"] == rocks.SPANS["cretaceous"][0]


def test_infer_metadata_from_csv_row_keeps_supplied_ages():
    row = {"title": "Some unit", "span": "Cretaceous", "min_age": "1000", "max_age": "2000"}
    inferred_row = rocks.infer_metadata_from_csv_row(row)
    assert inferred_row["min_age"] == "1000"
    assert inferred_row["max_age"] == "2000"
    assert inferred_row["est_age"] == 1500


def test_infer_metadata_from_csv_row_uses_geomaterial_when_text_has_no_lithology():
    row = {
        "title": "Upper part of Abo Formation",
        "description": "Upper part of Abo Formation.",
        "geomaterial": "Mostly mudstone"
    }
    inferred_row = rocks.infer_metadata_from_csv_row(row)
    assert inferred_row["lithology"] == "mudstone"
    assert inferred_row["rock_type"] == "sedimentary"


def test_infer_metadata_from_csv_row_uses_description_for_lithology_over_geomaterial():
    row = {
        "title": "Yeso Formation",
        "description": "Sandstones, siltstones, anhydrite, gypsum, halite, and dolomite.",
        "geomaterial": "Sedimentary rock"
    }
    inferred_row = rocks.infer_metadata_from_csv_row(row)
    assert inferred_row["lithology"] == "sandstone"


def make_map_package(tmp_path):
    """Make an ArcGIS map package (a 7z archive) containing a geodatabase with a
    polygon layer and a table of unit descriptions. The layer and table names
    follow the GeMS schema, as a realistic example, but nothing requires them"""
    polys_path = tmp_path / "polys.geojson"
    polys_path.write_text(json.dumps({
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"MapUnit": "Qa"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]
                }
            },
            {
                "type": "Feature",
                "properties": {"MapUnit": "Kd"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[1, 1], [2, 1], [2, 2], [1, 1]]]
                }
            }
        ]
    }))
    dmu_path = tmp_path / "dmu.csv"
    dmu_path.write_text(
        "MapUnit,FullName,Age,Description\n"
        "Qa,Alluvium,Holocene,Deposits in active stream channels\n"
        "Kd,Dakota Sandstone,Cenomanian,Cliff-forming\n"
    )
    package_path = tmp_path / "package"
    gdb_path = package_path / "v108" / "map.gdb"
    gdb_path.parent.mkdir(parents=True)
    subprocess.run([
        "ogr2ogr", "-f", "OpenFileGDB", str(gdb_path), str(polys_path),
        "-nln", "MapUnitPolys"
    ], check=True)
    subprocess.run([
        "ogr2ogr", "-update", str(gdb_path), str(dmu_path),
        "-nln", "DescriptionOfMapUnits"
    ], check=True)
    mpk_path = tmp_path / "map.mpk"
    subprocess.run(
        ["7z", "a", "-t7z", str(mpk_path), "v108"],
        cwd=package_path,
        check=True
    )
    return mpk_path


def process_map_package_source(tmp_path, monkeypatch, **kwargs):
    """Run process_usgs_source on a map package and return the resulting unit
    properties by code"""
    mpk_path = make_map_package(tmp_path)
    source_path = tmp_path / "source"
    source_path.mkdir()
    (source_path / "citation.json").write_text("[]")
    work_path = tmp_path / "work"
    work_path.mkdir()
    # process_usgs_source changes the working dir, so make sure it gets restored
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(rocks, "make_work_dir", lambda _path: str(work_path))
    rocks.process_usgs_source(
        base_path=str(source_path / "__init__.py"),
        url=f"file://{mpk_path}",
        extracted_file_path="v108/map.gdb",
        srs=rocks.SRS,
        layer_name="MapUnitPolys",
        polygons_join_col="MapUnit",
        mappable_metadata_layer_name="DescriptionOfMapUnits",
        mappable_metadata_mapping={
            "code": "MapUnit",
            "title": "FullName",
            "span": "Age",
            "description": "Description"
        },
        **kwargs
    )
    with open(work_path / "units.geojson", encoding="utf-8") as units_file:
        return {
            feature["properties"]["code"]: feature["properties"]
            for feature in json.load(units_file)["features"]
        }


def test_process_usgs_source_joins_geodatabase_layer_and_table_from_map_package(tmp_path, monkeypatch):
    units = process_map_package_source(tmp_path, monkeypatch)
    assert sorted(units.keys()) == ["Kd", "Qa"]
    assert units["Qa"]["title"] == "Alluvium"
    assert units["Qa"]["description"] == "Deposits in active stream channels"
    assert units["Qa"]["lithology"] == "alluvium"
    assert units["Kd"]["span"] == "Cenomanian"
    assert units["Kd"]["lithology"] == "sandstone"


def test_process_usgs_source_applies_metadata_overrides_before_inference(tmp_path, monkeypatch):
    overrides_path = tmp_path / "overrides.csv"
    overrides_path.write_text("code,lithology\nQa,conglomerate\n")
    units = process_map_package_source(
        tmp_path,
        monkeypatch,
        metadata_overrides_csv_path=str(overrides_path)
    )
    assert units["Qa"]["title"] == "Alluvium"
    assert units["Qa"]["lithology"] == "conglomerate"
    assert units["Qa"]["rock_type"] == "sedimentary"
    assert units["Kd"]["lithology"] == "sandstone"


def write_metadata_and_overrides(tmp_path, overrides):
    metadata_path = tmp_path / "metadata.csv"
    metadata_path.write_text(
        "code,title,description\n"
        "IPs,Sandia Formation,Predominantly clastic unit\n"
        "Qa,Alluvium,Alluvium\n"
    )
    overrides_path = tmp_path / "overrides.csv"
    overrides_path.write_text(overrides)
    return metadata_path, overrides_path


def test_override_metadata_from_csv_overrides_non_blank_values(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    metadata_path, overrides_path = write_metadata_and_overrides(
        tmp_path,
        "code,title,lithology\nIPs,,sandstone\n"
    )
    outfile_path = rocks.override_metadata_from_csv(str(metadata_path), str(overrides_path))
    with open(outfile_path, encoding="utf-8") as outfile:
        rows = {row["code"]: row for row in csv.DictReader(outfile)}
    assert rows["IPs"]["title"] == "Sandia Formation"
    assert rows["IPs"]["lithology"] == "sandstone"
    assert rows["Qa"]["title"] == "Alluvium"
    assert rows["Qa"]["lithology"] == ""


def test_override_metadata_from_csv_warns_about_overrides_matching_no_units(
    tmp_path,
    monkeypatch,
    capsys
):
    monkeypatch.chdir(tmp_path)
    metadata_path, overrides_path = write_metadata_and_overrides(
        tmp_path,
        "code,lithology\nIPx,sandstone\n"
    )
    rocks.override_metadata_from_csv(str(metadata_path), str(overrides_path))
    assert "IPx" in capsys.readouterr().out
