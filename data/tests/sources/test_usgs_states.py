# pylint: disable=missing-function-docstring
"""Tests for statewide USGS geology processing"""

import json
import os
import zipfile

import pandas as pd
import pytest
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


def write_shared_citation(tmp_path):
    base = tmp_path / "of2006_1272" / "__init__.py"
    base.parent.mkdir()
    base.write_text("")
    (base.parent / "citation.json").write_text(
        '[{"URL": "https://pubs.usgs.gov/of/2006/1272/#NH", "title": "x"}]'
    )
    work = tmp_path / "work"
    work.mkdir()
    return base, work


def test_copy_citation_links_to_the_single_state(tmp_path):
    base, work = write_shared_citation(tmp_path)
    usgs_states.copy_citation(str(base), str(work), ["VT"])
    citation = json.loads((work / "citation.json").read_text())[0]
    assert citation["URL"] == "https://pubs.usgs.gov/of/2006/1272/#VT"
    assert citation["title"] == "x"


def test_copy_citation_keeps_url_for_multiple_states(tmp_path):
    base, work = write_shared_citation(tmp_path)
    usgs_states.copy_citation(str(base), str(work), ["NH", "VT"])
    citation = json.loads((work / "citation.json").read_text())[0]
    assert citation["URL"].endswith("#NH")


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


def schemify_row(tmp_path, monkeypatch, overrides_path=None, **overrides):
    monkeypatch.chdir(tmp_path)
    row = dict(zip(HEADER.split(","), ME_ROW.split(",")))
    row.update(overrides)
    path = tmp_path / "merged_units.csv"
    pd.DataFrame([row]).to_csv(path, index=False)
    schemified_path = usgs_states.schemify_attributes(str(path), overrides_path=overrides_path)
    return pd.read_csv(schemified_path, keep_default_na=False).iloc[0]


# OF 2005-1351 names some lithologies differently from GeMS, e.g. without "rock"
@pytest.mark.parametrize("rocktype,lithology", [
    ("clastic", "clastic rock"),
    ("fine-grained mixed clastic", "fine-grained mixed clastic rock"),
    ("medium-grained mixed clastic", "medium-grained mixed clastic rock"),
    ("coarse-grained mixed clastic", "coarse-grained mixed clastic rock"),
    ("mixed clastic/carbonate", "mixed carbonate/clastic rock"),
    # e.g. in AL (#89)
    ("carbonate", "carbonate rock"),
])
def test_schemify_attributes_uses_known_name_for_lithology(
    tmp_path, monkeypatch, rocktype, lithology
):
    row = schemify_row(tmp_path, monkeypatch, ROCKTYPE1=rocktype)
    assert row["lithology"] == lithology
    assert row["rock_type"] == "sedimentary"


def test_schemify_attributes_uses_known_name_for_mixed_clastic_volcanic(
    tmp_path, monkeypatch
):
    # e.g. OR unit TRsv, "Sedimentary and volcanic rocks"
    row = schemify_row(tmp_path, monkeypatch, ROCKTYPE1="mixed clastic/volcanic")
    assert row["lithology"] == "mixed volcanic/clastic rock"


@pytest.mark.parametrize("rocktype,lithology", [
    # Truncated in the source, e.g. CO unit Tmi, "Middle Tertiary intrusive rocks"
    ("plutonic rock (phanerit", "plutonic rock"),
    # Truncated in the source, e.g. CO, "Volcanic rocks in northwestern Colorado"
    ("intermediate volcanic r", "intermediate volcanic rock"),
    # e.g. AR unit Kw, "Woodbine Formation"
    ("volcanic rock (aphanitic)", "volcanic rock"),
    # e.g. ME, "Silurian unnamed mafic volcanic rocks"
    ("mafic volcanic rocks", "mafic volcanic rock"),
    # e.g. ME, "Cretaceous alkali feldspar granite"
    ("alkali-granite (alaskite)", "alaskite"),
    # e.g. ME, "Alkali syenite"
    ("alkali syenite", "alkali-feldspar syenite"),
    # e.g. NV, "BRECCIA-Volcanic, thrust, and jasperoid breccia..."
    ("volcanic breccia (agglomerate)", "breccia"),
])
def test_schemify_attributes_uses_known_name_for_igneous_lithology(
    tmp_path, monkeypatch, rocktype, lithology
):
    row = schemify_row(tmp_path, monkeypatch, ROCKTYPE1=rocktype)
    assert row["lithology"] == lithology
    assert row["rock_type"] == "igneous"


@pytest.mark.parametrize("rocktype,lithology,rock_type", [
    ("dolostone (dolomite)", "dolomite", "sedimentary"),
    ("paragneiss", "gneiss", "metamorphic"),
    ("orthogneiss", "gneiss", "metamorphic"),
    ("tephrite (basanite)", "basanite", "igneous"),
    # Not a rock, so like alluvium it has no rock_type
    ("unconsolidated deposit", "unconsolidated material", ""),
    # e.g. MS unit "Jackson group"
    ("clay or mud", "clay", ""),
    # e.g. MA unit Oca, "Cobble Mountain Formation"
    ("pelitic schist", "schist", "metamorphic"),
    ("plutonic rock (phaneritic)", "plutonic rock", "igneous"),
])
def test_schemify_attributes_normalizes_lithology_synonyms(
    tmp_path, monkeypatch, rocktype, lithology, rock_type
):
    row = schemify_row(tmp_path, monkeypatch, ROCKTYPE1=rocktype)
    assert row["lithology"] == lithology
    assert row["rock_type"] == rock_type


# Describes how a unit was deposited, not what it's made of (#121)
LAKE_OR_MARINE = "lake or marine deposit (non-glacial)"


@pytest.mark.parametrize("rocktype,title,lithology,rock_type", [
    # Unknown values never reach the map, whatever they are
    ("not a real lithology", "Sedimentary rocks", "sedimentary rock", "sedimentary"),
    # e.g. NV unit QTs
    (LAKE_OR_MARINE, "Sedimentary rocks", "sedimentary rock", "sedimentary"),
    # e.g. NC unit Qp
    (LAKE_OR_MARINE, "Surficial Deposits, Undivided", "surficial deposits", ""),
    # Double space in the source, e.g. in WA
    (LAKE_OR_MARINE.replace(" (", "  ("), "Sedimentary rocks", "sedimentary rock", "sedimentary"),
])
def test_schemify_attributes_infers_lithology_from_title_for_non_lithology(
    tmp_path, monkeypatch, rocktype, title, lithology, rock_type
):
    row = schemify_row(tmp_path, monkeypatch, ROCKTYPE1=rocktype, UNIT_NAME=title)
    assert row["lithology"] == lithology
    assert row["rock_type"] == rock_type


def test_schemify_attributes_infers_lithology_from_description_for_non_lithology(
    tmp_path, monkeypatch
):
    row = schemify_row(
        tmp_path,
        monkeypatch,
        ROCKTYPE1=LAKE_OR_MARINE,
        UNIT_NAME="Quaternary nonmarine deposits",
        UNITDESC="Interbedded sandstone and shale"
    )
    assert row["lithology"] == "sandstone"
    assert row["rock_type"] == "sedimentary"


def test_schemify_attributes_leaves_unknown_lithology_blank_without_text_to_infer_from(
    tmp_path, monkeypatch
):
    # e.g. OK unit UNKNWN
    row = schemify_row(
        tmp_path,
        monkeypatch,
        ROCKTYPE1="indeterminate",
        UNIT_NAME="Unknown",
        UNITDESC="",
        UNIT_COM=""
    )
    assert row["lithology"] == ""
    assert row["rock_type"] == ""


@pytest.mark.parametrize("rocktype,rock_type", [
    ("amphibolite", "metamorphic"),
    ("granitic gneiss", "metamorphic"),
    ("ash-flow tuff", "igneous"),
    ("trondhjemite", "igneous"),
    ("black shale", "sedimentary"),
    ("iron formation", "sedimentary"),
    ("beach sand", ""),
    ("peat", ""),
])
def test_schemify_attributes_keeps_specific_lithology_and_its_rock_type(
    tmp_path, monkeypatch, rocktype, rock_type
):
    row = schemify_row(tmp_path, monkeypatch, ROCKTYPE1=rocktype)
    assert row["lithology"] == rocktype
    assert row["rock_type"] == rock_type


@pytest.mark.parametrize("rocktype,lithology", [
    ("meta-basalt", "metabasalt"),
    ("meta-conglomerate", "metaconglomerate"),
    ("meta-rhyolite", "metarhyolite"),
    ("alkali rhyolite", "alkali-feldspar rhyolite"),
    ("biogenic sediment", "biogenic material"),
    ("biotite schist", "mica schist"),
    ("eolian", "eolian material"),
    ("outwash", "glacial outwash sediment"),
    ("pyroclastic", "pyroclastic rock"),
    ("ultramafic rock", "ultramafitite"),
    ("ultramafitite (komatiite)", "ultramafitite"),
])
def test_schemify_attributes_normalizes_rocktype_to_name_used_by_map(
    tmp_path, monkeypatch, rocktype, lithology
):
    row = schemify_row(tmp_path, monkeypatch, ROCKTYPE1=rocktype)
    assert row["lithology"] == lithology


def test_schemify_attributes_prefers_rocktype_lithology_over_title(tmp_path, monkeypatch):
    row = schemify_row(
        tmp_path,
        monkeypatch,
        ROCKTYPE1="granitic gneiss",
        UNIT_NAME="Camp Hill Granite Gneiss"
    )
    assert row["lithology"] == "granitic gneiss"


def test_schemify_attributes_prefers_override_over_inferred_lithology(tmp_path, monkeypatch):
    overrides_path = tmp_path / "overrides.csv"
    overrides_path.write_text("code,lithology\nC1,clay\n", encoding="utf-8")
    row = schemify_row(
        tmp_path,
        monkeypatch,
        overrides_path=str(overrides_path),
        ROCKTYPE1=LAKE_OR_MARINE,
        UNIT_NAME="Sedimentary rocks"
    )
    assert row["lithology"] == "clay"


def test_schemify_attributes_uses_known_name_for_metamorphic_lithology(
    tmp_path, monkeypatch
):
    # e.g. MA unit Om, "Moretown Formation" (#115)
    row = schemify_row(tmp_path, monkeypatch, ROCKTYPE1="granofels")
    assert row["lithology"] == "granofel"
    assert row["rock_type"] == "metamorphic"


def test_schemify_attributes_applies_overrides(tmp_path, monkeypatch):
    overrides_path = tmp_path / "overrides.csv"
    overrides_path.write_text("code,lithology\nC1,mudstone\n", encoding="utf-8")
    row = schemify_row(tmp_path, monkeypatch, overrides_path=str(overrides_path))
    assert row["lithology"] == "mudstone"
    assert row["rock_type"] == "sedimentary"


def test_schemify_attributes_ignores_overrides_for_other_codes(tmp_path, monkeypatch):
    overrides_path = tmp_path / "overrides.csv"
    overrides_path.write_text("code,lithology\nC2,mudstone\n", encoding="utf-8")
    row = schemify_row(tmp_path, monkeypatch, overrides_path=str(overrides_path))
    assert row["lithology"] == "granite"
    assert row["rock_type"] == "igneous"


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


def write_shapes_zip(tmp_path, state, shp_basename):
    zip_path = tmp_path / f"{state}geol_dd.zip"
    with zipfile.ZipFile(zip_path, "w") as zip_f:
        for ext in ["shp", "shx", "dbf", "prj"]:
            zip_f.writestr(f"{shp_basename}.{ext}", "")


def test_download_shapes_finds_poly_dd_shapefile(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_shapes_zip(tmp_path, "WI", "wigeol_poly_dd")
    path = usgs_states.download_shapes("WI", "https://example.com")
    assert path == str(tmp_path / "wigeol_poly_dd.shp")


def test_download_shapes_finds_dd_polygon_shapefile(tmp_path, monkeypatch):
    # OF 2005-1351 names its polygon shapefiles <state>geol_dd_polygon.shp
    monkeypatch.chdir(tmp_path)
    write_shapes_zip(tmp_path, "IA", "iageol_dd_polygon")
    path = usgs_states.download_shapes("IA", "https://example.com")
    assert path == str(tmp_path / "iageol_dd_polygon.shp")


@pytest.mark.parametrize("unit_age,expected", [
    ("Phanerozoic | Mesozoic | Cretaceous-Late [Gulfian]", "Late Cretaceous"),
    ("Phanerozoic | Cenozoic | Tertiary | Eocene", "Eocene"),
    ("Phanerozoic | Paleozoic | Carboniferous Pennsylvanian-Middle", "Middle Pennsylvanian"),
    ("Phanerozoic | Cenozoic | Quaternary | Pleistocene Holocene", "Pleistocene to Holocene"),
    ("Phanerozoic | Paleozoic Mesozoic | Permian Triassic-Early(?)", "Permian to Early Triassic"),
    ("Phanerozoic | Cenozoic | Tertiary | Eocene (44-49 Ma)", "Eocene"),
    ("Phanerozoic | Mesozoic | Cretaceous-Early [Comanchean", "Early Cretaceous"),
    ("Phanerozoic | Paleozoic| Ordovician-Late", "Late Ordovician"),
    ("Phanerozoic | Paleozoic | Cambrian-Furongian", "Furongian"),
    ("Proterozoic | Paleoproterozoic", "Paleoproterozoic"),
    ("preCambrian-Proterozoic [Grenville]", "Proterozoic"),
    ("Upper Cretaceous", "Upper Cretaceous"),
    ("Cambrian-Ordovician", "Cambrian-Ordovician"),
])
def test_normalize_unit_age(unit_age, expected):
    assert usgs_states.normalize_unit_age(unit_age) == expected


def test_schemify_attributes_normalizes_hierarchical_unit_age(tmp_path, monkeypatch):
    # OF 2005-1351 lists ages from eon to epoch
    row = schemify_row(
        tmp_path, monkeypatch, UNIT_AGE="Phanerozoic | Mesozoic | Cretaceous-Late"
    )
    assert row["span"] == "Late Cretaceous"
    assert row["controlled_span"] == "upper cretaceous"


def test_schemify_attributes_strips_whitespace_from_lithology(tmp_path, monkeypatch):
    # Oregon unit Tba in OF 2005-1305 has a ROCKTYPE1 of "basalt "
    row = schemify_row(tmp_path, monkeypatch, ROCKTYPE1="basalt ")
    assert row["lithology"] == "basalt"
    assert row["rock_type"] == "igneous"


def test_process_usgs_states_works_in_the_directory_of_its_source(tmp_path, monkeypatch):
    # Sources of one family run in parallel in a pack like us-md, and would
    # overwrite each other's intermediate files if they shared a directory
    (tmp_path / "of_family").mkdir()
    (tmp_path / "of_family" / "citation.json").write_text("[]", encoding="utf-8")
    source_work_path = tmp_path / "work-of_family_md"
    source_work_path.mkdir()
    monkeypatch.setattr(usgs_states, "make_work_dir", lambda path: str(source_work_path)
        if os.path.basename(path) == "of_family_md.py" else str(tmp_path / "work-of_family"))
    work_dirs = []
    def record_work_dir(*args, **kwargs):
        work_dirs.append(os.getcwd())
        return "path"
    for name in ["download_shapes", "merge_shapes", "download_attributes", "merge_attributes",
                 "schemify_attributes", "join_polygons_and_metadata"]:
        monkeypatch.setattr(usgs_states, name, record_work_dir)
    monkeypatch.chdir(tmp_path)

    usgs_states.process_usgs_states(
        states=["MD"],
        base_path=str(tmp_path / "of_family" / "__init__.py"),
        base_url="https://example.com",
        source_path=str(tmp_path / "of_family_md.py"))

    assert set(work_dirs) == {str(source_work_path)}
    assert (source_work_path / "citation.json").is_file()
