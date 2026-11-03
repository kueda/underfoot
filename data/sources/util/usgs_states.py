"""
Methods for processing state-wide USGS geology files
"""

import csv
import json
import os
import re
import pandas as pd
from . import (
    call_cmd,
    download_file,
    log,
    make_work_dir,
    unzip
)
from .proj import SRS as DEST_SRS
from .rocks import (
    ages_from_span,
    controlled_span_from_span,
    join_polygons_and_metadata,
    LITHOLOGIES,
    LITHOLOGY_SYNONYMS,
    lithology_from_text,
    METADATA_COLUMN_NAMES,
    rock_type_from_lithology,
    span_from_lithology,
    span_from_text
)

SRS = "+proj=longlat +datum=NAD27 +no_defs"

# Columns of the <state>units.csv attribute files
ATTRIBUTE_COLUMN_NAMES = [
    "STATE",
    "ORIG_LABEL",
    "MAP_SYM1",
    "MAP_SYM2",
    "UNIT_LINK",
    "PROV_NO",
    "PROVINCE",
    "UNIT_NAME",
    "UNIT_AGE",
    "UNITDESC",
    "STRAT_UNIT",
    "UNIT_COM",
    "MAP_REF",
    "ROCKTYPE1",
    "ROCKTYPE2",
    "ROCKTYPE3",
    "UNIT_REF"
]

def download_shapes(state, base_url):
    """Download and extract shapefiles"""
    log(f"DOWNLOADING SHAPEFILES FOR {state}...")
    url = os.path.join(base_url, f"{state}geol_dd.zip")
    download_path = os.path.basename(url)
    if not os.path.isfile(download_path):
        log(f"DOWNLOADING {url}")
        download_file(url)
    # OF 2005-1351 names them <state>geol_dd_polygon.shp
    shp_paths = [
        f"{state.lower()}geol_poly_dd.shp",
        f"{state.lower()}geol_dd_polygon.shp"
    ]
    if not any(os.path.isfile(path) for path in shp_paths):
        log("EXTRACTING ARCHIVE...")
        unzip(download_path)
    for path in shp_paths:
        if os.path.isfile(path):
            return os.path.realpath(path)
    raise FileNotFoundError(f"Could not find polygon shapefile in {url}")


def download_attributes(state, base_url):
    """Download and extract attributes for state"""
    log(f"DOWNLOADING ATTRIBUTES FOR {state}...")
    url = os.path.join(base_url, f"{state}csv.zip")
    download_path = os.path.basename(url)
    if not os.path.isfile(download_path):
        log(f"DOWNLOADING {url}")
        download_file(url)
    csv_path = f"{state}units.csv"
    if not os.path.isfile(csv_path):
        log("EXTRACTING ARCHIVE...")
        unzip(download_path)
    if not os.path.isfile(csv_path):
        csv_path = re.sub(f'^{state}', state.lower(), csv_path)
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"Could not find attributes CSV in {url}")
    return os.path.realpath(csv_path)


AGE_MODIFIERS = ["Early", "Middle", "Late"]


def normalize_unit_age(unit_age):
    """
    Convert ages like "Phanerozoic | Mesozoic | Cretaceous-Late [Gulfian]"
    from OF 2005-1351, which list the age from eon to epoch, into a span like
    "Late Cretaceous". Other ages are returned unchanged.
    """
    if "|" not in unit_age and not unit_age.startswith("preCambrian-"):
        return unit_age
    # Drop stage names, uncertainty, and radiometric ages
    age = re.sub(r"\[[^\]]*(\]|$)", "", unit_age)
    age = re.sub(r"\([^)]*\)", "", age)
    # The last level is the most specific
    age = age.split("|")[-1]
    terms = []
    for term in age.split():
        if "-" in term:
            name, modifier = term.split("-", 1)
            term = f"{modifier} {name}" if modifier in AGE_MODIFIERS else modifier
        terms.append(term)
    # Carboniferous only qualifies Mississippian and Pennsylvanian
    terms = [
        term for i, term in enumerate(terms)
        if term != "Carboniferous" or i == len(terms) - 1
    ]
    if len(terms) == 1:
        return terms[0]
    return f"{terms[0]} to {terms[-1]}"


# Lithologies in these sources that Underfoot knows by another name. These
# take precedence over LITHOLOGY_SYNONYMS, whose values for "pelitic schist"
# and "plutonic rock (phaneritic)" have the wrong rock type or none here.
ROCKTYPE_LITHOLOGIES = {
    "carbonate": "carbonate rock",
    "clastic": "clastic rock",
    # e.g. in MS, "Jackson group"
    "clay or mud": "clay",
    "fine-grained mixed clastic": "fine-grained mixed clastic rock",
    "medium-grained mixed clastic": "medium-grained mixed clastic rock",
    "coarse-grained mixed clastic": "coarse-grained mixed clastic rock",
    "mixed clastic/carbonate": "mixed carbonate/clastic rock",
    "mixed clastic/volcanic": "mixed volcanic/clastic rock",
    "glaciolacustrine": "glaciolacustrine sediment",
    "granofels": "granofel",
    "volcanic rock (aphanitic)": "volcanic rock",
    # Truncated in the source, e.g. in CO
    "intermediate volcanic r": "intermediate volcanic rock",
    "plutonic rock (phanerit": "plutonic rock",
    "plutonic rock (phaneritic)": "plutonic rock",
    "pelitic schist": "schist",
    # e.g. in ME, "Cretaceous alkali feldspar granite"
    "alkali-granite (alaskite)": "alaskite",
    # e.g. in ME, "Alkali syenite"
    "alkali syenite": "alkali-feldspar syenite",
    # Plural in the source, e.g. in ME, "Silurian unnamed mafic volcanic rocks"
    "mafic volcanic rocks": "mafic volcanic rock",
    "meta-basalt": "metabasalt",
    "meta-conglomerate": "metaconglomerate",
    "meta-rhyolite": "metarhyolite",
    "alkali rhyolite": "alkali-feldspar rhyolite",
    "biogenic sediment": "biogenic material",
    # e.g. in GA, "Cross-Biotite Schist"
    "biotite schist": "mica schist",
    "eolian": "eolian material",
    "outwash": "glacial outwash sediment",
    "pyroclastic": "pyroclastic rock",
    "ultramafic rock": "ultramafitite",
    "ultramafitite (komatiite)": "ultramafitite",
    # e.g. in NV, "BRECCIA-Volcanic, thrust, and jasperoid breccia and
    # landslide megabreccia"
    "volcanic breccia (agglomerate)": "breccia",
}


def read_overrides(overrides_path):
    """Read an overrides CSV into a dict of non-blank values by code"""
    if not overrides_path:
        return {}
    with open(overrides_path, encoding="utf-8") as overrides_file:
        return {
            row["code"]: {col: val for col, val in row.items() if val}
            for row in csv.DictReader(overrides_file)
        }


def schemify_attributes(attributes_path, overrides_path=None):
    """
    Convert metadata attributes to the Underfoot schema, overriding values
    with non-blank values in rows with the same code in an overrides CSV,
    e.g. to fix lithologies the source only describes as mixed
    """
    log(f"SCHEMIFYING ATTRIBUTES for {attributes_path}...")
    overrides = read_overrides(overrides_path)
    overridden_codes = set()
    outfile_path = "units.csv"
    with open(attributes_path, encoding="utf-8") as attr_f:
        reader = csv.DictReader(attr_f)
        with open(outfile_path, "w", encoding="utf-8") as outfile:
            columns = METADATA_COLUMN_NAMES + ['UNIT_LINK']
            writer = csv.DictWriter(
                outfile,
                fieldnames=columns,
                extrasaction='ignore'
            )
            writer.writeheader()
            for row in reader:
                row["code"] = row["ORIG_LABEL"]
                row["title"] = row["UNIT_NAME"]
                joiner = ". "
                if re.search(r'\.\s*$', row["UNITDESC"]):
                    joiner = " "
                row['description'] = joiner.join(
                    [row['UNITDESC'], row["UNIT_COM"]]
                )
                row["description"] = re.sub(r"\s+", " ", row["description"])
                # Some values have stray whitespace, e.g. "basalt " in OR
                rocktype = " ".join(row['ROCKTYPE1'].split())
                lithology = ROCKTYPE_LITHOLOGIES.get(rocktype, rocktype)
                lithology = LITHOLOGY_SYNONYMS.get(lithology, lithology)
                # Some values aren't lithologies, e.g. "indeterminate", or "lake
                # or marine deposit (non-glacial)" for NV "Sedimentary rocks"
                if lithology not in LITHOLOGIES:
                    lithology = (
                        lithology_from_text(row["title"])
                        or lithology_from_text(row["description"])
                        or ""
                    )
                row["lithology"] = lithology
                row["span"] = normalize_unit_age(row["UNIT_AGE"])
                if override := overrides.get(row["code"]):
                    row.update(override)
                    overridden_codes.add(row["code"])
                row["rock_type"] = rock_type_from_lithology(row["lithology"])
                if not row["span"]:
                    row["span"] = span_from_text(row["title"])
                if not row["span"]:
                    row["span"] = span_from_lithology(row["lithology"])
                row["controlled_span"] = controlled_span_from_span(
                    row["span"]
                )
                if row["span"]:
                    row["min_age"], row["max_age"], row["est_age"] = ages_from_span(row["span"])
                writer.writerow(row)
    for code in overrides.keys() - overridden_codes:
        log(f"  Override for {code!r} doesn't match any unit")
    return os.path.realpath(outfile_path)


def merge_shapes(paths):
    """Merge and dissolve multiple shapefiles into a single shapefile"""
    log("MERGING SHAPEFILES...")
    merged_path = "merged_units.shp"
    call_cmd(["ogr2ogr", "-overwrite", merged_path, paths.pop()], check=True)
    for path in paths:
        call_cmd(["ogr2ogr", "-update", "-append", merged_path, path])
    log("DISSOLVING SHAPES AND REPROJECTING...")
    dissolved_path = "dissolved_units.shp"
    call_cmd([
        "ogr2ogr",
        "-s_srs", SRS,
        "-t_srs", DEST_SRS,
        dissolved_path, merged_path,
        "-overwrite",
        "-dialect", "sqlite",
        "-sql",
        "SELECT UNIT_LINK, ST_Union(geometry) as geometry FROM 'merged_units' GROUP BY UNIT_LINK"
    ], check=True)
    return dissolved_path


def read_attributes(path):
    """Read a state's attributes CSV, which may not have a header row (e.g. ME)"""
    with open(path, encoding="ISO-8859-1") as attr_f:
        has_header = "UNIT_LINK" in next(csv.reader(attr_f))
    if has_header:
        return pd.read_csv(path, encoding="ISO-8859-1")
    return pd.read_csv(path, encoding="ISO-8859-1", header=None, names=ATTRIBUTE_COLUMN_NAMES)


def merge_attributes(paths):
    """Merge multiple CSV attributes paths into a single file"""
    log("MERGING SHAPEFILES...")
    merged_path = "merged_units.csv"
    merged = pd.concat([read_attributes(path) for path in paths])
    merged.loc[:, [
      'ORIG_LABEL',
      'UNIT_LINK',
      'UNIT_NAME',
      'UNIT_AGE',
      'UNITDESC',
      'UNIT_COM',
      'ROCKTYPE1'
    ]].to_csv(merged_path, encoding='utf-8')
    return merged_path


def copy_citation(base_path, work_path, states=None):
    """Copy citation file to the work path

    The checked-in citation links to one state's section of the report, so
    when the source covers a single state, point the URL at that state's
    section.
    """
    log("COPYING CITATION")
    source_path = os.path.join(os.path.dirname(base_path), "citation.json")
    with open(source_path, encoding="utf-8") as citation_file:
        citations = json.load(citation_file)
    if states and len(states) == 1:
        for citation in citations:
            if citation.get("URL"):
                base_url = citation["URL"].split("#")[0]
                citation["URL"] = f"{base_url}#{states[0]}"
    with open(
        os.path.join(work_path, "citation.json"), "w", encoding="utf-8"
    ) as citation_file:
        json.dump(citations, citation_file, indent="\t")


def process_usgs_states(
    # List of strings of two-letter state codes
    states,
    # Path to the source script that has relevant data files as peers
    base_path,
    # URL where remote data files lives
    base_url,
    # Path to the source script that output files are being made for
    source_path,
    # Path to a CSV of metadata that overrides the source's, by code
    metadata_overrides_csv_path=None
):
    """Download and process files for states from of2006_1272"""
    # Each source gets its own directory, even when it shares a base_path with
    # others, since a pack's sources are processed in parallel
    work_path = make_work_dir(source_path)
    os.chdir(work_path)
    shape_paths = [download_shapes(state, base_url) for state in states]
    single_shapefile_path = merge_shapes(shape_paths)
    attribute_paths = [download_attributes(state, base_url) for state in states]  # noqa: E501
    single_attributes_path = merge_attributes(attribute_paths)
    schemified_attributes_path = schemify_attributes(
        single_attributes_path,
        overrides_path=metadata_overrides_csv_path
    )
    join_polygons_and_metadata(
      single_shapefile_path,
      schemified_attributes_path,
      polygons_join_col="UNIT_LINK",
      metadata_join_col="UNIT_LINK",
      output_path="units.geojson")
    copy_citation(base_path, work_path, states)
