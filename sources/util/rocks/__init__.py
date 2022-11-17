"""Functions for processing rock sources"""

import csv
import os
import re
from glob import glob

import xml.etree.ElementTree as ET

from .. import call_cmd, extless_basename, extract_e00, make_work_dir, met2xml, polygonize_arcs
from ..proj import NAD27_UTM10_PROJ4, SRS
from .constants import *


def lithology_from_text(text):
    """Extract normalized lithology from free text"""
    if not text:
        return
    lithology_matches = LITHOLOGY_PATTERN.search(text)
    if not lithology_matches:
        lithology_matches = LOW_PRIORITY_LITHOLOGY_PATTERN.search(text)
    lithology = (lithology_matches[0] if lithology_matches else '').lower()
    if lithology in LITHOLOGY_SYNONYMS:
        return LITHOLOGY_SYNONYMS[lithology]
    return lithology


def formation_from_text(text):
    """Extract normalized formation from free text"""
    if not text:
        return
    # basically any proper nouns
    formation_matches = FORMATION_PATTERN.search(text)
    if formation_matches:
        return formation_matches.group(0).title()


def grouping_from_text(text):
    """Extract normalized grouping from free text"""
    if not text:
        return
    grouping_matches = GROUPING_PATTERN.search(text)
    if grouping_matches:
        return grouping_matches.group(0).title()


def rock_type_from_lithology(lithology):
    """Extract rock type from normalized lithology"""
    rock_type = ''
    if lithology in IGNEOUS_ROCKS:
        rock_type = 'igneous'
    elif lithology in METAMORPHIC_ROCKS:
        rock_type = 'metamorphic'
    elif lithology in SEDIMENTARY_ROCKS:
        rock_type = 'sedimentary'
    return rock_type


def span_from_text(text):
    """Extract normalized geologic time span from free text"""
    if not text:
        return
    span_matches = SPAN_PATTERN.search(text.lower())
    if span_matches:
        return span_matches.group(1).lower()


def span_from_lithology(lithology):
    """Extract normalized geologic time span from normalized lithology"""
    if lithology in ("artificial", "water"):
        return "present"


def span_from_code(code):
    """Extract normalized geologic time span from map unit / code"""
    if code and len(code) <= 4:
        if code[0] == "K":
            return "cretaceous"
        if code[0] == "T":
            return "tertiary"
        if code[0] == "Q":
            return "quaternary"
        if code[0] == "J":
            return "jurassic"
        return "present"


def controlled_span_from_span(text):
    """
    Extract controlled geologic time span from free text.

    Unlike span_from_text, this attempts to make reasonable guesses about the
    meaning of terms like "early" or "upper"
    """
    if not text:
        return
    key = re.sub(r'\(.+?\)', "", text)
    key = re.sub(r'undivided', "", key)
    key = re.sub(r'\s+', " ", key)
    key = key.lower().strip()
    synonyms = {
      'present': 'holocene'
    }
    if key in synonyms and synonyms[key] in WIKI_SPANS:
        return synonyms[key]
    if key in WIKI_SPANS:
        return key
    key_sans_x_to_y = re.sub(
        r'(early|middle|late) to (early|middle|late)', "",
        key
    ).strip()
    if key_sans_x_to_y in WIKI_SPANS:
        return key_sans_x_to_y
    key_sans_lower = re.sub(r'early\s+', "lower ", key).strip()
    if key_sans_lower in WIKI_SPANS:
        return key_sans_lower
    key_sans_upper = re.sub(r'late\s+', "upper ", key).strip()
    if key_sans_upper in WIKI_SPANS:
        return key_sans_upper
    key_sans_subspan = re.sub(
        r'(upper|middle|lower|early|late)\s+', "",
        key
    ).strip()
    if key_sans_subspan in WIKI_SPANS:
        return key_sans_subspan
    # Split on (to|\-)
    matches = re.findall(r'([\s\w]+)\s+(to|\-|or|and\/or)\s+([\s\w]+)', key)
    if len(matches) > 0:
        # get controlled_span for each half
        start_span = controlled_span_from_span(matches[0][0])
        end_span = controlled_span_from_span(matches[0][2])
        if start_span and end_span:
            start_start_age = SPANS[start_span][0]
            end_end_age = SPANS[end_span][1]
            # sort wiki spans by start_date asc
            sorted_spans = sorted(
                [(k, SPANS[k][0], SPANS[k][1]) for k in SPANS],
                key=lambda s: s[1]
            )
            # find first span where start_age >= 1st half start age and end_age
            # <= 2nd half end age
            container_span = next(
                (
                    s for s in sorted_spans
                    if s[1] >= start_start_age and s[2] <= end_end_age
                ),
                None
            )
            if container_span:
                return container_span[0]


def ages_from_span(span):
    """
    Parses a text description of a time span using the names of geologic
    periods, epochs, and other time spans into a list of minimum, maximum, and
    estimated age in years BCE

    >>> ages_from_span("Cretaceous")
    (66000000.0, 145000000.0, 105500000)
    """
    if ";" in span:
        for piece in span.split(";"):
            ages = ages_from_span(piece)
            if ages and None not in ages:
                return ages
    min_age = None
    max_age = None
    est_age = None
    span_to_span_pattern = r'(.+)\s+?(to|\-|and)\s+?(.+)'
    part_to_part_span_pattern = r'(?P<part1>\w+)\s+?(to|\-)\s+?(?P<part2>\w+)\s+(?P<span>\w+)'
    if span is None:
        return (min_age, max_age, est_age)
    span = span.lower()
    span = span.replace('undivided', '')
    span = re.sub(r'\(.+\)', '', span).strip()
    span = span.replace('?-', ' -')
    span = span.replace('?', '')
    span = re.sub(r"\-\s*$", "", span)
    span = re.sub(r"^\s*-", "", span)
    span = span.strip()
    if len(span) == 0:
        return (min_age, max_age, est_age)
    ages = SPANS.get(span)
    if ages:
        max_age = ages[0]
        min_age = ages[1]
    else:
        min_ages = None
        max_ages = None
        if match := re.match(span_to_span_pattern, span):
            min_ages = SPANS.get(match[1])
            max_ages = SPANS.get(match[3])
        if not min_ages or not max_ages:
            if match := re.match(part_to_part_span_pattern, span):
                part1 = f"{match.group('part1')} {match.group('span')}".lower()
                part2 = f"{match.group('part2')} {match.group('span')}".lower()
                min_ages = SPANS.get(part1)
                max_ages = SPANS.get(part2)
        if min_ages:
            min_age = min_ages[1]
        if max_ages:
            max_age = max_ages[0]
    if min_age is not None and max_age is not None:
        est_age = int((min_age + max_age) / 2.0)
    return (min_age, max_age, est_age)


def span_from_usgs_code(code):
    """
    Derive a span from a USGS geologic map unit code

    USGS geologic data has a convention of using the first letter (or two) to
    indiciate the age of the unit, roughly corresponding to the symbols at
    https://ngmdb.usgs.gov/fgdc_gds/geolsymstd/fgdc-geolsym-sec32.pdf
    """
    mapping = {
        "Cz": "cenozoic",
        "Q": "quaternary",
        "T": "tertiary",
        "N": "neogene",
        "Pe": "paleogene",
        "Mz": "mesozoic",
        "K": "cretaceous",
        "J": "jurassic",
        "Tr": "triassic",
        "Pz": "paleozoic",
        "P": "permian",
        "C": "carboniferous",
        # "P": "pennsylvanian", # not sure how to render this symbol
        "M": "mississippian",
        "D": "devonian",
        "S": "silurian",
        "O": "ordovician",
        # "C": "Cambrian"
        "pC": "precambrian",
        # "P": "proterozoic",
        "Z": "late proterozoic",
        "Y": "middle proterozoic",
        "Y3": "late middle proterozoic",
        "Y2": "middle middle proterozoic"
    }
    for prefix, span in mapping.items():
        if re.match(rf"^{prefix}", code):
            return span

def metadata_from_usgs_met(path):
    """Parse metadata from a USGS met file

    USGS geologic databases seem to come with a somewhat machine-readable
    document with a .met extension that contains information about the
    geological units in the database. This method tries to convert that
    document into an array of arrays suitable for use in underfoot. It's not
    going to do a perfect job for every such document, but it will get you part
    of the way there.
    """
    data = [METADATA_COLUMN_NAMES.copy()]
    xml_path = met2xml(path)
    tree = ET.parse(xml_path)
    for enumerated_domain in tree.iterfind('.//Attribute[Attribute_Label="PTYPE"]//Enumerated_Domain'):
        row = {col: None for col in METADATA_COLUMN_NAMES}
        edv = enumerated_domain.find('Enumerated_Domain_Value')
        edvd = enumerated_domain.find('Enumerated_Domain_Value_Definition')
        if edv is None or edvd is None:
            continue
        row['code'] = edv.text
        row['title'] = edvd.text
        if row['code'] is None or row['title'] is None:
            continue
        row['title'] = re.sub(r"\n", " ", row['title'])
        row['lithology'] = lithology_from_text(row['title'])
        row['formation'] = formation_from_text(row['title'])
        row['rock_type'] = rock_type_from_lithology(row['lithology'])
        row['span'] = span_from_text(row['title'])
        if not row['span']:
            row['span'] = span_from_lithology(row['lithology'])
        if not row['span']:
            row['span'] = span_from_code(row['code'])
        row['min_age'], row['max_age'], row['est_age'] = ages_from_span(
            row['span']
        )
        row['controlled_span'] = controlled_span_from_span(row['span'])
        if row['controlled_span'] and not row['min_age']:
            row['min_age'], row['max_age'], row['est_age'] = ages_from_span(
                row['controlled_span']
            )
        data.append([row[col] for col in METADATA_COLUMN_NAMES])
    return data


def join_polygons_and_metadata(
    polygons_path,
    metadata_path,
    output_path="units.geojson",
    polygons_join_col="PTYPE",
    polygons_table_name=None,
    metadata_join_col="code",
    output_format="GeoJSON"
):
    """Add metadata as attributes of polygons"""
    polygons_table_name = polygons_table_name or extless_basename(
        polygons_path)
    column_names = METADATA_COLUMN_NAMES.copy()
    metadata_layer_name = extless_basename(metadata_path)
    sql = f"""
      SELECT
        {", ".join(column_names)}
      FROM {polygons_table_name}
        LEFT JOIN '{metadata_path}'.{metadata_layer_name}
          ON {polygons_table_name}.{polygons_join_col} = {metadata_layer_name}.{metadata_join_col}
    """
    call_cmd(["rm", "-f", output_path])
    call_cmd([
      "ogr2ogr",
      "-sql", sql.replace("\n", " "),
      "-f", output_format,
      output_path,
      polygons_path
    ])
    return output_path


def infer_metadata_from_csv_row(row):
    """Infer metadata from a row in a metadata file"""
    if not row.get('lithology') or len(row['lithology']) == 0:
        row['lithology'] = lithology_from_text(row['title'])
    if not row.get('lithology') or len(row['lithology']) == 0:
        row['lithology'] = lithology_from_text(row['description'])
    row['span'] = span_from_text(row['title'])
    if not row['span'] and row['lithology']:
        row['span'] = span_from_lithology(row['lithology'])
    if not row.get('span') and row.get('code'):
        row['span'] = span_from_code(row['code'])
    row['controlled_span'] = controlled_span_from_span(row['span'])
    row['formation'] = formation_from_text(row['title'])
    if row['lithology']:
        row['rock_type'] = rock_type_from_lithology(
            row['lithology']
        )
    if row['span']:
        min_age, max_age, est_age = ages_from_span(row['span'])
        row['min_age'] = min_age
        row['max_age'] = max_age
        row['est_age'] = est_age
    return row


def infer_metadata_from_csv(infile_path):
    """Fill in missing metadata columns in a CSV

    The metadata file *can* have every column, but in general it just has the
    info that can't be inferred and we infer the rest from the title and
    description.
    """
    outfile_path = "data.csv"
    with open(infile_path, encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        with open(outfile_path, 'w', encoding="utf-8") as outfile:
            writer = csv.DictWriter(
                outfile,
                fieldnames=METADATA_COLUMN_NAMES,
                extrasaction='ignore'
            )
            writer.writeheader()
            for row in reader:
                row = infer_metadata_from_csv_row(row)
                writer.writerow(row)
                uncertain_row = row.copy()
                uncertain_row['code'] = f"{row['code']}?"
                uncertain_row['title'] = f"[?] {row['title']}"
                uncertain_row['description'] = f"[UNCERTAIN] {row['description']}"
                writer.writerow(uncertain_row)
    return outfile_path


def process_usgs_source(
    base_path,
    url,
    e00_path,
    polygon_pattern=None,
    srs=NAD27_UTM10_PROJ4,
    metadata_csv_path=None,
    polygons_join_col="PTYPE",
    skip_polygonize_arcs=False,
    uncompress_e00=False,
    # As opposed to gunzip
    use_unzip=False
):
    """Process units from a USGS Arc Info archive given a couple configurations.

    Most USGS map databases seem to be in the form of a gzipped tarbal
    containing Arc Info coverages, so this method just wraps up some of the
    code I keep repeating.

    Args:
      base_path: path to the source module's __init__.py
      url: URL of the gzipped tarball
      e00_path: Relative path to the e00 to extract
      polygon_pattern (optional): Pattern to use when finding the polygon ID
        column in the e00 arcs
      srs (optional): Proj4 coordinate reference string for the geodata.
        Default is NAD27 UTM Zone 10
      metadata_csv_path: Full path to a CSV containing transcribed metadata for
        the map units. Default behavior is to try and import unit titles
      polygons_join_col: Name of the column to dissolve polygons by and to join
        them to the units column in the metadata
      skip_polygonize_arcs: If ogr2ogr successfully creates a useable polygon
        shapefile in PAL.shp, then we don't need to polygonize arcs, but this
        usually isn't the case. Default value is false.
      uncompress_e00: If the e00 is itself compressed, uncompress it with
        e00conv. Default is false.
    """
    work_path = make_work_dir(base_path)
    os.chdir(work_path)
    download_path = os.path.basename(url)
    # download the file if necessary
    if not os.path.isfile(download_path):
        print(f"DOWNLOADING {url}")
        call_cmd(["curl", "-OL", url])

    # extract the archive if necessary
    if len(glob(e00_path)) == 0:
        print("EXTRACTING ARCHIVE...")
        if (
            ".tar.gz" in download_path
            or ".tgz" in download_path
            or ".tar.Z" in download_path
        ):
            call_cmd(["tar", "xzvf", download_path])
        elif use_unzip:
            call_cmd(["unzip", download_path])
        else:
            copy_path = download_path + "-copy"
            call_cmd(["cp", download_path, copy_path])
            call_cmd(["gunzip", download_path])
            call_cmd(["cp", copy_path, download_path])
            call_cmd(["rm", copy_path])

    # convert the Arc Info coverages to shapefiles
    polygons_path = "e00_polygons.shp"
    if not os.path.isfile(polygons_path):
        print("CONVERTING E00 TO SHAPEFILES...")
        polygon_paths = []
        for path in glob(e00_path):
            if uncompress_e00:
                uncompressed_e00_path = "uncompressed.e00"
                if not os.path.isfile(uncompressed_e00_path):
                    print("\tUncompressing e00")
                    call_cmd([
                        "../../bin/e00compr/e00conv",
                        path,
                        uncompressed_e00_path
                    ])
                    path = uncompressed_e00_path
            print("\tExtracting e00...")
            shapefiles_path = extract_e00(path)
            print(f"\tshapefiles_path: {shapefiles_path}")
            print("\tPolygonizing arcs...")
            if skip_polygonize_arcs:
                polygon_paths.append(os.path.join(shapefiles_path, "PAL.shp"))
            elif polygon_pattern:
                polygon_paths.append(polygonize_arcs(
                    shapefiles_path,
                    polygon_pattern=polygon_pattern
                ))
            else:
                polygon_paths.append(polygonize_arcs(shapefiles_path))
        print("MERGING SHAPEFILES...")
        call_cmd(["ogr2ogr", "-overwrite", polygons_path, polygon_paths.pop()])
        for path in polygon_paths:
            call_cmd(["ogr2ogr", "-update", "-append", polygons_path, path])

    # dissolve all the shapes by polygons_join_col and project them into Google Mercator
    print("DISSOLVING SHAPES AND REPROJECTING...")
    final_polygons_path = "polygons.shp"
    call_cmd([
        "ogr2ogr",
        "-s_srs", srs,
        "-t_srs", SRS,
        final_polygons_path, polygons_path,
        "-overwrite",
        "-dialect", "sqlite",
        "-sql",
        # pylint: disable=line-too-long
        f"SELECT {polygons_join_col},ST_Union(geometry) as geometry FROM 'e00_polygons' GROUP BY {polygons_join_col}"
        # pylint: enable=line-too-long
    ])

    print("EXTRACTING METADATA...")
    metadata_path = "data.csv"
    globs = glob(os.path.join(os.path.dirname(e00_path), "*.met"))
    met_path = globs[0] if globs else None
    if metadata_csv_path:
        metadata_path = infer_metadata_from_csv(metadata_csv_path)
        if met_path:
            fill_in_custom_metadata_from_met(met_path, metadata_path)
    elif met_path:
        data = metadata_from_usgs_met(met_path)
        with open(metadata_path, "w", encoding="utf-8") as metadata_file:
            csv.writer(metadata_file).writerows(data)
    else:
        # write an empty metadata csv file
        data = [METADATA_COLUMN_NAMES]
        with open(metadata_path, "w", encoding="utf-8") as metadata_file:
            csv.writer(metadata_file).writerows(data)

    print("JOINING METADATA...")
    join_polygons_and_metadata(final_polygons_path, metadata_path,
      polygons_join_col=polygons_join_col)

    print("COPYING CITATION")
    call_cmd([
      "cp",
      os.path.join(os.path.dirname(base_path), "citation.json"),
      os.path.join(work_path, "citation.json")
    ])


def fill_in_custom_metadata_from_met(met_path, metadata_path):
    """Fill gaps in existing metadata file with USGS metadata file"""
    met_data = metadata_from_usgs_met(met_path)
    hashed_met_data = {}
    for row in met_data:
        hashed_met_data[row[0]] = dict(zip(METADATA_COLUMN_NAMES, row))
    with open(metadata_path, 'r', encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        outfile_path = "temp.csv"
        with open(outfile_path, 'w', encoding="utf-8") as outfile:
            writer = csv.DictWriter(
                outfile,
                fieldnames=METADATA_COLUMN_NAMES,
                extrasaction='ignore'
            )
            writer.writeheader()
            for row in reader:
                met_row = hashed_met_data.get(row['code'], None)
                for col in METADATA_COLUMN_NAMES:
                    if (
                        (not row[col] or len(row[col]) == 0)
                        and met_row
                        and met_row[col]
                    ):
                        row[col] = met_row[col]
                    writer.writerow(row)
        os.rename(outfile_path, metadata_path)
