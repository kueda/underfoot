"""
    General Distribution Of Geologic Materials In The San Francisco Bay
    Region, California: A Digital Map Database
"""

import csv
import os
import re

import fiona

import util
from util import rocks


def run():
    work_path = util.make_work_dir(os.path.realpath(__file__))
    os.chdir(work_path)
    url = "https://pubs.usgs.gov/of/1997/of97-744/of97-744-shapefiles.zip"

    # Get the archive
    download_path = os.path.basename(url)
    if not os.path.isfile(download_path):
        print(f"DOWNLOADING {url}")
        util.call_cmd(["curl", "-OL", url])
    dir_path = "of97-744-shapefiles"
    if not os.path.isdir(dir_path):
        print("EXTRACTING ARCHIVE...")
        util.call_cmd(["unzip", download_path])

    # In this case we're converting the existing shapefile to JSON and
    # translating some coded data into a usable form in one step
    units_path = "units.geojson"
    if not os.path.isfile(units_path):
        schema = {
            'geometry': 'Polygon',
            'properties': {
                'code': 'str',
                'title': 'str',
                'description': 'str',
                'lithology': 'str',
                'rock_type': 'str',
                'formation': 'str',
                'grouping': 'str',
                'span': 'str',
                'controlled_span': 'str',
                'min_age': 'int',
                'max_age': 'int',
                'est_age': 'int',
            }
        }
        # From https://pubs.usgs.gov/of/1997/of97-744/of97-744_2a.txt
        lith_dict = {
            'ab': 'agglomerate, breccia',
            'bv': 'mafic volcanic rocks',
            'cs': 'clay, silt, sand, gravel',
            'dm': 'diatomite, diatomaceous shale, some sandstone',
            'fv': 'felsic volcanic rocks',
            'gr': 'granitic rock',
            'hg': 'high-grade metamorphic rocks',
            'ld': 'landslide',
            'ls': 'limestone ',
            'm': 'mud and silt',
            'md': 'mudstone and shale, some sandstone',
            'mm': 'sheared sandstone and shale (melange)',
            'ms': 'low-grade metasandstone and shale',
            'mv': 'low-grade metavolcanic rocks (greenstone)',
            's': 'sand, gravel, silt, and mud',
            'sc': 'silica-carbonate rock',
            'sch': 'schist',
            'sl': 'porcelaneous or siliceous mudstone and shale; chert',
            'sm': 'sandstone and mudstone or shale  ',
            'sp': 'serpentinite',
            'ss': 'sandstone and conglomerate, some mudstone or shale',
            'tf': 'tuff, tuffaceous sandstone, some sandstone, volcanic rock',
            'wm': 'soft, water-saturated mud, some silt',
            'wt': 'welded tuff'
        }
        # From https://pubs.usgs.gov/of/1997/of97-744/of97-744_2a.txt
        age_dict = {
            'h': 'holocene',
            'p': 'pleistocene',
            'q': 'quaternary undivided',
            'qt': 'pliocene and/or quaternary',
            'tu': 'upper tertiary',
            'tl': 'lower tertiary',
            'mz': 'mesozoic',
        }
        # convert units.dbf to csv so we can read it
        units_csv_path = "units.csv"
        util.call_cmd([
            "ogr2ogr",
            "-f", "CSV",
            units_csv_path,
            os.path.join(dir_path, "units.dbf")
        ])
        unit_titles_by_nptype = {}
        with open(units_csv_path, encoding="utf-8") as units_csv_file:
            reader = csv.DictReader(units_csv_file)
            for row in reader:
                unit_titles_by_nptype[str(row['NPTYPE'])] = row['GEOLUNIT']
        # read in the shapefile and make a new geojson, assigning attributes as
        # we go
        input_path = os.path.join(dir_path, "of97-744_3d Folder", "mtlpys.shp")
        with fiona.collection(units_path, "w", "GeoJSON", schema) as output:
            with fiona.open(input_path) as units:
                for idx, unit in enumerate(units):
                    if unit['properties']['NPTYPE'] < 0:
                        continue
                    print("Filling in data for {} ({} / {}, {}%)".format(
                        str(unit['properties']['SF_MTLS2_D']).ljust(8),
                        idx, len(units), round(idx / len(units) * 100, 2)
                    ), end="\r", flush=True)
                    title = unit_titles_by_nptype.get(
                        str(unit['properties']['NPTYPE'])
                    )
                    formation = None
                    grouping = None
                    if title:
                        title = re.sub(r" Fm(\s*)", r" Formation\1", title)
                        formation = rocks.formation_from_text(title)
                        grouping = rocks.grouping_from_text(title)
                        if (
                            re.match(r".*Sonoma Volcanics.*", title)
                            and (formation == '' or formation is None)
                        ):
                            formation = "Sonoma Volcanics"
                    unit_age = None
                    unit_lith = None
                    if unit['properties']['AGELITH']:
                        unit_age, unit_lith = unit['properties']['AGELITH'].split("-")  # noqa: E501
                    lithology = None
                    if title:
                        lithology = rocks.lithology_from_text(title)
                    if unit_lith and (lithology == '' or lithology is None):
                        lithology = rocks.lithology_from_text(
                            lith_dict[unit_lith])
                    span = None
                    controlled_span = None
                    min_age = None
                    max_age = None
                    est_age = None
                    if not unit_age and title:
                        unit_age = rocks.span_from_text(title)
                    if unit_age:
                        span = age_dict[unit_age.lower()]
                        if span:
                            min_age, max_age, est_age = rocks.ages_from_span(
                                span)
                    if span:
                        controlled_span = rocks.controlled_span_from_span(span)
                    if controlled_span is None and lithology:
                        controlled_span = rocks.span_from_lithology(lithology)
                    output.write({
                        'properties': {
                            'code': unit['properties']['UNIT'],
                            'title': title,
                            'description': None,
                            'lithology': lithology,
                            'rock_type': rocks.rock_type_from_lithology(lithology),
                            'formation': formation,
                            'grouping': grouping,
                            'span': span,
                            'controlled_span': controlled_span,
                            'min_age': min_age,
                            'max_age': max_age,
                            'est_age': est_age,
                        },
                        'geometry': unit['geometry']
                    })

    # Copy over the citation JSON
    util.call_cmd([
        "cp",
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "citation.json"
        ),
        os.path.join(work_path, "citation.json")
    ])
