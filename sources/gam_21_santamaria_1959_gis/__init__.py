"""
Geologic map of California : Santa Maria Sheet
"""

import os
import re
from difflib import SequenceMatcher

from util.rocks import process_usgs_source
from util.proj import NAD83_CA_ALBERS

PARENTHETICAL_REFERENCE_PATTERN = r'\(([^\(]+? \d{4},?)+?\)'

def description_from_row(row):
    """
    Massage the description. This source has machine-readble descriptions but
    there's a lot of repetition.
    """
    without_paren_refs = re.sub(PARENTHETICAL_REFERENCE_PATTERN, '+++', row["Descr"]).split('+++')
    unabbreviated = [item.replace("fm.", "Formation") for item in without_paren_refs]
    spaced_commas = [re.sub(r",([^\s])", ", \\1", item) for item in unabbreviated]
    without_leading_periods = [re.sub(r'^\. ?', '', piece) for piece in spaced_commas]
    without_blanks = [piece for piece in without_leading_periods if len(piece) > 0]
    split_by_periods = [re.split(r'(?<!fm|is)\.', piece) for piece in without_blanks]
    # why doesn't python have flatten, whyyyyyy
    flattened_pieces = [item for array in split_by_periods for item in array]
    # why doesn't python have ordered uniq, whyyyyyy
    unique_pieces = list(dict.fromkeys([piece.strip() for piece in flattened_pieces]))
    pieces = [piece for piece in unique_pieces if len(piece) > 0]
    # Reverse so our similarity check removes similar things from the end
    pieces.reverse()
    new_pieces = []
    for idx, piece in enumerate(pieces):
        similar_piece = next(
            (
                candidate for candidate in pieces[idx+1:]
                if SequenceMatcher(None, piece, candidate).ratio() > 0.80
            ),
            None
        )
        if not similar_piece:
            new_pieces.append(piece)
    new_pieces.reverse()
    return ". ".join(new_pieces).strip()


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url=(
            "https://www.conservation.ca.gov/cgs/Documents/Publications/Geologic-Atlas-Maps/"
            "GAM_21-SantaMaria-1959-GIS.zip"
        ),
        extracted_file_path="GAM_21-SantaMaria-1959-GIS/GAM_21_SantaMaria-open/GM_MapUnitPolys.shp",
        srs=NAD83_CA_ALBERS,
        use_unzip=True,
        polygons_join_col="MapUnit",
        mappable_metadata_csv_path=
            "GAM_21-SantaMaria-1959-GIS/GAM_21_SantaMaria-open/DescriptionOfMapUnits.csv",
        mappable_metadata_mapping={
            "code": "MapUnit",
            "title": "FullName",
            "span": "Age",
            "description": description_from_row
        }
    )
