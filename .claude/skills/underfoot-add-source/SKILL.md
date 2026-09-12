---
name: underfoot-add-source
description: Use when adding a new geologic data source to the underfoot project — creating
  the required three-file structure, writing citation.json, and calling process_usgs_source
  with the correct parameters for GAM or USGS shapefile sources.
---

# Adding an Underfoot Geologic Source

## Overview

Every geologic source requires **three files** (sometimes four). Missing any one means the
source cannot be discovered or run.

## Required Files

### 1. `sources/<name>/__init__.py` — processing logic

```python
"""
<Human-readable title of the map>
"""

import os

from util.rocks import process_usgs_source
from util.proj import <SRS_CONSTANT>


def run():
    process_usgs_source(
        base_path=os.path.realpath(__file__),
        url="<ZIP URL>",
        extracted_file_path="<relative path to .shp inside ZIP>",
        srs=<SRS_CONSTANT>,
        use_unzip=True,
        polygons_join_col="<join column name from shapefile>",
        # Use mappable_metadata_csv_path when the archive ships a machine-readable CSV:
        mappable_metadata_csv_path="<relative path to CSV inside ZIP>",
        mappable_metadata_mapping={
            "code": "<column that identifies the map unit>",
            "title": "<column with the unit name>",
            "span": "<column with the geologic age>",
            "description": "<column with the description text>"
        },
        lithology_from_description=True  # set when title alone is not descriptive enough
    )
```

Use `mappable_metadata_csv_path` + `mappable_metadata_mapping` when the ZIP ships a
machine-readable description CSV (many GAM sources do). Use `metadata_csv_path` pointing
to a hand-authored `sources/<name>/units.csv` when descriptions must be transcribed from a
PDF pamphlet or image.

**Geodatabases and map packages.** For an ESRI file geodatabase (`.gdb`) or personal
geodatabase (`.mdb`), point `extracted_file_path` at it, set `layer_name` to the polygon
layer, and set `mappable_metadata_layer_name` to a table of unit descriptions if it has one,
instead of `mappable_metadata_csv_path`. Both take whatever names that geodatabase actually
uses, so list them with `ogrinfo` first. `.mpk` (an ArcGIS map package) and `.7z` downloads
are extracted with `7z`, and only `extracted_file_path` is extracted.

**Sources that follow GeMS.** [GeMS](https://ngmdb.usgs.gov/Info/standards/GeMS/) is a
schema used by many recent USGS and state survey maps. It is a convention about layer and
column names, not a file format, so a `.gdb` may or may not follow it and nothing in
`process_usgs_source` assumes it. Where a source does follow it, the polygon layer is
`MapUnitPolys`, the unit descriptions are in `DescriptionOfMapUnits`, the two join on
`MapUnit`, and each unit carries a controlled `GeoMaterial` term worth mapping as
`"geomaterial": "GeoMaterial"` so it can serve as a last-resort lithology. See
`sources/nmbgmr_ofgm_304/`.

**Overrides for mappable metadata.** When lithology inference gets a few units wrong (e.g.
"sand" from "Sandia Formation") or finds nothing, add a sparse
`sources/<name>/overrides.csv` with a `code` column plus only the columns to override (e.g.
`code,lithology`), and pass its full path as `metadata_overrides_csv_path`. Non-blank values
replace the mapped values before inference, so `rock_type` and ages stay consistent.
Lithologies must be known ones (see `LITHOLOGIES` in `sources/util/rocks/constants.py`).

### 2. `sources/<name>/citation.json` — CSL-JSON metadata

```json
[
  {
    "type": "map",
    "publisher": "<Publishing agency>",
    "scale": "<e.g. 1:250000>",
    "title": "<Full map title>",
    "URL": "<URL of the publication landing page, NOT the ZIP file>",
    "author": [
      {"family": "<Last>", "given": "<First initial with period>"}
    ],
    "issued": {
      "date-parts": [["<YYYY>"]]
    }
  }
]
```

**Common mistakes:**
- `URL` must point to the **publication page**, not the ZIP download link.
- `issued.date-parts` is `[["YYYY"]]` — a nested array, year as a string.
- The outer container is a JSON **array** (`[{...}]`), not a bare object.

### 3. `sources/<name>.py` — thin entry-point (REQUIRED, easy to forget)

```python
"""
<Same title as __init__.py docstring>
"""

from <name> import run

if __name__ == "__main__":
    run()
```

This file lives at `sources/<name>.py` (top level, not inside the package directory).
Without it, the source **cannot be invoked** as a script. It is easy to forget because
the package directory with `__init__.py` looks self-contained.

### 4. `sources/<name>/units.csv` — hand-authored descriptions (when needed)

Required when the source archive does NOT include a machine-readable CSV. Columns:

```
code,title,description,lithology
```

GAM sources that use `mappable_metadata_csv_path` skip this file because the description
CSV is bundled in the download.

**The `description` field is critical.** It is the primary text shown to users and the
main input for lithology inference. Populate it from the publication's pamphlet PDF
(the Description of Map Units section).

Reading a PDF pamphlet needs `pdftotext` (from poppler), which is in the Docker image.
Extract text with layout preservation:

```
docker compose run --rm app pdftotext -layout sources/<name>/pamphlet.pdf -
```

On a bare-metal (non-Docker) checkout: `brew install poppler` (macOS) or
`apt install poppler-utils` (Debian/Ubuntu).

Some units may legitimately have no description in the source — leave those blank rather
than fabricating text. But before giving up on a unit, ask the user (see below).

## Tools (in this skill directory)

**`inspect-dbf.py`** — show unique values of a join column and all associated fields.
Use this to discover the join column name and available columns before writing `__init__.py`:

```
docker compose run --rm app \
    python .claude/skills/underfoot-add-source/scripts/inspect-dbf.py file.dbf [JOIN_COL]
```

**`scaffold-units-csv.py`** — generate a `units.csv` stub from the DBF with `code`,
`title`, and `span` filled in, leaving `description` and `lithology` blank:

```
docker compose run --rm app \
    python .claude/skills/underfoot-add-source/scripts/scaffold-units-csv.py \
    file.dbf sources/<name>/units.csv --code UNIT --title Descriptio --span Age
```

Then populate `description` from the pamphlet PDF (see below).

## Determining Parameter Values

These must be determined from the source material, not assumed:

**`srs`** — inspect the `.prj` file that accompanies the shapefile, or read any metadata
files in the archive. `util.proj` exports constants for common projections; check that
module for the matching constant rather than writing a raw proj4 string. California CGS GAM
sources typically use a California Albers projection.

**`polygons_join_col`** — run `inspect-dbf.py` on the `.dbf` to see column names and
unique values. GAM sources commonly use `MapUnit`; older USGS Arc Info sources often use
`PTYPE`. Use whatever the file actually contains.

**`mappable_metadata_mapping`** — read the column headers of the description CSV in the
archive and map them to `code`, `title`, `span`, and `description`. Do not guess column
names.

**`lithology_from_description`** — set to `True` when the unit name/title field is not
descriptive enough to infer lithology on its own (e.g. it is an alphanumeric code or a
short label). Leave it out (defaults to `False`) when the title is self-describing.

**`use_unzip`** — use `True` for `.zip` archives. For `.tar.gz` or raw `.e00` files, omit
or use the appropriate decompression parameter. `.7z` and `.mpk` downloads are detected by
extension and need no flag.

## `process_usgs_source` Key Parameters

| Parameter | Purpose |
|-----------|---------|
| `base_path` | Always `os.path.realpath(__file__)` — locates the work dir and `citation.json` |
| `url` | Download URL for the archive |
| `extracted_file_path` | Path *inside* the archive to the shapefile, e00, mdb, or gdb |
| `srs` | Proj4 CRS string or constant — determine from `.prj` or metadata |
| `use_unzip` | `True` for ZIP archives |
| `layer_name` | Polygon layer inside an mdb or gdb, e.g. `MapUnitPolys` |
| `polygons_join_col` | Column name to dissolve/join polygons — read from shapefile attributes |
| `mappable_metadata_csv_path` | Path *inside* the archive to a bundled description CSV |
| `mappable_metadata_layer_name` | Table in the gdb to use instead, e.g. `DescriptionOfMapUnits` |
| `mappable_metadata_mapping` | Maps columns to `code/title/span/description` (optionally `geomaterial`) |
| `metadata_csv_path` | Filesystem path to a hand-authored `units.csv` |
| `metadata_overrides_csv_path` | Filesystem path to a sparse `overrides.csv` for mappable metadata |
| `lithology_from_description` | `True` when title is not descriptive enough for lithology |

Do **not** combine `mappable_metadata_csv_path` and `metadata_csv_path`.

## Naming Convention

California GAM sources follow: `gam_<NN>_<location>_<YYYY>_gis`

- `NN` = two-digit sheet number (zero-padded)
- `location` = lowercase, underscores, no hyphens
- `YYYY` = publication year

## Asking the User for Help with Descriptions

PDF text extraction is imperfect. Multi-column layouts, complex formatting, and map
symbols interspersed with text all cause regex and `pdftotext` to miss or mangle
description blocks. A human can often find and paste a passage in seconds.

**Ask the user when:**
- You have tried the PDF text and cannot locate a description for a unit after a
  reasonable search (2–3 targeted attempts).
- The extracted text for a unit looks garbled or truncated.
- Several units are missing descriptions and you suspect a section of the PDF was not
  captured cleanly.

**How to ask:** List the unit codes and names you are missing, describe what section of
the PDF they should be in (e.g., "Description of Map Units"), and ask the user to paste
the text. Do not spend many more tool calls searching — it is faster for them to copy it.

**When a description genuinely doesn't exist** (the user confirms it's not in the PDF,
or the unit is too minor to have one), leave `description` blank. Do not fabricate text.

## Checklist

- [ ] `sources/<name>/__init__.py` with `run()` and `process_usgs_source` call
- [ ] `sources/<name>/citation.json` — array of one CSL-JSON map object
- [ ] `sources/<name>.py` — thin entry-point that imports `run` and calls it under `__main__`
- [ ] `sources/<name>/units.csv` — only if archive has no machine-readable CSV
- [ ] `description` field populated from pamphlet PDF (`pdftotext`, in the image; ask user if stuck; blank is ok when description genuinely doesn't exist)
- [ ] citation `URL` points to publication page, not the ZIP
- [ ] `issued.date-parts` is `[["YYYY"]]`
- [ ] `srs` determined from `.prj` file or metadata, not assumed
- [ ] `polygons_join_col` confirmed from actual shapefile attributes
