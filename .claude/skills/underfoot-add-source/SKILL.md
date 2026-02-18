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
main input for lithology inference. Do not leave it blank. Populate it from the
publication's pamphlet PDF (the Description of Map Units section).

To read a PDF pamphlet, you need `pdftotext` (from poppler):

```
brew install poppler   # macOS
apt install poppler-utils  # Debian/Ubuntu
```

Then extract text with layout preservation:

```
pdftotext -layout pamphlet.pdf - | less
```

**If `pdftotext` is not installed, ask the user to install poppler before proceeding.**
Do not leave descriptions blank and move on — stop and ask.

## Tools (in this skill directory)

**`inspect-dbf.py`** — show unique values of a join column and all associated fields.
Use this to discover the join column name and available columns before writing `__init__.py`:

```
python .claude/skills/underfoot-add-source/scripts/inspect-dbf.py file.dbf [JOIN_COL]
```

**`scaffold-units-csv.py`** — generate a `units.csv` stub from the DBF with `code`,
`title`, and `span` filled in, leaving `description` and `lithology` blank:

```
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
or use the appropriate decompression parameter.

## `process_usgs_source` Key Parameters

| Parameter | Purpose |
|-----------|---------|
| `base_path` | Always `os.path.realpath(__file__)` — locates the work dir and `citation.json` |
| `url` | Download URL for the archive |
| `extracted_file_path` | Path *inside* the archive to the shapefile or e00 |
| `srs` | Proj4 CRS string or constant — determine from `.prj` or metadata |
| `use_unzip` | `True` for ZIP archives |
| `polygons_join_col` | Column name to dissolve/join polygons — read from shapefile attributes |
| `mappable_metadata_csv_path` | Path *inside* the archive to a bundled description CSV |
| `mappable_metadata_mapping` | Maps CSV columns to `code/title/span/description` |
| `metadata_csv_path` | Filesystem path to a hand-authored `units.csv` |
| `lithology_from_description` | `True` when title is not descriptive enough for lithology |

Do **not** combine `mappable_metadata_csv_path` and `metadata_csv_path`.

## Naming Convention

California GAM sources follow: `gam_<NN>_<location>_<YYYY>_gis`

- `NN` = two-digit sheet number (zero-padded)
- `location` = lowercase, underscores, no hyphens
- `YYYY` = publication year

## Checklist

- [ ] `sources/<name>/__init__.py` with `run()` and `process_usgs_source` call
- [ ] `sources/<name>/citation.json` — array of one CSL-JSON map object
- [ ] `sources/<name>.py` — thin entry-point that imports `run` and calls it under `__main__`
- [ ] `sources/<name>/units.csv` — only if archive has no machine-readable CSV
- [ ] `description` field populated from pamphlet PDF (do NOT leave blank; requires poppler)
- [ ] citation `URL` points to publication page, not the ZIP
- [ ] `issued.date-parts` is `[["YYYY"]]`
- [ ] `srs` determined from `.prj` file or metadata, not assumed
- [ ] `polygons_join_col` confirmed from actual shapefile attributes
