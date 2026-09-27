---
name: underfoot-create-pack
description: Use when the user asks to create, add, or make a new underfoot pack file or pack metadata.
---

# underfoot-create-pack

Paths in this skill are relative to `data/`, and commands run from `data/`.

## Overview

Pack files are JSON metadata files in `packs/` that bundle rock and water sources for a map
area. Always create them with `create_pack.py` — never write the JSON by hand, as the script
auto-generates `bbox`, `geojson`, `osm`, and `water` fields from the source data.

## Required Information — Ask the User

Before running the script, collect these five things. Do NOT assume or guess them:

| Field | Arg | Example | Notes |
|-------|-----|---------|-------|
| Rock sources | positional | `sim3040 sim3151` | Space-separated source identifiers |
| Pack ID | `--id` | `us-ca-darwin-hills` | Hyphenated, lowercase; use two-letter ISO codes for country/state |
| Pack name | `--name` | `"Darwin Hills, CA, USA"` | Human-readable display name |
| Description | `--description` | `"Surficial geology of the Darwin Hills..."` | One-sentence summary |
| Country | `--admin1` | `"United States"` | Full name, not abbreviation |
| State/province | `--admin2` | `"California"` | Full name, not abbreviation |

Do NOT ask the user for `bbox`, `osm`, `water`, or `geojson` — those are computed automatically.
The one exception is `osm` when the script refuses its pick (see below).

## Running the Script

```bash
python create_pack.py sim3040 sim3151 \
    --id us-ca-darwin-hills \
    --name "Darwin Hills, CA, USA" \
    --description "Surficial and bedrock geology of the Darwin Hills area, Inyo County" \
    --admin1 "United States" \
    --admin2 "California"
```

The script will run any sources that haven't been processed yet, compute a convex-hull boundary
from the source geometries, find the Geofabrik OSM extract and NHD/TIGER water sources that
cover the boundary, and write `packs/<id>.json` plus `packs/<id>.geojson`.

Water sources are written as bare identifiers: `nhdplus_h_<huc4>_hu4` for NHDPlus HR and
`tiger_water_<GEOID>` for TIGER county water. `water.py` downloads and processes these itself,
so do NOT create `sources/nhdplus_*.py` or `sources/tiger_water_*.py` scripts for them. The
script writes only those two files in `packs/`; anything else new in `sources/` is a mistake.

**Oversized OSM extracts:** If the smallest Geofabrik extract containing the boundary is over
1.5 GB, or none contains it, the script exits with an error listing the extracts that cover
most of the boundary, with their coverage and size. Show that list to the user and ask which one
to use (usually the smallest one covering nearly all of the pack), then rerun with
`--osm <url>`. Don't edit the `osm` field in the generated JSON instead.

**Interactive mode:** If the user hasn't provided all the info yet, you can run
`python create_pack.py -i` to be prompted for each field interactively.

## ID Naming Convention

`<country-code>-<state-code>[-<area>]`

- Use two-letter ISO 3166 codes: `us` for United States, `ca` for Canada, etc.
- State/province: use US postal codes (`ca`, `oh`, `nh`) or equivalent
- Area: short, lowercase, hyphenated (`san-francisco`, `joshua-tree`, `darwin-hills`)
- Examples: `us-ca-sfba`, `us-ca-oakland`, `us-oh`, `us-nh`

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Writing `packs/<id>.json` by hand | Always use `create_pack.py` |
| Using `rocks` as the field name | The field is `rock` (singular) |
| Using `title` as the field name | The field is `name` |
| Asking the user for bbox/osm/water | These are auto-generated — only ask about `osm` when the script refuses its pick |
| Hand-editing `osm` in the pack JSON | Rerun `create_pack.py` with `--osm <url>` |
| Adding `sources/<id>.py` scripts for NHD or TIGER water sources | Not needed; `water.py` resolves the identifiers |
| Using full country/state names for `--id` | Use two-letter codes in the ID |
| Using abbreviated names for `--admin1`/`--admin2` | Use full names for those fields |
