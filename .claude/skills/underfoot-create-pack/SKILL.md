---
name: underfoot-create-pack
description: Use when the user asks to create, add, or make a new underfoot pack file or pack metadata.
---

# underfoot-create-pack

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
| Asking the user for bbox/osm/water | These are auto-generated — never ask |
| Using full country/state names for `--id` | Use two-letter codes in the ID |
| Using abbreviated names for `--admin1`/`--admin2` | Use full names for those fields |
