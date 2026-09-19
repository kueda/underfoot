---
name: underfoot-fix-unit-lithology
description: Use when a geologic map unit in underfoot shows the wrong lithology, e.g. a
  GitHub issue saying a unit "shouldn't be treated as limestone", or when adding or reviewing
  a lithology override or a units.csv lithology value for a rocks source.
---

# Fixing a Unit's Lithology

Paths in this skill are relative to `data/`, and commands run from `data/`.

## Overview

A unit's lithology is inferred from text unless the source supplies one, and the inference is
easy to fool. The fix is almost always one row of data, but choosing the right value means
checking what the source itself says instead of guessing or copying a sibling unit. Do that
investigation first; the edit is the small part. If the investigation does not make the right
lithology clear, ask the user instead of choosing one.

## Why lithology comes out wrong

`infer_metadata_from_csv_row()` in `sources/util/rocks/__init__.py` takes the first of these
that produces a value:

1. A `lithology` supplied by the source's `units.csv` or `overrides.csv`. It must be in
   `LITHOLOGIES` or `LITHOLOGY_SYNONYMS` (`sources/util/rocks/constants.py`), otherwise the
   build raises `ValueError`.
2. Text inference on the title. The order of title and description flips when the source sets
   `lithology_from_description=True`.
3. Text inference on the other one of title and description.
4. `geomaterial`, for sources that map it (GeMS sources; see the add-source skill).

Text inference (`lithology_from_text()`) tries `LITHOLOGY_PRIORITY_PATTERNS` (e.g. "lava
flow"), then takes the lithology word that starts earliest in the text. So it reads formation
names as lithologies: "Horquilla Limestone" gives limestone, "Sandia Formation" gives sand,
and a description that lists many formations is classified by one word. Because text beats
`geomaterial`, a source's own classification is ignored whenever the text contains any
lithology word.

## Workflow

### 1. Find the unit

Get the unit's `code`, title, description, and current lithology. For a pack that has been
built, `build/<pack>.pmtiles/rocks-rock_units_attrs.csv` has all of them. Otherwise run the
source (step 2). `packs/<pack>.json` lists a pack's sources under `rock`.

Confirm the code from the unit name in the issue. A map link in an issue can point at a
neighboring unit.

### 2. Run the source to get its intermediate files

```
docker compose run --rm app python sources/<name>.py
```

This downloads and extracts the source once; later runs reuse the download and regenerate the
metadata from the current overrides. Look in `sources/work-<name>/`:

| File | Contents |
|------|----------|
| `<layer>.csv`, e.g. `DescriptionOfMapUnits.csv` | Raw unit table from a geodatabase, with every source column, including `GeoMaterial` even if the source does not map it |
| `metadata_from_csv.csv` | The mapped columns (`code`, `title`, `span`, `description`, `geomaterial`) |
| `metadata_with_overrides.csv` | The same after `overrides.csv` is applied |
| `data.csv` | The final inferred metadata: `lithology`, `rock_type`, ages |

To look at one unit: `grep '^<code>,' sources/work-<name>/data.csv`. Values with commas are
quoted, so anchoring on the leading code works.

### 3. Look for what the source says, in this order

Stop when one of these settles it.

1. **A classification field in the source.** GeMS sources have `GeoMaterial` (a controlled
   vocabulary such as "Mostly carbonate rock" or "Sedimentary rock") and
   `GeoMaterialConfidence`. "Rock" says nothing useful. For other sources, look at the other
   columns of the polygon or description table (`inspect-dbf.py` in the add-source skill).
2. **The source's `units.csv`**, if it has one. Its `lithology` column is what to edit.
3. **The pamphlet or handbook**, if there is one: `pdftotext -layout <file>.pdf -` (poppler
   is in the image), then search for the unit's name. The pamphlet often has exactly the
   description the pipeline already reads, so it only helps if it says more.
4. **The map image or legend.** Some sources only describe a unit there. You can view an
   image directly, or ask the user to check it (see "Asking the User for Help with
   Descriptions" in the add-source skill).

If none of these settles it, stop and **ask the user** (see "When to Ask the User"). Do not
pick a value to keep going, and do not copy a sibling unit's value because the titles share a
word like "undivided". Different units under the same title pattern can have different actual
lithologies, and the source may say so.

### 4. Choose the value

- Use a value from `LITHOLOGIES` (or a key of `LITHOLOGY_SYNONYMS`). Map the source's term to
  the closest one: `GeoMaterial` "Mostly carbonate rock" is `carbonate rock`, "Sedimentary
  rock" is `sedimentary rock`.
- Be no more specific than the evidence. A unit the source calls mostly carbonate is
  `carbonate rock`, not `limestone`, even if a formation named "Limestone" is listed in it.
- `rock_type` is derived from the lithology, so a value from another category changes it.

### 5. Apply it

- **Source with `metadata_overrides_csv_path`** (`sources/nmbgmr_ofgm_304/` is an example):
  add a `code,lithology` row to `sources/<name>/overrides.csv`. Keep the file in plain byte
  order by code (uppercase before lowercase, so `MD`, `PIP`, `PZ`, `Pa`).
- **Source with a hand-authored `units.csv`**: edit the unit's `lithology` cell.
- **Source with a mappable CSV or table but no overrides yet**: create `overrides.csv` and
  pass its full path as `metadata_overrides_csv_path` in `__init__.py` (see the add-source
  skill for the details).

Never edit `units.geojson`, a work dir file, or the database directly; those are regenerated.

### 6. Verify

1. Before re-running the source, save the old output: `cp sources/work-<name>/data.csv
   sources/work-<name>/data.before.csv`.
2. `docker compose run --rm app pytest tests/sources/test_csv_metadata.py
   tests/sources/test_rocks.py`. These check that every override and `units.csv` lithology is
   known and that the override mechanism works. No new test is needed for a data row.
3. Re-run the source and check the log. An `Override for '<code>' doesn't match any unit` line
   means the code has a typo.
4. `grep '^<code>,' sources/work-<name>/data.csv` for the new `lithology` and `rock_type`.
5. `diff sources/work-<name>/data.before.csv sources/work-<name>/data.csv`. Only the units
   you fixed should differ. Anything else is a stale earlier run or unrelated code changes;
   say so in the PR instead of hiding it.

### 7. Clean up and look for siblings

- Work dirs are gitignored (`sources/work-*`) but can be large. The New Mexico source is a
  1 GB download. Delete `sources/work-<name>/` when finished.
- `packs.py` skips a source whose table in the database already has rows, so a change only
  shows up in a built pack after `--clean-rocks` (which re-downloads) or dropping that
  source's table. CI builds from scratch, so it needs nothing special.
- The same cause usually affects other units. In `metadata_with_overrides.csv`, compare each
  unit's `geomaterial` with its `lithology` in `data.csv`, and mention units that disagree in
  the PR or a new issue instead of quietly widening the change.

## When to Ask the User

Ask when you have worked through step 3 and still cannot say which lithology is right. That
includes:

- The evidence conflicts, e.g. `GeoMaterial` says one thing and the pamphlet or description
  another.
- The only evidence is uninformative: `GeoMaterial` is just "Rock", the description only
  lists formation names, or the unit is a mixed or complex one.
- Nothing in `LITHOLOGIES` fits what the evidence says.
- The issue asks for a specific lithology but the evidence only supports a broad class.
- The answer may be on the map image or legend and you cannot read it reliably.

**How to ask:** send one message with:

- the unit's code, title, and current lithology, and why that looks wrong
- what you checked and what each source said, quoting the `GeoMaterial` value and the
  relevant description text
- the candidate lithologies, with your recommendation. If the user has nothing more specific,
  the broadest term that is certainly true is the fallback to offer: `sedimentary rock`,
  `volcanic rock`, `plutonic rock`, or `metamorphic rock`
- for a legend or image, where to look

Then wait for the answer. Do not write the override or edit `units.csv` first, and do not
spend many more tool calls searching: a user can often settle it in seconds from the legend or
local knowledge. If you cannot reach the user at all, stop and report the evidence and
candidates instead of committing a guess.

## Commit

Follow the repo's style, e.g. `fix(data/sources): treat NM "<title>" as <lithology>`. Say in
the body what evidence chose the value (e.g. the source's `GeoMaterial`), and end with
`Fixes #<issue>`.

## Common Mistakes

- Guessing when the evidence is thin, including defaulting to `sedimentary rock`, instead of
  asking the user.
- Copying a sibling unit's override without checking the source.
- Overriding to a specific rock (`limestone`) when the source only supports a class
  (`carbonate rock`).
- A lithology that is not in `LITHOLOGIES`, which fails `test_override_lithologies_are_known`.
- Comparing against an old build's lithologies and blaming your change for differences that
  come from stale caches.
