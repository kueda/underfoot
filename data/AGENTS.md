Data prep for managing data packages for Underfoot, a mobile app for viewing
geologic and hydrologic maps. The Progressive Web App that displays this data
lives in `../web/`. Paths and commands in this file are relative to `data/`, so
run commands from this directory.

Each pack is specified as a .json file in packs/, including basic metadata and
lists of `rocks` sources for geology and `water` sources for hydrology. Each
source identifier references a python module in sources that will download
the source, process it, and insert it into a local PostGIS database. The
exceptions are the NHDPlus HR water sources (`nhdplus_h_<huc4>_hu4`) and
`tiger_water_<fips>`, which have no module; `water.py` builds them from the
identifier alone. Most
geologic source modules include a citation.json file in CSL-JSON, and may
include a units.csv file that lists geologic unit descriptions that the
source did not include in a machine-readable format and needed to be derived
from a PDF pamphlet or a PDF or image map that came with the source.

## Development environment

The canonical environment is Docker Compose (`compose.yaml`): an `app` container
built from `Dockerfile` (Ubuntu 24.04, GDAL 3.8, Python 3.12) with the repo
bind-mounted at `/app`, plus a `db` container running PostGIS 16. On Apple
Silicon `db` uses `imresamu/postgis` (native arm64); CI uses the official image.

```bash
docker compose build      # first run, and after Dockerfile / requirements.txt changes
docker compose up -d db   # Postgres, reachable from the host at localhost:5433
docker compose run --rm app python packs.py us-ca-oakland
```

The venv is baked into the image at `/opt/venv` and is on `PATH`, so there is no
activation step. `bin/imposm` and `bin/e00compr/e00conv` are compiled into the
image and selected via the `UNDERFOOT_IMPOSM` / `UNDERFOOT_E00CONV` env vars
(their defaults keep bare-metal use working). `numpy` is pinned `<2.0` for GDAL
and Fiona compatibility. `pdftotext` (poppler, for source pamphlets) and `7z`
(for ArcGIS `.mpk` map packages, which are 7z archives) are in the image.
`tippecanoe` (felt's fork, built from source) tiles the rocks, water, and
contours layers; ways and context still use GDAL's MVT driver.

On-disk caches (`sources/work-<name>/`, `elevation-tiles/`, `*.osm.pbf`,
`imposm-cache/`, `build/`) live in the bind-mounted tree, so they persist and
stay visible on the host. `imposm-cache/` is imposm's leveldb build cache
(`UNDERFOOT_IMPOSM_CACHEDIR` to relocate it); it can get large and is safe to
delete. The `pgdata` Docker volume holds the database across `docker compose
down`.

A `Makefile` wraps the common commands: `make build`, `make shell`, `make pack
PACK=us-ca-oakland`, `make test`, `make psql`.

### Running without Docker

The pipeline runs on any Linux host with PostgreSQL/PostGIS 16 and GDAL >= 3.8:
install the apt packages from the `Dockerfile` `runtime` stage plus `golang-go
libleveldb-dev libgeos-dev build-essential` to build `bin/imposm` and
`bin/e00compr`, build and install
[tippecanoe](https://github.com/felt/tippecanoe) (`libsqlite3-dev zlib1g-dev`),
make a venv from `requirements.txt`, and export
`PGHOST`/`PGUSER`/`PGPASSWORD`. Not supported directly on macOS.

## Building packs

```bash
docker compose run --rm app python packs.py list                 # list pack IDs
docker compose run --rm app python packs.py us-ca-oakland        # full pack into build/
docker compose run --rm app python packs.py us-ca-oakland --only rocks water
docker compose run --rm app python packs.py us-ca-oakland --clean-rocks
```

`packs.py` orchestrates the per-layer builders: `rocks.py` (geology),
`water.py` (hydrology), `elevation.py` (contours), and `osm.py` (ways and
context). Each builder runs the relevant source modules, loads their output
into the `underfoot` PostGIS database (`underfoot_osm` for OSM), and writes
PMTiles to `build/`. Cached source output lives in `sources/work-<name>/` and is
reused across builds unless a `--clean` or `--clean-<layer>` flag is passed.

When a pack is built, `water.py` also renames waterways from the pack's OSM extract
(`waterway_names.py`): an OSM river, stream, or canal that runs along a waterway gives it
its name, replacing the source's name, so corrections made in OSM show up in Underfoot at
the next build. It matches by geometry and skips matches that are likely wrong, like short
segments and siphons; see the module docstring for the rules. Then it continues waterways
that end short of where they go, like a creek NHD stops above a culvert, along OSM
waterways until they reach another waterway or a waterbody (`waterway_gaps.py`). Those
continuations are added as waterways from the `osm` source and linked into the flow, so
flow traces follow them. Both use the OSM waterways read once from the PBF with pyosmium
(`osm_waterways.py`; `osmium` in `requirements.txt`).

Packs with `"hydrography": "osm"` (written by `create_pack.py --osm-hydrography`;
`packs.py --osm-hydrography` forces it for any pack) skip all that and take their
waterways and flow from the OSM extract alone (`osm_hydrography.py`), along with their
waterbodies, which line up with OSM's waterways better than NHD's do; their water sources
still provide watersheds. That suits places OSM maps waterways well, like the Bay Area.

NHD and TIGER don't map the open sea, and OSM only implies it with its coastline, so every
pack takes the sea from the water polygons Overture Maps makes from it, read from the latest
release of its base theme on S3 with DuckDB (`duckdb` in `requirements.txt`), less the
islands in Overture's land, which its ocean sometimes covers. The sea fills the pack's bbox
instead of stopping at its boundary, which often follows the shore, so the sea between
islands isn't blank, and it's in the water tiles at every zoom. The web app also draws it
under the rocks, where geologic sources usually stop at the shore.

## Working on a single source

```bash
docker compose run --rm app python sources/<name>.py
```

This runs just that source module and writes standardized files to
`sources/work-<name>/` (for rocks, `units.geojson`) without touching the
database. Use it to check a new or changed source before adding it to a pack.
The expected output formats for rocks and water sources are documented in
`README.md`; helper functions are in the `sources/util/` module.

Water sources without a module can't be run this way. `python water.py
<source>` processes them into the same work dir, but it also loads them into
the database and writes `water.pmtiles`.

For adding a new geologic source, use the `underfoot-add-source` skill. For
creating a new pack, use the `underfoot-create-pack` skill; packs are always
generated by `create_pack.py` and never hand-edited. For a unit that shows the
wrong lithology, use the `underfoot-fix-unit-lithology` skill.

## Tests

```bash
docker compose run --rm app pytest
```

Coverage is limited; tests live in `tests/` and `tests/sources/`. CI
(`../.github/workflows/run-tests.yml`) runs them in the same image, so local and
CI are both Python 3.12.

## CI

`../.github/workflows/build-image.yml` builds `Dockerfile` once per commit and
pushes `ghcr.io/<repo>/app:<sha>`; `run-tests.yml` and `build-pack.yml` run in
container jobs on that image (with a `postgis/postgis:16-3.5` service
container), so the build environment is defined in exactly one place. `Dockerfile`
is arch-agnostic: developers build arm64 locally, CI builds amd64.
