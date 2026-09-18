Underfoot is an offline-first app for viewing geologic and hydrologic maps. This repo holds
both halves of it:

- `data/`: the data prep pipeline (Python, GDAL, PostGIS, run with Docker Compose). It
  downloads geologic and hydrologic sources, processes them, and builds map packs. See
  `data/AGENTS.md`.
- `web/`: the Progressive Web App at underfoot.rocks (React, TypeScript, Vite, MapLibre). See
  `web/AGENTS.md`.

Nested `AGENTS.md` files may not be loaded until you work with files in that directory, so read
the one for the subproject you're working in before starting.

## How the halves connect

They share no code. `data/packs.py manifest` writes `manifest.json`, and CI uploads it along
with each `<pack>.pmtiles.zip` to `https://static.underfoot.rocks`. The web app fetches and
reads those files in `web/src/packs/`. A change to the manifest or pack contents may need
changes on both sides.

## Working in this repo

Run commands from inside `data/` or `web/`; neither subproject's commands work from the repo
root. After changes:

- TypeScript in `web/`: `npm run lint`, `npm test`, `npm run test:e2e`, and `npm run build`
- Python in `data/`: `docker compose run --rm app pytest`

Git hooks are installed by `npm install` at the repo root (Husky, configured in `.husky/`). The
pre-commit hook only runs the web checks when `web/` has staged changes.

CI lives in `.github/workflows/`: `deploy-web.yml` deploys `web/`, and the rest test `data/` and
build packs. The skills in `.claude/skills/` are for work in `data/`.
