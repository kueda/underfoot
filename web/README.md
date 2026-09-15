# Underfoot Web

Web app and PWA for displaying Underfoot geologic and hydrologic maps, available at [underfoot.rocks](https://underfoot.rocks).

Underfoot is an offline-first app for viewing geologic and hydrologic maps. Map data is
distributed as downloadable "packs" that get stored in the browser, so the app keeps working
without a network connection.

## Tech stack

- [React](https://react.dev/) 18 and [TypeScript](https://www.typescriptlang.org/), bundled with [Vite](https://vitejs.dev/) 5
- [MapLibre GL JS](https://maplibre.org/) for map rendering, with [PMTiles](https://protomaps.com/docs/pmtiles) vector tiles
- [MUI](https://mui.com/) (Material UI) 5 with [Emotion](https://emotion.sh/) for styling
- [Zustand](https://github.com/pmndrs/zustand) for state and [localForage](https://localforage.github.io/localForage/) for offline (IndexedDB) storage
- [vite-plugin-pwa](https://vite-pwa-org.netlify.app/) for the service worker and web app manifest

## Requirements

- Node.js 20 or newer (Vite 5 also runs on 18, but 20+ is recommended)
- npm 10 or newer, which ships with Node 20

## Setup

```sh
git clone git@github.com:kueda/underfoot-web.git
cd underfoot-web
npm install
```

`npm install` runs `husky` through the `prepare` script to install the Git hooks (see
[Git hooks](#git-hooks) below).

## Running the dev server

```sh
npm run dev
```

This starts Vite at `https://localhost:5173`. The dev server uses HTTPS via
[`@vitejs/plugin-basic-ssl`](https://github.com/vitejs/vite-plugin-basic-ssl) because
geolocation and service workers require a secure context. The certificate is self-signed, so
the browser will warn on first load; accept it to continue.

`vite-plugin-pwa` has `devOptions.enabled` set, so a service worker is built in development
too. Its output lands in `dev-dist/` (git-ignored, regenerated on every run). If the app
serves stale assets during development, unregister the service worker and clear site data in
the browser's dev tools.

### Map data

The app fetches its pack manifest and map data from `https://static.underfoot.rocks` at
runtime. Nothing needs to be configured or downloaded locally, but a network connection is
required the first time a pack is loaded. There are no environment variables.

The data behind those packs is produced by a separate project,
[kueda/underfoot](https://github.com/kueda/underfoot).

## Scripts

| Command | Description |
| --- | --- |
| `npm run dev` | Start the Vite dev server on `https://localhost:5173` |
| `npm run build` | Type-check with `tsc`, then build the production bundle to `dist/` |
| `npm run preview` | Serve the built `dist/` locally |
| `npm run lint` | Run ESLint over `src` (`.ts`/`.tsx`); zero warnings allowed |

There is no automated test suite yet.

## Git hooks

[Husky](https://typicode.github.io/husky/) installs a `pre-commit` hook that:

1. runs `lint-staged`, which applies `eslint --fix` to staged `.ts`/`.tsx` files
2. runs `tsc --noEmit` over the whole project

A commit fails if either step fails.

The `post-commit` and `prepare-commit-msg` hooks are dispatchers: each runs the shared steps
in the tracked `.husky/<hook>` file, then sources an optional per-developer
`.husky/<hook>.local` file if present. The `.local` files are git-ignored (via `*.local`), so
you can add personal automation without touching what everyone else runs.

## Linting and type-checking

Run `npm run lint` and `npm run build` after any TypeScript change; both must pass before a
change is considered done. `npm run build` runs `tsc` first, so it also surfaces type errors.

## Deployment

Pushing to `main` triggers the `.github/workflows/vite-github-pages-deploy.yml` workflow,
which builds the app and deploys it to GitHub Pages. The `CNAME` file points the Pages site
at the custom domain `underfoot.rocks`.
