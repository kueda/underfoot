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

- Node.js 22.13 or newer, which Vitest and jsdom need to run the tests
- npm 10 or newer, which ships with Node 22

## Setup

```sh
git clone git@github.com:kueda/underfoot.git
cd underfoot
npm install
cd web
npm install
```

The first `npm install`, at the repo root, runs `husky` through the root `package.json`'s
`prepare` script to install the Git hooks (see [Git hooks](#git-hooks) below). The second
installs the app's dependencies. Everything else in this README runs from `web/`.

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

The data behind those packs is produced by [`data/`](../data/) in this repo.

## Scripts

| Command | Description |
| --- | --- |
| `npm run dev` | Start the Vite dev server on `https://localhost:5173` |
| `npm run build` | Type-check with `tsc`, then build the production bundle to `dist/` |
| `npm run preview` | Serve the built `dist/` locally |
| `npm run lint` | Run ESLint over `src` (`.ts`/`.tsx`); zero warnings allowed |
| `npm test` | Run the unit tests once |
| `npm run test:watch` | Run the unit tests and rerun them on changes |
| `npm run test:e2e` | Build the app and run the end-to-end tests in a browser |

## Tests

Unit tests use [Vitest](https://vitest.dev/) with
[React Testing Library](https://testing-library.com/docs/react-testing-library/intro/) in a
[jsdom](https://github.com/jsdom/jsdom) environment. Vitest reads its settings from the `test`
block in `vite.config.ts`. Test files sit next to the code they test as `*.test.ts`.

`src/test/setup.ts` runs before each test file. It replaces the browser's IndexedDB with
[fake-indexeddb](https://github.com/dumbmatter/fakeIndexedDB), so tests exercise the real
localForage storage, and it swaps jsdom's `Blob` for Node's so that blobs survive being stored.
`src/test/packFixtures.ts` has helpers for building pack zips and faking
`static.underfoot.rocks`.

End-to-end tests in `e2e/` use [Playwright](https://playwright.dev/). Install its browser once
with `npx playwright install chromium`. `npm run test:e2e` builds the app, serves it with
`vite preview` on port 4174, and runs the tests in headless Chromium. The tests stand in for
`static.underfoot.rocks` with `page.route`, serving a cut-down copy of the `us-ca-oakland`
pack from `e2e/fixtures/`. `e2e/fixtures/make-oakland-pack.sh` rebuilds it from the real pack,
which is worth doing when the pack format changes.

Pushing changes under `web/` runs `.github/workflows/test-web.yml` at the repo root, which runs
the linter, both test suites, and the build. When end-to-end tests fail there, their traces are
uploaded as the `playwright-test-results` artifact; open one with `npx playwright show-trace`.

## Git hooks

[Husky](https://typicode.github.io/husky/) is installed from the root `package.json` and runs
the hooks in the root `.husky/` directory. When a commit has staged changes under `web/`,
deletions included, the `pre-commit` hook:

1. runs `lint-staged`, which applies `eslint --fix` to staged `.ts`/`.tsx` files
2. runs `tsc --noEmit` over the whole project

A commit fails if either step fails. Commits that don't touch `web/` skip both steps, so they
don't need `npm install` in `web/`.

The `post-commit` and `prepare-commit-msg` hooks are dispatchers: each runs the shared steps
in the tracked `.husky/<hook>` file at the repo root, then sources an optional per-developer
`.husky/<hook>.local` file if present. The `.local` files are git-ignored by the root
`.gitignore`, so you can add personal automation without touching what everyone else runs.

## Linting and type-checking

Run `npm run lint`, `npm test`, `npm run test:e2e`, and `npm run build` after any TypeScript
change; all must pass before a change is considered done. `npm run build` runs `tsc` first, so
it also surfaces type errors.

## Deployment

Pushing changes under `web/` to `main` triggers `.github/workflows/deploy-web.yml` at the repo
root, which builds the app and deploys it to GitHub Pages. The custom domain `underfoot.rocks`
is set in the repository's Pages settings.
