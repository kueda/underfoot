# underfoot
Underfoot is a mobile app for revealing the hydrological and geological world beneath your
feet. Well, sort of. It's mostly just something I tinker with in my spare time. It'll probably
never be done. If you're interested in a more fully-functional app for geological exploration,
check out [rockd](https://rockd.org).

Still reading? Underfoot runs at [underfoot.rocks](https://underfoot.rocks), and this repo has
both halves of it:

- [`data/`](data/): data prep. Python scripts that download geologic and hydrologic sources,
  process them, and publish map packs. See [data/README.md](data/README.md).
- [`web/`](web/): the Progressive Web App that downloads those packs and displays them. See
  [web/README.md](web/README.md).

The two share no code. The web app only depends on the manifest and pack files that `data/`
publishes to `static.underfoot.rocks`, so each half has its own tooling, and its commands run
from inside its directory.

## Git hooks
Run `npm install` at the repo root to install the Git hooks. The pre-commit hook only runs
checks for `web/` when a commit touches it; see [web/README.md](web/README.md#git-hooks).

## License
[MIT](LICENSE). The license covers everything in this repo, including `data/` and `web/`.
