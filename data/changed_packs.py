"""List the packs and layers that need rebuilding after changes to the files in a git diff

Reads paths relative to the repo root from stdin, one per line, like the output of
`git diff --name-only`, and prints the affected packs as a JSON array of objects with the
pack ID and the layers to rebuild, like `{"pack": "us-ca-oakland", "layers": ["rocks"]}`.
A pack is affected if its definition changed, which rebuilds every layer, or if it uses a
source whose module or files changed, including shared modules the source imports, like
of2005_1305, which rebuilds the layer it uses the source for.

Changes to sources/util/ affect nearly every pack, so the Build all packs workflow
rebuilds every pack when they happen. To avoid building packs twice, this prints an
empty list if any of the paths are ones that workflow handles. With --build-all-packs,
it prints every pack with the layers that workflow should rebuild instead, or with
--every-layer too, every layer of every pack without reading stdin.

With --definition-hash, it prints the hash of a pack's definition instead, which
build-pack.yml compares with the one the published pack was built from.

Only uses the standard library so CI can run it without the app image.
"""

import argparse
import ast
import hashlib
import json
import os
import pathlib
import sys
from urllib.parse import urlparse

DATA_DIR = pathlib.Path(__file__).parent.absolute()

# Layers of a pack that build-pack.yml builds separately. Keep in sync with the default of
# its layers input and ALL_LAYERS in its plan job
LAYERS = ["rocks", "water", "contours", "ways", "context"]

# Layers affected by changes to files or dirs in sources/util/. Changes to anything else
# in it rebuild every layer
UTIL_LAYERS = {
    "citations.py": ["rocks", "water"],
    # Scripts run by util.met2xml() and util.polygonize_arcs(), which only util.rocks calls
    "met2xml.py": ["rocks"],
    "polygonize_arcs.py": ["rocks"],
    "rocks": ["rocks"],
    "tiger_water.py": ["water"],
    "usgs_states.py": ["rocks"],
    "water.py": ["water"],
}


def built_by_build_all_packs(path):
    """True if changes to a path relative to the data dir trigger the Build all packs
    workflow. Keep in sync with its `paths` in .github/workflows/build-all-packs.yml"""
    parts = pathlib.PurePosixPath(path).parts
    return parts[:2] == ("sources", "util") and parts[2:] != ("__init__.py",)


def changed_module(path):
    """Name of the module in sources/ that a path relative to the data dir belongs to,
    either sources/<module>.py or a file in sources/<module>/"""
    parts = pathlib.PurePosixPath(path).parts
    if len(parts) < 2 or parts[0] != "sources":
        return None
    if len(parts) == 2:
        return parts[1][:-3] if parts[1].endswith(".py") else None
    return parts[1]


def imported_modules(py_path):
    """Names of the top-level modules a Python file imports"""
    tree = ast.parse(py_path.read_text(encoding="utf-8"), filename=str(py_path))
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            modules.add(node.module.split(".")[0])
    return modules


def importers_by_module(sources_dir):
    """Map of each module name to the names of the modules in sources/ that import it"""
    importers = {}
    for py_path in sources_dir.rglob("*.py"):
        relative = py_path.relative_to(sources_dir).as_posix()
        module = changed_module(f"sources/{relative}")
        if module.startswith("work-"):
            continue
        for imported in imported_modules(py_path):
            importers.setdefault(imported, set()).add(module)
    return importers


def affected_modules(modules, sources_dir):
    """The given module names plus the names of all modules that import them, directly
    or indirectly"""
    importers = importers_by_module(sources_dir)
    affected = set()
    pending = list(modules)
    while pending:
        module = pending.pop()
        if module in affected:
            continue
        affected.add(module)
        pending.extend(importers.get(module, ()))
    return affected


def ordered_layers(layers):
    """Layers in the order of LAYERS"""
    return [layer for layer in LAYERS if layer in layers]


def data_paths_in(paths, data_dir):
    """Paths relative to the data dir of the paths relative to the repo root that are in it"""
    repo_dir = data_dir.parent
    data_paths = []
    for path in paths:
        relative = os.path.relpath(repo_dir / path, data_dir)
        if not relative.startswith(".."):
            data_paths.append(pathlib.PurePosixPath(*pathlib.PurePath(relative).parts))
    return data_paths


def layers_by_pack(data_paths, data_dir):
    """Map of the IDs of the packs affected by changes to paths relative to the data dir to
    the sets of their layers that need rebuilding, ignoring changes to sources/util/"""
    layers = {}
    for path in data_paths:
        if path.parent.as_posix() == "packs" and path.suffix == ".json":
            if (data_dir / path).exists():
                layers[path.stem] = set(LAYERS)
    # Nearly every source imports util, so util/__init__.py doesn't trigger a build,
    # same as in Build all packs
    modules = {changed_module(path) for path in data_paths} - {None, "util"}
    if modules:
        sources = affected_modules(modules, data_dir / "sources")
        for pack_path in (data_dir / "packs").glob("*.json"):
            pack = json.loads(pack_path.read_text(encoding="utf-8"))
            for key, layer in (("rock", "rocks"), ("water", "water")):
                if sources.intersection(pack.get(key, [])):
                    layers.setdefault(pack_path.stem, set()).add(layer)
    return layers


def changed_packs(paths, data_dir=DATA_DIR):
    """Packs affected by changes to paths relative to the repo root, as dicts with the pack
    ID and the layers to rebuild"""
    data_dir = pathlib.Path(data_dir)
    data_paths = data_paths_in(paths, data_dir)
    build_all_paths = [path for path in data_paths if built_by_build_all_packs(path)]
    if build_all_paths:
        print(
            f"Leaving packs to the Build all packs workflow because of {build_all_paths[0]}",
            file=sys.stderr
        )
        return []
    return [
        {"pack": pack_id, "layers": ordered_layers(layers)}
        for pack_id, layers in sorted(layers_by_pack(data_paths, data_dir).items())
    ]


def build_all_packs(paths, data_dir=DATA_DIR, every_layer=False):
    """Every pack, as dicts with the pack ID and the layers the Build all packs workflow
    should rebuild after changes to paths relative to the repo root: the layers the util
    changes affect, plus the layers of changes Build changed packs leaves to it. With
    every_layer, every layer of every pack"""
    data_dir = pathlib.Path(data_dir)
    data_paths = data_paths_in(paths, data_dir)
    util_layers = set(LAYERS) if every_layer else set()
    for path in data_paths:
        if built_by_build_all_packs(path):
            util_layers.update(UTIL_LAYERS.get(path.parts[2], LAYERS))
    other_layers = layers_by_pack(data_paths, data_dir)
    matrix = []
    for pack_id in sorted(path.stem for path in (data_dir / "packs").glob("*.json")):
        layers = util_layers | other_layers.get(pack_id, set())
        if layers:
            matrix.append({"pack": pack_id, "layers": ordered_layers(layers)})
    return matrix


def definition_hash(pack_path):
    """SHA-256 of a pack definition file and the boundary GeoJSON file it references, which
    every layer of the pack depends on"""
    pack_path = pathlib.Path(pack_path)
    pack_bytes = pack_path.read_bytes()
    digest = hashlib.sha256(pack_bytes)
    ref = json.loads(pack_bytes).get("geojson", {}).get("$ref")
    if ref:
        # Resolved the same way as in packs.py
        parsed_uri = urlparse(ref)
        digest.update((pack_path.parent / f"{parsed_uri.netloc}{parsed_uri.path}").read_bytes())
    return digest.hexdigest()


def main(args=None, data_dir=DATA_DIR):
    """Print the packs affected by the paths on stdin as JSON, with --build-all-packs, the
    packs and layers the Build all packs workflow should rebuild, or with
    --definition-hash, the hash of a pack definition"""
    parser = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0])
    parser.add_argument(
        "--build-all-packs",
        action="store_true",
        help="Print every pack with the layers the Build all packs workflow should rebuild"
    )
    parser.add_argument(
        "--every-layer",
        action="store_true",
        help="With --build-all-packs, print every layer of every pack without reading stdin"
    )
    parser.add_argument(
        "--definition-hash",
        metavar="PACK_PATH",
        help="Print the hash of the definition of the pack at this path"
    )
    args = parser.parse_args(args)
    if args.definition_hash:
        print(definition_hash(args.definition_hash))
        return
    if args.build_all_packs and args.every_layer:
        print(json.dumps(build_all_packs([], data_dir, every_layer=True)))
        return
    paths = [line.strip() for line in sys.stdin if line.strip()]
    if args.build_all_packs:
        print(json.dumps(build_all_packs(paths, data_dir)))
    else:
        print(json.dumps(changed_packs(paths, data_dir)))


if __name__ == "__main__":
    main()
