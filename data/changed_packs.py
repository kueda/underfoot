"""List the packs that need rebuilding after changes to the files in a git diff

Reads paths relative to the repo root from stdin, one per line, like the output of
`git diff --name-only`, and prints the IDs of the affected packs as a JSON array. A pack
is affected if its definition changed or if it uses a source whose module or files
changed, including shared modules the source imports, like of2005_1305.

Changes to sources/util/ affect nearly every pack, so the Build all packs workflow
rebuilds every pack when they happen. To avoid building packs twice, this prints an
empty list if any of the paths are ones that workflow handles.

Only uses the standard library so CI can run it without the app image.
"""

import ast
import json
import os
import pathlib
import sys

DATA_DIR = pathlib.Path(__file__).parent.absolute()


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


def changed_packs(paths, data_dir=DATA_DIR):
    """IDs of the packs affected by changes to paths relative to the repo root"""
    data_dir = pathlib.Path(data_dir)
    repo_dir = data_dir.parent
    data_paths = []
    for path in paths:
        relative = os.path.relpath(repo_dir / path, data_dir)
        if not relative.startswith(".."):
            data_paths.append(pathlib.PurePosixPath(*pathlib.PurePath(relative).parts))
    build_all_paths = [path for path in data_paths if built_by_build_all_packs(path)]
    if build_all_paths:
        print(
            f"Leaving packs to the Build all packs workflow because of {build_all_paths[0]}",
            file=sys.stderr
        )
        return []
    pack_ids = set()
    for path in data_paths:
        if path.parent.as_posix() == "packs" and path.suffix == ".json":
            if (data_dir / path).exists():
                pack_ids.add(path.stem)
    # Nearly every source imports util, so util/__init__.py, the only util file that
    # can get here, doesn't trigger a build, same as in Build all packs
    modules = {changed_module(path) for path in data_paths} - {None, "util"}
    if modules:
        sources = affected_modules(modules, data_dir / "sources")
        for pack_path in (data_dir / "packs").glob("*.json"):
            pack = json.loads(pack_path.read_text(encoding="utf-8"))
            if sources.intersection(pack.get("rock", []) + pack.get("water", [])):
                pack_ids.add(pack_path.stem)
    return sorted(pack_ids)


def main(data_dir=DATA_DIR):
    """Print the IDs of the packs affected by the paths on stdin as JSON"""
    paths = [line.strip() for line in sys.stdin if line.strip()]
    print(json.dumps(changed_packs(paths, data_dir)))


if __name__ == "__main__":
    main()
