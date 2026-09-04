import ast
import pathlib
import re
import sys
import tomllib
from importlib.metadata import packages_distributions

PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent
PYPROJECT = PACKAGE_ROOT / "pyproject.toml"
SRC = PACKAGE_ROOT / "src" / "apiadapters"


def _declared_dependency_names() -> set[str]:
    """Distribution names listed in pyproject.toml's ``dependencies``."""
    data = tomllib.loads(PYPROJECT.read_text())
    names = set()
    for entry in data["project"]["dependencies"]:
        # e.g. "httpx>=0.28.1" or "xmlparser @ git+https://...@master"
        name = re.split(r"[<>=@\s\[]", entry, maxsplit=1)[0]
        names.add(name)
    return names


def _imported_top_level_modules() -> set[str]:
    """Top-level modules imported anywhere under src/apiadapters."""
    modules = set()
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    modules.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.level == 0 and node.module:
                    modules.add(node.module.split(".")[0])
    return modules


def test_imported_third_party_modules_are_declared_dependencies() -> None:
    """Every third-party module imported by the package must be declared
    in pyproject.toml's ``dependencies``.

    Regression test for BUG-03: straininfo.py imported tinydb and pydantic
    without either being declared, so a clean `pdm install` could not
    import apiadapters.straininfo.
    """
    stdlib = sys.stdlib_module_names
    third_party_modules = {
        module
        for module in _imported_top_level_modules()
        if module not in stdlib and module != "apiadapters"
    }

    distributions = packages_distributions()
    declared = _declared_dependency_names()

    undeclared = set()
    for module in third_party_modules:
        dist_names = set(distributions.get(module, [module]))
        if not dist_names & declared:
            undeclared.add(module)

    assert not undeclared, (
        f"modules imported but not declared in pyproject.toml dependencies: "
        f"{sorted(undeclared)}"
    )
