"""WRC decisions scraping and transformation pipeline."""

import sys

# Fail loudly and early. `requires-python` in pyproject.toml is only enforced by
# pip when installing a *published* package from an index -- for a local editable
# install it is advisory at most. Without this guard, an interpreter that is too
# old surfaces as an obscure error deep in an import (e.g. Python 3.9 raises
# "dataclass() got an unexpected keyword argument 'slots'" from partitions.py),
# which is a poor first experience for anyone cloning the repo. macOS ships 3.9
# as its system Python, so this is the single most likely setup failure.
if sys.version_info < (3, 11):
    raise RuntimeError(
        f"wrc-pipeline requires Python 3.11 or newer, but found "
        f"{sys.version.split()[0]} at {sys.executable}.\n"
        "Recreate the virtual environment with a supported interpreter:\n"
        "    deactivate && rm -rf .venv\n"
        "    python3.12 -m venv .venv && source .venv/bin/activate\n"
        "    pip install -e '.[dev]'"
    )

__version__ = "0.1.0"
