"""
Filesystem anchors for the aps package.

The package always runs from a git checkout (editable install or
PYTHONPATH=src), so the repo root is derived from this file's location:
src/aps/paths.py -> repo root.  Import these instead of re-deriving
Path(__file__).parent chains in individual modules.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_DIR = REPO_ROOT / "schema"
OUT_ROOT = REPO_ROOT / "out"
