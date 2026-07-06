"""Root import shim for the test suite.

Puts the repo root and data_processing_scripts/ on sys.path so both import
styles used across the codebase resolve no matter which directory pytest is
invoked from:

    from data_processing_scripts.foo import bar   # package style (tests, server)
    from db_config import get_connection          # flat style (pipeline scripts)

This replaces per-file sys.path.insert() hacks in tests. Pipeline scripts keep
their own fallbacks because they also run standalone via the nightly pipeline.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
for _path in (str(_ROOT), str(_ROOT / "data_processing_scripts")):
    if _path not in sys.path:
        sys.path.insert(0, _path)
