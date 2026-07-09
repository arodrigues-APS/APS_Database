"""Root import shim for the test suite.

Puts the repo root and src/ on sys.path so imports resolve no matter which
directory pytest is invoked from and without requiring an editable install:

    from aps.proxy.mechanistic_energy_proxy import ...   # pipeline package
    import server                                        # root Flask app

This replaces per-file sys.path.insert() hacks in tests.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
for _path in (str(_ROOT), str(_ROOT / "src")):
    if _path not in sys.path:
        sys.path.insert(0, _path)
