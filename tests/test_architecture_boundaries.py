"""Small executable checks for the responsibility boundaries in the review."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from aps.paths import REPO_ROOT


SRC_ROOT = REPO_ROOT / "src" / "aps"
SUPERSET_ROOT = SRC_ROOT / "superset"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    result = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def test_superset_modules_do_not_import_ingestion_implementations():
    offenders = {
        path.name: sorted(
            imported for imported in _imports(path) if imported.startswith("aps.ingest")
        )
        for path in SUPERSET_ROOT.glob("*.py")
    }
    offenders = {name: imports for name, imports in offenders.items() if imports}

    assert offenders == {}


def test_superset_api_helper_does_not_import_export_or_presentation_policy():
    imports = _imports(SUPERSET_ROOT / "superset_api.py")

    assert "aps.superset.dashboard_png_export" not in imports
    assert "aps.superset.nonproxy_dashboard_support" not in imports


def test_application_code_never_invokes_pip():
    offenders = []
    for path in SRC_ROOT.rglob("*.py"):
        text = path.read_text()
        if "subprocess.check_call" in text and '"pip"' in text:
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []


def test_proxy_model_sql_has_one_python_owner():
    owners = []
    target_names = {
        "025_proxy_readiness_waveforms.sql",
        "028_mechanistic_energy_proxy.sql",
        "029_proxy_viz_support.sql",
    }
    for path in SRC_ROOT.rglob("*.py"):
        text = path.read_text()
        if any(f'"{name}"' in text for name in target_names):
            owners.append(str(path.relative_to(REPO_ROOT)))

    assert owners == ["src/aps/db/models.py"]


def test_only_database_facade_opens_psycopg2_connections():
    offenders = []
    for path in SRC_ROOT.rglob("*.py"):
        if path == SRC_ROOT / "db_config.py":
            continue
        if re.search(r"\bpsycopg2\s*\.\s*connect\s*\(", path.read_text()):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []


def test_flask_request_module_contains_no_schema_ddl():
    server_text = (REPO_ROOT / "server.py").read_text()

    assert re.search(r"\b(?:CREATE|ALTER|DROP)\s+(?:TABLE|VIEW|INDEX)\b", server_text, re.I) is None


def test_release_b_bootstrap_keeps_services_stopped_until_verified():
    script = (REPO_ROOT / "scripts" / "bootstrap_release_b_systemd.sh").read_text()

    assert "systemctl disable --now aps-nightly.timer" in script
    assert "systemctl enable " not in script
    assert "systemctl restart " not in script
    assert "systemctl start " not in script
