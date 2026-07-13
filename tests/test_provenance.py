from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from aps.config import ConfigurationError, Settings
from aps.provenance import collect_source_provenance, require_clean_production_source


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def _committed_source_tree(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / ".github" / "workflows").mkdir(parents=True)
    (repo / "src" / "demo.py").write_text("VALUE = 1\n")
    (repo / ".github" / "workflows" / "ci.yml").write_text("name: demo\n")
    (repo / "pyproject.toml").write_text("[project]\nname = 'demo'\n")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "APS tests")
    _git(repo, "config", "user.email", "aps-tests@example.invalid")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "initial")
    return repo


def test_provenance_fingerprints_tracked_and_untracked_source(tmp_path: Path):
    repo = _committed_source_tree(tmp_path)
    clean = collect_source_provenance(repo)

    assert clean.git_available is True
    assert clean.dirty is False
    assert clean.changed_paths == ()

    (repo / "src" / "demo.py").write_text("VALUE = 2\n")
    (repo / ".github" / "workflows" / "new.yml").write_text("name: new\n")
    dirty = collect_source_provenance(repo)

    assert dirty.dirty is True
    assert dirty.fingerprint != clean.fingerprint
    assert dirty.changed_paths == (
        ".github/workflows/new.yml",
        "src/demo.py",
    )


def test_dirty_production_source_is_rejected(tmp_path: Path):
    repo = _committed_source_tree(tmp_path)
    (repo / "src" / "demo.py").write_text("VALUE = 2\n")
    source = collect_source_provenance(repo)
    settings = Settings.from_environ({"APS_PROFILE": "production"})

    with pytest.raises(ConfigurationError, match="refuses a dirty production source"):
        require_clean_production_source(
            settings,
            source,
            operation="test mutation",
        )
