"""Truthful source-tree provenance for mutating APS operations.

Git's commit id is insufficient when a process executes a dirty checkout. A
nightly run or model build must either identify the exact changed source bytes
or, in production, refuse to run them. This module deliberately limits the
fingerprint to executable/configuration surfaces and excludes ignored personal
documentation and generated artifacts.
"""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from aps.config import ConfigurationError, Settings
from aps.paths import REPO_ROOT


SOURCE_PATHS = (
    ".github",
    "pyproject.toml",
    "requirements",
    "schema",
    "scripts",
    "server.py",
    "server_config",
    "src",
    "superset",
    "wsgi.py",
)


@dataclass(frozen=True)
class SourceProvenance:
    code_sha: str
    dirty: bool
    fingerprint: str
    changed_paths: tuple[str, ...]
    git_available: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "code_sha": self.code_sha,
            "dirty": self.dirty,
            "fingerprint": self.fingerprint,
            "changed_paths": list(self.changed_paths),
            "git_available": self.git_available,
        }


def _git(
    args: list[str],
    *,
    repo_root: Path,
    environ: Mapping[str, str] | None = None,
) -> bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        env=None if environ is None else dict(environ),
        check=True,
        capture_output=True,
    )
    return result.stdout


def collect_source_provenance(
    repo_root: Path | str = REPO_ROOT,
    *,
    environ: Mapping[str, str] | None = None,
) -> SourceProvenance:
    """Fingerprint HEAD, tracked changes, and untracked executable sources."""
    root = Path(repo_root).resolve()
    try:
        code_sha = _git(
            ["rev-parse", "HEAD"], repo_root=root, environ=environ
        ).decode().strip()
        diff = _git(
            ["diff", "--binary", "HEAD", "--", *SOURCE_PATHS],
            repo_root=root,
            environ=environ,
        )
        changed = _git(
            ["diff", "--name-only", "HEAD", "--", *SOURCE_PATHS],
            repo_root=root,
            environ=environ,
        ).decode().splitlines()
        untracked_raw = _git(
            [
                "ls-files",
                "--others",
                "--exclude-standard",
                "-z",
                "--",
                *SOURCE_PATHS,
            ],
            repo_root=root,
            environ=environ,
        )
    except (OSError, UnicodeError, subprocess.CalledProcessError) as exc:
        digest = hashlib.sha256(f"git-unavailable:{type(exc).__name__}".encode())
        return SourceProvenance(
            code_sha="unknown",
            dirty=True,
            fingerprint=digest.hexdigest(),
            changed_paths=(),
            git_available=False,
        )

    untracked = sorted(
        value.decode("utf-8", errors="surrogateescape")
        for value in untracked_raw.split(b"\0")
        if value
    )
    digest = hashlib.sha256()
    digest.update(code_sha.encode())
    digest.update(b"\0tracked-diff\0")
    digest.update(diff)
    for relative in untracked:
        digest.update(b"\0untracked\0")
        digest.update(relative.encode("utf-8", errors="surrogateescape"))
        digest.update(b"\0")
        path = root / relative
        try:
            digest.update(path.read_bytes())
        except OSError as exc:
            digest.update(f"unreadable:{type(exc).__name__}".encode())

    changed_paths = tuple(sorted(set(changed) | set(untracked)))
    return SourceProvenance(
        code_sha=code_sha or "unknown",
        dirty=bool(diff or untracked),
        fingerprint=digest.hexdigest(),
        changed_paths=changed_paths,
        git_available=True,
    )


def require_clean_production_source(
    settings: Settings,
    provenance: SourceProvenance,
    *,
    operation: str,
) -> None:
    """Refuse unidentifiable or dirty source for a production mutation."""
    if settings.profile != "production":
        return
    if not provenance.git_available:
        raise ConfigurationError(
            f"{operation} requires readable Git metadata in production; "
            "the source commit could not be identified"
        )
    if provenance.dirty:
        sample = ", ".join(provenance.changed_paths[:8]) or "unknown paths"
        suffix = " ..." if len(provenance.changed_paths) > 8 else ""
        raise ConfigurationError(
            f"{operation} refuses a dirty production source tree at "
            f"{provenance.code_sha[:12]}: {sample}{suffix}"
        )
