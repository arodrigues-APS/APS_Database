"""Publish the generated damage-signature viewer as an explicit pipeline step."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from aps.paths import OUT_ROOT


VIEWER_ARTIFACT = (
    OUT_ROOT / "avalanche_irrad_pilot" / "damage_signature_3d_interactive.html"
)
LEGACY_REDIRECT = """<!doctype html>
<meta charset="utf-8">
<meta http-equiv="refresh" content="0; url=/tools/damage-signature-3d/">
<title>Redirecting to damage-signature viewer</title>
<link rel="canonical" href="/tools/damage-signature-3d/">
<p>This viewer moved to <a href="/tools/damage-signature-3d/">/tools/damage-signature-3d/</a>.</p>
"""


def publish_damage_signature_viewer(
    *,
    source: Path = VIEWER_ARTIFACT,
    web_tools_dir: Path | None = None,
) -> Path:
    """Atomically publish the current artifact and maintain the legacy redirect."""
    if not source.is_file() or source.stat().st_size == 0:
        raise RuntimeError(f"viewer artifact is missing or empty: {source}")

    root = web_tools_dir or Path(
        os.environ.get("APS_WEB_TOOLS_DIR", "/data/www/tools")
    )
    destination_dir = root / "damage-signature-3d"
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / "index.html"
    temporary = destination.with_suffix(".html.tmp")
    shutil.copyfile(source, temporary)
    temporary.chmod(0o644)
    temporary.replace(destination)

    legacy_dir = root / "phenotype-3d"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    legacy_destination = legacy_dir / "index.html"
    legacy_temporary = legacy_destination.with_suffix(".html.tmp")
    legacy_temporary.write_text(LEGACY_REDIRECT)
    legacy_temporary.chmod(0o644)
    legacy_temporary.replace(legacy_destination)
    return destination


def main() -> int:
    destination = publish_damage_signature_viewer()
    print(f"published damage-signature viewer to {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
