from pathlib import Path

import pytest

from aps.ml.iv_damage_research_operations import (
    ResearchOperationError,
    load_artifact_verified,
    save_artifact_immutable,
)


def test_exact_artifact_replay_is_resumable(tmp_path: Path):
    destination = tmp_path / "research" / "run.joblib"
    payload = {"predictions": [1.0, 2.0], "seed": 17}

    first = save_artifact_immutable(payload, destination, root=tmp_path)
    second = save_artifact_immutable(payload, destination, root=tmp_path)

    assert second == first
    assert load_artifact_verified(first, root=tmp_path) == payload


def test_conflicting_artifact_replay_fails_closed(tmp_path: Path):
    destination = tmp_path / "research" / "run.joblib"
    save_artifact_immutable({"prediction": 1.0}, destination, root=tmp_path)

    with pytest.raises(ResearchOperationError, match="replay conflicts"):
        save_artifact_immutable({"prediction": 2.0}, destination, root=tmp_path)
