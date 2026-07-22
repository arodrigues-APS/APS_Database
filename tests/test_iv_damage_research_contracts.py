from pathlib import Path

import pytest

from aps.ml.iv_damage_research_contracts import (
    CLAIM_CLASS,
    EVIDENCE_STATUS,
    HORIZON_STATUS,
    PREDICTION_CONTEXT,
    ResearchContractError,
    validate_feature_names,
)
from aps.ml.iv_damage_research_operations import (
    ResearchOperationError,
    load_artifact_verified,
    save_artifact_immutable,
)


def test_research_claim_is_permanently_non_decision_capable():
    assert CLAIM_CLASS == "retrospective_research"
    assert PREDICTION_CONTEXT == "historical_out_of_fold"
    assert EVIDENCE_STATUS == "exploratory"
    assert HORIZON_STATUS == "unknown_or_heterogeneous"


@pytest.mark.parametrize(
    "feature",
    [
        "post_vth_v",
        "observed_delta_vth_v",
        "delta_vth_v",
        "physical_device_key",
        "pair_key",
        "residual_component",
        "v2_prediction",
    ],
)
def test_leakage_features_fail_closed(feature):
    with pytest.raises(ResearchContractError):
        validate_feature_names(["pre_vth_v", feature])


def test_immutable_artifact_is_contained_and_checksum_verified(tmp_path):
    root = tmp_path / "artifacts"
    identity = save_artifact_immutable({"features": ["pre_vth_v"]}, root / "run.joblib", root=root)
    assert load_artifact_verified(identity, root=root) == {"features": ["pre_vth_v"]}
    with pytest.raises(ResearchOperationError):
        save_artifact_immutable({}, tmp_path / "outside.joblib", root=root)
    with pytest.raises(ResearchOperationError):
        save_artifact_immutable({}, root / "run.joblib", root=root)


def test_migration_is_research_only_and_append_only():
    sql = Path("schema/046_iv_damage_research_prediction.sql").read_text()
    assert "iv_damage_research_guard_immutable" in sql
    assert "historical_out_of_fold" in sql
    assert "CHECK (NOT decision_eligible)" in sql
    assert "INSERT INTO iv_damage_predictions" not in sql
    assert "INSERT INTO iv_damage_response_units" not in sql
