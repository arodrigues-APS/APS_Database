import pandas as pd
import pytest

from aps.paths import REPO_ROOT
from aps.exports.export_proxy_method_comparison_union_csv import _validate


def test_comparison_components_use_the_same_failure_boundary_fallback_as_v3():
    model_sql = (REPO_ROOT / "schema" / "028_mechanistic_energy_proxy.sql").read_text()
    comparison_sql = (REPO_ROOT / "schema" / "029_proxy_viz_support.sql").read_text()

    expected_fallback = "b.failure_fraction_overlap_score"
    assert expected_fallback in model_sql
    assert "v3.failure_fraction_overlap_score AS v3_failure_fraction_overlap_score" in comparison_sql
    assert "b.v3_failure_fraction_overlap_score" in comparison_sql
    assert "COALESCE(b.v3_failure_fraction_log_delta, 4.0)" not in comparison_sql


def test_export_validation_rejects_component_distance_drift():
    row = {
        "target_stress_record_key": "target",
        "candidate_stress_record_key": "candidate",
        "v1_winner_key": "candidate",
        "v2_winner_key": "candidate",
        "v3_winner_key": "candidate",
        "picked_by_v1": True,
        "picked_by_v2": True,
        "picked_by_v3": True,
        "v1_rank_percentile": 1.0,
        "v2_rank_percentile": 1.0,
        "v3_rank_percentile": 1.0,
        "v3_rank_available": True,
        "v3_component_weighted_sq_total": 4.0,
        "v3_combined_vector_distance": 3.0,
    }

    with pytest.raises(RuntimeError, match="weighted component sum"):
        _validate(pd.DataFrame([row]))

    row["v3_combined_vector_distance"] = 2.0
    _validate(pd.DataFrame([row]))
