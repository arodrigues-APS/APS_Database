import pytest

from aps.ml.iv_damage_research_contracts import ResearchContractError
from aps.ml.iv_damage_research_dataset import audit_pair, deterministic_split_assignments
from aps.ml.iv_damage_research_scalar import (
    feature_record,
    run_grouped_scalar_benchmark,
)
from tests.test_iv_damage_research_dataset import make_pair


def cohort():
    return [
        audit_pair(
            make_pair(
                index,
                device=f"device-{index}",
                run=f"run-{index % 2}",
                campaign=f"campaign-{index % 2}",
                shift=0.02 * index,
            )
        )
        for index in range(1, 6)
    ]


def test_feature_contract_contains_no_post_truth_or_device_identity():
    record = feature_record(cohort()[0])
    assert "post_vth_v" not in record
    assert "observed_delta_vth_v" not in record
    assert "physical_device_key" not in record
    assert record["fluence_missing"] == 1.0


def test_zero_and_v2_are_genuine_fold_safe_benchmarks():
    pairs = cohort()
    assignments = deterministic_split_assignments(pairs, "leave_device")
    zero = run_grouped_scalar_benchmark(pairs, assignments, method="zero_damage", validation_scheme="leave_device")
    donor = run_grouped_scalar_benchmark(pairs, assignments, method="v2_donor", validation_scheme="leave_device")
    assert len(zero.predictions) == len(pairs)
    assert len(donor.predictions) == len(pairs)
    for prediction in donor.predictions:
        device = next(
            row.candidate.physical_device_key for row in pairs if row.candidate.pair_key == prediction.pair_key
        )
        assert device not in prediction.training_device_keys


def test_learned_models_fit_preprocessing_inside_each_outer_fold():
    pairs = cohort()
    assignments = deterministic_split_assignments(pairs, "leave_device")
    for method in ("huber", "extra_trees"):
        result = run_grouped_scalar_benchmark(
            pairs,
            assignments,
            method=method,
            validation_scheme="leave_device",
        )
        assert result.metrics["supported_devices"] == 5
        assert len(result.fold_manifests) == 5


def test_assignments_must_be_complete():
    pairs = cohort()
    assignments = deterministic_split_assignments(pairs, "leave_device")[:-1]
    with pytest.raises(ResearchContractError):
        run_grouped_scalar_benchmark(pairs, assignments, method="zero_damage", validation_scheme="leave_device")
