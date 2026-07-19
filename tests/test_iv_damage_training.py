import pytest

from aps.ml.iv_damage_model import DamageExample
from aps.ml.iv_damage_readiness import EvidenceUnit
from aps.ml.iv_damage_training import SnapshotExample, evaluate_partition
from aps.ml.iv_damage_validation import ValidationUnit


def features(x):
    return {
        "pre_value": 3.0 + x * 0.01,
        "beam_energy_mev": 100 + x,
        "let_surface": 20 + x * 0.2,
        "range_um": 10 + x * 0.1,
        "fluence_or_dose": 1e7 * (1 + x * 0.01),
        "irradiation_bias_v": 100 + x,
        "temperature_c": 25 + x * 0.01,
        "post_measurement_delay_s": 60 + x,
    }


def row(prefix, x):
    damage = DamageExample(
        f"{prefix}-{x}", f"device-{prefix}-{x}", f"session-{prefix}-{x}",
        "irradiation", "delta_vth_v", "C2M", 0.01 + 0.002 * x,
        features(x), ion_species="Xe", manufacturer="Wolfspeed",
        protocol_signature="protocol",
        prediction_horizon_s=features(x)["post_measurement_delay_s"],
    )
    validation = ValidationUnit(
        damage.response_unit_key, damage.physical_device_key, damage.stress_session_key,
        damage.target_type, damage.observed_response, campaign_key=f"campaign-{x % 3}",
        ion_species="Xe", device_type="C2M",
    )
    evidence = EvidenceUnit(
        damage.response_unit_key, damage.physical_device_key, damage.stress_session_key,
        "irradiation", "delta_vth_v", "C2M", f"campaign-{x % 3}", f"run-{x}",
        "protocol", damage.observed_response, 0.01, 2, "train", "Xe", features(x),
    )
    return SnapshotExample(x, damage, validation, evidence, f"campaign-{x % 3}", "train", None)


def test_partition_evaluation_fits_only_train_calibrates_independently_and_benchmarks():
    result = evaluate_partition(
        [row("train", x) for x in range(20)],
        [row("cal", x + 0.1) for x in range(5, 10)],
        [row("test", x + 0.2) for x in range(7, 12)],
        stress_type="irradiation", target_type="delta_vth_v",
        estimator_kind="huber", interval_coverage=0.8, ood_quantile=0.95,
        min_neighbor_devices=1, min_calibration_groups=3,
        catastrophic_error_threshold=0.2, random_state=1,
    )
    assert result.model.training_groups == 20
    assert result.model.calibration_groups == 5
    assert result.metrics.total_units == 5
    assert result.metrics.supported_fraction == 1.0
    assert result.best_baseline in result.baseline_maes
    assert len(result.predictions) == 5
    assert all(prediction.baseline_predictions for prediction in result.predictions)


def test_partition_requires_three_nonempty_roles():
    with pytest.raises(Exception, match="partitions are required"):
        evaluate_partition(
            [row("train", x) for x in range(5)], [], [row("test", 1)],
            stress_type="irradiation", target_type="delta_vth_v",
            estimator_kind="huber", interval_coverage=0.8, ood_quantile=0.95,
            min_neighbor_devices=1, min_calibration_groups=1,
            catastrophic_error_threshold=0.2, random_state=1,
        )
