from copy import deepcopy
import json

import pytest

from aps.ml.iv_damage_manifest import EvidenceManifestError, validate_manifest, write_plan
from aps.ml.iv_damage_predictor_cli import build_parser


def manifest_v1():
    features = {
        "beam_energy_mev": 100.0, "let_surface": 10.0, "range_um": 20.0,
        "fluence_or_dose": 1.0e9, "irradiation_bias_v": 0.0,
        "temperature_c": 25.0, "post_measurement_delay_s": 3600.0,
        "prediction_horizon_s": 3600.0, "stress_condition_key": "ca-100mev",
    }
    acquisitions, observations = [], []
    for index, phase in enumerate(("pre", "pre", "post", "post"), start=1):
        acquisition_key = f"acq-{index}"
        acquisitions.append({
            "item_key": f"acquisition-{index}", "acquisition_key": acquisition_key,
            "metadata_id": index, "physical_device_key": "device-1",
            "measurement_protocol_id": "idvg-vth-fixed-1h-v1",
            "curve_family": "IdVg",
            "measured_at": f"2026-01-0{index}T00:00:00+00:00",
            "identity_source": "metadata_exact",
            "source_relation": "baselines_metadata", "source_checksum": "a" * 64,
        })
        observations.append({
            "item_key": f"observation-{index}", "acquisition_key": acquisition_key,
            "replicate_group_key": f"device-1-{phase}",
            "source_relation": "baselines_measurements", "source_checksum": "b" * 64,
            "source_row_ids": [index],
        })
    return {
        "manifest_version": 1, "batch_key": "irradiation-dvth-history-001",
        "prepared_by": "scientist-a", "prepared_at": "2026-02-02T00:00:00+00:00",
        "source_cutoff": "2026-02-01T00:00:00+00:00",
        "claim": {
            "stress_type": "irradiation", "target_type": "delta_vth_v",
            "intended_split_role": "train", "reference_policy": "same_device",
            "measurement_protocol_id": "idvg-vth-fixed-1h-v1",
            "prediction_horizon_s": 3600.0, "fixed_horizon": True,
        },
        "extraction_config": {
            "config_version": "vth-fixed-1h-v1", "target_type": "delta_vth_v",
            "target_current_a": 0.001, "required_vds_v": 0.1,
        },
        "acquisitions": acquisitions,
        "stress_sessions": [{
            "item_key": "stress-session-1", "stress_session_key": "stress-1",
            "physical_device_key": "device-1", "stress_type": "irradiation",
            "campaign_key": "campaign-1", "run_key": "run-1",
            "stress_condition_key": "ca-100mev", "stress_features": features,
            "identity_source": "campaign_registry",
        }],
        "observations": observations,
        "response_units": [{
            "item_key": "response-1", "unit_key": "unit-1",
            "physical_device_key": "device-1", "stress_session_key": "stress-1",
            "stress_type": "irradiation", "target_type": "delta_vth_v",
            "device_type": "IFX-Trench", "manufacturer": "Infineon",
            "measurement_protocol_id": "idvg-vth-fixed-1h-v1",
            "campaign_key": "campaign-1", "run_key": "run-1", "ion_species": "Ca",
            "pre_observation_keys": ["observation-1", "observation-2"],
            "post_observation_keys": ["observation-3", "observation-4"],
            "stress_features": features, "reference_policy": "same_device",
            "minimum_replicates": 2,
        }],
    }


def test_manifest_is_canonical_and_historical_roles_are_development_only():
    validated = validate_manifest(manifest_v1())
    assert len(validated["items"]) == 10
    invalid = deepcopy(manifest_v1())
    invalid["claim"]["intended_split_role"] = "external_test"
    with pytest.raises(EvidenceManifestError, match="train or calibration"):
        validate_manifest(invalid)


@pytest.mark.parametrize("relation", [
    "iv_physical_parameter_predictions", "iv_damage_validation_results", "model_residuals",
])
def test_manifest_rejects_predicted_or_validation_sources(relation):
    invalid = deepcopy(manifest_v1())
    invalid["observations"][0]["source_relation"] = relation
    with pytest.raises(EvidenceManifestError, match="baselines_measurements"):
        validate_manifest(invalid)


def test_manifest_requires_fixed_horizon_and_two_replicates():
    invalid = deepcopy(manifest_v1())
    invalid["claim"]["prediction_horizon_s"] = None
    with pytest.raises(EvidenceManifestError, match="positive finite"):
        validate_manifest(invalid)
    invalid = deepcopy(manifest_v1())
    invalid["response_units"][0]["post_observation_keys"] = ["observation-3"]
    with pytest.raises(EvidenceManifestError, match="two pre and two post"):
        validate_manifest(invalid)


def test_write_plan_is_idempotent_and_cli_has_all_commands(tmp_path):
    manifest = manifest_v1()
    report = {
        "batch_key": manifest["batch_key"], "manifest_sha": "c" * 64,
        "admissible": True, "manifest": manifest,
    }
    report_path = tmp_path / "report.json"
    first = write_plan(report, tmp_path, report_path)
    assert write_plan(report, tmp_path, report_path) == first
    assert json.loads(first.read_text())["batch_key"] == manifest["batch_key"]
    parser = build_parser()
    assert parser.parse_args([
        "evidence-plan", "--manifest", "@manifest.json",
        "--report-json", str(report_path),
    ]).command == "evidence-plan"
    for command in ("evidence-approve", "evidence-apply"):
        assert parser.parse_args([
            command, "--batch-key", "batch", "--expected-plan-sha", "a" * 64,
            "--actor", "actor-b",
        ]).command == command
    assert parser.parse_args(["evidence-status", "--batch-key", "batch"]).command == "evidence-status"


def test_manifest_rejects_unsafe_paths_invalid_checksums_and_late_sources(tmp_path):
    manifest = manifest_v1()
    manifest["batch_key"] = "../../outside"
    with pytest.raises(EvidenceManifestError, match="batch_key"):
        validate_manifest(manifest)

    manifest = manifest_v1()
    manifest["observations"][0]["source_checksum"] = "not-a-sha"
    with pytest.raises(EvidenceManifestError, match="64 lowercase hexadecimal"):
        validate_manifest(manifest)

    manifest = manifest_v1()
    manifest["acquisitions"][-1]["measured_at"] = "2026-02-02T00:00:00+00:00"
    with pytest.raises(EvidenceManifestError, match="exceeds source_cutoff"):
        validate_manifest(manifest)

    report = {
        "batch_key": "../../outside",
        "manifest_sha": "c" * 64,
        "admissible": True,
        "manifest": manifest_v1(),
    }
    with pytest.raises(EvidenceManifestError, match="batch_key"):
        write_plan(report, tmp_path, tmp_path / "report.json")


def test_manifest_rejects_cross_item_identity_and_empty_batches():
    manifest = manifest_v1()
    manifest["response_units"][0]["physical_device_key"] = "different-device"
    with pytest.raises(EvidenceManifestError, match="stress session"):
        validate_manifest(manifest)

    manifest = manifest_v1()
    manifest["response_units"] = []
    with pytest.raises(EvidenceManifestError, match="at least one response unit"):
        validate_manifest(manifest)

    manifest = manifest_v1()
    manifest["response_units"][0]["pre_observation_keys"] = [
        "observation-1",
        "observation-3",
    ]
    manifest["response_units"][0]["post_observation_keys"] = [
        "observation-2",
        "observation-4",
    ]
    with pytest.raises(EvidenceManifestError, match="replicate groups"):
        validate_manifest(manifest)
