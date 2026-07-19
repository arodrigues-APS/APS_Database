
import pytest

from aps.ml.iv_damage_cli import _json_features, _observation_context, build_parser


def test_operational_cli_exposes_manual_release_and_active_scoring_commands():
    parser = build_parser()
    release = parser.parse_args(["release", "--model-version", "v1", "--actor", "lab"])
    assert release.command == "release"
    score = parser.parse_args(["score", "--limit", "20"])
    assert score.limit == 20
    rollback = parser.parse_args([
        "rollback", "--stress-type", "irradiation", "--target-type", "delta_vth_v",
        "--actor", "lab",
    ])
    assert rollback.command == "rollback"
    deactivate = parser.parse_args([
        "deactivate", "--stress-type", "irradiation",
        "--target-type", "delta_vth_v", "--actor", "lab",
        "--reason", "monitoring gate failed",
    ])
    assert deactivate.command == "deactivate"


def test_operational_cli_exposes_governed_evidence_lifecycle():
    parser = build_parser()
    assert parser.parse_args([
        "register-method", "--config-json", "{}",
    ]).command == "register-method"
    assert parser.parse_args([
        "approve-policy", "--policy-version", "p1", "--actor", "owner",
    ]).command == "approve-policy"
    assert parser.parse_args([
        "materialize-response", "--spec-json", "{}",
    ]).command == "materialize-response"


def test_features_json_accepts_object_or_file_and_rejects_array(tmp_path):
    assert _json_features('{"fluence_or_dose": 1}') == {"fluence_or_dose": 1}
    path = tmp_path / "features.json"
    path.write_text('{"pulse_count": 2}')
    assert _json_features(f"@{path}") == {"pulse_count": 2}
    with pytest.raises(ValueError, match="object"):
        _json_features("[]")


def test_observation_context_uses_authoritative_input_and_source_fingerprints():
    context = _observation_context(
        (
            '{"metadata_id": 1, "measurement_protocol_id": "protocol-v1", '
            '"replicate_group_key": "pre-1", '
            '"measured_at": "2025-01-01T12:00:00+00:00"}'
        ),
        input_sha256="abc",
        source_provenance={"code_sha": "def", "fingerprint": "ghi"},
    )
    assert context.measured_at.utcoffset().total_seconds() == 0
    assert context.source_fingerprint["input_sha256"] == "abc"
