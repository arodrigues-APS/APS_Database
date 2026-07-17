
import pytest

from aps.ml.iv_damage_cli import _json_features, build_parser


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


def test_features_json_accepts_object_or_file_and_rejects_array(tmp_path):
    assert _json_features('{"fluence_or_dose": 1}') == {"fluence_or_dose": 1}
    path = tmp_path / "features.json"
    path.write_text('{"pulse_count": 2}')
    assert _json_features(f"@{path}") == {"pulse_count": 2}
    with pytest.raises(ValueError, match="object"):
        _json_features("[]")
