from aps.ml.iv_damage_cli import build_parser


def test_snapshot_command_requires_explicit_holdout_campaigns():
    args = build_parser().parse_args([
        "snapshot",
        "--snapshot-version", "irr-v1",
        "--stress-type", "irradiation",
        "--target-type", "delta_vth_v",
        "--extraction-versions-json", '{"vth_v": "iv-parameters-v3.0"}',
        "--external-campaign", "campaign-external",
        "--calibration-campaign", "campaign-calibration",
    ])
    assert args.command == "snapshot"
    assert args.external_campaign == ["campaign-external"]
    assert args.calibration_campaign == ["campaign-calibration"]
    assert args.n_splits == 5
