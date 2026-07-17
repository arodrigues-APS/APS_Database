from aps.ml.iv_damage_readiness import (
    DOMAIN_REQUIRED_FEATURES,
    EvidenceUnit,
    ReadinessRequirements,
    assess_readiness,
    missing_required_features,
)


def unit(
    index,
    *,
    campaign=None,
    physical=None,
    session=None,
    role="train",
    features=None,
    replicate_count=2,
):
    complete = {name: float(index + 1) for name in DOMAIN_REQUIRED_FEATURES["irradiation"]}
    complete.update(features or {})
    return EvidenceUnit(
        unit_key=f"unit-{index}",
        physical_device_key=physical or f"device-{index}",
        stress_session_key=session or f"session-{index}",
        stress_type="irradiation",
        target_type="delta_vth_v",
        device_type="part-a",
        campaign_key=campaign or f"campaign-{index % 3}",
        run_key=f"run-{index}",
        measurement_protocol_id="protocol-v1",
        response_value=float(index % 5) / 10.0,
        response_uncertainty=0.01,
        replicate_count=replicate_count,
        split_role=role,
        ion_species="Ni",
        features=complete,
    )


def small_requirements():
    return ReadinessRequirements(
        min_independent_groups=6,
        min_physical_devices=6,
        min_campaigns=3,
        min_external_groups=2,
        min_calibration_groups=2,
        max_campaign_share=0.50,
    )


def test_complete_balanced_independent_evidence_is_ready():
    roles = ["train", "train", "calibration", "calibration", "external_test", "external_test"]
    units = [unit(index, campaign=f"campaign-{index % 3}", role=role) for index, role in enumerate(roles)]

    report = assess_readiness(
        units,
        stress_type="irradiation",
        target_type="delta_vth_v",
        requirements=small_requirements(),
    )
    assert report.status == "model_ready"
    assert report.independent_groups == 6
    assert report.campaigns == 3


def test_missing_fluence_or_dose_blocks_domain_and_is_counted():
    rows = [unit(index) for index in range(6)]
    broken = rows[0]
    features = dict(broken.features)
    features["fluence_or_dose"] = None
    rows[0] = unit(0, features=features)

    report = assess_readiness(
        rows,
        stress_type="irradiation",
        target_type="delta_vth_v",
        requirements=ReadinessRequirements(
            min_independent_groups=1, min_physical_devices=1, min_campaigns=1,
            min_external_groups=0, min_calibration_groups=0,
            max_campaign_share=1.0,
        ),
    )
    assert report.status == "data_blocked"
    assert report.missing_feature_counts["fluence_or_dose"] == 1
    assert "required_features" in report.blockers


def test_repeated_files_do_not_inflate_independent_group_count():
    rows = [
        unit(0, physical="device-a", session="session-a"),
        unit(1, physical="device-a", session="session-a"),
    ]
    report = assess_readiness(
        rows,
        stress_type="irradiation",
        target_type="delta_vth_v",
        requirements=ReadinessRequirements(
            min_independent_groups=2, min_physical_devices=1, min_campaigns=1,
            min_external_groups=0, min_calibration_groups=0,
            max_campaign_share=1.0,
        ),
    )
    assert report.independent_groups == 1
    assert "no_duplicate_independent_groups" in report.blockers
    assert "independent_groups" in report.blockers


def test_missing_external_and_calibration_groups_blocks_readiness():
    rows = [unit(index, role="train") for index in range(6)]
    report = assess_readiness(
        rows,
        stress_type="irradiation",
        target_type="delta_vth_v",
        requirements=small_requirements(),
    )
    assert set(report.blockers) >= {"external_groups", "calibration_groups"}


def test_single_campaign_dominance_blocks_readiness():
    rows = [unit(index, campaign="one", role="external_test" if index > 3 else "calibration") for index in range(6)]
    report = assess_readiness(
        rows,
        stress_type="irradiation",
        target_type="delta_vth_v",
        requirements=small_requirements(),
    )
    assert "campaigns" in report.blockers
    assert "campaign_balance" in report.blockers


def test_missing_feature_helper_rejects_unknown_stress_type():
    row = unit(0)
    assert missing_required_features(row) == ()
    unknown = EvidenceUnit(**{**row.__dict__, "stress_type": "unknown"})
    try:
        missing_required_features(unknown)
    except ValueError as exc:
        assert "unsupported stress_type" in str(exc)
    else:
        raise AssertionError("unknown stress type must fail closed")
