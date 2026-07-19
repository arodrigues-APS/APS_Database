from pathlib import Path


def sql(number, name):
    return Path(f"schema/{number}_iv_damage_{name}.sql").read_text().lower()


def test_authoritative_writer_guard_checks_exact_registered_point_hash():
    text = sql("038", "authoritative_writer_guards")
    assert "acquisition_point_payload_hash" in text
    assert "registered authoritative acquisition payload" in text
    assert "iv_damage_bind_response_session" in text
    assert "iv_damage_response_observation_linker" in text


def test_scalar_shadow_and_promotion_fail_closed_in_postgresql():
    shadow = sql("040", "scalar_shadow")
    promotion = sql("041", "promotion_guard")
    assert "deployment_mode <> 'shadow' or not decision_eligible" in shadow
    assert "external_certifications" in shadow
    assert "shadow_prediction_not_for_decision_use" not in shadow  # runtime reason, not SQL policy
    assert "shadow_promotion" in promotion
    assert "assessment.passed" in promotion


def test_projection_certification_is_separate_from_method_approval():
    certification = sql("036", "certification_guards")
    release = sql("042", "session_and_projection_release")
    assert "iv_damage_curve_projection_certifications" in certification
    assert "projection method approval and curve-level external certification" in release
    assert "external_certification_passed" in release
