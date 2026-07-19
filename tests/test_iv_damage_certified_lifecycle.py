from pathlib import Path

from aps.ml.iv_damage_predictor_cli import build_parser
from aps.pipelines.nightly import default_steps


SCALAR = Path("src/aps/ml/iv_damage_scalar_certification.py").read_text()
CURVE = Path("src/aps/ml/iv_damage_curve_training.py").read_text()
SHADOW = Path("src/aps/ml/iv_damage_scalar_shadow.py").read_text()
SQL = "\n".join(
    Path(f"schema/{number}_iv_damage_{suffix}.sql").read_text()
    for number, suffix in (
        ("035", "certified_curves"),
        ("036", "certification_guards"),
        ("037", "curve_operations"),
        ("038", "authoritative_writer_guards"),
        ("040", "scalar_shadow"),
        ("041", "promotion_guard"),
        ("044", "release_observability"),
    )
).lower()


def test_complete_cli_exposes_separate_development_certification_and_shadow_commands():
    parser = build_parser()
    commands = next(action for action in parser._actions if action.dest == "command").choices
    expected = {
        "register-acquisition", "register-stress-session", "extract-observation",
        "train-scalar-development", "select-scalar", "certify-scalar",
        "start-scalar-shadow", "score-scalar-all", "assess-scalar-shadow",
        "record-scalar-outcomes", "promote-scalar",
        "validate-projection-development",
        "certify-projection", "train-curve", "select-curve", "certify-curve",
        "start-curve-shadow", "score-curves", "assess-curve-shadow",
        "promote-curve",
    }
    assert expected <= set(commands)


def test_development_queries_filter_roles_before_rows_reach_python():
    assert "assignment.split_role = any(%s)" in SCALAR.lower()
    assert 'roles=("train", "calibration")' in SCALAR
    assert "external_outcomes_not_accessed" in SCALAR
    assert "assignment.split_role <> 'external_test'" in CURVE
    assert '"external_certification": "not_accessed"' in CURVE


def test_external_holdouts_are_single_use_and_certification_is_immutable():
    assert "dataset_snapshot_id bigint not null unique" in SQL
    assert "selection and certification records are immutable" in SQL
    assert "has already been consumed" in SCALAR
    assert "has already been consumed" in CURVE


def test_shadow_does_not_consume_request_or_become_decision_eligible():
    assert "shadow_prediction_not_for_decision_use" in SHADOW
    assert "set request_status = 'scored'" not in SHADOW.lower()
    assert "shadow prediction must be screening-only and non-decision" in SQL
    assert "first release requires a passed prospective shadow assessment" in SQL


def test_scalar_candidate_requires_development_only_attestation():
    assert "iv_damage_scalar_development_attestation_guard" in SQL
    assert "external certification not accessed" in SQL
    assert "new.release_status <> 'candidate'" in SQL


def test_nightly_only_scores_and_reconciles_never_trains_selects_or_promotes():
    relevant = [step for step in default_steps() if "iv-damage" in step.name]
    command_text = "\n".join(" ".join(step.command) for step in relevant)
    assert "score-scalar-all" in command_text
    assert "score-curves" in command_text
    assert "create_iv_damage_prediction_dashboard_v3" in command_text
    for forbidden in ("train-", "select-", "certify-", "promote-"):
        assert forbidden not in command_text
