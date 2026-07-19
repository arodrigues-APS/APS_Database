from aps.pipelines.nightly import default_steps


def test_nightly_scores_only_active_v3_models_and_never_trains_or_releases():
    steps = {step.name: step for step in default_steps()}
    assert "ml-post-iv-physical-prediction" not in steps
    assert "dashboard-iv-physical-prediction" not in steps
    assert "dashboard-sc-irradiation-prediction" not in steps
    scoring = steps["score-iv-damage-v3"]
    assert scoring.command == (
        "-m", "aps.ml.iv_damage_predictor_cli", "score-scalar-all"
    )
    command_text = " ".join(scoring.command)
    assert "train" not in command_text
    assert "release" not in command_text
    curve_scoring = steps["score-iv-damage-curves-v3"]
    assert curve_scoring.command == (
        "-m", "aps.ml.iv_damage_predictor_cli", "score-curves"
    )
    assert steps["dashboard-iv-damage-v3"].command == (
        "-m", "aps.superset.create_iv_damage_prediction_dashboard_v3"
    )
    assert "score-iv-damage-v3" not in steps["dashboard-iv-damage-v3"].depends_on
