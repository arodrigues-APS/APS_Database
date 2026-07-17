from aps.pipelines.nightly import default_steps


def test_nightly_scores_only_active_v3_models_and_never_trains_or_releases():
    steps = {step.name: step for step in default_steps()}
    assert "ml-post-iv-physical-prediction" not in steps
    assert "dashboard-iv-physical-prediction" not in steps
    assert "dashboard-sc-irradiation-prediction" not in steps
    scoring = steps["score-iv-damage-v3"]
    assert scoring.command == ("-m", "aps.ml.iv_damage_cli", "score")
    command_text = " ".join(scoring.command)
    assert "train" not in command_text
    assert "release" not in command_text
    assert steps["dashboard-iv-damage-v3"].depends_on == ("score-iv-damage-v3",)
