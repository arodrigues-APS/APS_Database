from aps.superset import create_iv_damage_prediction_dashboard_v3 as complete


def test_dashboard_34_contract_leads_with_research_and_keeps_one_readiness_tab():
    dashboard = complete.dashboard
    assert dashboard.DASHBOARD_TITLE == "IV Damage Predictor V3 — Research Predictions & Certified Readiness"
    assert dashboard.DASHBOARD_SLUG == "iv-damage-predictor-v3"
    assert list(dashboard.TABS)[:4] == [
        "research_scalar",
        "research_curve",
        "research_residual",
        "research_limits",
    ]
    assert "research_overview" not in dashboard.TABS
    assert "activation" in dashboard.TABS
    assert len(dashboard.TABS) == 5


def test_research_charts_never_mix_certified_datasets_or_claims():
    definitions = complete.dashboard.definitions()
    research = [row for row in definitions if row["ds"].startswith("research")]
    certified = [row for row in definitions if not row["ds"].startswith("research")]
    assert len(research) >= 8
    assert certified
    assert all("Research" in row["name"] for row in research)
    assert any("POST = TRUTH ONLY" in row["name"] for row in research)
    assert complete.dashboard.DATASETS["research_curve"] == ("iv_damage_research_curve_plot_view")
    assert complete.dashboard.DATASETS["activation"] == ("iv_damage_claim_activation_status_view")


def test_every_research_tab_has_visible_nondecision_guidance():
    dashboard = complete.dashboard
    for key in (
        "research_scalar",
        "research_curve",
        "research_residual",
        "research_limits",
    ):
        guidance = dashboard.GUIDANCE[dashboard.TABS[key][1]].upper()
        assert "RESEARCH" in guidance or "DIAGNOSTIC" in guidance or "LIMITATIONS" in guidance
    combined = " ".join(dashboard.GUIDANCE.values()).lower()
    assert "not decision eligible" in combined
    assert "post curves are joined only after prediction as held-out truth" in combined
