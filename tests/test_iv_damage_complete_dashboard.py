from aps.superset import create_iv_damage_prediction_dashboard_v3 as complete


def test_complete_dashboard_exposes_research_results_and_readiness_only():
    definitions = complete.dashboard.definitions()
    by_dataset = {row["ds"] for row in definitions}
    assert by_dataset == {
        "activation",
        "research_cohort",
        "research_curve",
        "research_curve_metrics",
        "research_limitations",
        "research_residual",
        "research_scalar",
        "research_status",
    }
    assert complete.dashboard.DATASETS["curve_projection"] == "iv_damage_curve_projection_view"
    assert complete.dashboard.DATASETS["curve_prediction"] == "iv_damage_curve_prediction_view"


def test_research_scalar_and_curve_units_are_explicit():
    definitions = complete.dashboard.definitions()
    names = {row["name"] for row in definitions}
    assert "V3 Research — Scalar OOF Error (SCREENING ONLY)" in names
    assert "V3 Research — Historical OOF Curve Explorer (POST = TRUTH ONLY)" in names
    scalar = next(row for row in definitions if "Scalar OOF Error" in row["name"])
    labels = {metric["label"] for metric in scalar["params"]["metrics"]}
    assert labels == {"mean |error| (V)"}
    curve_metrics = next(row for row in definitions if "Curve/Device Metrics" in row["name"])
    assert {"physical_device_key", "mae_a", "normalized_rmse"} <= set(curve_metrics["params"]["all_columns"])


def test_dashboard_keeps_certified_readiness_without_empty_release_tabs():
    definitions = complete.dashboard.definitions()
    readiness = next(row for row in definitions if row["name"] == "V3 Activation — Claim Readiness")
    assert {
        "claim_type",
        "stress_type",
        "evidence_count",
        "model_count",
        "blocking_stage",
        "next_action",
    } <= set(readiness["params"]["all_columns"])

def test_research_curve_overlay_is_explicitly_in_amperes():
    definitions = complete.dashboard.definitions()
    learned = next(row for row in definitions if "Historical OOF Curve Explorer" in row["name"])
    labels = {metric["label"] for metric in learned["params"]["metrics"]}
    assert labels == {"drain current (A)"}
    assert learned["params"]["x_axis"] == "v_gate_v"
    assert {"series_name", "pair_key", "model_version"} <= set(learned["params"]["groupby"])


def test_reconciliation_preserves_legacy_charts_but_removes_dashboard_membership():
    class Response:
        ok = True
        status_code = 200

        def json(self):
            return {"result": {"dashboards": [{"id": 34}, {"id": 99}]}}

    class Session:
        def __init__(self):
            self.puts = []

        def get(self, _url):
            if "/dashboard/" in _url:
                return type("DashboardResponse", (), {"ok": True, "status_code": 200, "json": lambda self: {"result": [{"id": 505}, {"id": 506}]}})()
            return Response()

        def put(self, url, json):
            self.puts.append((url, json))
            return Response()

    session = Session()
    complete.dashboard.reconcile_chart_membership(session, 34, [505, 506])
    assert session.puts[0][1] == {"dashboards": [34]}
    legacy_updates = [
        payload for url, payload in session.puts
        if any(url.endswith(f"/{chart_id}") for chart_id in range(495, 505))
    ]
    assert len(legacy_updates) == 10
    assert all(payload == {"dashboards": [99]} for payload in legacy_updates)
