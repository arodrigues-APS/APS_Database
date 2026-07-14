import json

from aps.superset.backfill_chart_descriptions import apply_plan, build_plan
from tests.test_superset_portfolio_reconciliation import Response


class Session:
    def __init__(self):
        self.puts = []

    def get(self, endpoint, params=None):
        if endpoint == "/api/v1/chart/":
            page = json.loads(params["q"])["page"]
            if page:
                return Response({"result": []})
            return Response({"result": [
                {"id": 1, "slice_name": "Device Library – Data Summary", "description": ""},
                {"id": 2, "slice_name": "documented", "description": "keep me"},
            ]})
        if endpoint == "/api/v1/dashboard/22":
            return Response({"result": {
                "dashboard_title": "Baselines Device Library",
                "position_json": json.dumps({
                "one": {"meta": {"chartId": 1}},
                "two": {"meta": {"chartId": 2}},
                }),
            }})
        raise AssertionError(endpoint)

    def put(self, endpoint, json=None):
        self.puts.append((endpoint, json))
        return Response()


def test_plan_updates_only_empty_descriptions_in_selected_layout():
    plan = build_plan(Session(), (22,))
    assert [item["chart_id"] for item in plan] == [1]
    assert "coverage" in plan[0]["description"].lower()


def test_apply_changes_description_only():
    session = Session()
    plan = build_plan(session, (22,))
    assert apply_plan(session, plan) == [{"chart_id": 1, "status": "updated"}]
    endpoint, payload = session.puts[0]
    assert endpoint == "/api/v1/chart/1"
    assert list(payload) == ["description"]


def test_plan_refuses_if_the_default_dashboard_id_changed_identity():
    session = Session()
    original_get = session.get

    def mismatched_get(endpoint, params=None):
        if endpoint == "/api/v1/dashboard/22":
            return Response({
                "result": {
                    "dashboard_title": "Unrelated dashboard",
                    "position_json": "{}",
                }
            })
        return original_get(endpoint, params=params)

    session.get = mismatched_get
    try:
        build_plan(session, (22,))
    except RuntimeError as exc:
        assert "expected 'Baselines Device Library'" in str(exc)
    else:
        raise AssertionError("a dashboard title mismatch must block backfill")
