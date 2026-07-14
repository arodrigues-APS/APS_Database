import json

from aps.superset.reconcile_portfolio import (
    DEFAULT_ARCHIVE_TARGETS,
    _layout_chart_ids,
    apply_archive_plan,
    build_report,
)


class Response:
    def __init__(self, payload=None, ok=True):
        self._payload = payload or {}
        self.ok = ok
        self.status_code = 200 if ok else 500
        self.text = ""

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError("request failed")


class Session:
    def __init__(self):
        self.puts = []

    def get(self, endpoint, params=None):
        page = json.loads(params["q"]).get("page", 0) if params else 0
        if page > 0 and endpoint in {"/api/v1/dashboard/", "/api/v1/chart/"}:
            return Response({"result": []})
        if endpoint == "/api/v1/dashboard/":
            return Response({"result": [{"id": 14}, {"id": 20}]})
        if endpoint == "/api/v1/chart/":
            return Response({"result": [
                {"id": 1, "slice_name": "visible", "dashboards": [{"id": 20}]},
                {"id": 2, "slice_name": "hidden", "dashboards": [{"id": 20}]},
                {"id": 3, "slice_name": "orphan", "dashboards": []},
            ]})
        if endpoint == "/api/v1/dashboard/14":
            return Response({"result": {"dashboard_title": "Mosfets", "published": True,
                              "position_json": json.dumps({})}})
        if endpoint == "/api/v1/dashboard/20":
            return Response({"result": {"dashboard_title": "Current", "published": True,
                              "position_json": json.dumps({"C": {"meta": {"chartId": 1}}})}})
        raise AssertionError(endpoint)

    def put(self, endpoint, json=None):
        self.puts.append((endpoint, json))
        return Response()


def test_layout_chart_ids_accepts_serialized_layout_and_bad_values():
    layout = {"a": {"meta": {"chartId": 7}}, "b": {"meta": {"chartId": "8"}},
              "c": {"meta": {"chartId": "bad"}}}
    assert _layout_chart_ids(json.dumps(layout)) == {7, 8}


def test_report_distinguishes_hidden_attachment_and_true_orphan():
    report = build_report(Session(), archive_ids=(14,))
    assert report["orphan_chart_ids"] == [3]
    assert report["hidden_attachments"] == [{"chart_id": 2, "dashboard_ids": [20]}]
    by_id = {row["id"]: row for row in report["dashboards"]}
    assert by_id[14]["planned_action"] == "unpublish"
    assert by_id[20]["planned_action"] == "retain"


def test_apply_only_unpublishes_and_never_deletes_or_detaches():
    session = Session()
    report = build_report(session, archive_ids=(14,))
    actions = apply_archive_plan(session, report)
    assert actions == [{"dashboard_id": 14, "action": "unpublished"}]
    assert session.puts == [("/api/v1/dashboard/14", {"published": False})]


def test_apply_refuses_if_a_default_id_now_names_another_dashboard():
    session = Session()
    report = build_report(session, archive_ids=(14,))
    report["dashboards"][0]["title"] = "Unrelated dashboard"
    report["dashboards"][0]["archive_title_matches"] = False
    report["dashboards"][0]["planned_action"] = "blocked_title_mismatch"

    try:
        apply_archive_plan(session, report)
    except RuntimeError as exc:
        assert "changed identity" in str(exc)
        assert DEFAULT_ARCHIVE_TARGETS[14] in str(exc)
    else:
        raise AssertionError("a dashboard title mismatch must block every mutation")
    assert session.puts == []


def test_disabled_legacy_cv_dpt_dashboard_is_in_default_archive_targets():
    assert DEFAULT_ARCHIVE_TARGETS[33] == "CV & Double-Pulse Characterization"
