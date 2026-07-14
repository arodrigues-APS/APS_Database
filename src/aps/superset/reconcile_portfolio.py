#!/usr/bin/env python3
"""Inventory and safely reconcile the APS Superset dashboard portfolio.

Dry-run is the default.  Applying the plan only unpublishes explicitly listed
legacy dashboards; it never deletes charts or datasets and never detaches a
chart that may be shared by another dashboard.  The JSON report records layout
membership, API associations, hidden attachments, and true orphan charts.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aps.paths import OUT_ROOT
from aps.superset.superset_api import get_session


DEFAULT_ARCHIVE_TARGETS = {
    14: "Mosfets",
    16: "Mosfets DPT ONLY",
    28: "Post-IV Prediction V1 - LEGACY Curve Model",
    33: "CV & Double-Pulse Characterization",
}
DEFAULT_ARCHIVE_IDS = tuple(DEFAULT_ARCHIVE_TARGETS)
CONFIRMATION = "ARCHIVE_LEGACY_DASHBOARDS"
DEFAULT_REPORT = OUT_ROOT / "superset_portfolio_reconciliation.json"


def _items(response) -> list[dict[str, Any]]:
    response.raise_for_status()
    payload = response.json()
    result = payload.get("result", payload)
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        for key in ("data", "result"):
            if isinstance(result.get(key), list):
                return result[key]
    return []


def _api_url(session, path: str) -> str:
    """Use absolute URLs for requests.Session; keep test doubles lightweight."""
    base = getattr(session, "_superset_url", "")
    return f"{base.rstrip('/')}{path}" if base else path


def _list(session, endpoint: str) -> list[dict[str, Any]]:
    # Superset commonly caps page_size at 100 even when a larger value is
    # requested. Explicit pagination avoids silently auditing only the first
    # 100 charts in a larger portfolio.
    result: list[dict[str, Any]] = []
    page_size = 100
    for page in range(10_000):
        response = session.get(
            _api_url(session, endpoint),
            params={"q": json.dumps({"page": page, "page_size": page_size})},
        )
        batch = _items(response)
        result.extend(batch)
        if len(batch) < page_size:
            return result
    raise RuntimeError(f"pagination safety limit reached for {endpoint}")


def _dashboard_detail(session, dashboard_id: int) -> dict[str, Any]:
    response = session.get(_api_url(session, f"/api/v1/dashboard/{dashboard_id}"))
    response.raise_for_status()
    return response.json().get("result", {})


def _layout_chart_ids(position_json: object) -> set[int]:
    if isinstance(position_json, str):
        try:
            position_json = json.loads(position_json)
        except json.JSONDecodeError:
            return set()
    found: set[int] = set()
    if isinstance(position_json, dict):
        for value in position_json.values():
            if isinstance(value, dict):
                meta = value.get("meta", {})
                chart_id = meta.get("chartId") if isinstance(meta, dict) else None
                if chart_id is not None:
                    try:
                        found.add(int(chart_id))
                    except (TypeError, ValueError):
                        pass
    return found


def _association_ids(chart: dict[str, Any]) -> set[int]:
    result: set[int] = set()
    for value in chart.get("dashboards") or []:
        raw = value.get("id") if isinstance(value, dict) else value
        try:
            result.add(int(raw))
        except (TypeError, ValueError):
            pass
    return result


def build_report(session, archive_ids=DEFAULT_ARCHIVE_IDS) -> dict[str, Any]:
    dashboards = _list(session, "/api/v1/dashboard/")
    charts = _list(session, "/api/v1/chart/")
    dashboard_rows = []
    layout_by_dashboard: dict[int, set[int]] = {}
    for dashboard in dashboards:
        dashboard_id = int(dashboard["id"])
        detail = _dashboard_detail(session, dashboard_id)
        layout_ids = _layout_chart_ids(detail.get("position_json", {}))
        layout_by_dashboard[dashboard_id] = layout_ids
        title = detail.get(
            "dashboard_title", dashboard.get("dashboard_title", "")
        )
        expected_title = DEFAULT_ARCHIVE_TARGETS.get(dashboard_id)
        title_matches = expected_title is None or title == expected_title
        if dashboard_id not in set(archive_ids):
            planned_action = "retain"
        elif title_matches:
            planned_action = "unpublish"
        else:
            planned_action = "blocked_title_mismatch"
        dashboard_rows.append({
            "id": dashboard_id,
            "title": title,
            "slug": detail.get("slug", dashboard.get("slug", "")),
            "published": bool(detail.get("published", dashboard.get("published", False))),
            "layout_chart_ids": sorted(layout_ids),
            "layout_chart_count": len(layout_ids),
            "expected_archive_title": expected_title,
            "archive_title_matches": title_matches,
            "planned_action": planned_action,
        })

    known_dashboard_ids = set(layout_by_dashboard)
    chart_rows = []
    orphan_ids = []
    hidden_attachments = []
    for chart in charts:
        chart_id = int(chart["id"])
        associated = _association_ids(chart)
        visible = {dash_id for dash_id, ids in layout_by_dashboard.items() if chart_id in ids}
        hidden = sorted(associated - visible)
        if not associated and not visible:
            orphan_ids.append(chart_id)
        if hidden:
            hidden_attachments.append({"chart_id": chart_id, "dashboard_ids": hidden})
        chart_rows.append({
            "id": chart_id,
            "name": chart.get("slice_name", ""),
            "associated_dashboard_ids": sorted(associated),
            "visible_dashboard_ids": sorted(visible),
            "hidden_attachment_dashboard_ids": hidden,
            "unknown_association_ids": sorted(associated - known_dashboard_ids),
            "orphan": not associated and not visible,
        })

    return {
        "contract_version": "aps-superset-portfolio-reconciliation-v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dry_run_default": True,
        "safety_policy": (
            "archive means published=false; default IDs must match expected titles; "
            "charts/datasets are never deleted or detached"
        ),
        "archive_dashboard_ids": list(archive_ids),
        "summary": {
            "dashboards": len(dashboard_rows),
            "charts": len(chart_rows),
            "visible_chart_placements": sum(len(ids) for ids in layout_by_dashboard.values()),
            "orphan_charts": len(orphan_ids),
            "hidden_attachments": len(hidden_attachments),
        },
        "dashboards": dashboard_rows,
        "charts": chart_rows,
        "orphan_chart_ids": sorted(orphan_ids),
        "hidden_attachments": hidden_attachments,
    }


def apply_archive_plan(session, report: dict[str, Any]) -> list[dict[str, Any]]:
    blocked = [
        dashboard
        for dashboard in report["dashboards"]
        if dashboard["planned_action"] == "blocked_title_mismatch"
    ]
    if blocked:
        details = ", ".join(
            f"{row['id']}={row['title']!r} (expected "
            f"{row['expected_archive_title']!r})"
            for row in blocked
        )
        raise RuntimeError(
            "refusing the archive plan because dashboard IDs changed identity: "
            + details
        )

    results = []
    for dashboard in report["dashboards"]:
        if dashboard["planned_action"] != "unpublish":
            continue
        response = session.put(
            _api_url(session, f"/api/v1/dashboard/{dashboard['id']}"),
            json={"published": False},
        )
        if not response.ok:
            raise RuntimeError(
                f"failed to unpublish dashboard {dashboard['id']}: "
                f"{response.status_code} {response.text[:200]}"
            )
        results.append({"dashboard_id": dashboard["id"], "action": "unpublished"})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--archive-id", type=int, action="append", dest="archive_ids")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", default="")
    args = parser.parse_args()
    archive_ids = tuple(args.archive_ids or DEFAULT_ARCHIVE_IDS)
    session = get_session()
    report = build_report(session, archive_ids)
    report["mode"] = "apply" if args.apply else "dry-run"
    if args.apply:
        if args.confirm != CONFIRMATION:
            raise SystemExit(f"--apply requires --confirm {CONFIRMATION}")
        report["applied_actions"] = apply_archive_plan(session, report)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    summary = report["summary"]
    print(f"Wrote {args.report} ({report['mode']})")
    print(
        f"dashboards={summary['dashboards']} charts={summary['charts']} "
        f"orphans={summary['orphan_charts']} hidden={summary['hidden_attachments']}"
    )


if __name__ == "__main__":
    main()
