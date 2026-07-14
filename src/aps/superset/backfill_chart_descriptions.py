#!/usr/bin/env python3
"""Backfill empty descriptions for charts in explicitly selected dashboards.

This exists for retained live dashboards whose historical generator is no
longer present in the repository (currently the Baselines Device Library).
Dry-run is the default; apply mode updates only ``description`` and never chart
queries, datasets, associations, or dashboard layout.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from aps.paths import OUT_ROOT
from aps.superset.nonproxy_dashboard_support import chart_description
from aps.superset.reconcile_portfolio import _api_url, _dashboard_detail, _layout_chart_ids, _list
from aps.superset.superset_api import get_session


DEFAULT_DASHBOARD_IDS = (22,)
DEFAULT_DASHBOARD_TITLES = {22: "Baselines Device Library"}
CONFIRMATION = "BACKFILL_EMPTY_DESCRIPTIONS"
DEFAULT_REPORT = OUT_ROOT / "superset_chart_description_backfill.json"


def build_plan(session, dashboard_ids=DEFAULT_DASHBOARD_IDS):
    charts = {int(row["id"]): row for row in _list(session, "/api/v1/chart/")}
    planned = []
    for dashboard_id in dashboard_ids:
        detail = _dashboard_detail(session, int(dashboard_id))
        expected_title = DEFAULT_DASHBOARD_TITLES.get(int(dashboard_id))
        actual_title = detail.get("dashboard_title", "")
        if expected_title is not None and actual_title != expected_title:
            raise RuntimeError(
                f"refusing description backfill for dashboard {dashboard_id}: "
                f"expected {expected_title!r}, found {actual_title!r}"
            )
        for chart_id in sorted(_layout_chart_ids(detail.get("position_json", {}))):
            row = charts.get(chart_id)
            if row is None or (row.get("description") or "").strip():
                continue
            name = row.get("slice_name", f"chart {chart_id}")
            planned.append({
                "dashboard_id": int(dashboard_id),
                "chart_id": chart_id,
                "chart_name": name,
                "description": chart_description(name),
            })
    return planned


def apply_plan(session, plan):
    results = []
    for item in plan:
        response = session.put(
            _api_url(session, f"/api/v1/chart/{item['chart_id']}"),
            json={"description": item["description"]},
        )
        if not response.ok:
            raise RuntimeError(
                f"description update failed for chart {item['chart_id']}: "
                f"{response.status_code} {response.text[:200]}"
            )
        results.append({"chart_id": item["chart_id"], "status": "updated"})
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dashboard-id", type=int, action="append", dest="dashboard_ids")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm", default="")
    args = parser.parse_args()
    dashboard_ids = tuple(args.dashboard_ids or DEFAULT_DASHBOARD_IDS)
    session = get_session()
    plan = build_plan(session, dashboard_ids)
    payload = {"mode": "apply" if args.apply else "dry-run",
               "dashboard_ids": list(dashboard_ids), "planned": plan}
    if args.apply:
        if args.confirm != CONFIRMATION:
            raise SystemExit(f"--apply requires --confirm {CONFIRMATION}")
        payload["applied"] = apply_plan(session, plan)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {args.report}: mode={payload['mode']} planned={len(plan)}")


if __name__ == "__main__":
    main()
