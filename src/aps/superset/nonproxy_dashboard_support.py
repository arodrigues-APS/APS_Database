"""Shared presentation contracts for the non-Proxy Superset dashboards.

The dashboard generators historically created charts with empty Superset
descriptions and placed every chart in its own full-width row.  This module
keeps the scientific generators small while enforcing two user-facing rules:

* every chart receives a non-empty description which states its evidence
  level and denominator; and
* tabbed dashboards begin with a short interpretation panel and honour the
  requested chart widths (two six-column charts therefore share a row).

Proxy Readiness deliberately does not import this module.  Its layout and
methodology text are maintained by its own generator.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence


DASHBOARD_GUIDANCE = {
    "baselines": (
        "### Baseline raw-measurement explorer\n\n"
        "Use this dashboard to inspect acquisition coverage, raw curve shape, "
        "and instrument settings. Counts distinguish physical files, sweep "
        "instances, and sampled points. This is a forensic/raw-data view; use "
        "the Baselines Device Library for authoritative device-level parameter "
        "comparisons. Lines do not bridge missing sweep regions."
    ),
    "avalanche": (
        "### How to read this dashboard\n\n"
        "Overview counts are **captures**, not independent physical devices. "
        "Pre/post plots are curve evidence and do not by themselves establish "
        "a paired parameter shift. In the waveform tab select one capture; "
        "instantaneous power is $|V_{DS}I_D|$ and the stored energy field is the "
        "ingestion energy estimate documented in the shot table."
    ),
    "irradiation": (
        "### How to read this dashboard\n\n"
        "Coverage is reported in devices/files rather than sampled points. "
        "Single-event rates are normalized by observed fluence where fluence is "
        "available; missing exposure remains missing rather than being treated "
        "as zero. Curve overlays are contextual. Extracted damage fingerprints "
        "state their reference basis, IQR, and supporting sample count."
    ),
    "short-circuit": (
        "### How to read this dashboard\n\n"
        "Curve overlays show acquisition context. The Damage Metrics tab is the "
        "decision layer: each row joins post-SC extracted parameters to the same "
        "sample group's pristine median and reports both support counts. Select "
        "one waveform file before interpreting voltage, current, or power."
    ),
    "post-iv-physical-prediction": (
        "### Validation-first workflow\n\n"
        "Read **Readiness & Gates** before generated outputs. Predictions are "
        "exploratory unless the intended stress/target gate passes and support "
        "is `ok`. Validation plots keep ΔVth (V) separate from log RDS(on) ratio "
        "so unlike units never share an axis. Generated curves are limited to a "
        "selected pair and retain model, support, and confidence provenance."
    ),
}


_EXACT_DESCRIPTIONS = {
    "Baselines – Available Data": (
        "Acquisition inventory grouped by experiment, device, and measurement "
        "category. Files count distinct metadata records; sweep instances count "
        "distinct file/step combinations; data points count stored samples."
    ),
    "SC – Paired Damage Metrics": (
        "One row per physical sample group and SC condition. Deltas are the "
        "post-SC median minus that same sample group's pristine median. Vth is "
        "in V, RDS(on) in mΩ, and BVDSS in V; pre/post support counts are shown."
    ),
    "SC – Damage vs Pulse Duration": (
        "Same-sample extracted damage versus SC pulse duration. Each point is a "
        "sample/condition, not a curve point; select a metric to keep units "
        "comparable and inspect support counts in the paired table."
    ),
    "Irrad – SE Event Rate per 1e5 Fluence vs LET": (
        "Detected SEB/SELC events divided by summed observed fluence and scaled "
        "to 100,000 fluence units. Rows without a measured fluence denominator "
        "remain null and are excluded rather than treated as zero exposure."
    ),
    "Irrad – Extracted Damage Fingerprints": (
        "Extracted post-irradiation parameter deltas by device/run. The table "
        "shows median ΔVth (V), ΔRDS(on) (mΩ), ΔBVDSS (V), IQR, and N. "
        "The current fingerprint pipeline uses its documented pristine reference "
        "pool and is not necessarily a same-device pair."
    ),
    "Comparable Device Coverage": (
        "Evidence gate for measured equivalence. Counts are stress fingerprints "
        "and comparable pairs, not physical-device success rates. Axis labels "
        "state which extracted damage dimensions are jointly available."
    ),
    "Nearest Equivalents by Pair": (
        "Top-three measured candidates per target and comparison pair. Distance "
        "is computed only over the listed comparable axes; IQR, sample counts, "
        "sign mismatches, and candidate-pool size must be read with the rank."
    ),
    "Predicted Damage Coverage": (
        "Validation-gated coverage for predicted irradiation fingerprints. "
        "Counts separate predictions, eligible matches, and strong/usable/weak "
        "comparability; the latest-model filter is enabled by default."
    ),
    "Predicted Validation Interval Coverage": (
        "Observed fraction of evaluable held-out responses inside the predicted "
        "interval. The denominator is interval_evaluable_count; this is an "
        "empirical validation statistic, not a posterior probability."
    ),
    "Predicted Unsupported Reasons (Pareto)": (
        "Unsupported prediction or validation records grouped by reason. Bar "
        "height is n_records from the support-reason view; use it to prioritize "
        "data/model blockers before interpreting equivalence ranks."
    ),
    "IV Physical Prediction - Model Gate Summary": (
        "Authoritative readiness table for each model/validation/reference/stress "
        "target. Generated output is exploratory unless target_gate_status and "
        "gate_pass permit it and supported validation-pair counts meet the gate."
    ),
    "IV Physical Prediction - Latest Residual Gates": (
        "Median and P90 absolute held-out residuals plotted beside their configured "
        "limits for the latest validated model. Target selection determines the "
        "unit: ΔVth is V; log RDS(on) ratio is dimensionless."
    ),
    "IV Physical Prediction - Support Status": (
        "Validation-pair support counts by physical target and support status. "
        "The denominator is n_validation_pairs, not raw curve points."
    ),
}


def chart_description(name: str) -> str:
    """Return a concise, non-empty scientific description for *name*.

    Exact decision-critical descriptions live above.  The fallbacks cover the
    repeated curve/waveform/table vocabulary and intentionally avoid claiming
    paired damage, calibrated uncertainty, or physical independence when the
    underlying chart does not establish those properties.
    """

    if name in _EXACT_DESCRIPTIONS:
        return _EXACT_DESCRIPTIONS[name]

    lower = name.lower()
    if "waveform" in lower and "power" in lower:
        return (
            "Time-resolved instantaneous terminal power |VDS·ID| in W for the "
            "selected capture/file. It is a pointwise diagnostic, not cumulative "
            "energy; inspect capture provenance and sampling before integration."
        )
    if "waveform" in lower:
        return (
            "Time-resolved oscilloscope or monitoring trace. The x-axis is the "
            "stored time basis and the y-axis unit is stated in the title. Select "
            "one file/capture to avoid averaging unrelated transients."
        )
    if "coverage" in lower or "summary" in lower or "available data" in lower:
        return (
            "Evidence/coverage summary. Counts use the labeled denominator (for "
            "example devices, files, runs, fingerprints, events, or sampled "
            "points); they must not be interchanged when judging support."
        )
    if "individual" in lower or "detail" in lower or "raw" in lower:
        return (
            "Forensic detail view retaining per-file/per-run provenance. It is "
            "intended for QA and drill-through; repeated rows or points are not "
            "independent physical-device observations."
        )
    if "curve" in lower or "characteristic" in lower or "idvg" in lower or "idvd" in lower:
        return (
            "Condition-grouped electrical curve overlay. Axis titles carry the "
            "physical units; curves show shape/context and should not be read as "
            "a paired parameter shift unless a separate extracted-delta view says so."
        )
    if "residual" in lower or "observed vs predicted" in lower:
        return (
            "Held-out validation diagnostic for supported pairs in the latest "
            "validated model. Interpret residual magnitude with the selected "
            "target's units, gate thresholds, donor support, and validation mode."
        )
    if "fingerprint" in lower or "damage:" in lower or "equivalent" in lower:
        return (
            "Damage-signature comparison using extracted parameter deltas. Values "
            "are medians; uncertainty/support are represented by companion IQR and "
            "N fields, and distance is meaningful only over comparable axes."
        )
    if "prediction" in lower or "predicted" in lower:
        return (
            "Model-derived output with validation, support, reference-tier, and "
            "model-run provenance. It is screening evidence unless the associated "
            "validation gate passes."
        )
    return (
        "Dashboard diagnostic using the units and denominator stated by its axes "
        "or columns. Use the active filters and companion coverage/QA views when "
        "judging whether the displayed evidence is sufficient."
    )


def create_documented_chart(create_chart_fn, session, name, datasource_id,
                            viz_type, params):
    """Call the shared chart helper with the non-Proxy description contract."""

    return create_chart_fn(
        session,
        name,
        datasource_id,
        viz_type,
        params,
        description=chart_description(name),
    )


def _pack_rows(items: Sequence[tuple], max_width: int = 12) -> list[list[tuple]]:
    """Pack chart tuples into rows without reordering them."""

    rows: list[list[tuple]] = []
    current: list[tuple] = []
    used = 0
    for item in items:
        width = max(1, min(max_width, int(item[3])))
        if current and used + width > max_width:
            rows.append(current)
            current = []
            used = 0
        current.append(item)
        used += width
        if used == max_width:
            rows.append(current)
            current = []
            used = 0
    if current:
        rows.append(current)
    return rows


def build_tabbed_layout(
    title: str,
    prefix: str,
    tab_defs: Iterable[tuple[str, str, Sequence[tuple]]],
    guidance_by_tab: Mapping[str, str] | None = None,
):
    """Build a Superset tab layout with guidance and width-aware rows.

    ``chart_list`` entries are ``(id, uuid, name, width, height)``.  Empty or
    failed charts are omitted. ``guidance_by_tab`` is keyed by tab id; ``*`` is
    used as a fallback.
    """

    tab_defs = list(tab_defs)
    guidance_by_tab = guidance_by_tab or {}
    tabs_id = f"TABS-{prefix}"
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [tabs_id],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
        tabs_id: {
            "type": "TABS",
            "id": tabs_id,
            "children": [tab_id for _, tab_id, _ in tab_defs],
            "parents": ["ROOT_ID", "GRID_ID"],
        },
    }

    node_no = 0
    row_no = 0
    for tab_name, tab_id, chart_list in tab_defs:
        tab_parents = ["ROOT_ID", "GRID_ID", tabs_id, tab_id]
        row_ids: list[str] = []
        guidance = guidance_by_tab.get(tab_id, guidance_by_tab.get("*"))
        if guidance:
            row_no += 1
            node_no += 1
            row_id = f"ROW-{prefix}-{row_no}"
            node_id = f"MARKDOWN-{prefix}-{node_no}"
            layout[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [node_id],
                "parents": tab_parents,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            layout[node_id] = {
                "type": "MARKDOWN",
                "id": node_id,
                "children": [],
                "parents": tab_parents + [row_id],
                "meta": {"width": 12, "height": 7, "code": guidance},
            }
            row_ids.append(row_id)

        visible = [item for item in chart_list if item[0] is not None]
        for packed in _pack_rows(visible):
            row_no += 1
            row_id = f"ROW-{prefix}-{row_no}"
            child_ids: list[str] = []
            layout[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": child_ids,
                "parents": tab_parents,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            for cid, cuuid, cname, width, height in packed:
                node_no += 1
                node_id = f"CHART-{prefix}-{node_no}"
                child_ids.append(node_id)
                layout[node_id] = {
                    "type": "CHART",
                    "id": node_id,
                    "children": [],
                    "parents": tab_parents + [row_id],
                    "meta": {
                        "chartId": cid,
                        "width": max(1, min(12, int(width))),
                        "height": height,
                        "sliceName": cname,
                        "uuid": cuuid,
                    },
                }
            row_ids.append(row_id)

        layout[tab_id] = {
            "type": "TAB",
            "id": tab_id,
            "children": row_ids,
            "parents": ["ROOT_ID", "GRID_ID", tabs_id],
            "meta": {"text": tab_name},
        }
    return layout
