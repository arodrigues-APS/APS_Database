from aps.superset.create_proxy_readiness_dashboard import (
    DATASET_PROVENANCE,
    DATASET_TABLES,
    FILTER_TARGET_COLUMNS,
    build_chart_catalog,
    build_chart_defs,
    build_native_filters,
)


def _dashboard_contract():
    dataset_ids = {
        name: index + 1 for index, name in enumerate(DATASET_TABLES)
    }
    definitions = build_chart_defs(dataset_ids)
    chart_ids = list(range(1000, 1000 + len(definitions)))
    catalog = build_chart_catalog(definitions, chart_ids)
    filters = build_native_filters(chart_ids, dataset_ids, catalog)
    return dataset_ids, definitions, chart_ids, catalog, filters


def test_every_proxy_dataset_has_an_evidence_provenance_contract():
    assert set(DATASET_PROVENANCE) == set(DATASET_TABLES)
    assert set(FILTER_TARGET_COLUMNS) <= set(DATASET_TABLES)
    assert all(unit.strip() and evidence.strip()
               for unit, evidence in DATASET_PROVENANCE.values())


def test_proxy_filter_targets_and_scopes_match_the_deployed_catalog():
    dataset_ids, definitions, chart_ids, catalog, filters = _dashboard_contract()
    dataset_key_by_id = {
        dataset_id: key for key, dataset_id in dataset_ids.items()
    }

    assert len(catalog) == len(definitions) == len(chart_ids)
    assert {row["chart_id"] for row in catalog} == set(chart_ids)
    for native_filter in filters:
        scope = set(native_filter["chartsInScope"])
        assert scope
        assert scope <= set(chart_ids)
        for target in native_filter["targets"]:
            dataset_key = dataset_key_by_id[target["datasetId"]]
            column = target["column"]["name"]
            assert column in FILTER_TARGET_COLUMNS[dataset_key]


def test_native_filter_count_stays_small():
    """2026-07-22 redesign: 19 filters (a private Event/Source/Scope/Claim
    quadruplet per v1/v2/v3/Review tab) collapsed to 8 shared ones. Cap at 10
    so a future per-tab filter can't silently re-accumulate back toward 19."""
    _dataset_ids, _definitions, _chart_ids, _catalog, filters = (
        _dashboard_contract()
    )
    assert len(filters) <= 10


# Forensic/export tables and top-N candidate pools are exempt from the
# decision-table column budget below: they exist for drill-through and CSV
# export, not on-screen decision reading. destruction_boundary_cols and
# candidate_boundary_cols(Candidate Destruction Boundary Data Gaps) predate
# the 2026-07-22 column-shortlist pass and are grandfathered rather than
# newly trimmed; revisit if they grow further.
WIDE_TABLE_ALLOWLIST = {
    "Proxy Readiness - Candidate Evidence Detail",
    "Proxy Readiness - Stress Test Context",
    "Proxy Readiness - Event Feature Coverage",
    "Proxy Readiness - Irradiation Energy Chain Detail",
    "Proxy Readiness - v2 Candidate Pool (Top 10)",
    "Proxy Readiness - v3 Candidate Pool (Top 10)",
    "Proxy Readiness - Decision-Safe Curation Queue",
    "Proxy Readiness - Candidate Destruction Boundary Data Gaps",
}
DECISION_TABLE_COLUMN_BUDGET = 12


def test_decision_tables_stay_within_a_column_budget():
    """2026-07-22 redesign: the pre-redesign dashboard put a 110-column and a
    91-column raw dump directly on the decision surface. Named shortlists
    replaced them; this caps every non-exempt table so that regression can't
    silently return."""
    _dataset_ids, definitions, _chart_ids, _catalog, _filters = (
        _dashboard_contract()
    )
    for name, _ds_id, viz_type, params, _w, _h, _tab, _group in definitions:
        if viz_type != "table" or name in WIDE_TABLE_ALLOWLIST:
            continue
        columns = params.get("all_columns") or []
        assert len(columns) <= DECISION_TABLE_COLUMN_BUDGET, (
            f"{name!r} has {len(columns)} columns "
            f"(budget {DECISION_TABLE_COLUMN_BUDGET}); trim or add to "
            "WIDE_TABLE_ALLOWLIST with a reason"
        )
