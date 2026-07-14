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
