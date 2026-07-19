from pathlib import Path

from aps.superset.create_iv_damage_prediction_dashboard import (
    DATASETS,
    DATASET_COLUMNS,
    DESCRIPTIONS,
    chart_definitions,
    native_filters,
)


ROOT = Path(__file__).resolve().parents[1]


def test_dashboard_uses_only_v3_governed_views():
    assert DATASETS["eligible"] == "iv_damage_decision_eligible_prediction_view"
    assert all("iv_physical" not in view for view in DATASETS.values())
    assert DATASETS["backlog"] == "iv_damage_prediction_backlog_view"


def test_every_chart_has_scientific_description_and_known_columns():
    ids = {key: index + 10 for index, key in enumerate(DATASETS)}
    definitions = chart_definitions(ids)
    assert definitions
    assert {row["name"] for row in definitions} == set(DESCRIPTIONS)
    assert all(DESCRIPTIONS[row["name"]].strip() for row in definitions)
    for row in definitions:
        known = DATASET_COLUMNS[row["ds"]]
        params = row["params"]
        for column in params.get("all_columns", []):
            assert column in known
        for column in params.get("groupby", []):
            assert column in known
        if params.get("x_axis"):
            assert params["x_axis"] in known


def test_decision_table_is_bound_to_canonical_eligible_dataset():
    definitions = chart_definitions({key: index for index, key in enumerate(DATASETS)})
    decision = next(row for row in definitions if "Decision-Eligible" in row["name"])
    assert decision["ds"] == "eligible"
    assert decision["params"].get("adhoc_filters", []) == []


def test_filter_targets_exist_on_targeted_datasets():
    ids = {key: index + 100 for index, key in enumerate(DATASETS)}
    reverse = {value: key for key, value in ids.items()}
    catalog = [
        {**row, "chart_id": index + 1}
        for index, row in enumerate(chart_definitions(ids))
    ]
    for dashboard_filter in native_filters(catalog, ids):
        for target in dashboard_filter["targets"]:
            dataset = reverse[target["datasetId"]]
            assert target["column"]["name"] in DATASET_COLUMNS[dataset]


def test_validation_charts_do_not_combine_subgroup_quantiles_or_coverage():
    definitions = chart_definitions({key: index for index, key in enumerate(DATASETS)})
    error = next(row for row in definitions if row["name"].endswith("Validation Error"))
    coverage = next(row for row in definitions if row["name"].endswith("Interval Coverage"))
    assert {"model_version", "stress_type", "device_type", "ion_species", "support_status"} <= set(error["params"]["groupby"])
    assert {"model_version", "stress_type", "device_type", "ion_species", "support_status"} <= set(coverage["params"]["groupby"])
    assert all("AVG(interval_coverage)" not in metric["sqlExpression"] for metric in coverage["params"]["metrics"])


def test_downstream_migration_consumes_canonical_view_without_destructive_sql():
    sql = (ROOT / "schema" / "033_iv_damage_downstream.sql").read_text()
    lowered = sql.lower()
    assert "from iv_damage_decision_eligible_prediction_view" in lowered
    assert "iv_physical_parameter_predictions" not in lowered
    statements = [part.strip() for part in lowered.split(";") if part.strip()]
    assert not any(statement.startswith("drop ") for statement in statements)
    assert not any(statement.startswith("truncate ") for statement in statements)
