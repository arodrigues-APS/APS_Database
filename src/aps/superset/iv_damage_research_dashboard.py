"""Superset chart definitions for the retrospective V3 research lane."""

from __future__ import annotations


def _metric(label: str, expression: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def _table(columns: list[str]) -> dict:
    return {
        "query_mode": "raw",
        "all_columns": columns,
        "adhoc_filters": [],
        "row_limit": 5000,
        "include_search": True,
        "table_timestamp_format": "smart_date",
    }


def _bar(x_axis: str, metrics: list[dict], groupby: list[str]) -> dict:
    return {
        "x_axis": x_axis,
        "metrics": metrics,
        "groupby": groupby,
        "adhoc_filters": [],
        "row_limit": 10000,
        "show_legend": True,
        "rich_tooltip": True,
        "stack": False,
        "x_axis_sort_asc": True,
        "y_axis_format": ".3g",
    }


def _line(x_axis: str, metrics: list[dict], groupby: list[str]) -> dict:
    return {
        "x_axis": x_axis,
        "metrics": metrics,
        "groupby": groupby,
        "adhoc_filters": [],
        "row_limit": 50000,
        "show_legend": True,
        "rich_tooltip": True,
        "x_axis_sort_asc": True,
        "y_axis_format": ".4g",
        "markerEnabled": False,
    }


def definitions() -> list[dict]:
    return [
        dict(
            name="V3 Research — Snapshot Status (SCREENING/RESEARCH ONLY)",
            ds="research_status",
            tab="research_limits",
            viz="table",
            width=12,
            height=34,
            params=_table(
                [
                    "snapshot_version",
                    "research_protocol_id",
                    "target_current_a",
                    "claim_class",
                    "horizon_status",
                    "pair_count",
                    "device_count",
                    "campaign_count",
                    "run_count",
                    "model_runs",
                    "preferred_models",
                    "decision_eligible",
                ]
            ),
        ),
        dict(
            name="V3 Research — Cohort and Missingness",
            ds="research_cohort",
            tab="research_limits",
            viz="table",
            width=12,
            height=40,
            params=_table(
                [
                    "snapshot_version",
                    "pair_key",
                    "physical_device_key",
                    "device_type",
                    "campaign_key",
                    "run_key",
                    "ion_species",
                    "fluence_missing",
                    "observed_delta_vth_v",
                    "admission_status",
                    "horizon_status",
                    "decision_eligible",
                ]
            ),
        ),
        dict(
            name="V3 Research — Scalar OOF Error (SCREENING ONLY)",
            ds="research_scalar",
            tab="research_scalar",
            viz="echarts_timeseries_bar",
            width=6,
            height=44,
            params=_bar(
                "validation_scheme",
                [_metric("mean |error| (V)", "AVG(absolute_error_v)")],
                ["method", "support_status"],
            ),
        ),
        dict(
            name="V3 Research — Observed vs Historical OOF ΔVth",
            ds="research_scalar",
            tab="research_scalar",
            viz="table",
            width=6,
            height=44,
            params=_table(
                [
                    "model_version",
                    "method",
                    "validation_scheme",
                    "fold_number",
                    "held_out_group_key",
                    "pair_key",
                    "physical_device_key",
                    "observed_delta_vth_v",
                    "predicted_delta_vth_v",
                    "absolute_error_v",
                    "support_status",
                    "prediction_context",
                    "decision_eligible",
                ]
            ),
        ),
        dict(
            name="V3 Research — Historical OOF Curve Explorer (POST = TRUTH ONLY)",
            ds="research_curve",
            tab="research_curve",
            viz="echarts_timeseries_line",
            width=12,
            height=62,
            params=_line(
                "v_gate_v",
                [_metric("drain current (A)", "MAX(i_drain_a)")],
                [
                    "series_name",
                    "pair_key",
                    "model_version",
                    "validation_scheme",
                    "fold_number",
                ],
            ),
        ),
        dict(
            name="V3 Research — Curve/Device Metrics",
            ds="research_curve_metrics",
            tab="research_curve",
            viz="table",
            width=12,
            height=36,
            params=_table(
                [
                    "model_version",
                    "method",
                    "validation_scheme",
                    "fold_number",
                    "held_out_group_key",
                    "pair_key",
                    "physical_device_key",
                    "mae_a",
                    "max_abs_error_a",
                    "normalized_rmse",
                    "transformed_mae",
                    "supported_voltage_fraction",
                    "correction_applied",
                    "fallback_reason",
                    "decision_eligible",
                ]
            ),
        ),
        dict(
            name="V3 Research — Residual Correction Diagnostics",
            ds="research_residual",
            tab="research_residual",
            viz="table",
            width=12,
            height=46,
            params=_table(
                [
                    "snapshot_version",
                    "model_version",
                    "method",
                    "validation_scheme",
                    "held_out_group_key",
                    "pair_key",
                    "physical_device_key",
                    "pca_explained_variance",
                    "correction_norm",
                    "correction_applied",
                    "fallback_reason",
                    "support_status",
                ]
            ),
        ),
        dict(
            name="V3 Research — Generalization and Known Limitations",
            ds="research_limitations",
            tab="research_limits",
            viz="table",
            width=12,
            height=48,
            params=_table(
                [
                    "snapshot_version",
                    "limitation_key",
                    "limitation_value",
                    "claim_class",
                    "horizon_status",
                    "decision_eligible",
                ]
            ),
        ),
    ]
