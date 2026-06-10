"""Local artifact exports for Superset dashboard chart definitions.

The dashboard builders in this repository define their Superset charts in
Python dictionaries before sending them to the Superset REST API.  This module
uses those same dictionaries to write a local artifact for each chart:

* plot visualizations are rendered to PNG as static previews;
* table and scalar/data visualizations are exported to CSV.

The output is intentionally pragmatic and local.  It is not a full
reimplementation of Superset's browser query/render pipeline.
"""

from __future__ import annotations

import inspect
import json
import os
import re
import unicodedata
import warnings
from pathlib import Path

import pandas as pd

from db_config import get_connection


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_OUT_DIR = REPO_ROOT / "out" / "superset_charts"
EXPORT_ENV = "APS_DASHBOARD_EXPORT"
EXPORT_DIR_ENV = "APS_DASHBOARD_EXPORT_DIR"
STRICT_ENV = "APS_DASHBOARD_EXPORT_STRICT"
LEGACY_EXPORT_ENV = "APS_DASHBOARD_PNG_EXPORT"
LEGACY_EXPORT_DIR_ENV = "APS_DASHBOARD_PNG_EXPORT_DIR"
LEGACY_STRICT_ENV = "APS_DASHBOARD_PNG_STRICT"

DEFAULT_SERIES_LIMIT = 25
DEFAULT_CATEGORY_LIMIT = 60
PLOT_VIZ_TYPES = {
    "echarts_timeseries_bar",
    "echarts_timeseries_line",
    "echarts_timeseries_scatter",
}

_DATASETS: dict[int, tuple[str, str]] = {}


def register_dataset_for_png_export(
    dataset_id: int | None,
    table_name: str,
    schema: str = "public",
) -> None:
    """Remember which SQL view/table belongs to a Superset dataset id.

    The function name is kept for compatibility with the existing
    ``superset_api`` import; the exporter now writes CSV or PNG artifacts
    depending on chart type.
    """
    if dataset_id is None:
        return
    _DATASETS[int(dataset_id)] = (schema, table_name)


def export_chart_png(chart_name: str, datasource_id: int | None,
                     viz_type: str, params: dict) -> Path | None:
    """Export one Superset chart definition to a local file.

    The function name is kept for compatibility with the existing dashboard
    helper import.  Plot visualizations are still PNG files, while data-like
    charts are CSV files.

    Exports are enabled by default and can be disabled with
    ``APS_DASHBOARD_EXPORT=0``.  The legacy ``APS_DASHBOARD_PNG_EXPORT`` flag
    is also honored.  Failures are warnings by default so a local artifact
    export problem does not stop data ingestion; set
    ``APS_DASHBOARD_EXPORT_STRICT=1`` to make export failures fatal.
    """
    if not _enabled(EXPORT_ENV, default=True, aliases=(LEGACY_EXPORT_ENV,)):
        return None
    if datasource_id is None:
        return _handle_export_error(chart_name, "chart has no datasource id")

    dataset = _DATASETS.get(int(datasource_id))
    if not dataset:
        return _handle_export_error(
            chart_name,
            f"dataset id {datasource_id} was not registered for export",
        )

    out_dir = _output_dir() / _caller_dashboard_key()
    out_dir.mkdir(parents=True, exist_ok=True)
    is_plot = _is_plot_viz(viz_type)
    suffix = ".png" if is_plot else ".csv"
    path = out_dir / f"{_slugify(chart_name)}{suffix}"

    try:
        query_kind, df = _chart_dataframe(dataset, viz_type, params)
        if is_plot:
            _render_chart(df, chart_name, viz_type, params, path)
            _remove_stale_artifact(path.with_suffix(".csv"))
            print(f"  PNG plot export saved: {path}")
        else:
            _write_csv(_display_dataframe(df, query_kind, params), path)
            _remove_stale_artifact(path.with_suffix(".png"))
            print(f"  CSV data export saved: {path}")
    except Exception as exc:  # noqa: BLE001 - log and optionally continue.
        return _handle_export_error(chart_name, str(exc), exc)

    return path


def _enabled(env_name: str, default: bool, aliases: tuple[str, ...] = ()) -> bool:
    for name in (env_name, *aliases):
        raw = os.environ.get(name)
        if raw is not None:
            return raw.strip().lower() not in {"0", "false", "no", "off", ""}
    return default


def _output_dir() -> Path:
    override = os.environ.get(EXPORT_DIR_ENV) or os.environ.get(LEGACY_EXPORT_DIR_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return DEFAULT_OUT_DIR


def _handle_export_error(chart_name: str, message: str,
                         exc: Exception | None = None) -> None:
    text = f"  WARNING: chart artifact export failed for '{chart_name}': {message}"
    if _enabled(STRICT_ENV, default=False, aliases=(LEGACY_STRICT_ENV,)):
        if exc is not None:
            raise exc
        raise RuntimeError(text)
    print(text)
    return None


def _caller_dashboard_key() -> str:
    for frame in inspect.stack()[2:]:
        filename = Path(frame.filename).name
        match = re.fullmatch(r"create_(.+)_dashboard\.py", filename)
        if match:
            return _slugify(match.group(1).replace("_", "-"))
    return "charts"


def _slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value).strip("-").lower()
    return slug or "chart"


def _is_plot_viz(viz_type: str) -> bool:
    return viz_type in PLOT_VIZ_TYPES


def _chart_dataframe(dataset: tuple[str, str], viz_type: str,
                     params: dict) -> tuple[str, pd.DataFrame]:
    query_kind = _query_kind(viz_type, params)
    if query_kind == "big_number":
        df = _read_sql(_build_big_number_query(dataset, params))
    elif query_kind == "table":
        df = _read_sql(_build_table_query(dataset, params))
    else:
        df = _read_sql(_build_chart_query(dataset, params))
    return query_kind, df


def _render_chart(df: pd.DataFrame, chart_name: str, viz_type: str,
                  params: dict, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if viz_type == "echarts_timeseries_bar":
        _render_bar(plt, df, chart_name, params, path)
    elif viz_type == "echarts_timeseries_scatter":
        _render_xy(plt, df, chart_name, params, path, scatter=True)
    else:
        _render_xy(plt, df, chart_name, params, path, scatter=False)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def _remove_stale_artifact(path: Path) -> None:
    if path.exists():
        path.unlink()


def _display_dataframe(df: pd.DataFrame, query_kind: str,
                       params: dict) -> pd.DataFrame:
    rename_map = _display_column_map(query_kind, params)
    if not rename_map:
        return df

    renamed_columns = {}
    seen = set()
    for column in df.columns:
        label = str(rename_map.get(column, column))
        candidate = label or str(column)
        if candidate in seen:
            base = candidate
            i = 2
            while candidate in seen:
                candidate = f"{base}_{i}"
                i += 1
        seen.add(candidate)
        renamed_columns[column] = candidate
    return df.rename(columns=renamed_columns)


def _display_column_map(query_kind: str, params: dict) -> dict[str, str]:
    if query_kind == "big_number":
        metric = params.get("metric")
        if not metric:
            return {}
        _expression, label = _metric_sql(metric, "value")
        return {"metric_0": label}

    if query_kind == "table" and params.get("query_mode") == "aggregate":
        return {
            item["alias"]: item["label"]
            for item in [
                *_select_field_defs(params.get("groupby"), "group"),
                *_metric_defs(params.get("metrics")),
            ]
        }

    if query_kind == "xy":
        rename = {}
        x_axis = params.get("x_axis")
        if x_axis:
            _expression, label = _field_sql(x_axis, "x")
            rename["x_value"] = label
        rename.update(
            {
                item["alias"]: item["label"]
                for item in [
                    *_select_field_defs(params.get("groupby"), "group"),
                    *_metric_defs(params.get("metrics")),
                ]
            }
        )
        return rename

    return {}


def _read_sql(query: str) -> pd.DataFrame:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=columns)


def _query_kind(viz_type: str, params: dict) -> str:
    if _is_plot_viz(viz_type):
        return "xy"
    if viz_type == "big_number_total":
        return "big_number"
    if viz_type == "table" or params.get("query_mode") in {"raw", "aggregate"}:
        return "table"
    return "xy"


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _from_sql(dataset: tuple[str, str]) -> str:
    schema, table_name = dataset
    return f"{_quote_ident(schema)}.{_quote_ident(table_name)}"


def _where_sql(params: dict) -> str:
    filters = []
    for item in params.get("adhoc_filters", []) or []:
        if not isinstance(item, dict):
            continue
        if item.get("clause", "WHERE") != "WHERE":
            continue
        expression = item.get("sqlExpression")
        if expression:
            filters.append(f"({expression})")
    return "WHERE " + " AND ".join(filters) if filters else ""


def _field_sql(field, fallback_label: str) -> tuple[str, str]:
    if isinstance(field, str):
        return _quote_ident(field), field
    if isinstance(field, dict):
        expression = field.get("sqlExpression") or field.get("column_name")
        label = field.get("label") or field.get("column_name") or fallback_label
        if expression:
            return f"({expression})", str(label)
    raise ValueError(f"unsupported field definition: {field!r}")


def _metric_sql(metric, fallback_label: str) -> tuple[str, str]:
    if isinstance(metric, str):
        return _quote_ident(metric), metric
    if isinstance(metric, dict):
        expression = metric.get("sqlExpression")
        label = metric.get("label") or fallback_label
        if expression:
            return f"({expression})", str(label)
    raise ValueError(f"unsupported metric definition: {metric!r}")


def _select_field_defs(fields, prefix: str) -> list[dict]:
    defs = []
    for i, field in enumerate(fields or []):
        expression, label = _field_sql(field, f"{prefix}_{i}")
        defs.append({"expression": expression, "label": label,
                     "alias": f"{prefix}_{i}"})
    return defs


def _metric_defs(metrics) -> list[dict]:
    defs = []
    for i, metric in enumerate(metrics or []):
        expression, label = _metric_sql(metric, f"metric_{i}")
        defs.append({"expression": expression, "label": label,
                     "alias": f"metric_{i}"})
    return defs


def _limit(params: dict, default: int = 10000, maximum: int = 100000) -> int:
    try:
        value = int(params.get("row_limit") or default)
    except (TypeError, ValueError):
        value = default
    return max(1, min(value, maximum))


def _build_big_number_query(dataset: tuple[str, str], params: dict) -> str:
    metric = params.get("metric")
    if not metric:
        raise ValueError("big_number_total chart is missing metric")
    expression, _label = _metric_sql(metric, "value")
    where = _where_sql(params)
    return f"SELECT {expression} AS metric_0 FROM {_from_sql(dataset)} {where}"


def _build_table_query(dataset: tuple[str, str], params: dict) -> str:
    query_mode = params.get("query_mode", "raw")
    if query_mode == "aggregate":
        return _build_aggregate_query(dataset, params)
    columns = list(params.get("all_columns") or [])
    if not columns:
        columns = ["*"]
    select_sql = (
        "*"
        if columns == ["*"]
        else ", ".join(_quote_ident(col) for col in columns)
    )
    where = _where_sql(params)
    order = _order_by_sql(params.get("order_by_cols"))
    return (
        f"SELECT {select_sql} FROM {_from_sql(dataset)} {where} {order} "
        f"LIMIT {_limit(params)}"
    )


def _build_aggregate_query(dataset: tuple[str, str], params: dict) -> str:
    group_defs = _select_field_defs(params.get("groupby"), "group")
    metrics = _metric_defs(params.get("metrics"))
    if not group_defs and not metrics:
        return _build_table_query(dataset, {**params, "query_mode": "raw"})

    select_parts = [
        f"{item['expression']} AS {_quote_ident(item['alias'])}"
        for item in group_defs
    ] + [
        f"{item['expression']} AS {_quote_ident(item['alias'])}"
        for item in metrics
    ]
    group_sql = (
        "GROUP BY " + ", ".join(str(i) for i in range(1, len(group_defs) + 1))
        if group_defs else ""
    )
    where = _where_sql(params)
    order = _aggregate_order_by_sql(
        params.get("order_by_cols") or params.get("order_by"),
        group_defs,
        metrics,
    )
    return (
        f"SELECT {', '.join(select_parts)} FROM {_from_sql(dataset)} "
        f"{where} {group_sql} {order} LIMIT {_limit(params)}"
    )


def _build_chart_query(dataset: tuple[str, str], params: dict) -> str:
    x_axis = params.get("x_axis")
    if not x_axis:
        raise ValueError("chart is missing x_axis")
    x_expression, _x_label = _field_sql(x_axis, "x")
    group_defs = _select_field_defs(params.get("groupby"), "group")
    metrics = _metric_defs(params.get("metrics"))
    if not metrics:
        raise ValueError("chart is missing metrics")

    select_parts = [f"{x_expression} AS x_value"]
    select_parts.extend(
        f"{item['expression']} AS {_quote_ident(item['alias'])}"
        for item in group_defs
    )
    select_parts.extend(
        f"{item['expression']} AS {_quote_ident(item['alias'])}"
        for item in metrics
    )
    group_count = 1 + len(group_defs)
    group_sql = "GROUP BY " + ", ".join(str(i) for i in range(1, group_count + 1))
    where = _where_sql(params)
    return (
        f"SELECT {', '.join(select_parts)} FROM {_from_sql(dataset)} "
        f"{where} {group_sql} ORDER BY 1 ASC LIMIT {_limit(params)}"
    )


def _order_by_sql(order_by_cols) -> str:
    parts = []
    for raw in order_by_cols or []:
        parsed = _parse_order_item(raw)
        if not parsed:
            continue
        column, asc = parsed
        parts.append(f"{_quote_ident(column)} {'ASC' if asc else 'DESC'}")
    return "ORDER BY " + ", ".join(parts) if parts else ""


def _aggregate_order_by_sql(order_by_cols, group_defs, metric_defs_) -> str:
    label_to_alias = {
        item["label"]: item["alias"] for item in [*group_defs, *metric_defs_]
    }
    alias_parts = []
    raw_parts = []
    for raw in order_by_cols or []:
        parsed = _parse_order_item(raw)
        if not parsed:
            continue
        column, asc = parsed
        direction = "ASC" if asc else "DESC"
        alias = label_to_alias.get(column)
        if alias:
            alias_parts.append(f"{_quote_ident(alias)} {direction}")
        else:
            raw_parts.append(f"{_quote_ident(column)} {direction}")
    parts = alias_parts + raw_parts
    return "ORDER BY " + ", ".join(parts) if parts else ""


def _parse_order_item(raw) -> tuple[str, bool] | None:
    item = raw
    if isinstance(raw, str):
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            item = [raw, True]
    if not isinstance(item, (list, tuple)) or not item:
        return None
    column = str(item[0])
    asc = bool(item[1]) if len(item) > 1 else True
    return column, asc


def _render_xy(plt, df: pd.DataFrame, chart_name: str, params: dict,
               path: Path, scatter: bool) -> None:
    if df.empty:
        _render_message(plt, chart_name, "No data returned", path)
        return
    metric_cols = [col for col in df.columns if col.startswith("metric_")]
    if not metric_cols:
        raise ValueError("query returned no metric columns")
    y_col = metric_cols[0]
    series_cols = [col for col in df.columns if col.startswith("group_")]
    series_limit = int(params.get("series_limit") or DEFAULT_SERIES_LIMIT)

    fig, ax = plt.subplots(figsize=(12, 7))
    if series_cols:
        plotted = 0
        for label, group_df in df.groupby(series_cols, dropna=False, sort=False):
            if plotted >= series_limit:
                break
            label_text = _series_label(label)
            color = (params.get("label_colors") or {}).get(label_text)
            plot_df = _prepare_xy_frame(group_df, params, y_col)
            if plot_df.empty:
                continue
            if scatter:
                ax.scatter(plot_df["x_value"], plot_df[y_col], s=24,
                           label=label_text, color=color)
            else:
                ax.plot(plot_df["x_value"], plot_df[y_col], marker="o",
                        markersize=2.5, linewidth=1.2, label=label_text,
                        color=color)
            plotted += 1
        if plotted and params.get("show_legend", True):
            ax.legend(fontsize=7, loc="best", ncols=1)
    else:
        plot_df = _prepare_xy_frame(df, params, y_col)
        if scatter:
            ax.scatter(plot_df["x_value"], plot_df[y_col], s=24)
        else:
            ax.plot(plot_df["x_value"], plot_df[y_col], marker="o",
                    markersize=2.5, linewidth=1.2)

    _apply_axes(ax, params)
    ax.set_title(chart_name, loc="left", fontsize=12)
    ax.set_xlabel(params.get("x_axis_title") or str(params.get("x_axis") or "x"))
    ax.set_ylabel(params.get("y_axis_title") or _metric_label(params))
    ax.grid(True, alpha=0.25)
    _save_fig(fig, path)


def _render_bar(plt, df: pd.DataFrame, chart_name: str, params: dict,
                path: Path) -> None:
    if df.empty:
        _render_message(plt, chart_name, "No data returned", path)
        return
    metric_cols = [col for col in df.columns if col.startswith("metric_")]
    if not metric_cols:
        raise ValueError("query returned no metric columns")
    y_col = metric_cols[0]
    group_cols = [col for col in df.columns if col.startswith("group_")]
    fig, ax = plt.subplots(figsize=(12, 7))

    data = df.copy()
    data[y_col] = pd.to_numeric(data[y_col], errors="coerce")
    data = data.dropna(subset=[y_col])
    if data.empty:
        _render_message(plt, chart_name, "No numeric data returned", path)
        return
    if len(data) > DEFAULT_CATEGORY_LIMIT:
        data = data.head(DEFAULT_CATEGORY_LIMIT)

    if group_cols:
        group_col = group_cols[0]
        pivot = data.pivot_table(
            index="x_value",
            columns=group_col,
            values=y_col,
            aggfunc="sum",
            fill_value=0,
        )
        pivot.plot(kind="bar", stacked=bool(params.get("stack")), ax=ax)
        if params.get("show_legend", True):
            ax.legend(fontsize=7, loc="best")
    else:
        ax.bar(data["x_value"].astype(str), data[y_col])

    _apply_axes(ax, params)
    ax.set_title(chart_name, loc="left", fontsize=12)
    ax.set_xlabel(params.get("x_axis_title") or str(params.get("x_axis") or "x"))
    ax.set_ylabel(params.get("y_axis_title") or _metric_label(params))
    ax.tick_params(axis="x", labelrotation=45)
    ax.grid(True, axis="y", alpha=0.25)
    _save_fig(fig, path)


def _prepare_xy_frame(df: pd.DataFrame, params: dict, y_col: str) -> pd.DataFrame:
    plot_df = df[["x_value", y_col]].dropna().copy()
    plot_df[y_col] = pd.to_numeric(plot_df[y_col], errors="coerce")
    x_numeric = pd.to_numeric(plot_df["x_value"], errors="coerce")
    if x_numeric.notna().any():
        plot_df["x_value"] = x_numeric
    plot_df = plot_df.dropna(subset=["x_value", y_col])
    log_axis = params.get("logAxis")
    if log_axis in {"x", "both"}:
        plot_df = plot_df[plot_df["x_value"] > 0]
    if log_axis in {"y", "both"}:
        plot_df = plot_df[plot_df[y_col] > 0]
    return plot_df.sort_values("x_value")


def _apply_axes(ax, params: dict) -> None:
    log_axis = params.get("logAxis")
    if log_axis in {"x", "both"}:
        ax.set_xscale("log")
    if log_axis in {"y", "both"}:
        ax.set_yscale("log")


def _metric_label(params: dict) -> str:
    metrics = params.get("metrics") or []
    if metrics and isinstance(metrics[0], dict):
        return str(metrics[0].get("label") or "Value")
    return "Value"


def _series_label(label) -> str:
    if not isinstance(label, tuple):
        label = (label,)
    return ", ".join("" if pd.isna(item) else str(item) for item in label)


def _render_message(plt, chart_name: str, message: str, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.axis("off")
    ax.set_title(chart_name, loc="left", fontsize=12)
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=14)
    _save_fig(fig, path)


def _save_fig(fig, path: Path) -> None:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Tight layout not applied.*",
            category=UserWarning,
        )
        fig.tight_layout()
    fig.savefig(path, dpi=140)
    fig.clf()
    import matplotlib.pyplot as plt

    plt.close(fig)
