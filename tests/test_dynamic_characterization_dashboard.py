from pathlib import Path

from aps.superset.create_dynamic_characterization_dashboard import (
    DESCRIPTIONS,
    LEGACY_SOURCE_TABLES,
    SCHEMA_PATH,
    TABS,
    assert_legacy_source_tables,
    chart_definitions,
    native_filters,
)


def _catalog():
    definitions = chart_definitions({"cv": 11, "dpt": 12, "metrics": 13})
    return definitions, [{**definition, "chart_id": 100 + i} for i, definition in enumerate(definitions)]


def test_all_charts_are_documented_and_use_known_tabs():
    definitions, _ = _catalog()
    assert len(definitions) >= 12
    assert {definition["tab"] for definition in definitions} == set(TABS)
    assert all(DESCRIPTIONS[definition["name"]].strip() for definition in definitions)


def test_waveforms_do_not_bridge_nulls_or_mix_physical_units():
    definitions, _ = _catalog()
    waveform = [definition for definition in definitions if definition["tab"] == "waveforms"]
    assert all(definition["params"].get("connectNulls") is False
               for definition in waveform if definition["viz"] == "echarts_timeseries_line")
    names = {definition["name"] for definition in waveform}
    assert "CV/DPT – Drain Voltage Waveform" in names
    assert "CV/DPT – Drain Current Waveform" in names
    assert "CV/DPT – Gate Voltage Waveform" in names


def test_filters_fail_closed_to_first_device_sample_and_capture():
    _, catalog = _catalog()
    filters = native_filters(catalog, {"cv": 11, "dpt": 12, "metrics": 13})
    by_name = {item["name"]: item for item in filters}
    assert by_name["Device Type"]["controlValues"]["defaultToFirstItem"] is True
    assert by_name["Sample"]["controlValues"]["defaultToFirstItem"] is True
    assert by_name["DPT Capture"]["controlValues"]["defaultToFirstItem"] is True
    cv_chart_ids = {row["chart_id"] for row in catalog if row["ds"] == "cv"}
    assert set(by_name["DPT Capture"]["chartsInScope"]).isdisjoint(cv_chart_ids)


def test_schema_states_recovered_units_and_non_eon_energy_basis():
    sql = Path(SCHEMA_PATH).read_text()
    assert "apply_schema: pipeline-owned" in sql
    assert "* 1e-6" in sql
    assert "time_us_recovered_from_ingestion_x1e6" in sql
    assert "imported_capture_window" in sql
    assert "not a curated Eon/Eoff metric" in sql


class _LegacyCursor:
    def __init__(self, present):
        self.present = set(present)
        self.queries = []
        self._last = None

    def execute(self, query, params):
        self.queries.append((query, params))
        self._last = params[0] if params[0] in self.present else None

    def fetchone(self):
        return (self._last,)


def test_legacy_source_preflight_accepts_complete_snapshot():
    cursor = _LegacyCursor(LEGACY_SOURCE_TABLES)
    assert_legacy_source_tables(cursor)
    assert len(cursor.queries) == len(LEGACY_SOURCE_TABLES)


def test_legacy_source_preflight_fails_closed_when_snapshot_is_incomplete():
    cursor = _LegacyCursor({"public.cpvd"})
    try:
        assert_legacy_source_tables(cursor)
    except RuntimeError as exc:
        assert "public.dptgraphs" in str(exc)
        assert "DatabaseScript.py" in str(exc)
    else:
        raise AssertionError("missing legacy tables must block the dashboard")
