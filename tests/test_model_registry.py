from pathlib import Path

from aps.db.models import ModelDefinition, model_checksum, model_plan


def test_proxy_model_plan_owns_all_three_analytical_sql_assets():
    plan = model_plan("proxy_analytics")

    assert plan.name == "proxy-analytics"
    assert plan.files == (
        "schema/025_proxy_readiness_waveforms.sql",
        "schema/028_mechanistic_energy_proxy.sql",
        "schema/029_proxy_viz_support.sql",
    )
    assert "stress_proxy_candidate_combined_v3" in plan.expected_relations


def test_model_checksum_covers_file_names_and_contents(tmp_path: Path):
    first = tmp_path / "001_first.sql"
    second = tmp_path / "002_second.sql"
    first.write_text("SELECT 1;")
    second.write_text("SELECT 2;")
    model = ModelDefinition("demo", "demo", (first, second))

    original = model_checksum(model)
    second.write_text("SELECT 3;")

    assert model_checksum(model) != original
