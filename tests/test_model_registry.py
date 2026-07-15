from pathlib import Path

import pytest

from aps.db import models as model_module
from aps.config import ConfigurationError, Settings
from aps.db.models import ModelDefinition, build_model, model_checksum, model_plan


def test_proxy_model_plan_owns_all_three_analytical_sql_assets():
    plan = model_plan("proxy_analytics")

    assert plan.name == "proxy-analytics"
    assert plan.files == (
        "schema/025_proxy_readiness_waveforms.sql",
        "schema/028_mechanistic_energy_proxy.sql",
        "schema/029_proxy_viz_support.sql",
    )
    assert "stress_proxy_candidate_combined_v3" in plan.expected_relations
    assert "stress_proxy_method_comparison_union_view" in plan.expected_relations


def test_legacy_cv_dpt_model_is_registered_but_disabled_by_default():
    plan = model_plan("legacy_cv_dpt")

    assert plan.files == ("schema/030_dynamic_characterization.sql",)
    assert plan.activation_setting == "APS_ENABLE_LEGACY_CV_DPT"
    assert plan.required_relations == (
        "public.cpvd",
        "public.dptgraphs",
        "public.dptslopes",
    )

    with pytest.raises(ConfigurationError, match="legacy CV/DPT is disabled"):
        build_model(
            None,
            "legacy-cv-dpt",
            settings=Settings.from_environ({"APS_PROFILE": "test"}),
        )


def test_model_checksum_covers_file_names_and_contents(tmp_path: Path):
    first = tmp_path / "001_first.sql"
    second = tmp_path / "002_second.sql"
    first.write_text("SELECT 1;")
    second.write_text("SELECT 2;")
    model = ModelDefinition("demo", "demo", (first, second))

    original = model_checksum(model)
    second.write_text("SELECT 3;")

    assert model_checksum(model) != original


def test_model_completion_uses_wall_clock_not_transaction_timestamp():
    assert "completed_at = clock_timestamp()" in (
        model_module._MARK_BUILD_SUCCEEDED_SQL
    )
    assert "completed_at = clock_timestamp()" in model_module._MARK_BUILD_FAILED_SQL
