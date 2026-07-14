from io import StringIO

import pytest

import aps.cli as cli
from aps.config import Settings
from aps.pipelines.nightly import (
    PipelineSelectionError,
    Step,
    default_steps,
    run_pipeline,
    select_steps,
)
from aps.pipelines.publish_viewer import publish_damage_signature_viewer


class _MemoryLedger:
    def __init__(self):
        self.started_steps = []
        self.finished_steps = []
        self.run_status = None

    def start_run(self, *, settings, step_names):
        self.settings = settings
        self.step_names = tuple(step_names)
        return 41

    def start_step(self, run_id, step):
        self.started_steps.append((run_id, step.name))
        return len(self.started_steps)

    def finish_step(self, step_run_id, *, status, duration_ms, error=None):
        self.finished_steps.append((step_run_id, status, error))

    def finish_run(self, run_id, status):
        self.run_status = (run_id, status)


def _settings():
    return Settings.from_environ({"APS_PROFILE": "test"})


def test_full_manifest_has_valid_dependency_references():
    selected = select_steps(default_steps())

    assert selected[0].name == "seed-device-library"
    assert any(step.name == "build-proxy-analytics" for step in selected)


def test_interactive_viewer_declares_every_generated_input():
    steps = {step.name: step for step in default_steps()}
    viewer = steps["viewer-interactive-damage-signature"]

    assert set(viewer.depends_on) == {
        "viewer-source-damage-signature",
        "viewer-damage-signature-delta",
        "export-proxy-energy-v2",
        "export-proxy-concordance",
        "export-proxy-combined-v3",
        "export-proxy-method-comparison",
    }
    assert steps["export-proxy-method-comparison"].command == (
        "-m",
        "aps.exports.export_proxy_method_comparison_union_csv",
    )


def test_unsafe_partial_model_run_is_rejected():
    with pytest.raises(PipelineSelectionError, match="build-proxy-analytics ->"):
        select_steps(default_steps(), only=("build-proxy-analytics",))


def test_optional_failure_marks_run_degraded_and_continues():
    steps = (
        Step("core", ("-m", "core"), "critical"),
        Step("optional", ("-m", "optional"), "optional", depends_on=("core",), critical=False),
        Step("after", ("-m", "after"), "critical after optional", depends_on=("core",)),
    )
    ledger = _MemoryLedger()

    def runner(step):
        if step.name == "optional":
            raise RuntimeError("viewer unavailable")

    result = run_pipeline(ledger, settings=_settings(), steps=steps, runner=runner)

    assert result.run_id == 41
    assert result.status == "degraded"
    assert [step.status for step in result.steps] == ["succeeded", "failed", "succeeded"]
    assert ledger.run_status == (41, "degraded")


def test_critical_failure_stops_following_steps():
    steps = (
        Step("broken", ("-m", "broken"), "critical"),
        Step("never", ("-m", "never"), "must not run"),
    )
    ledger = _MemoryLedger()

    result = run_pipeline(
        ledger,
        settings=_settings(),
        steps=steps,
        runner=lambda step: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    assert result.status == "failed"
    assert [step.name for step in result.steps] == ["broken"]
    assert ledger.run_status == (41, "failed")


def test_downstream_optional_step_is_skipped_after_failed_dependency():
    steps = (
        Step("core", ("-m", "core"), "critical"),
        Step("viewer", ("-m", "viewer"), "optional", depends_on=("core",), critical=False),
        Step("publish", ("-m", "publish"), "optional", depends_on=("viewer",), critical=False),
    )
    ledger = _MemoryLedger()

    def runner(step):
        if step.name == "viewer":
            raise RuntimeError("viewer generation failed")

    result = run_pipeline(ledger, settings=_settings(), steps=steps, runner=runner)

    assert result.status == "degraded"
    assert [step.status for step in result.steps] == ["succeeded", "failed", "skipped"]
    assert "viewer" in (result.steps[-1].error or "")


def test_cli_nightly_plan_does_not_open_database(monkeypatch):
    output = StringIO()

    def unexpected_connection():
        raise AssertionError("nightly plan must not open a database connection")

    monkeypatch.setattr(cli, "get_connection", unexpected_connection)

    assert cli.main(["nightly", "plan", "--only", "seed-device-library"], output=output) == 0
    assert "seed-device-library" in output.getvalue()


def test_viewer_publish_is_atomic_and_keeps_legacy_redirect(tmp_path):
    source = tmp_path / "generated.html"
    source.write_text("<html>viewer</html>")
    web_root = tmp_path / "web-tools"

    destination = publish_damage_signature_viewer(source=source, web_tools_dir=web_root)

    assert destination.read_text() == "<html>viewer</html>"
    assert "damage-signature-3d" in (
        web_root / "phenotype-3d" / "index.html"
    ).read_text()
