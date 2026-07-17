"""Declarative nightly APS data-pipeline manifest and run ledger.

The systemd shell wrapper remains responsible for the OS lock, backup, and
service health checks. This module owns the ordered data steps, dependency
validation, failure policy, and machine-readable run state. It is intentionally
usable with a fake ledger/runner so the DAG can be tested without PostgreSQL,
Superset, a NAS mount, or production data.
"""

from __future__ import annotations

from contextlib import closing

import importlib.util
import subprocess
import sys
from dataclasses import dataclass
from time import monotonic
from typing import Callable, Iterable, Mapping, Protocol

from aps.config import Settings
from aps.paths import REPO_ROOT
from aps.provenance import collect_source_provenance


class PipelineError(RuntimeError):
    """Base error for nightly pipeline planning and execution."""


class PipelineSelectionError(PipelineError):
    """Raised when a requested partial run omits a required dependency."""


StepEnabled = Callable[[], bool]


def _always_enabled() -> bool:
    return True


def _clean_source_for_irradiation_seed() -> bool:
    """Preserve the historical guard against seeding from a dirty source tree."""
    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(REPO_ROOT),
                "status",
                "--short",
                "--untracked-files=no",
                "--",
                "src",
                "schema",
                "scripts",
                "superset",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return False
    return not result.stdout.strip()


def _dashboard_device_library_available() -> bool:
    return importlib.util.find_spec("aps.superset.create_baselines_dashboard_device_library") is not None


def _iv_damage_v3_schema_available() -> bool:
    """Keep nightly safe until forward migrations 032-033 are deployed."""
    try:
        from aps.db_config import get_connection

        with closing(get_connection()) as conn, conn.cursor() as cursor:
            cursor.execute(
                "SELECT to_regclass(%s), to_regclass(%s)",
                (
                    "public.iv_damage_prediction_requests",
                    "public.iv_damage_decision_eligible_prediction_view",
                ),
            )
            return all(cursor.fetchone())
    except Exception:
        return False


@dataclass(frozen=True)
class Step:
    """One named, independently recorded pipeline command."""

    name: str
    command: tuple[str, ...]
    description: str
    depends_on: tuple[str, ...] = ()
    critical: bool = True
    enabled: StepEnabled = _always_enabled


@dataclass(frozen=True)
class StepResult:
    """Status captured for one run of a pipeline step."""

    name: str
    status: str
    error: str | None = None


@dataclass(frozen=True)
class PipelineRunResult:
    """End state of a manifest execution."""

    run_id: int
    status: str
    steps: tuple[StepResult, ...]


class RunLedger(Protocol):
    """Persistence boundary used by the runner and replaceable by unit tests."""

    def start_run(self, *, settings: Settings, step_names: Iterable[str]) -> int: ...

    def start_step(self, run_id: int, step: Step) -> int: ...

    def finish_step(
        self,
        step_run_id: int,
        *,
        status: str,
        duration_ms: int,
        error: str | None = None,
    ) -> None: ...

    def finish_run(self, run_id: int, status: str) -> None: ...


PIPELINE_LEDGER_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    code_sha TEXT NOT NULL,
    configuration_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_steps JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'degraded', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS pipeline_step_runs (
    id BIGSERIAL PRIMARY KEY,
    pipeline_run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    step_name TEXT NOT NULL,
    critical BOOLEAN NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'skipped', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    duration_ms BIGINT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS pipeline_step_runs_pipeline_step_idx
    ON pipeline_step_runs (pipeline_run_id, step_name);
"""


class PostgresRunLedger:
    """Run ledger that commits transitions independently of pipeline commands."""

    def __init__(
        self,
        conn,
        *,
        source_provenance: Mapping[str, object] | None = None,
    ):
        from psycopg2.extras import Json

        self.conn = conn
        self._json = Json
        self.source_provenance = dict(source_provenance or collect_source_provenance().as_dict())
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(PIPELINE_LEDGER_SQL)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def start_run(self, *, settings: Settings, step_names: Iterable[str]) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO pipeline_runs (
                    pipeline_name, code_sha, configuration_fingerprint,
                    source_fingerprint, requested_steps, status
                )
                VALUES (%s, %s, %s, %s, %s, 'running')
                RETURNING id
                """,
                (
                    "nightly",
                    str(self.source_provenance.get("code_sha", "unknown")),
                    self._json(settings.redacted_summary()),
                    self._json(self.source_provenance),
                    self._json(list(step_names)),
                ),
            )
            run_id = int(cur.fetchone()[0])
            self.conn.commit()
            return run_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def start_step(self, run_id: int, step: Step) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO pipeline_step_runs (pipeline_run_id, step_name, critical, status)
                VALUES (%s, %s, %s, 'running')
                RETURNING id
                """,
                (run_id, step.name, step.critical),
            )
            step_run_id = int(cur.fetchone()[0])
            self.conn.commit()
            return step_run_id
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def finish_step(
        self,
        step_run_id: int,
        *,
        status: str,
        duration_ms: int,
        error: str | None = None,
    ) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                UPDATE pipeline_step_runs
                SET status = %s, completed_at = now(), duration_ms = %s, error = %s
                WHERE id = %s
                """,
                (status, duration_ms, error[:8000] if error else None, step_run_id),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def finish_run(self, run_id: int, status: str) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET status = %s, completed_at = now()
                WHERE id = %s
                """,
                (status, run_id),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()


def default_steps() -> tuple[Step, ...]:
    """Return the one owned manifest for the established nightly workflow."""
    return (
        Step(
            "seed-device-library",
            ("-m", "aps.seeds.seed_device_library"),
            "seed the canonical device reference library",
        ),
        Step(
            "seed-device-mapping-rules",
            ("-m", "aps.seeds.seed_device_mapping_rules"),
            "seed source-aware device mapping rules",
            depends_on=("seed-device-library",),
        ),
        Step(
            "ingest-baselines",
            ("-m", "aps.ingest.ingestion_baselines"),
            "ingest pristine baseline measurements",
        ),
        Step(
            "ingest-short-circuit",
            ("-m", "aps.ingest.ingestion_sc"),
            "ingest short-circuit measurements",
        ),
        Step(
            "seed-irradiation-campaigns",
            ("-m", "aps.seeds.seed_irradiation_campaigns"),
            "seed irradiation campaign metadata only from clean source",
            critical=False,
            enabled=_clean_source_for_irradiation_seed,
        ),
        Step(
            "ingest-irradiation",
            ("-m", "aps.ingest.ingestion_irradiation"),
            "ingest irradiation measurements",
        ),
        Step(
            "assign-logbook-runs",
            ("-m", "aps.ingest.parse_logbooks_assign_runs"),
            "assign measurement rows to parsed logbook runs",
            depends_on=("ingest-irradiation",),
        ),
        Step(
            "irradiation-energy-windows",
            ("-m", "aps.enrich.irradiation_energy_windows"),
            "derive active irradiation energy windows",
            depends_on=("ingest-irradiation",),
        ),
        Step(
            "extract-single-events",
            ("-m", "aps.enrich.extract_single_event_effects"),
            "extract irradiation single-event effects",
            depends_on=("irradiation-energy-windows",),
        ),
        Step(
            "radiation-stress-dose",
            ("-m", "aps.enrich.radiation_stress_dose"),
            "derive radiation stopping-power and dose features",
            depends_on=("extract-single-events",),
        ),
        Step(
            "ingest-avalanche",
            ("-m", "aps.ingest.ingestion_avalanche"),
            "ingest avalanche measurements",
        ),
        Step(
            "refresh-baselines-run-max-current",
            ("-m", "aps.db.operations", "refresh-baselines-run-max-current"),
            "refresh the retained baseline materialized summary",
            depends_on=("ingest-baselines",),
        ),
        Step(
            "extract-damage-metrics",
            ("-m", "aps.enrich.extract_damage_metrics"),
            "derive source-independent damage metrics",
            depends_on=("ingest-baselines", "ingest-short-circuit", "ingest-avalanche"),
        ),
        Step(
            "dashboard-baselines",
            ("-m", "aps.superset.create_baselines_dashboard"),
            "reconcile the baseline dashboard",
            depends_on=("ingest-baselines",),
        ),
        Step(
            "dashboard-baselines-device-library",
            ("-m", "aps.superset.create_baselines_dashboard_device_library"),
            "reconcile the optional device-library dashboard",
            depends_on=("dashboard-baselines",),
            critical=False,
            enabled=_dashboard_device_library_available,
        ),
        Step(
            "dashboard-short-circuit",
            ("-m", "aps.superset.create_sc_dashboard"),
            "reconcile the short-circuit dashboard",
            depends_on=("ingest-short-circuit",),
        ),
        Step(
            "dashboard-irradiation",
            ("-m", "aps.superset.create_irradiation_dashboard"),
            "reconcile the irradiation dashboard",
            depends_on=("ingest-irradiation",),
        ),
        Step(
            "dashboard-avalanche",
            ("-m", "aps.superset.create_avalanche_dashboard"),
            "reconcile the avalanche dashboard",
            depends_on=("ingest-avalanche",),
        ),
        Step(
            "score-iv-damage-v3",
            ("-m", "aps.ml.iv_damage_cli", "score"),
            "score pending prospective requests with active released V3 models only",
            depends_on=("ingest-baselines", "ingest-short-circuit", "ingest-irradiation"),
            critical=False,
            enabled=_iv_damage_v3_schema_available,
        ),
        Step(
            "dashboard-iv-damage-v3",
            ("-m", "aps.superset.create_iv_damage_prediction_dashboard"),
            "reconcile V3 release, validation, abstention, and outcome monitoring",
            depends_on=("score-iv-damage-v3",),
            critical=False,
            enabled=_iv_damage_v3_schema_available,
        ),
        Step(
            "ml-sc-irradiation-equivalence",
            ("-m", "aps.ml.ml_sc_irrad_equivalence", "--rebuild"),
            "rebuild short-circuit/irradiation equivalence",
            depends_on=("ingest-short-circuit", "ingest-irradiation"),
        ),
        Step(
            "build-proxy-analytics",
            ("-m", "aps.cli", "models", "build", "proxy-analytics"),
            "build proxy analytical views once after upstream data is ready",
            depends_on=(
                "extract-single-events",
                "radiation-stress-dose",
                "ingest-avalanche",
                "ml-sc-irradiation-equivalence",
            ),
        ),
        Step(
            "dashboard-proxy-readiness",
            ("-m", "aps.superset.create_proxy_readiness_dashboard"),
            "reconcile the proxy-readiness dashboard using prepared models",
            depends_on=("build-proxy-analytics",),
        ),
        Step(
            "validate-proxy-analytics",
            ("-m", "aps.proxy.apply_mechanistic_energy_proxy", "--validate-only"),
            "run non-mutating mechanistic proxy diagnostics",
            depends_on=("build-proxy-analytics",),
            critical=False,
        ),
        Step(
            "viewer-source-damage-signature",
            ("-m", "aps.viewers.plot_source_damage_signature_3d"),
            "export the source-damage-signature viewer artifact",
            depends_on=("build-proxy-analytics",),
            critical=False,
        ),
        Step(
            "viewer-damage-signature-delta",
            ("-m", "aps.viewers.plot_damage_signature_delta_3d"),
            "export the damage-signature delta viewer artifact",
            depends_on=("build-proxy-analytics",),
            critical=False,
        ),
        Step(
            "export-proxy-energy-v2",
            ("-m", "aps.exports.export_proxy_candidate_energy_v2_csv"),
            "export v2 mechanistic proxy candidates",
            depends_on=("build-proxy-analytics",),
            critical=False,
        ),
        Step(
            "export-proxy-concordance",
            ("-m", "aps.exports.export_proxy_method_concordance_csv"),
            "export proxy method concordance",
            depends_on=("build-proxy-analytics",),
            critical=False,
        ),
        Step(
            "export-proxy-combined-v3",
            ("-m", "aps.exports.export_proxy_candidate_combined_v3_csv"),
            "export combined proxy candidates",
            depends_on=("build-proxy-analytics",),
            critical=False,
        ),
        Step(
            "export-proxy-method-comparison",
            ("-m", "aps.exports.export_proxy_method_comparison_union_csv"),
            "export the complete v1/v2/v3 winner union",
            depends_on=("build-proxy-analytics",),
            critical=False,
        ),
        Step(
            "viewer-interactive-damage-signature",
            ("-m", "aps.viewers.create_interactive_damage_signature_viewer"),
            "build the self-contained interactive damage-signature viewer",
            depends_on=(
                "viewer-source-damage-signature",
                "viewer-damage-signature-delta",
                "export-proxy-energy-v2",
                "export-proxy-concordance",
                "export-proxy-combined-v3",
                "export-proxy-method-comparison",
            ),
            critical=False,
        ),
        Step(
            "publish-damage-signature-viewer",
            ("-m", "aps.pipelines.publish_viewer"),
            "publish the generated damage-signature viewer atomically",
            depends_on=("viewer-interactive-damage-signature",),
            critical=False,
        ),
        Step(
            "dashboard-sc-irradiation",
            ("-m", "aps.superset.create_sc_irrad_dashboard"),
            "reconcile the SC/irradiation dashboard",
            depends_on=("ml-sc-irradiation-equivalence",),
        ),
    )


def _index_steps(steps: Iterable[Step]) -> tuple[Step, ...]:
    indexed = tuple(steps)
    names = [step.name for step in indexed]
    if len(names) != len(set(names)):
        raise PipelineError("pipeline step names must be unique")
    known = set(names)
    unknown_dependencies = {dependency for step in indexed for dependency in step.depends_on if dependency not in known}
    if unknown_dependencies:
        raise PipelineError("pipeline references unknown dependency: " + ", ".join(sorted(unknown_dependencies)))
    return indexed


def select_steps(
    steps: Iterable[Step],
    *,
    only: Iterable[str] = (),
    start_from: str | None = None,
    until: str | None = None,
    skip: Iterable[str] = (),
) -> tuple[Step, ...]:
    """Select a safe subset and reject omitted required dependencies."""
    indexed = _index_steps(steps)
    by_name = {step.name: step for step in indexed}
    only_names = tuple(only)
    skip_names = set(skip)
    unknown = (
        set(only_names) | skip_names | ({start_from} if start_from else set()) | ({until} if until else set())
    ) - set(by_name)
    if unknown:
        raise PipelineSelectionError("unknown nightly step(s): " + ", ".join(sorted(unknown)))
    if only_names and (start_from or until):
        raise PipelineSelectionError("--only cannot be combined with --from or --until")

    if only_names:
        requested = set(only_names)
        selected = tuple(step for step in indexed if step.name in requested)
    else:
        start_index = (
            0 if start_from is None else next(index for index, step in enumerate(indexed) if step.name == start_from)
        )
        end_index = (
            len(indexed) - 1
            if until is None
            else next(index for index, step in enumerate(indexed) if step.name == until)
        )
        if end_index < start_index:
            raise PipelineSelectionError("--until must not precede --from")
        selected = indexed[start_index : end_index + 1]

    selected = tuple(step for step in selected if step.name not in skip_names)
    selected_names = {step.name for step in selected}
    missing = {
        f"{step.name} -> {dependency}"
        for step in selected
        for dependency in step.depends_on
        if dependency not in selected_names
    }
    if missing:
        raise PipelineSelectionError(
            "unsafe partial run omits required dependencies: "
            + ", ".join(sorted(missing))
            + ". Select the dependency chain or run the full manifest."
        )
    return selected


def plan_steps(
    *,
    only: Iterable[str] = (),
    start_from: str | None = None,
    until: str | None = None,
    skip: Iterable[str] = (),
) -> tuple[Step, ...]:
    """Return a validated nightly plan without touching a database."""
    return select_steps(
        default_steps(),
        only=only,
        start_from=start_from,
        until=until,
        skip=skip,
    )


def run_subprocess_step(step: Step) -> None:
    """Run a step with the active installed Python and fail on nonzero status."""
    subprocess.run(
        [sys.executable, *step.command],
        cwd=REPO_ROOT,
        check=True,
    )


def run_pipeline(
    ledger: RunLedger,
    *,
    settings: Settings,
    steps: Iterable[Step] | None = None,
    only: Iterable[str] = (),
    start_from: str | None = None,
    until: str | None = None,
    skip: Iterable[str] = (),
    runner: Callable[[Step], None] = run_subprocess_step,
) -> PipelineRunResult:
    """Run the selected DAG, recording failed optional work as degraded."""
    selected = select_steps(
        default_steps() if steps is None else steps,
        only=only,
        start_from=start_from,
        until=until,
        skip=skip,
    )
    run_id = ledger.start_run(settings=settings, step_names=(step.name for step in selected))
    results = []
    degraded = False
    outcomes: dict[str, str] = {}

    for step in selected:
        step_run_id = ledger.start_step(run_id, step)
        started = monotonic()
        unavailable = [dependency for dependency in step.depends_on if outcomes.get(dependency) != "succeeded"]
        if unavailable:
            error = "required dependency did not succeed: " + ", ".join(unavailable)
            status = "failed" if step.critical else "skipped"
            ledger.finish_step(
                step_run_id,
                status=status,
                duration_ms=int((monotonic() - started) * 1000),
                error=error,
            )
            results.append(StepResult(step.name, status, error))
            outcomes[step.name] = status
            if step.critical:
                ledger.finish_run(run_id, "failed")
                return PipelineRunResult(run_id, "failed", tuple(results))
            continue

        try:
            enabled = step.enabled()
        except Exception as exc:
            enabled = False
            condition_error = f"{type(exc).__name__}: {exc}"
        else:
            condition_error = None

        if condition_error is not None:
            ledger.finish_step(
                step_run_id,
                status="failed",
                duration_ms=int((monotonic() - started) * 1000),
                error=condition_error,
            )
            results.append(StepResult(step.name, "failed", condition_error))
            outcomes[step.name] = "failed"
            if step.critical:
                ledger.finish_run(run_id, "failed")
                return PipelineRunResult(run_id, "failed", tuple(results))
            degraded = True
            continue

        if not enabled:
            ledger.finish_step(
                step_run_id,
                status="skipped",
                duration_ms=int((monotonic() - started) * 1000),
            )
            results.append(StepResult(step.name, "skipped"))
            outcomes[step.name] = "skipped"
            continue

        try:
            runner(step)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            ledger.finish_step(
                step_run_id,
                status="failed",
                duration_ms=int((monotonic() - started) * 1000),
                error=error,
            )
            results.append(StepResult(step.name, "failed", error))
            outcomes[step.name] = "failed"
            if step.critical:
                ledger.finish_run(run_id, "failed")
                return PipelineRunResult(run_id, "failed", tuple(results))
            degraded = True
            continue

        ledger.finish_step(
            step_run_id,
            status="succeeded",
            duration_ms=int((monotonic() - started) * 1000),
        )
        results.append(StepResult(step.name, "succeeded"))
        outcomes[step.name] = "succeeded"

    status = "degraded" if degraded else "succeeded"
    ledger.finish_run(run_id, status)
    return PipelineRunResult(run_id, status, tuple(results))


def render_plan(steps: Iterable[Step]) -> str:
    """Render the plan for CLI and shell logs without hiding optional policy."""
    rows = []
    for step in steps:
        enabled = "enabled" if step.enabled() else "disabled"
        policy = "critical" if step.critical else "optional"
        dependencies = ",".join(step.depends_on) or "-"
        rows.append(f"{step.name}\t{policy}\t{enabled}\tdeps={dependencies}")
    return "\n".join(rows)
