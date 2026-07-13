"""Command line boundary for APS configuration, databases, and models.

The first commands intentionally cover inspection and explicit mutation of
database lifecycle state. Legacy python -m aps.<package>.<module> entry
points remain supported while their callers migrate to this stable interface.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import TextIO

from aps.config import ConfigurationError, get_settings
from aps.db.migrations import (
    MigrationError,
    discover_migrations,
    migration_status,
    run_migrations,
)
from aps.db.models import (
    ModelError,
    build_model,
    get_model,
    list_models,
    model_plan,
    model_status,
)
from aps.db_config import get_connection
from aps.provenance import (
    collect_source_provenance,
    require_clean_production_source,
)


def _add_pipeline_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        metavar="STEP",
        help="run or display only an independent dependency-complete step set",
    )
    parser.add_argument(
        "--from",
        dest="start_from",
        metavar="STEP",
        help="start a dependency-complete manifest segment at STEP",
    )
    parser.add_argument(
        "--until",
        metavar="STEP",
        help="end a dependency-complete manifest segment at STEP",
    )
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        metavar="STEP",
        help="omit STEP only when nothing selected depends on it",
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser separately so unit tests need no environment."""
    parser = argparse.ArgumentParser(prog="aps", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    config = commands.add_parser("config", help="inspect validated runtime configuration")
    config_commands = config.add_subparsers(dest="config_command", required=True)
    config_commands.add_parser("show", help="print a redacted settings summary")
    config_commands.add_parser(
        "provenance", help="print the source commit/dirty-tree fingerprint"
    )
    config_commands.add_parser(
        "validate-nightly",
        help="validate secrets and mounted source roots without running the DAG",
    )

    database = commands.add_parser("db", help="inspect or update forward migrations")
    database_commands = database.add_subparsers(dest="db_command", required=True)
    database_commands.add_parser("plan", help="show local migration assets without DB access")
    database_commands.add_parser("status", help="compare local assets with the database ledger")
    migrate = database_commands.add_parser("migrate", help="apply pending forward migrations")
    migrate.add_argument(
        "--baseline-existing-through",
        metavar="FILENAME",
        help=(
            "on first adoption only, baseline the historical migration prefix "
            "through this exact filename and execute newer migrations"
        ),
    )
    migrate.add_argument(
        "--dry-run",
        action="store_true",
        help="show the local plan without opening a database connection",
    )

    models = commands.add_parser("models", help="inspect or build repeatable models")
    model_commands = models.add_subparsers(dest="models_command", required=True)
    model_commands.add_parser("list", help="list owned repeatable models")
    model_plan_parser = model_commands.add_parser("plan", help="show one model's SQL bundle")
    model_plan_parser.add_argument("name")
    model_commands.add_parser("status", help="show the latest recorded build for each model")
    build = model_commands.add_parser("build", help="build one repeatable model")
    build.add_argument("name")
    build.add_argument(
        "--dry-run",
        action="store_true",
        help="show the model bundle without opening a database connection",
    )

    nightly = commands.add_parser("nightly", help="plan or run the declarative nightly DAG")
    nightly_commands = nightly.add_subparsers(dest="nightly_command", required=True)
    nightly_plan = nightly_commands.add_parser(
        "plan", help="show a dependency-validated nightly execution plan"
    )
    _add_pipeline_selection_arguments(nightly_plan)
    nightly_run = nightly_commands.add_parser(
        "run", help="run the selected nightly DAG and record every step"
    )
    _add_pipeline_selection_arguments(nightly_run)

    release = commands.add_parser(
        "release", help="inspect source/configuration release eligibility"
    )
    release_commands = release.add_subparsers(dest="release_command", required=True)
    release_commands.add_parser(
        "status", help="print redacted configuration and source provenance"
    )
    return parser


def _print_migration_plan(plan, output: TextIO) -> None:
    for item in plan:
        print(
            f"{item.state:18} {item.migration.filename} {item.migration.checksum[:12]}",
            file=output,
        )
    if not plan:
        print("no numbered non-model migration assets found", file=output)


def _print_model_plan(name: str, output: TextIO) -> None:
    plan = model_plan(name)
    print(f"model: {plan.name}", file=output)
    print(f"checksum: {plan.checksum}", file=output)
    print(f"build mode: {plan.build_mode}", file=output)
    for field, values in (
        ("sql", plan.files),
        ("upstream dependencies", plan.dependencies),
        ("required relations", plan.required_relations),
        ("expected relations", plan.expected_relations),
    ):
        print(f"{field}:", file=output)
        if values:
            for value in values:
                print(f"  - {value}", file=output)
        else:
            print("  - none declared", file=output)


def dispatch(args: argparse.Namespace, output: TextIO) -> int:
    """Dispatch a parsed command, keeping database access at mutation boundaries."""
    if args.command == "config":
        settings = get_settings()
        if args.config_command == "provenance":
            payload = collect_source_provenance().as_dict()
        else:
            if args.config_command == "validate-nightly":
                settings.validate_nightly()
            payload = settings.redacted_summary()
        print(json.dumps(payload, indent=2, sort_keys=True), file=output)
        return 0

    if args.command == "release":
        settings = get_settings()
        source = collect_source_provenance()
        payload = {
            "configuration": settings.redacted_summary(),
            "source": source.as_dict(),
            "production_mutations_allowed": (
                settings.profile != "production"
                or (source.git_available and not source.dirty)
            ),
        }
        print(json.dumps(payload, indent=2, sort_keys=True), file=output)
        return 0

    if args.command == "db":
        if args.db_command == "plan":
            from aps.db.migrations import plan_migrations

            _print_migration_plan(plan_migrations(discover_migrations(), ()), output)
            return 0
        if args.db_command == "migrate" and args.dry_run:
            from aps.db.migrations import plan_migrations

            _print_migration_plan(plan_migrations(discover_migrations(), ()), output)
            return 0

        if args.db_command == "status":
            with get_connection() as conn:
                _print_migration_plan(migration_status(conn), output)
            return 0

        settings = get_settings()
        source = collect_source_provenance()
        require_clean_production_source(
            settings,
            source,
            operation="aps db migrate",
        )
        with get_connection() as conn:
            result = run_migrations(
                conn,
                baseline_existing_through=args.baseline_existing_through,
            )
        if result.applied:
            print("applied: " + ", ".join(result.applied), file=output)
        if result.baselined:
            print("baselined: " + ", ".join(result.baselined), file=output)
        if not result.applied and not result.baselined:
            print("forward migrations already up to date", file=output)
        return 0

    if args.command == "nightly":
        from aps.pipelines.nightly import (
            PostgresRunLedger,
            plan_steps,
            render_plan,
            run_pipeline,
        )

        selected = plan_steps(
            only=args.only,
            start_from=args.start_from,
            until=args.until,
            skip=args.skip,
        )
        if args.nightly_command == "plan":
            print(render_plan(selected), file=output)
            return 0

        settings = get_settings()
        settings.validate_nightly()
        source = collect_source_provenance()
        require_clean_production_source(
            settings,
            source,
            operation="aps nightly run",
        )
        with get_connection() as conn:
            result = run_pipeline(
                PostgresRunLedger(conn, source_provenance=source.as_dict()),
                settings=settings,
                steps=selected,
            )
        print(
            f"nightly run {result.run_id}: {result.status} "
            f"({len(result.steps)} step records)",
            file=output,
        )
        return 0 if result.status == "succeeded" else 1

    if args.models_command == "list":
        for model in list_models():
            print(f"{model.name}\t{model.description}", file=output)
        return 0
    if args.models_command == "plan":
        _print_model_plan(args.name, output)
        return 0
    if args.models_command == "build" and args.dry_run:
        _print_model_plan(args.name, output)
        return 0
    if args.models_command == "status":
        with get_connection() as conn:
            rows = model_status(conn)
        for row in rows:
            print("\t".join("" if value is None else str(value) for value in row), file=output)
        if not rows:
            print("no model builds recorded", file=output)
        return 0

    model = get_model(args.name)
    settings = get_settings()
    with get_connection() as conn:
        result = build_model(conn, model, settings=settings)
    print(
        f"{result.status}: {result.model} build_id={result.build_id} "
        f"checksum={result.checksum[:12]}",
        file=output,
    )
    return 0


def main(argv: Sequence[str] | None = None, *, output: TextIO | None = None) -> int:
    """Run APS CLI and return a shell-friendly status code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    stream = output if output is not None else sys.stdout
    try:
        return dispatch(args, stream)
    except (ConfigurationError, MigrationError, ModelError) as exc:
        print(f"aps: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
