# Schema migrations

SQL assets for two different database lifecycles:

1. forward structural migrations, applied once and checksum-protected; and
2. repeatable analytical model bundles, rebuilt by their registered owner.

Flask and dashboard requests never apply schema. Historical ingestion callers
still use common.apply_schema as a compatibility path while their embedded DDL
is migrated incrementally.

## Convention

- One `.sql` per core table or one closely related pipeline table/view group.
- A recorded forward migration is immutable. Add a new numbered migration
  rather than editing an applied file.
- Include the table's `CREATE INDEX IF NOT EXISTS` statements in the
  same file.
- Do not include seed data here — seeds belong in
  `src/aps/seeds/` and are applied separately.
- Core UI and ingestion views usually live with their owning ingestion
  script (e.g. `baselines_view` in `ingestion_baselines.py`).
- Pipeline dashboards may keep their reporting views beside their pipeline
  tables in `schema/` when the SQL is marked `-- apply_schema: pipeline-owned`
  and registered with its owning repeatable model. Dashboards only consume
  prepared relations.
- Legacy snapshot models must also be explicitly opt-in and verify their
  historical source tables before execution. They are not a substitute for a
  reproducible ingestion contract and must not be applied by server startup.

## Loading

New structural releases use `aps db plan/status/migrate`. Numbered,
non-pipeline SQL is checksum-protected in `aps_forward_migrations`;
editing a recorded file stops the release. Existing un-ledgered databases must
be reviewed and explicitly baselined through the exact last historical file:

    aps db migrate --baseline-existing-through 026_irradiation_energy_windows.sql

This baselines only that historical prefix. Newer migrations still execute.

Repeatable analytical SQL uses `aps models plan/build` and the separate
`aps_model_builds` ledger. The 025/028/029 bundle is
`proxy-analytics`; 030 is the opt-in `legacy-cv-dpt` model.
Dashboard builders never apply these files.

031_flask_avalanche_admin.sql is the first post-adoption forward migration. It
moves avalanche administration DDL out of Flask request handling.

`common.apply_schema(conn)` remains a compatibility helper for historical
idempotent/source-table bundles while DDL ownership is migrated incrementally.
It skips files marked `-- apply_schema: pipeline-owned` unless a legacy
caller explicitly opts in. Do not use this behavior for new forward migrations
or expensive derived models.

## Ledger

Every file legacy `apply_schema` executes is recorded in the
`schema_migrations`
table in the same transaction: one row per (filename, content version) with
a sha256 checksum; re-applying unchanged SQL only bumps `last_applied_at`,
while edited SQL gets a new row.  This makes "was 025 applied to this
database, and which version?" answerable from the database itself.

- Status report (per file: `in_sync` / `edited_since_apply` /
  `never_recorded` / `missing_file`), and optional apply:

      python -m aps.common            # status only
      python -m aps.common --apply
      python -m aps.common --apply --include-pipeline            # all pipeline SQL
      python -m aps.common --apply --include-pipeline 025_x.sql  # selected

- Applying SQL directly via `psql` bypasses every lifecycle contract.
  Prefer the owning `aps db` or `aps models` command.

## Phase mapping

| Table | Phase | Retires |
|---|---|---|
| `device_mapping_rules` | Phase 1 (A) | `_EXPERIMENT_RULES`, `DEVICE_DIR_MAP`, `CHIP_ID_TO_DEVICE` |
| `measurement_parameters` | Phase 2 (B) | `CATEGORY_TO_PARAM`, `_REFINE_*` constants, hardcoded Vth / compliance thresholds |
| `logbook_configs` | Phase 3 (C) | `LOGBOOK_CONFIG` + per-campaign parser functions |
