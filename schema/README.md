# Schema migrations

Idempotent SQL for two kinds of database contract:

1. core application tables and source-of-truth human-input parameters
   (datasheets, papers, logbooks), and
2. pipeline-owned table/view bundles that are applied by the scripts that
   populate them.

Core files are applied on `server.py` startup and before ingestion writes.
Pipeline-owned files are opt-in because they can depend on ingestion-created
tables or populated model outputs.

## Convention

- One `.sql` per core table or one closely related pipeline table/view group.
- Use `CREATE TABLE IF NOT EXISTS` and
  `DO $$ BEGIN ALTER TABLE ... ADD COLUMN ...; EXCEPTION WHEN duplicate_column THEN NULL; END $$;`
  so re-running is safe.
- Include the table's `CREATE INDEX IF NOT EXISTS` statements in the
  same file.
- Do not include seed data here — seeds belong in
  `data_processing_scripts/seed_*.py` and are applied separately.
- Core UI and ingestion views usually live with their owning ingestion
  script (e.g. `baselines_view` in `ingestion_baselines.py`).
- Pipeline dashboards may keep their reporting views beside their pipeline
  tables in `schema/` when the SQL is marked `-- apply_schema: pipeline-owned`
  and is applied by the owning script.

## Loading

`common.apply_schema(conn)` reads `.sql` files in lexicographic order and
executes each as a single statement batch.  Files marked
`-- apply_schema: pipeline-owned` are skipped by default.  Pipeline scripts
that own those files should apply them directly, or call
`common.apply_schema(conn, include_pipeline={"022_irradiation_single_events.sql"})`
for a specific file.

## Phase mapping

| Table | Phase | Retires |
|---|---|---|
| `device_mapping_rules` | Phase 1 (A) | `_EXPERIMENT_RULES`, `DEVICE_DIR_MAP`, `CHIP_ID_TO_DEVICE` |
| `measurement_parameters` | Phase 2 (B) | `CATEGORY_TO_PARAM`, `_REFINE_*` constants, hardcoded Vth / compliance thresholds |
| `logbook_configs` | Phase 3 (C) | `LOGBOOK_CONFIG` + per-campaign parser functions |
