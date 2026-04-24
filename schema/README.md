# Schema migrations

Idempotent SQL for tables that are the source of truth for human-input
parameters (datasheets, papers, logbooks).  Each file in this directory
owns one logical table and is applied unconditionally on every
`server.py` startup and every ingestion run.

## Convention

- One `.sql` per table.  Filename matches the table name.
- Use `CREATE TABLE IF NOT EXISTS` and
  `DO $$ BEGIN ALTER TABLE ... ADD COLUMN ...; EXCEPTION WHEN duplicate_column THEN NULL; END $$;`
  so re-running is safe.
- Include the table's `CREATE INDEX IF NOT EXISTS` statements in the
  same file.
- Do not include seed data here — seeds belong in
  `data_processing_scripts/seed_*.py` and are applied separately.
- Do not include views here — views live with their owning ingestion
  script (e.g. `baselines_view` in `ingestion_baselines.py`).

## Loading

`common.apply_schema(conn)` reads every `.sql` in this directory in
lexicographic order and executes each as a single statement batch.
Callers that need a new table wire that call in once at startup.

## Phase mapping

| Table | Phase | Retires |
|---|---|---|
| `device_mapping_rules` | Phase 1 (A) | `_EXPERIMENT_RULES`, `DEVICE_DIR_MAP`, `CHIP_ID_TO_DEVICE` |
| `measurement_parameters` | Phase 2 (B) | `CATEGORY_TO_PARAM`, `_REFINE_*` constants, hardcoded Vth / compliance thresholds |
| `logbook_configs` | Phase 3 (C) | `LOGBOOK_CONFIG` + per-campaign parser functions |
