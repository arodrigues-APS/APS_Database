# ML Data Readiness Audit — Pristine / SC / Irradiation / Avalanche

**Date:** 2026-04-27  
**Scope:** Read-only audit of all four ingestion pipelines and shared helpers prior to cross-stress ML work.  
**Status:** Diagnostics-only round; no code changes made yet.

---

## Summary

Ten real gaps were identified across the four ingestion pipelines. None are data-destroying bugs, but several will silently degrade ML feature quality — particularly the NULL device_type issue (records disappear from per-device views), the irradiation logbook flag (NULL LET/ion_species), and the avalanche device_type bypass (inconsistent labelling). The other three pipelines share a common `common.match_device()` path; avalanche does not.

---

## Verified Gaps

### Gap 1 — Avalanche `.wfm` files silently ignored (12 files)

- **File:** `ingestion_avalanche.py:66`
- **Detail:** `VALID_EXTENSIONS = {".h5", ".hdf5"}`. Tektronix waveform captures (e.g. `Selam/UIDSelam/RT20_0.1J00003_ch1.wfm`) follow the same shot/channel naming convention as the `.h5` files but are never collected. Currently 12 such files on disk.
- **Risk for ML:** If these captures contain measurement sweeps not duplicated in the paired `.h5`, they represent missing datapoints.
- **Proposed fix:** Add a separate walk for `*.wfm` and decide: parse (if h5py-readable) or log with a count warning. If they're pure scope captures with no equivalent `.h5`, consider whether Tek WFM parsing is worth the effort.

---

### Gap 2 — Avalanche has zero `device_mapping_rules`

- **Files:** `seed_device_mapping_rules.py` (dict `inserted = {"baselines": 0, "sc": 0, "irradiation": 0}` — no `"avalanche"` key), `ingestion_avalanche.py:437` (`map_device_type()`)
- **Detail:** `seed_device_mapping_rules.py` only seeds rules for `baselines`, `sc`, and `irradiation`. Avalanche ingestion uses a local `map_device_type(paths, device_library)` function that bypasses `common.match_device()` entirely. The other three pipelines all go through `common.match_device()`.
- **Risk for ML:** Device labels are assigned differently for avalanche than for the other three sources, making cross-source joins on `device_type` less reliable.
- **Proposed fix:** Add avalanche rules to `seed_device_mapping_rules.py` and replace the local `map_device_type()` with a call to `common.match_device('avalanche', …)`.

---

### Gap 3 — Irradiation logbook `--create-missing-runs` flag is OFF by default

- **File:** `parse_logbooks_assign_runs.py:407`
- **Detail:** The flag `--create-missing-runs` uses `action="store_true"`, so it defaults to `False`. Any irradiation measurement file whose run/ion combination isn't already pre-seeded in `irradiation_runs` gets `irrad_run_id = NULL` with no warning. This silently drops the ion species, beam energy, LET surface, LET Bragg peak, and range for those files.
- **Risk for ML:** The most physically meaningful features for irradiation ML (LET, fluence, ion type) are NULL for any measurement ingested before the corresponding `irradiation_runs` row was seeded.
- **Proposed fix:** Flip the default to ON (add `--no-create-missing-runs` to opt out). Alternatively, at minimum emit a WARNING log per file where `irrad_run_id` is NULL after the logbook pass.

---

### Gap 4 — Avalanche files ingest without an `avalanche_campaigns` entry

- **File:** `ingestion_avalanche.py` (`load_avalanche_campaigns()`)
- **Detail:** `load_avalanche_campaigns()` returns `{}` if the `avalanche_campaigns` table is empty or missing. When no campaign is matched, `outcome` defaults to `'unknown'` and `temperature_c`/`inductance_mh` are populated only via path-token parsing (brittle). There is no warning emitted.
- **Risk for ML:** The most important avalanche stress parameters (energy, inductance, temperature, outcome) may be absent or wrong for any campaign that isn't pre-registered in `avalanche_campaigns`.
- **Proposed fix:** Emit a WARNING when a file ingest results in `avalanche_outcome = 'unknown'`. Optionally, gate ingest on campaign presence (or create a minimal stub row automatically with a flag similar to `--create-missing-runs`).

---

### Gap 5 — `extract_damage_metrics.py` is never auto-invoked

- **File:** `extract_damage_metrics.py`
- **Detail:** This script backfills `vth_v`, `rdson_mohm`, `bvdss_v`, and `vsd_v` into the `gate_params` JSONB column of `baselines_metadata`. It must be run manually after every ingestion; no ingestion script calls it. If omitted, `gate_params` is stale and all downstream damage-delta computations are wrong.
- **Risk for ML:** The `stress_features_view` (once created) and any ML pipeline reading `gate_params` will see stale or missing damage parameters.
- **Proposed fix:** Call `extract_damage_metrics.main()` (or equivalent) at the end of each ingestion script's `main()`, filtered to the just-ingested `data_source` for speed.

---

### Gap 6 — SC `test_condition` silently defaults to `'pristine'`

- **File:** `ingestion_sc.py:496` (via `common.classify_test_condition`)
- **Detail:** When no post-SC filename/directory pattern matches, `classify_test_condition` returns `'pristine'` as the default. Any misnamed SC directory (e.g. a post-stress folder with an unexpected naming convention) will have its files silently classified as pristine measurements.
- **Risk for ML:** Post-stress SC measurements could leak into the pristine/baseline pool, corrupting the reference distribution.
- **Proposed fix:** Change the default return value to `'unknown'` and add a warning log. Backfill historical rows with a one-shot SQL script that sets `test_condition = 'unknown'` for the ambiguous rows.

---

### Gap 7 — Rejected SC pristine files remain visible in `sc_ruggedness_view`

- **File:** `ingestion_sc.py:156` (view definition), `promote_to_baselines.py` (IQR gate)
- **Detail:** Files that fail the IQR gate in `promote_to_baselines.py` get `promotion_decision = 'rejected_iqr'` (or similar) but keep `data_source = 'sc_ruggedness'` and `test_condition = 'pristine'`. The `sc_ruggedness_view` filters by `WHERE md.sample_group IS NOT NULL`, not by `promotion_decision`. These rejected files are therefore visible to downstream ML queries.
- **Risk for ML:** Outlier SC pristine measurements that were intentionally flagged as suspect are not excluded from the ML feature pool.
- **Proposed fix:** Add `AND (md.promotion_decision IS NULL OR md.promotion_decision NOT LIKE 'rejected_%')` to the `sc_ruggedness_view` WHERE clause. (Or handle this exclusively in `stress_features_view`.)

---

### Gap 8 — `extract_damage_metrics.py` collapses multi-step IdVd files

- **File:** `extract_damage_metrics.py` (`EXTRACT_PER_FILE_SQL`)
- **Detail:** For multi-step IdVd files (e.g. progressive stress sweeps with multiple step indices), the SQL aggregates across all steps and returns a single Rdson per file. This is also used by `promote_to_baselines.py`'s IQR gate. The two are coupled; reshaping the SQL would require surgery on both.
- **Risk for ML:** For multi-step files, Rdson is not per-step — it's a collapsed value. Step-level degradation trajectories are lost.
- **Proposed fix (non-trivial):** Reshape `EXTRACT_PER_FILE_SQL` to emit one row per `(metadata_id, step_index)` and update `promote_to_baselines.py` to gate on step 0 (or the first step) only. Requires schema consideration for how to store per-step `gate_params`.

---

### Gap 9 — NULL `device_type` rows inserted silently across all four pipelines

- **Files:** All four `ingestion_*.py` scripts
- **Detail:** When `common.match_device()` (or the local avalanche equivalent) fails to identify a device type, `device_type = NULL` is inserted with no warning. These rows populate `baselines_metadata` but are excluded from all per-device views and from `promote_to_baselines.py`'s candidate set.
- **Risk for ML:** If many files have `device_type = NULL`, they are silently absent from any ML query that joins through device-aware views. The magnitude of this gap is unknown without a coverage report.
- **Proposed fix:** Emit a WARNING (with filename and the matched path tokens) for every NULL device_type insert. Optionally, add a `--strict` flag to abort ingestion on NULL device_type.

---

### Gap 10 — Stress metadata is denormalised across three different locations

- **Files:** `irradiation_runs` table, `baselines_metadata` (SC columns), `baselines_metadata` (avalanche columns)
- **Detail:** For cross-source ML, the physically meaningful stress parameters live in three different places: irradiation ion/energy/LET in `irradiation_runs` (FK join required), SC bias/duration directly on `baselines_metadata`, and avalanche energy/inductance/temperature also on `baselines_metadata` but in different columns. There is no single flat view joining all of this.
- **Risk for ML:** Any ML pipeline must write its own join logic. If the join is wrong (e.g. NULL `irrad_run_id` not handled), features silently drop to NULL.
- **Proposed fix:** Create `stress_features_view` — a single flat SQL view with one row per `metadata_id`, NULLing irrelevant source columns and fully joining irradiation tables. See the Planned Deliverables section.

---

## Verified Non-Issues

These were investigated and confirmed to NOT require fixing:

| Concern | Finding |
|---|---|
| Irradiation uppercase `.CSV` files being skipped | These are macOS resource forks (`._*`) and Padova Tektronix `TRACE*.CSV` files. They would not match the Keithley `_SN\d{3}_run\d+` regex anyway — correctly excluded. |
| SC `.tsp` files not being parsed | `.tsp` files are in `SKIP_EXTENSIONS`, but `find_matching_tsp()` IS called at `ingestion_sc.py:1006`. They're parsed as measurement metadata, not as measurement data files — correct. |
| Pristine `.xml` / `.ini` / `.zip` files | Genuinely non-data configuration/project files — correctly ignored. |

---

## Files Audited

| File | Role |
|---|---|
| `data_processing_scripts/ingestion_baselines.py` | Pristine baseline ingestion |
| `data_processing_scripts/ingestion_sc.py` | Short-circuit ruggedness ingestion |
| `data_processing_scripts/ingestion_irradiation.py` | Irradiation ingestion |
| `data_processing_scripts/ingestion_avalanche.py` | Avalanche (HDF5 waveforms) ingestion |
| `data_processing_scripts/common.py` | Shared helpers: `match_device`, `apply_schema`, `categorize_measurement`, `expand_multistep_rows`, `find_matching_tsp` |
| `data_processing_scripts/extract_damage_metrics.py` | Backfills `gate_params` (Vth, Rdson, BV, Vsd) for all sources |
| `data_processing_scripts/promote_to_baselines.py` | IQR-gate promotion of pre-irrad/SC pristine files |
| `data_processing_scripts/parse_logbooks_assign_runs.py` | Links irradiation files to `irradiation_runs` rows |
| `data_processing_scripts/seed_device_mapping_rules.py` | Seeds `device_mapping_rules` table (baselines/SC/irradiation only) |
| `data_processing_scripts/db_config.py` | `DATA_ROOT`, `NAS_ROOT`, `get_connection()` |

---

## Planned Deliverables (Not Yet Implemented)

These were designed but not written during this session. Implement when ready to proceed.

### 1. `schema/02_stress_features.sql` — ML-ready flat view

Creates `stress_features_view` — one row per `metadata_id` with damage params and stress metadata from all sources joined in.

**Filters applied:**
- `device_type IS NOT NULL`
- `data_source IN ('baselines', 'sc_ruggedness', 'irradiation', 'avalanche')`
- `promotion_decision IS NULL OR promotion_decision NOT LIKE 'rejected_%'`

**Columns:**
- Identity: `metadata_id`, `device_id`, `device_type`, `manufacturer`, `data_source`, `test_condition`, `measurement_category`, `experiment`, `filename`
- Damage params (from `gate_params` JSONB): `vth_v`, `rdson_mohm`, `bvdss_v`, `vsd_v`
- Irradiation: `irrad_role`, `irrad_campaign_id`, `irrad_run_id`, `campaign_name`, `facility`, `ion_species`, `beam_energy_mev`, `let_mev_cm2_mg`, `let_bragg_peak`, `range_um`, `fluence_at_meas`
- SC: `sc_voltage_v`, `sc_duration_us`, `sc_vgs_on_v`, `sc_vgs_off_v`, `sc_condition_label`, `sc_sequence_num`, `sample_group`, `is_sc_degraded`
- Avalanche: `avalanche_family`, `avalanche_mode`, `avalanche_energy_j`, `avalanche_peak_current_a`, `avalanche_inductance_mh`, `avalanche_temperature_c`, `avalanche_gate_bias_v`, `avalanche_outcome`
- Provenance: `promotion_decision`, `is_likely_irradiated`

**Note:** Use `DROP VIEW IF EXISTS stress_features_view CASCADE; CREATE VIEW ...`
so the view rebuild is idempotent.  If this SQL lives under `schema/`, mark it
`-- apply_schema: pipeline-owned` and have the owning pipeline apply it
explicitly rather than relying on default boot-time `common.apply_schema()`.

### 2. `data_processing_scripts/coverage_report.py` — read-only verification harness

Standalone script, no writes to the DB.

```
python data_processing_scripts/coverage_report.py [--source {baselines,sc,irradiation,avalanche,all}] [--csv] [--verbose]
```

**Metrics per source:**

| Metric | Description |
|---|---|
| `files_on_disk` | Files matching each ingestion script's walk + filter rules |
| `wfm_skipped` | Avalanche-only: `*.wfm` files found but not parsed |
| `ingested_rows` | `SELECT COUNT(*) FROM baselines_metadata WHERE data_source = 'x'` |
| `null_device_type` | Rows with `device_type IS NULL` |
| `null_stress_metadata` | Irrad: `irrad_run_id IS NULL`; SC: missing voltage/duration; Avalanche: `outcome = 'unknown'` |
| `gate_params_populated` | Rows where any of `vth_v`, `rdson_mohm`, `bvdss_v`, `vsd_v` is present |
| `multi_step_files` | `metadata_id`s with `COUNT(DISTINCT step_index) > 1` in `baselines_measurements` |
| `rejected_pristine` | SC: `promotion_decision LIKE 'rejected_%'` |
| `unknown_test_condition` | SC: `test_condition NOT IN ('pristine','post_sc') OR IS NULL` |
| `ml_ready_pct` | `(ingested_rows - null_device_type - null_stress_metadata) / ingested_rows` |

**Implementation notes:**
- Use `psycopg2` via `db_config.get_connection()` — `psql` is not available on this host.
- Do NOT import the ingestion modules — they have CLI side effects. Replicate constants (`PRISTINE_ROOT`, `SC_ROOTS`, `IRRADIATION_ROOT`, `AVALANCHE_ROOT`) at the top with source-script references.

---

## Priority Ranking for Follow-up Fixes

Ordered by ML impact:

| Priority | Gap | Why it matters |
|---|---|---|
| 1 | Gap 9 — NULL device_type (silent) | Unknown fraction of records invisible to all per-device queries |
| 2 | Gap 3 — Irrad logbook flag OFF by default | LET/ion_species NULL → irradiation features useless for ML |
| 3 | Gap 10 — No flat stress view | Every ML pipeline must reimplement the join logic |
| 4 | Gap 5 — `extract_damage_metrics` not auto-invoked | Damage features (Vth, Rdson, BV) may be stale after every ingest |
| 5 | Gap 2 — Avalanche no `device_mapping_rules` | Device labelling inconsistent for avalanche vs. other sources |
| 6 | Gap 7 — Rejected SC pristine in `sc_ruggedness_view` | Outlier measurements in ML baseline pool |
| 7 | Gap 4 — Avalanche no campaign entry warning | Stress parameters missing without alert |
| 8 | Gap 6 — SC `test_condition` defaults to `'pristine'` | Misnamed dirs silently corrupt pristine pool |
| 9 | Gap 1 — Avalanche `.wfm` files not parsed | 12 files; assess uniqueness before deciding to parse |
| 10 | Gap 8 — Multi-step IdVd collapses Rdson | Non-trivial surgery; lowest urgency unless step-level trajectories are needed |
