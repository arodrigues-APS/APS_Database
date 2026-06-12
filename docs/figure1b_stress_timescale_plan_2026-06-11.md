# Plan: Recreate GaN Review Figure 1(b) — Stress vs Timescale Landscape

Date: 2026-06-11
Status: planned, not implemented
Scope: proxy readiness dashboard + `stress_test_context_view`

This document is self-contained. It includes all context needed to implement
without access to the conversation that produced it.

---

## 1. Goal and background

Source paper: `docs/relevant_papers/Stability_Reliability_and_Robustness_of_GaN_Power_Devices_A_Review.pdf`
(Kozak et al., IEEE TPEL vol. 38 no. 7, July 2023). Figure 1 is on page 2
(printed page 8443). A prior analysis of this paper and its mapping onto this
database is in `docs/gan_review_dashboard_mapping.md` — read it before
implementing; it defines the stress-taxonomy vocabulary used below.

**Figure 1(a)** plots device electrical stressors in I_D-vs-V_D space relative
to the device SOA (short circuit, power cycling, switching stress/HTOL,
dynamic Ron, HTRB, overvoltage/ESD, avalanche, breakdown voltage). This panel
is already recreated: the chart
"Proxy Readiness - Normalized Observed V/I Stress Scatter by Test Type"
(`normalized_vds` × `normalized_current`, grouped by `source`) on the
Proxy Readiness dashboard.

**Figure 1(b)** — the target of this plan — is a conceptual scatter:

- **X axis** = stimulus stress severity ("Switching Stimulus, e.g., frequency,
  voltage, current"), with three labeled landmarks left to right:
  "< Device Rating", "Stressed Conditions", "Destruction Limit".
- **Y axis** = evaluation timescale ("Time ~ Reliability"), log-like, from
  "Single Event" at the bottom, through "Acceptable Test Time (e.g., 1000
  hours)", up to "Specified lifetime (e.g., 15 years)".
- Region blobs: **Robustness Test** (high stress, short time, ends at a red
  "Destruction Limit" dot), **Reliability Test** (moderate stress, long time),
  a **Qualification** point (by device manufacturers, ~1000 h), and a
  **Field Test** point (by device users, ~specified lifetime).

The recreation places every stress record in this database (short-circuit
pulses, avalanche shots, irradiation single events) as a point in that plane.
The expected — and desired — outcome is that all points land in the bottom
band (sub-µs to ~50 s). With reference lines drawn at 1000 h and 15 years,
the chart becomes a one-glance statement that this is a *robustness* database
with an empty reliability region. That coverage-gap visualization is the
point; do not try to fake or extrapolate reliability-region data.

---

## 2. Environment and repository context

- Repo root: `/home/arodrigues/APS_Database/APS_Database`
- Python venv for everything (pytest, scripts): `/home/arodrigues/aps_venv/bin/python`
- Backend: PostgreSQL. Connection helper:
  `data_processing_scripts/db_config.py` → `get_connection()` (no args).
  Scripts in `data_processing_scripts/` use flat sibling imports
  (`from db_config import ...`), so run them with
  `cwd=data_processing_scripts/` or import as a package from repo root.
- Dashboards: Apache Superset, driven entirely by Python scripts via
  `data_processing_scripts/superset_api.py`. Nothing is hand-edited in the
  Superset UI; re-running the creation script is the deployment mechanism.
- Key script: `data_processing_scripts/create_proxy_readiness_dashboard.py`
  - `--schema-only` rebuilds the database views and exits (no Superset).
  - `--skip-schema` updates Superset charts without touching views.
  - no flags = both.
- Key schema file: `schema/025_proxy_readiness_waveforms.sql`. It starts by
  `DROP MATERIALIZED VIEW ... CASCADE` for everything it owns and recreates
  all of them, so view edits are deployed by re-running the dashboard script
  with `--schema-only` (it also re-applies pipeline schemas
  `022_irradiation_single_events.sql` and `027_radiation_stress_dose.sql`).
- Tests: `tests/` run with `/home/arodrigues/aps_venv/bin/python -m pytest tests/`.
  Baseline as of 2026-06-11: **20 passed**. There is currently no test file for
  `stress_test_context_view` itself.

### House rules (from prior project feedback — follow these)

1. **Phased, additive rollout.** Schema/architecture changes touching live
   data are rolled out in additive phases, never big-bang. Each phase below
   is independently shippable; Phase 1 requires no schema change.
2. **Tag quality, don't widen.** When data is excluded by a constraint,
   the fix is a quality flag plus explicit filtering, never silently
   loosening the constraint. The avalanche `normalized_vds` artifacts below
   are already handled this way — keep it that way.
3. Distance-settings tuning (`stress_proxy_distance_settings`) is done by
   inserting a new named row, not editing the default row. (Not needed for
   this plan, but do not violate it incidentally.)

---

## 3. Data model context

### 3.1 The source view

`stress_test_context_view` (materialized view, defined at
`schema/025_proxy_readiness_waveforms.sql` line ~1466) has one row per stress
record across three sources: `sc`, `avalanche`, `irradiation`. The
`stress_record_key` column (`source:metadata_id:event_id|file`) is unique.
All columns needed for Figure 1(b) already exist:

| column | meaning | relevance |
| --- | --- | --- |
| `normalized_vds` | observed \|VDS\| / device rated voltage (rating parsed from `device_library.voltage_rating` with regex fallbacks) | **X axis** |
| `normalized_current` | peak \|ID\| / rated current | optional X variant |
| `stress_duration_s` | event window duration; from waveform `event_duration_s`, else `sc_duration_us / 1e6` | **Y axis** |
| `figure1_regime_family` | `robustness` (sc, avalanche, SEB), `reliability` (SELCI/SELCII/MIXED), `radiation` (other irradiation), `unknown` | series color |
| `test_method_class` | `circuit_short_circuit`, `inductive_avalanche_<mode>`, `radiation_single_event` | alt. series color |
| `test_timescale_class` | `sub_10us_transient` / `microsecond_to_millisecond_pulse` / `subsecond_event` / `long_duration_or_file_window` / `unknown_timescale` | filter/QA |
| `response_reversibility` | `destructive_or_catastrophic`, `potentially_reversible_or_latent`, `post_iv_measured`, `unknown_no_post_iv` | destruction-limit markers |
| `pulse_count_in_sequence`, `stress_pulse_index`, `pulse_sequence_key`, `cumulative_pulse_energy_j` | repetitive-pulse context from `stress_pulse_history` (joined on `metadata_id`; avalanche only) | Phase 2 effective time |
| `context_flags` | text[] of quality/availability flags, incl. `avalanche_normalized_vds_above_quality_limit` (set when `source='avalanche' AND normalized_vds > 1.60`) | quality filtering |
| `source`, `device_type`, `voltage_class`, `event_type`, `let_bin`, `stress_energy_j` | grouping/filters already used by sibling charts | reuse |

### 3.2 Measured data coverage (live DB, 2026-06-11 — use as regression baseline)

Rows with BOTH `normalized_vds` AND `stress_duration_s` non-null:

| source | total rows | has duration | has norm. VDS | has both | destructive w/ both | in multi-pulse sequence |
| --- | --- | --- | --- | --- | --- | --- |
| avalanche | 1258 | 1258 | 1087 | 1087 | 0 | 1085 |
| irradiation | 1811 | 1128 | 1632 | 981 | 11 | 0 |
| sc | 26 | 23 | 22 | 22 | 0 | 0 |

Duration ranges among plottable rows: SC 13–26 µs; avalanche 0.25 µs–3 ms;
irradiation 0.16 s–50 s (118 rows are 1–50 s file windows). Total y-axis
span ≈ 8 decades, all far below the 1000 h qualification line (3.6×10⁶ s).

Other measured facts:

- Destructive rows (`response_reversibility = 'destructive_or_catastrophic'`)
  exist **only for irradiation**: 76 total, of which 3 lack `normalized_vds`
  and 63 lack `stress_duration_s` (→ 11 plottable). No avalanche or SC row is
  currently classified destructive, so the empirical "destruction limit"
  marker comes from irradiation SEB only. Do not invent destructive labels
  for sc/avalanche; if that looks wrong, flag it as a data follow-up instead.
- 676 avalanche rows currently have `normalized_vds > 1.60`. These are the
  known unit/scale artifact family (probe scaling in some Selam .h5/.mat
  channels; same family has integrated energies up to 657 kJ). They are
  quality-flagged, not deleted. **Exclude them from the new charts via SQL
  filter; do not change the 1.60 limit and do not "fix" the values.**
- Pulse history: 160 sequences, max 54 pulses in a sequence, avalanche only.
- Irradiation cumulative exposure (fluence/TID-like context) has **no flux or
  beam-on-time columns anywhere in the schema** — true exposure time cannot
  be computed. Such records either plot at their event/file-window duration
  (with an honest basis tag, Phase 2) or are excluded; never guess a time.

### 3.3 Semantics caveat for labeling

`stress_duration_s` for irradiation is the *detected event / energy window*
duration (scope capture), not the physical ion-strike duration. Axis and
tooltip labels must say "stress/measurement window duration", not "event
physical duration".

---

## 4. Axis mapping design

| Figure 1(b) element | Database realization |
| --- | --- |
| X: stimulus stress severity | `normalized_vds` (dimensionless; rating boundary at exactly 1.0) |
| X landmark "< Device Rating" | region x < 1.0 |
| X landmark "Destruction Limit" | empirical: destructive points + Phase 2 per-device boundary rollup |
| Y: evaluation timescale | `stress_duration_s` on log axis (Phase 2: `effective_stress_time_s` for repetitive sequences) |
| Y landmark "Single Event" | bottom of axis (where nearly all data lives) |
| Y landmark "1000 h" / "15 y" | Phase 3 reference lines at 3.6e6 s and 4.73e8 s |
| Region color (reliability/robustness) | `figure1_regime_family` |
| Single vs repetitive distinction | `pulse_count_in_sequence` (avalanche sequences up to 54 pulses) |

---

## 5. Phase 1 — charts only (no schema change)

All edits in `data_processing_scripts/create_proxy_readiness_dashboard.py`.

### 5.1 Helper available

```python
def scatter_params(x_col, y_col, x_label, y_label,
                   groupby=None, filters=None, show_legend=False,
                   log_x=False, log_y=False) -> dict
```

(line ~145). It builds an `echarts_timeseries_scatter` payload with
`x_axis = x_col` and metric `AVG(y_col)`, auto-adds a
`x IS NOT NULL AND y IS NOT NULL` SQL filter, supports `logAxis`, and applies
`CANDIDATE_COLORS` as `label_colors`. Note the existing panel-(a) chart
accepts the same minor caveat this chart will: with a coarse `groupby`,
records sharing an identical x value within a group are averaged by the
metric. The continuous x axis makes this rare; follow the existing precedent
rather than adding `stress_record_key` to `groupby` (which floods the legend).

### 5.2 New chart 1 — the main landscape

Append to the list returned by `build_chart_defs()` (after the
"Proxy Readiness - Irradiation Deposited Energy vs Terminal Electrical
Energy" tuple, before the "Stress Test Context" table is a sensible spot):

```python
(
    "Proxy Readiness - Figure 1(b): Stress vs Timescale Landscape",
    dataset_ids["context"],
    "echarts_timeseries_scatter",
    scatter_params(
        "normalized_vds",
        "stress_duration_s",
        "Observed |VDS| / device voltage rating (1.0 = rating)",
        "Stress/measurement window duration (s, log)",
        groupby=["figure1_regime_family"],
        filters=[
            sql_filter(
                "NOT (source = 'avalanche' AND normalized_vds > 1.60)"
            ),
        ],
        show_legend=True,
        log_y=True,
    ),
    12,
    68,
),
```

Expected plottable rows: ~2090 minus the avalanche artifact overlap
(artifact rows are a subset of the 1087 avalanche both-axes rows; the chart
row count must land between ~1000 and ~2100 — if it is far below 1000,
the filters are wrong).

### 5.3 New chart 2 — destructive / destruction-limit markers

```python
(
    "Proxy Readiness - Figure 1(b): Destructive Outcomes (Destruction Limit Markers)",
    dataset_ids["context"],
    "echarts_timeseries_scatter",
    scatter_params(
        "normalized_vds",
        "stress_duration_s",
        "Observed |VDS| / device voltage rating (1.0 = rating)",
        "Stress/measurement window duration (s, log)",
        groupby=["event_type"],
        filters=[
            sql_filter(
                "response_reversibility = 'destructive_or_catastrophic'"
            ),
        ],
        show_legend=True,
        log_y=True,
    ),
    12,
    44,
),
```

Expected rows: 11 (all irradiation). Small on purpose — it is the empirical
red "Destruction Limit" dot of the paper. If a future ingestion adds
destructive sc/avalanche outcomes they will appear automatically.

### 5.4 Optional chart 3 — repetition view (single vs repetitive)

Same axes, `filters=[sql_filter("pulse_count_in_sequence IS NOT NULL")]`,
`groupby=["source"]` or by a binned pulse count if a suitable column exists
(it does not yet; binning belongs to Phase 2). This can be deferred entirely
to Phase 2 — implementer's choice.

### 5.5 Wiring (required, easy to miss)

1. Add both new chart names to the `context_chart_names` set inside
   `create_dashboard()` (line ~1180) so the native device/source filters
   scope onto them.
2. Add label colors to `CANDIDATE_COLORS` (line ~49) for the new groupby
   values so colors are stable across runs:

   ```python
   "robustness": "#d62728",
   "reliability": "#1f77b4",
   "radiation": "#54a24b",
   # "unknown" already exists as UNKNOWN; add lowercase if needed:
   "unknown": "#9d755d",
   ```

   (`event_type` values SEB/SELCI/SELCII/MIXED already have colors.)

### 5.6 Deploy and verify Phase 1

```bash
cd /home/arodrigues/APS_Database/APS_Database/data_processing_scripts
/home/arodrigues/aps_venv/bin/python create_proxy_readiness_dashboard.py --skip-schema
```

(`--skip-schema` is enough; Phase 1 touches no SQL.) Then open the dashboard
(slug `proxy-readiness-waveforms`) and check: the landscape chart renders,
log y axis spans ~1e-7 to ~1e2 s, three regime colors visible, destructive
chart shows 11 points, and the avalanche artifact cluster (x > 1.6) is gone.

---

## 6. Phase 2 — derived columns + destruction boundary rollup (additive schema)

All SQL edits in `schema/025_proxy_readiness_waveforms.sql`. Keep changes
additive: new columns and one new view; do not rename or repurpose existing
columns. The whole file is rebuilt by the dashboard script, so dependent
views recreate automatically.

### 6.1 `effective_stress_time_s` and `figure1b_time_basis`

Add to the final SELECT of `stress_test_context_view` (it already joins
`stress_pulse_history sph`):

```sql
CASE
    WHEN sph.pulse_count_in_sequence IS NOT NULL
     AND sph.pulse_count_in_sequence > 1
     AND s.stress_duration_s IS NOT NULL
        THEN sph.pulse_count_in_sequence * s.stress_duration_s
    ELSE s.stress_duration_s
END AS effective_stress_time_s,
CASE
    WHEN s.stress_duration_s IS NULL THEN 'unknown_no_duration'
    WHEN sph.pulse_count_in_sequence IS NOT NULL
     AND sph.pulse_count_in_sequence > 1
        THEN 'repetitive_sequence_scaled'
    WHEN s.test_timescale_class IS NOT NULL  -- see note below
        THEN NULL
END AS figure1b_time_basis,
```

Implementation notes (the snippet above is intent, not paste-ready):

- `test_timescale_class` is computed in the same SELECT, so it cannot be
  referenced there; either duplicate the duration CASE conditions or compute
  the basis from the same primitives: `'single_pulse_or_event'` when
  duration came from `event_duration_s`/`sc_duration_us` and
  `event_record_type = 'detected_single_event'` or source is sc/avalanche;
  `'file_window'` when the row is a file-level record
  (`event_record_type <> 'detected_single_event'`) or duration > 1 s for
  irradiation; `'unknown_no_duration'` when NULL.
- The `pulse_count × duration` scaling assumes equal pulse durations within
  a sequence — acceptable because sequences are same-setting ladders. State
  this assumption in a SQL comment. The basis tag exists precisely so
  consumers can tell scaled values from measured ones (house rule 2).
- Cumulative irradiation exposure stays `unknown`-tagged or at its window
  duration — never synthesized from fluence (no flux data exists; §3.2).

### 6.2 New rollup view: `stress_destruction_boundary_view`

Per-device empirical destruction boundary (the paper's red dot, made
quantitative). Place it after `stress_test_context_view`'s indexes; add a
matching `DROP MATERIALIZED VIEW IF EXISTS ... CASCADE;` at the top of the
file next to the existing drops.

```sql
CREATE MATERIALIZED VIEW stress_destruction_boundary_view AS
SELECT
    device_type,
    voltage_class,
    COUNT(*) FILTER (WHERE response_reversibility = 'destructive_or_catastrophic')
        AS destructive_count,
    MIN(normalized_vds) FILTER (
        WHERE response_reversibility = 'destructive_or_catastrophic'
          AND NOT (source = 'avalanche' AND normalized_vds > 1.60)
    ) AS min_destructive_normalized_vds,
    MAX(normalized_vds) FILTER (
        WHERE response_reversibility <> 'destructive_or_catastrophic'
          AND NOT (source = 'avalanche' AND normalized_vds > 1.60)
    ) AS max_survived_normalized_vds,
    COUNT(*) AS record_count
FROM stress_test_context_view
WHERE normalized_vds IS NOT NULL
GROUP BY device_type, voltage_class;
```

Caveat to carry into the chart/table description: "survived" here means
"not classified destructive", which includes unknown-outcome rows; with only
irradiation destructive labels, the boundary is a lower-bound estimate.

### 6.3 Dashboard updates for Phase 2

1. Add `effective_stress_time_s` and `figure1b_time_basis` to the
   `context_cols` list (line ~697) so they appear in the context table.
2. Switch the Phase 1 landscape chart's y column to `effective_stress_time_s`
   (label: "Effective cumulative stress time (s, log; repetitive sequences
   scaled by pulse count)"), or add it as a separate chart — separate chart
   preferred, so single-pulse and effective views can be compared.
3. Register `stress_destruction_boundary_view` as a new dataset in
   `DATASET_TABLES` (key suggestion: `"destruction_boundary"`) and add a
   `table_params` chart listing it, plus optionally a scatter
   `max_survived_normalized_vds` vs `min_destructive_normalized_vds` by
   device.
4. `register_datasets()` / `refresh_dataset_columns` handle new columns and
   datasets automatically on re-run.

### 6.4 Tests for Phase 2

Add `tests/test_stress_context_figure1b.py` (pattern-match the existing
`tests/test_stress_pulse_history.py` for DB-touching test style):

- every row with `pulse_count_in_sequence > 1` and non-null duration has
  `effective_stress_time_s = pulse_count_in_sequence * stress_duration_s`;
- `figure1b_time_basis` is non-null whenever `effective_stress_time_s` is;
- `stress_destruction_boundary_view` has ≥ 1 row with non-null
  `min_destructive_normalized_vds` (currently the C2M/irradiation devices);
- baseline counts of §3.2 still hold (assert with tolerant `>=` bounds, not
  exact equality, so future ingestion does not break the suite).

### 6.5 Deploy Phase 2

```bash
cd /home/arodrigues/APS_Database/APS_Database/data_processing_scripts
/home/arodrigues/aps_venv/bin/python create_proxy_readiness_dashboard.py --schema-only
/home/arodrigues/aps_venv/bin/python -m pytest ../tests/
/home/arodrigues/aps_venv/bin/python create_proxy_readiness_dashboard.py --skip-schema
```

Expect 20 baseline tests + new ones to pass. The full schema rebuild takes a
while (it rebuilds all proxy matviews); that is normal.

---

## 7. Phase 3 — reference lines and presentation polish

1. **Reference lines** at y = 3.6e6 s ("Acceptable test time: 1000 h") and
   y = 4.73e8 s ("Specified lifetime: 15 y") on the landscape chart(s).
   Superset ECharts timeseries charts support `annotation_layers` with
   `annotationType: "FORMULA"` (a constant expression like `3.6e6` draws a
   horizontal line). Extend `scatter_params` with an optional
   `annotation_layers=None` parameter that passes through to the params dict
   — do not hardcode into the shared helper's defaults, other charts use it.
   With log y this stretches the visible axis ~7 decades above the data;
   that gap **is the message** (robustness-only database). If formula
   annotations fight the log axis in the installed Superset version, fall
   back to documenting the lines in the chart description and setting
   `y_axis_bounds` upper to ~1e9.
2. **Rating line** at x = 1.0: ECharts formula annotations are horizontal
   only. The axis label already states "1.0 = rating"; optionally add a
   constant-column trick or leave as labeled. Do not over-engineer.
3. Chart description text (Superset chart `description` field, or the
   dashboard markdown header if one is added): cite the paper figure, state
   the irradiation window-duration semantics (§3.3), the destruction-marker
   irradiation-only caveat (§3.2), and the artifact exclusion rule.

---

## 8. Explicit non-goals / guardrails

- Do **not** loosen the avalanche `normalized_vds > 1.60` quality limit or
  rescale artifact rows (open upstream follow-up owns the root cause).
- Do **not** synthesize exposure time from fluence for cumulative
  irradiation records.
- Do **not** label sc/avalanche rows destructive without new outcome data.
- Do **not** modify `stress_proxy_candidate_view`, distance settings, or the
  mechanism table — this plan is presentation + context only.
- Keep all new columns/views additive; nothing existing renames or drops
  (except the coordinated DROP/CREATE of the matview being extended, which
  is the file's normal rebuild pattern).

## 9. Acceptance checklist

- [ ] Landscape chart on dashboard `proxy-readiness-waveforms`, ~1000–2100
      points, three regime colors, log y spanning ~1e-7…1e2 s.
- [ ] Avalanche cluster at x > 1.6 absent from new charts, still present in
      the context table (quality flag visible).
- [ ] Destructive-markers chart shows exactly the irradiation destructive
      rows with both axes (11 at baseline).
- [ ] Phase 2: `effective_stress_time_s` lifts ~1085 repetitive avalanche
      rows above their single-pulse duration; basis tag populated.
- [ ] `stress_destruction_boundary_view` registered and rendered.
- [ ] Reference lines (or documented fallback) at 1000 h and 15 y.
- [ ] `pytest tests/` green (≥ 20 passed baseline + new tests).
- [ ] No changes to candidate/ranking views (diff of `025_…sql` confined to
      context view final SELECT + new rollup view + drops).
