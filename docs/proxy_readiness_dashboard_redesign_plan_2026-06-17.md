# Implementation Plan: Proxy Readiness Dashboard Redesign

Date: 2026-06-17

Inputs:

- `docs/proxy_readiness_dashboard_review_2026-06-17.md` (treated as the action
  plan / what-is-broken-now)
- `docs/proxy_readiness_dashboard_audit_2026-06-17.md` (broader reference,
  source of the trimmed column sets in Phase 2)

Target files:

- `data_processing_scripts/create_proxy_readiness_dashboard.py` (builder)
- `schema/025_proxy_readiness_waveforms.sql` (views; only touched if the
  optional Phase 4 dashboard views are added)
- `docs/readiness_screenshots/` (fresh evidence after each deploy)

Canonical source of truth: **the committed builder script.** The live 11:21
Superset rework is not in version control and will be reconciled *into* the
script (Phase 0), not preserved as-is.

Deploy commands (from `data_processing_scripts/`):

- Superset-only (most phases): `python create_proxy_readiness_dashboard.py --skip-schema`
- With view rebuild (Phase 4 only): `python create_proxy_readiness_dashboard.py`
- Views only: `python create_proxy_readiness_dashboard.py --schema-only`

Guiding rule: this is presentation work. The data model in
`stress_proxy_candidate_view` et al. is **not** being changed except for the
small, additive view edits explicitly called out in Phase 4.

---

## Resolved decisions for implementation

The plan should be executable without another design round. Use these defaults
unless a later request explicitly changes them:

1. **Discard live-only rework charts in Phase 0.** The 11:21 screenshots show a
   live Superset rework that is not present in source:
   `Irradiation Fluence vs Proxy Match Distance`,
   `Radiation Dose vs Proxy Match Distance` ("Waiting on Models"),
   `Irradiation Fluence vs Electrical Event Energy`,
   `Irradiation Dose vs Proxy Electrical Energy`,
   `Irradiation Dose vs Terminal Energy`,
   `Stress Landscape: Severity vs Timescale` (`soa_axis_score`), and
   `Figure 1(b): Normalized V/I Stress Map`. Do not preserve those live-only
   charts as manual Superset state. The next builder run would overwrite them
   anyway. Only port a live chart later if it has a working query and is added
   to `create_proxy_readiness_dashboard.py`.

2. **Do not add `soa_axis_score` now.** Drop the live `soa_axis_score` chart
   instead of adding a new numeric SOA score to `stress_test_context_view`.
   Reason: `soa_relation` is currently categorical methodology context; adding
   a numeric severity score is a modeling decision, not just a dashboard fix.
   If a numeric SOA axis becomes useful, add it as a separate method-design
   task after the operational dashboard is usable.

3. **Use one dashboard with tabs.** Keep the slug
   `proxy-readiness-waveforms`. Tabs solve the hierarchy problem without adding
   a second dashboard and a second deployment path.

4. **No new SQL is required until Phase 4.** Phases 0-3 should be achievable in
   the builder by removing/reordering charts, changing column lists, adding
   markdown/tab layout support, and fixing filters. If a desired display column
   does not exist, either omit it or defer it to Phase 4 rather than inventing a
   Python-side calculation.

## Implementation source map

These source locations are from the current files and may drift as edits land:

| Area | Current location | Implementation note |
| --- | --- | --- |
| Dataset registry | `DATASET_TABLES` near top of `create_proxy_readiness_dashboard.py` | Add Phase-4 dashboard views here only if created. |
| Table params | `table_params()` | Add `show_cell_bars=False` default and an opt-in argument. |
| Scatter params | `scatter_params()` | Do not use `log_x=True`; this chart type only gives a reliable log y-axis. |
| Layout builder | `build_dashboard_layout()` | Must learn `MARKDOWN`, `TABS`, and `TAB` components. |
| Native filters | `select_filter()` and `build_native_filters()` | Must support multi-dataset targets before planning charts can be filtered correctly. |
| Chart definitions | `build_chart_defs()` | Best place to add tab metadata and reduce table columns. |
| Chart groups | `candidate_chart_names`, `context_chart_names`, etc. | Keep in sync with tabs/filter scopes; avoid orphan charts. |
| Gate-zero data | `stress_proxy_gate_zero_view` | One-row source for the first-screen verdict. |
| Readiness matrix | `stress_proxy_readiness_view` | No `next_required_action` column exists until optional Phase 4. |
| Planning queue | `stress_proxy_experiment_plan_view` | Source for next measurement/action callouts. |
| Candidate triage | `stress_proxy_candidate_view` and summary view | Source for trimmed candidate tables and the two candidate scatters. |

---

## Phase 0 — Reconcile live/source drift and stop the bleeding

Goal: the live dashboard is reproducible from the script again, and no panel
renders an error. No restructuring yet.

Changes:

1. **Inventory the live rework, but do not preserve manual drift.** In
   Superset, list the current charts on `proxy-readiness-waveforms` and diff
   against `build_chart_defs()`. Record which live charts are (a) renames of
   existing charts, (b) genuinely new, (c) broken/experimental. The committed
   builder remains canonical. Manual live charts that are not encoded in the
   builder should disappear on the next deploy.

2. **Remove the broken log–log energy scatter.** Delete the
   `Candidate Pairs: Target vs Best Proxy Terminal Energy` chart def
   (`create_proxy_readiness_dashboard.py:1063`). It is redundant with the
   energy-mismatch scatter and renders on a linear x-axis. (`scatter_params`
   `log_x` is a no-op for `echarts_timeseries_scatter` — comment at lines
   274–278.)

3. **Fix or cut the energy-density scatter axis.** For
   `Candidate Pairs: Energy Density Ratio vs Phenotype Mismatch`
   (`:1129`), stop using `log_x=True`. Preferred Phase-0 fix: swap axes so the
   working log axis is y:
   `scatter_params("phenotype_distance", "energy_density_ratio", ...,
   log_y=True)`. If that still reads poorly, cut the chart until Phase 4 adds a
   precomputed `energy_density_ratio_log10` column.

4. **Drop `soa_axis_score` live chart.** Do not port the live
   `Stress Landscape: Severity vs Timescale` chart. The current
   `stress_test_context_view` has `soa_relation`, not `soa_axis_score`, and a
   numeric severity score needs a separate modeling decision.

5. **Verify Figure 1(b) annotation layers stay valid.** The committed script
   already sets `showMarkers`/`hideLine` on every FORMULA layer
   (`:112–145`); confirm any ported landscape chart keeps them so the
   `showMarkers: Missing data for required field` error from the 09:56
   screenshot does not return.

6. **Redeploy and screenshot every panel.** Run
   `python create_proxy_readiness_dashboard.py --skip-schema`, then confirm
   zero "Data error" panels.

Acceptance criteria:

- `git diff` shows the script now produces the live dashboard; no chart exists
  in Superset that is intentionally maintained outside the script.
- No panel shows a Superset data error or "Waiting on Models".
- The two log_x scatters are gone or rebuilt on real axes.

---

## Phase 1 — Lead with the gate-zero answer

Goal: the first screen states the verdict in words and the next action, before
any candidate or physics chart.

Changes (all in `build_chart_defs()` + layout):

1. **Add a top Markdown header panel.** Extend `build_dashboard_layout()` to
   support a `MARKDOWN` component (new node type alongside `CHART`). The layout
   metadata should include stable `width`, `height`, and markdown `code`.
   Content: the gate rule in one paragraph — "Gate-zero passes only when ≥3
   device families have *both* electrical-proxy (SC/UIS) waveform+post-IV
   overlap *and* irradiation waveform/event+post-IV overlap."

2. **Replace the 4 KPI cards with one verdict block.** Remove three of the four
   `big_number_total` defs. Keep `Candidate Families` as a single big number
   with subheader "of 3 required — gate-zero status". Add a **one-row gate-zero
   table** from the `gate_zero` dataset showing `gate_zero_status`,
   `candidate_device_families`, `device_families_with_electrical_proxy_post_iv_overlap`,
   `device_families_with_irradiation_post_iv_overlap`, `candidate_device_types`,
   so the 1-vs-7 asymmetry is explicit on screen.

3. **Promote the action queue.** Move
   `Experiment Planning Queue` (`:1017`) directly under the verdict block.

4. **Add a "next unlock" callout.** Small table from `experiment_plan` filtered
   to `planning_rank <= 3`, columns: `planning_rank`, `plan_action_type`,
   `measurement_device_type`, `measurement_plan`, `affected_target_count`,
   `expected_unlock`.

Implementation note: the builder currently treats every returned item from
`build_chart_defs()` as a Superset chart. The cleanest change is to introduce a
small layout item shape, for example dictionaries with `kind` equal to `chart`
or `markdown`, plus `name`, `width`, `height`, and `tab`. Superset chart
creation should only run for `kind == "chart"`; `build_json_metadata()` should
still receive only real chart IDs.

Acceptance criteria:

- First screen (no scroll) answers: pass/fail, why (bottleneck side), and the
  top 1–3 next measurements.
- No bare four-integer KPI row remains.

---

## Phase 2 — Restructure into tabs and trim the wide tables

Goal: one workflow per tab; no 60+ column table on an operational tab. This is
the main legibility win.

Changes:

1. **Add tab support to the layout builder.** Extend
   `build_dashboard_layout()` (`:300`) to emit `TABS`/`TAB` nodes and assign
   each chart to a tab. Add a `tab` field to the chart tuples returned by
   `build_chart_defs()` (or a name→tab map in `create_dashboard()`).
   Tabs:
   - **Tab 1 — Readiness & Actions:** verdict block, planning queue,
     blocker matrix.
   - **Tab 2 — Candidate Triage:** candidate summary (compact), best-proxy
     table (trimmed), energy-vs-phenotype scatter, waveform-vs-damage scatter.
   - **Tab 3 — Method Diagnostics:** normalized V/I, bias-vs-energy,
     bias-vs-power, deposited-energy, amplification, Figure 1(b) ×4,
     energy-density scatter.
   - **Tab 4 — Raw / QA:** stress-context table, event-feature table,
     candidate evidence detail. (Or replace with drill-through links.)

2. **Trim the blocker matrix** (`readiness_cols`, `:560`) to the operational
   set (from the existing audit): `device_type_label`,
   `proxy_readiness_status`, `gate_zero_candidate`, `sc_waveform_files`,
   `uid_uis_waveform_files`, `irradiation_events`,
   `electrical_proxy_waveform_plus_post_iv_files`,
   `irradiation_events_with_waveform_plus_post_iv`,
   `comparable_damage_axis_count`.

   Do **not** add a next-action column here in Phases 1-3. The current
   readiness view does not have one. The adjacent planning queue provides the
   action context. If a row-level `next_required_action` is desired, add it in
   Phase 4 as part of `stress_proxy_readiness_dashboard_view`.

3. **Trim Best Proxy Candidates** (`candidate_cols`, `:626` — ~95 columns) to
   the ~15-column triage set: `target_stress_record_key`, `device_type`,
   `target_event_type`, `target_match_tier`, `target_energy_j`,
   `target_energy_censored_reason`, `candidate_source`,
   `candidate_device_label`, `candidate_stress_condition_label`,
   `candidate_status`, `replacement_confidence`, `match_scope`,
   `waveform_distance`, `best_damage_distance`, `combined_screening_distance`,
   `candidate_blockers`.

4. **Trim the Planning Queue** (`experiment_plan_cols`, `:576`) to:
   `planning_rank`, `planning_priority_tier`, `plan_action_type`,
   `measurement_device_type`, `measurement_plan`, `affected_target_count`,
   `expected_unlock`, `planning_rationale`. Hide recipe keys + repeated
   candidate metadata.

5. **Compact the candidate summary** (`summary_cols`, `:605`): drop the long
   `device_types` / `candidate_device_types` strings; keep counts + medians, or
   convert to a stacked bar by `candidate_status` × `candidate_source`.

6. **Demote Censored SEB coverage** to a small counts panel rather than a
   duplicate of the summary table shape.

Implementation note: Superset table charts can only show columns present in
the selected dataset. If a trimmed column list mentions a derived display field
that is not already in the dataset, either remove it from the Phase-2 column
list or create it later in Phase 4. Do not add hidden Python-side joins in the
builder.

Acceptance criteria:

- Tab 1 contains only the three operational panels and fits ~1–2 screens.
- No table on Tabs 1–2 exceeds ~15 columns.
- Raw 2500-row tables live only on Tab 4.

---

## Phase 3 — Make candidate triage honest about confidence

Goal: stop the scatters visually overstating readiness.

Changes (in `scatter_params` usage for the Tab 2 scatters):

1. **Separate screening-only rows from evidence-supported rows.** On
   `Energy Mismatch vs Phenotype Mismatch` (`:1090`), keep a Tab-2 operational
   chart filtered to statuses that can drive decisions:
   `measured_damage_candidate`, `predicted_damage_candidate`,
   `device_run_measured_candidate`, `weak_measured_candidate`,
   `analog_questionable`, `inspect_manually`, `phenotype_mismatch`, and
   `missing_damage_context`. Move an all-status version to Tab 3 if the
   screening cloud is still useful diagnostically. This prevents
   `cross_device_screening_only` / `waveform_only` rows from visually implying
   readiness.

2. **Add threshold guide lines** as FORMULA annotation layers where Superset
   supports them. Reuse the `showMarkers`/`hideLine` pattern from the Figure
   1(b) reference lines. Add a horizontal guide at
   `phenotype_distance = 2.50`. Add a vertical guide at
   `log_energy_delta = 4.0` only if the chart plugin supports x-axis formula
   annotations; otherwise add the threshold to the chart description and table
   columns. Do not ship another annotation-layer error.

3. **Gate the waveform-vs-damage scatter** behind a note that it only contains
   rows with damage evidence (by construction `best_damage_distance` is null
   otherwise), so an empty/sparse plot means "no measured/predicted damage yet,"
   not "no candidates."

4. **Question-framed titles** across kept charts, e.g. "Where do top candidates
   fail — energy, phenotype, or damage?", "Which families block gate-zero?",
   "What measurement unlocks the most targets?"

Acceptance criteria:

- The default candidate scatter is not dominated by screening-only points.
- The phenotype threshold is visible on the plot; the energy threshold is either visible or explicitly documented if x-axis formula annotations are unsupported.

---

## Phase 4 — Optional view-side cleanup (only if Phases 1–3 leave column trims awkward)

Goal: move display shaping from Python column lists into thin SQL views.

Changes (`schema/025_proxy_readiness_waveforms.sql`, additive — new views only,
existing views untouched):

1. `stress_proxy_readiness_dashboard_view` — the Phase-2 blocker-matrix columns
   plus a derived `next_required_action`.
2. `stress_proxy_candidate_triage_view` — the Phase-2 best-proxy columns only.
3. `stress_proxy_planning_dashboard_view` — the Phase-2 planning columns only.
4. Precomputed log columns where a real log axis is wanted
   (`target_energy_log10_j`, `candidate_energy_log10_j`,
   `energy_density_ratio_log10`, etc.).

Register the new views in `DATASET_TABLES` (`:37`) and point the trimmed tables
at them. Requires `--schema-only` then `--skip-schema`.

Acceptance criteria:

- Dashboard tables select from purpose-built views, not raw feature views.
- Existing views and the candidate engine are byte-for-byte unchanged.

---

## Global fixes (apply in the earliest phase that touches the relevant code)

- **Cell bars off by default.** `table_params()` (`:188`) — change
  `show_cell_bars` default to `False`; add an opt-in arg and enable it only on
  the small numeric summary/gate-zero tables. (Phase 1/2.)
- **Fix native-filter scope** in `build_native_filters()` (`:402`) and the
  chart-group sets (`:1401–1428`):
  - first, change `select_filter()` to accept multiple targets. The current
    helper accepts only one `dataset_id`/`column`, so adding planning charts to
    scope is not enough. The Device filter should target at least
    `candidates.device_type`, `context.device_type`, `readiness.device_type`,
    and `experiment_plan.measurement_device_type` (or
    `experiment_plan.target_device_type` if the intended semantics are target
    family rather than measurement family);
  - include the planning chart(s) in the Device filter scope after the helper
    supports the planning target;
  - add `Energy Density Ratio vs Phenotype Mismatch` to `candidate_chart_names` if the chart is retained;
  - assign `Event Feature Coverage` and `Destruction Boundary by Device` to a
    group (or the diagnostics tab scope) so they respond to filters.
  - When tabs land, set each native filter's `tabsInScope` (`select_filter`,
    `:373`) so a filter only claims the tab it belongs to.

---

## End-to-end verification (run after Phase 0, repeat after Phases 2 and 3)

1. `python create_proxy_readiness_dashboard.py` (or `--skip-schema` when views
   are unchanged) completes with no chart `FAIL` lines.
2. Open `${SUPERSET_URL}/superset/dashboard/proxy-readiness-waveforms/`:
   - no panel shows a data error;
   - Tab 1 states gate-zero pass/fail, the bottleneck, and the next action
     without scrolling;
   - applying the Device Type filter updates every panel that should respond
     (including the planning queue);
   - no operational-tab table exceeds ~15 columns;
   - candidate scatters are readable (screening-only rows not dominant).
3. Capture a fresh screenshot set into `docs/readiness_screenshots/` and note
   the new gate-zero numbers.

---

## Rollout notes

- Phased and additive, consistent with the repo's refactor convention: each
  phase is independently deployable and the data model is untouched until the
  optional additive dashboard views in Phase 4.
- After each phase, write a `docs/n_phase_plan/`-style
  `..._phase_N_completed.md` with the deploy command and evidence, matching the
  existing Figure 1(b) phase docs.
- Phase 0 is the only mandatory-first phase; Phases 1–3 deliver the usability
  win; Phase 4 is optional.
