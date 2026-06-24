# Proxy Readiness Dashboard — Independent Review

Date: 2026-06-17
Reviewer pass: independent (built from source + SQL + screenshots before
reading the existing audit's conclusions; comparison is in the last section).

Dashboard: `Proxy Readiness - Waveform Failure Features`
Slug: `proxy-readiness-waveforms`

Sources read for this review:

- Builder: `data_processing_scripts/create_proxy_readiness_dashboard.py`
  (1492 lines, last touched 2026-06-12).
- Views: `schema/025_proxy_readiness_waveforms.sql` (3289 lines).
- Screenshots: `docs/readiness_screenshots/` (12 images, 09:55–11:21).

Method note: no live database query was run. Counts quoted below are read
directly off the screenshots, so they are a point-in-time snapshot, not a
fresh query.

---

## 0. Two versions are in play — read this first

The screenshots are **not all of the same dashboard**, and this matters for
every conclusion below.

- The **09:55–10:05 screenshots** match the committed builder script exactly:
  same chart titles (`Proxy Readiness - Figure 1(b): ...`,
  `... Normalized Observed V/I Stress Scatter by Test Type`, etc.).
- The **11:21 screenshots** show a **reworked dashboard that exists nowhere in
  the repository**. Chart titles there (`Stress Landscape: Severity vs
  Timescale`, `Energy vs Damage Signature Distance`, `Irradiation Fluence vs Proxy
  Match Distance`, `Radiation Dose vs Proxy Match Distance`) do not appear in
  any `.py` or `.sql` file. `grep` across `data_processing_scripts/` and
  `schema/` finds none of them.

So the live Superset dashboard has **drifted from its builder script**. That is
a governance problem independent of legibility: whatever is currently deployed
cannot be reproduced from `create_proxy_readiness_dashboard.py`, and the next
`python create_proxy_readiness_dashboard.py` run will overwrite the live rework
with the older committed layout. Before any redesign, the live state and the
script need to be reconciled (decide which is canonical and commit it).

This review audits the **committed script** (the reproducible artifact). Where
the 11:21 rework adds information, it is flagged.

---

## 1. Headline verdict

The underlying data model is careful and defensible. The dashboard built on top
of it is not usable as a decision tool, for three separate reasons:

1. **It has no hierarchy.** After a 4-card KPI row, it is 21 consecutive
   full-width panels (≈25 charts total), several 60+ column raw tables, and
   ~10 scatter plots, in one flat scroll with no tabs, no section headers, and
   no narrative. The reading path does not distinguish "go/no-go decision" from
   "raw audit dump."

2. **It buries its own answer.** The single most important fact —
   **gate-zero fails, 0 of the required 3 candidate device families** — is one
   small `0` among four lookalike KPI cards. Everything after it (dozens of
   candidate rows, scatter clouds, physics landscapes) visually implies the
   method is further along than the evidence supports.

3. **Parts of it are actually broken on screen**, not just cluttered (see §6):
   a log–log energy scatter that is rendering on linear axes, an annotation
   layer that errored out, and — in the live rework — a chart querying a column
   that does not exist.

The fix is mostly **subtraction and ordering**, not new analysis.

---

## 2. What the dashboard is trying to be (and why that's the problem)

It is currently five products stacked in one page:

| # | Job | User question | Belongs to |
|---|-----|---------------|------------|
| A | Readiness gate | "Is there enough overlapping evidence to attempt proxy matching at all?" | Operational |
| B | Coverage / blocker matrix | "Which device families are blocked, and on what?" | Operational |
| C | Action queue | "What measurement closes the biggest gap next?" | Operational |
| D | Candidate triage | "For each irradiation event, what's the best electrical proxy, and is it trustworthy?" | Operational |
| E | Physics / method diagnostics | "How do energy, dose, bias, timescale, amplification behave?" | Research / paper |

A–D are one workflow (gate → blockers → action → candidates). E is a different
audience (method validation, Figure 1(b) reproduction). Interleaving E into the
A–D scroll is the main reason the page "got out of hand."

---

## 3. Data lineage — how each panel computes what it shows

The builder registers 10 Superset datasets (script lines 37–48). Each is a
materialized view in `schema/025_proxy_readiness_waveforms.sql`. Computation, in
dependency order:

### 3.1 Feature extraction (foundation, not shown raw except in two tables)

- **`stress_waveform_file_features`** (SQL 177): one row per SC / avalanche /
  irradiation waveform *file*. Computes per file: `energy_vds_id_j`
  (trapezoidal integral of positive Vds·Id over adjacent samples, SQL ~437),
  `peak_abs_*`, `duration_s`, `duration_above_half_peak_power_s` (SQL 465),
  `vds_collapse_fraction` (drop from max to final Vds / max Vds, SQL 664),
  `gate_delta_fraction` (= `gate_peak_fraction`, peak Ig / peak(Ig+Id)),
  avalanche stored/commanded energy or `0.5·L·I²`. Classifies each file
  (SQL 797): `metadata_blocked` (no device type) → `waveform_blocked`
  (<2 valid power points) → `no_condition_post_iv_context` (waveform but no
  matching post-IV damage fingerprint) → `ready_for_descriptive_matching`.
- **`stress_waveform_event_features`** (SQL 858): SC/avalanche files become
  `file_as_event`; irradiation events come from
  `irradiation_single_event_energy_view` as `detected_single_event`. Carries
  event type (SEB/SELCI/SELCII/MIXED), event window, integrated terminal energy
  vs. rectangular proxy energy, `energy_window_basis`, `energy_censored_reason`,
  `active_window_confidence`, `energy_is_comparable`, collapse/gate, inherited
  post-IV evidence. This is where **censoring** lives — many irradiation events
  are not event-level energy-comparable.
- **`stress_waveform_basis_feature_view`** (SQL 1208): unnested feature-basis
  flags. Registered but not charted; effectively dead weight on the dashboard.

### 3.2 Readiness and the gate (the decision layer)

- **`stress_proxy_readiness_view`** (SQL 1246): one row per `device_type`.
  Rolls up file/event/damage counts and derives, per family,
  `gate_zero_candidate = device_type NOT NULL AND
  electrical_proxy_waveform_plus_post_iv_files > 0 AND
  (irradiation_waveform_plus_post_iv_files + irradiation_events_with_…) > 0`
  (SQL 1373). `proxy_readiness_status` is a **blocker ladder** (SQL 1381):
  `missing_device_type` → `missing_sc_or_uid_uis_waveforms` →
  `missing_irradiation_waveforms_or_events` →
  `missing_electrical_proxy_post_iv_overlap` →
  `missing_irradiation_post_iv_overlap` → `gate_zero_candidate`.
- **`stress_proxy_gate_zero_view`** (SQL 1409): one row, counts candidate
  families and **passes only when `candidate_device_families >= 3`** (SQL 1444).

### 3.3 Stress context (the physics/diagnostics layer)

- **`stress_test_context_view`** (SQL 1467): normalizes every event. Key
  derived fields: `electrical_terminal_energy_j` (integrated event Vds·Id for
  comparable irradiation events, else proxy/file/commanded fallback, SQL 1483);
  `normalized_vds = observed |Vds| / rated_voltage_v` (SQL 1684); bucketed
  `voltage_class` (650/900/1200/1700, SQL 1684); `normalized_current`;
  radiation deposited energy/dose from `radiation_stress_dose_summary_view`;
  regime labels `stress_regime`/`figure1_regime_family`/`soa_relation`/
  `test_timescale_class`/`response_reversibility` (SQL 1952–2012, purely a
  function of source + event_type + outcome); `context_flags` quality array
  (SQL 2019). Note `soa_relation` exists but **`soa_axis_score` does not** —
  relevant to §6.

### 3.4 Candidate ranking (the matching engine, drives most charts)

**`stress_proxy_candidate_view`** (SQL 2101) is the heart of the dashboard:

- **Targets** = irradiation `detected_single_event`s, in two tiers
  (SQL 2102): `energy_comparable` (event-level integrated energy available) and
  `energy_censored_damage_signature_only` (energy censored/not comparable; a
  `target_energy_floor_j` is kept for `failure_cutoff`).
- **Candidates** = SC/avalanche records with positive terminal energy (SQL 2139).
- **Links** (SQL 2147): `same_device` (matching `device_type`), plus
  `cross_device` (same bucketed `voltage_class`, **only when no same-device
  candidate exists**). Energy-comparable targets are prefiltered to
  `|ln(cand/target energy)| ≤ 5.0`.
- **Per-axis deltas** (SQL 2374): `normalized_vds_delta` (**NULL for avalanche**
  by design — known clamp/scaling artifact), `log_energy_delta`,
  `collapse_delta`, `gate_delta`, `duration_log_delta`, and a seeded
  `path_penalty` from `stress_mechanism_compatibility`.
- **Distances** (SQL 2452): `damage_signature_distance = sqrt(mean(normalized squared
  axis deltas) + path_penalty²)`; `waveform_distance` adds the energy-log term
  (weight 1.0) and a small duration term (weight 0.01).
- **Damage evidence** (SQL 2493): lateral join to measured matches first
  (`damage_equivalence_match_view`), then predicted (`…prediction_match_view`);
  `best_damage_distance` and `damage_evidence_tier` (measured/predicted/
  waveform_only) follow.
- **Status** (SQL 2623): an ordered ladder using the seeded thresholds
  (`stress_proxy_distance_settings`, default row): `energy_out_of_range`
  (log delta > 4.0) → `missing_damage_signature_overlap` → `damage_signature_mismatch`
  (> 2.50) → `cross_device_screening_only` → `measured_damage_candidate`
  (waveform ≤ 1.75, exact-condition, strong/usable) → `predicted_damage_candidate`
  → `device_run_measured_candidate` (≤ 2.25) → `weak_measured_candidate`
  (≤ 3.00) → `waveform_only_candidate` (≤ 1.25) → `missing_damage_context` →
  `inspect_manually`. `combined_screening_distance =
  sqrt(waveform² + best_damage²)` (damage fallback 2.50). Top 10 per target kept.
- **Confidence + rank** (SQL 2721): `replacement_confidence` and
  `candidate_rank` (same-device first, then status priority, then distance).

- **`stress_proxy_candidate_summary_view`** (SQL 2983): rank-1 rows grouped by
  tier/scope/source/event/mechanism/status/confidence with counts and medians.
- **`stress_proxy_experiment_plan_view`** (SQL 3029): turns blockers into ranked
  actions. Four sources, priority-tiered: same-device SC post-IV (tier 1),
  same-device avalanche post-IV (tier 1), cross-device → same-device stress
  ladders (tiers 2/3/5, SEB-aware), irradiation data recovery (tier 4, the
  "electrical evidence exists but no irradiation target" gap). Ranked by tier,
  then pair count, then affected-target count (SQL 3265).
- **`stress_destruction_boundary_view`** (SQL 2074): per device/voltage-class
  destructive vs survived normalized-Vds rollup; "survived" includes
  unknown-outcome rows, so it is a lower bound.

---

## 4. What the numbers actually say (decoded from the KPI screenshot)

From `Screenshot 09-55-30`, the four KPI cards read:

| KPI | Value | Meaning |
|-----|-------|---------|
| Gate-Zero Pass | **0** | Fails (needs ≥3 candidate families) |
| Candidate Families | **0** | Families with proxy **and** irradiation waveform+post-IV overlap |
| Electrical Proxy + Post-IV | **1** | Families where SC/UIS waveforms overlap post-IV damage |
| Irradiation + Post-IV | **7** | Families where irradiation coverage overlaps post-IV damage |

The story is in the **1 vs 7 asymmetry**: seven device families have
irradiation+post-IV evidence, but only **one** family has electrical-proxy
(SC/UIS)+post-IV evidence — and since the candidate count is 0, that one
electrical family is **not** among the seven irradiation families. The binding
constraint is the electrical-proxy post-IV side, and specifically its
non-overlap with the irradiation families. That is exactly the
`irradiation_recovery` / "SCT2080KE-style gap" the plan view encodes
(SQL 3249). **This single sentence is the dashboard's headline and it is
nowhere stated.**

---

## 5. Chart-by-chart audit

Verdict key: **Keep** (operational) · **Demote** (move to a diagnostics
tab/dashboard) · **Trim** (keep but cut columns/fix) · **Cut/replace**.

### Operational (jobs A–D)

| Panel | Purpose | Computation | Verdict |
|---|---|---|---|
| 4× Gate-Zero KPIs | Go/no-go | one row of `gate_zero` view | **Trim → 1 textual status card.** Four bare integers (`0 0 1 7`) read as a data row, not a verdict. Replace with one sentence card ("Gate-zero FAILS: 0 of 3 families. Bottleneck: electrical-proxy post-IV overlap (1 family, none shared with the 7 irradiation families)") plus a small breakdown. |
| Device Coverage / Blocker Matrix | Which families blocked, on what | `readiness` view, 14 cols | **Keep, trim.** Most useful operational table. Cut to device, status, gate_zero_candidate, SC/UIS counts, irradiation events, the two "+post-IV" overlap counts, comparable-damage-axis count, next action. Drop cell bars here. |
| Experiment Planning Queue | Next measurement to run | `experiment_plan` view, 26 cols | **Keep, move up, trim.** This is the most actionable panel and it sits 8th. Show rank, priority tier, action type, measurement device, plan text, affected-target count, expected unlock, rationale. Hide recipe keys + repeated candidate metadata. |
| Candidate Summary | Aggregate of rank-1 proxies | `candidate_summary`, 19 cols | **Trim → compact counts / stacked bar.** Long `device_types` / `candidate_device_types` strings are unreadable as columns; the value is the counts by status/source/tier. |
| Censored SEB Candidate Coverage | SEB events where energy is censored | same view, filtered to `energy_censored_damage_signature_only` + `SEB` | **Replace with a small panel.** Conceptually important (SEB censoring is a real failure mode) but it just duplicates the summary table shape. Make it 4–5 counts. |
| Best Proxy Candidates | Best proxy per target | `candidates`, rank=1, **~95 cols** | **Trim hard.** This is an export, not a dashboard table. Visible set should be ~15 cols: target key/device/event/tier, target energy or censor reason, candidate source/device/condition, status, confidence, scope, waveform/damage/combined distance, blockers. |
| Candidate Evidence Detail | Debug top-10 statuses | `candidates`, rank≤10, ~80 cols, 2500 rows | **Demote to drill-through/export.** Screenshot shows 13 pages of 60+ columns. Not a dashboard object. |
| Energy Mismatch vs Damage Signature Mismatch (scatter) | Are candidates close in both energy and damage signature? | x=`log_energy_delta`, y=`damage_signature_distance`, rank=1 | **Keep — best candidate plot.** Maps directly to the matching logic. Add threshold guide lines (energy out-of-range = 4.0; damage signature mismatch = 2.50). But see §6.3: the dense horizontal band at y≈0.77 is cross-device avalanche screening rows — color/facet by status so they don't dominate. |
| Waveform vs Damage Distance (scatter) | Waveform similarity vs post-IV damage similarity | x=`waveform_distance`, y=`best_damage_distance`, rank=1 | **Keep as validation diagnostic.** By construction only rows *with* damage evidence appear (screenshot legend is all `measured_damage`), so it is the most "real" plot — but it's sparse and belongs after the summary establishes that damage evidence exists. |
| Target vs Best Proxy Terminal Energy (scatter) | Compare target vs proxy energy | x=`target_energy_j`, y=`candidate_energy_j`, `log_x=log_y=True` | **Cut or rebuild.** Confirmed broken on screen (see §6.1). |
| Energy Density Ratio vs Damage Signature Mismatch (scatter) | Does local energy-density ratio explain mismatch? | x=`energy_density_ratio` (`log_x=True`), y=`damage_signature_distance` | **Demote + fix.** Research diagnostic; x-axis broken (linear 1e-18→1.37e12, §6.1); also missing from the candidate filter group (§6.4). |

### Diagnostics / physics (job E) — all **Demote** off the operational page

| Panel | Purpose | Computation | Note |
|---|---|---|---|
| Normalized Observed V/I Scatter | Stress space by test type | x=`normalized_vds`, y=`normalized_current`, avalanche nVds>1.60 excluded | Useful context, not readiness. |
| Blocking Bias vs Terminal Energy | Energy vs normalized Vds | x=`normalized_vds`, y=`stress_energy_j` (log y) | Competes with the candidate energy plot. |
| Blocking Bias vs Avg Terminal Power | Power severity vs Vds | y=`average_terminal_power_w` (log y) | Largely duplicative of the energy version. |
| Irradiation Deposited Energy vs Bias | Dose context by LET bin | irradiation events, deposited energy>0 | Scientific context. |
| Irradiation Energy Amplification | Terminal/deposited ratio | y=`terminal/deposited` (log, `~e` format) | Good explanatory plot (ion as trigger). Keep — but on a context surface. |
| Figure 1(b): Stress vs Timescale | Kozak Fig 1(b) recreation | x=`normalized_vds`, y=`stress_duration_s` (log), reference lines | Publication material. Annotation layer was the source of the §6.2 error. |
| Figure 1(b): Effective Stress-Time | Same w/ cumulative time | y=`effective_stress_time_s` | Same. |
| Figure 1(b): Destructive Outcomes | Destruction-limit markers | filtered to `destructive_or_catastrophic` | Currently only irradiation SEB rows qualify. |
| Figure 1(b): Destruction Boundary by Device | Per-device boundary table | `destruction_boundary` view | Lower-bound estimate; method material. |

### Raw audit tables — **Demote/Cut from main page**

| Panel | Computation | Note |
|---|---|---|
| Stress Test Context | direct `context` table, ~80 cols, 2500 rows | Export, not dashboard. |
| Event Feature Coverage | direct `event_features` table, 2500 rows | QA drill-through, not dashboard. Also in **no** native-filter group (§6.4). |

---

## 6. Concrete defects (prioritize these over redesign)

### 6.1 The log–log energy scatter renders on linear axes
`scatter_params` sets `logAxis="both"` when `log_x and log_y`, but the script's
own comment (lines 274–278) states `echarts_timeseries_scatter` **only supports
a log y-axis**; `"x"`/`"both"` are truthy strings that just re-trigger the
y-checkbox. `Screenshot 09-56-08` confirms it: "Target vs Best Proxy Terminal
Energy" has an x-axis running **linearly** from `1e-18` to `18`, with every
point crushed against the left edge and y pinned near `1`. The
"Energy Density Ratio" plot shows the same pathology (x linear to `1.37e12`).
**Fix:** precompute `log10_target_energy_j` / `log10_candidate_energy_j` in SQL
and plot them on linear axes, or drop the chart. Do not rely on `log_x`.

### 6.2 Figure 1(b) annotation layers errored (now fixed in the committed script)
`Screenshot 09-56-59` shows both Figure 1(b) landscape charts throwing
`Data error … annotation_layers … showMarkers: ['Missing data for required
field.']`. `Screenshot 10-04-55` (8 minutes later) shows them rendering with the
dashed reference lines. The committed script now sets `showMarkers`/`hideLine`
on every FORMULA layer (lines 112–145) with an explicit comment that Superset's
schema requires them. So this specific break is fixed in source — but it
documents that FORMULA annotation layers are a recurring fragility; any new
reference-line chart must carry those keys.

### 6.3 Candidate scatters are dominated by screening-only rows
In `Screenshot 09-56-01`, "Energy Mismatch vs Damage Signature Mismatch" is a thick
horizontal band of cyan points at `damage_signature_distance ≈ 0.77`. Those are
`avalanche, cross_device_screening_only, waveform_only` rows — avalanche drops
the `normalized_vds` axis (SQL 2382), so their damage signature distance collapses to a
near-constant from collapse+gate+path_penalty. By the method's own rules these
are **capped at screening confidence**, yet they visually dominate the plot and
make the candidate space look dense. This is a *usefulness* bug, not just
cosmetics: the plots overstate readiness. Color/facet by `candidate_status` and
de-emphasize or filter screening-only rows by default.

### 6.4 Native-filter scope is inconsistent (some charts silently ignore filters)
In the builder:
- `chart_groups["planning"]` is computed (script 1427) but
  `build_native_filters` (402) **never uses it** — so the Experiment Planning
  Queue is excluded from *every* filter. Apply a device filter and the action
  queue does not respond.
- "Energy Density Ratio vs Damage Signature Mismatch" is **not** in
  `candidate_chart_names` (1401–1409), so candidate filters skip it.
- "Event Feature Coverage" and "Destruction Boundary by Device" are in **no**
  group at all.

Users can apply a filter and not realize several panels didn't update.

### 6.5 Global cell bars on every table
`table_params` hard-codes `show_cell_bars=True` (line 199) for all tables,
including the 60–95 column ones, adding green in-cell bars to columns where
magnitude is meaningless. Disable globally; enable only on small numeric
summaries.

### 6.6 (Live rework only) chart queries a non-existent column
`Screenshot 11-21-46` shows "Stress Landscape: Severity vs Timescale" failing
with `column 'soa_axis_score' does not exist … AVG(soa_axis_score) AS 'Figure 1
stress severity score'`. `stress_test_context_view` exposes `soa_relation`
(categorical) but no `soa_axis_score`. This is a live-edited chart in the
un-committed rework. It will keep erroring until either the metric is changed to
an existing column or the view adds a numeric SOA score. Another 11:21 panel
reads "Waiting on Models," i.e., an empty/zero-row chart shipped to the layout.

---

## 7. Cross-cutting legibility problems

1. **No information hierarchy** — flat scroll, no tabs, no markdown section
   headers (`build_dashboard_layout` only emits ROW/CHART nodes; no TABS, no
   MARKDOWN). Superset *does* support both; the page uses neither.
2. **Tables doing analysis work** — three tables exceed 60 columns. Superset's
   table renderer is the wrong tool for inspecting radiation dose + waveform
   damage signature + damage evidence + geometry + pulse history at once.
3. **Titles name implementation nouns, not questions** — "Candidate Pairs:
   Energy Mismatch vs Damage Signature Mismatch" vs. "Where do top candidates fail —
   energy, damage signature, or damage?"
4. **Censoring/evidence caveats are under-stated** — the SQL carefully tracks
   energy censoring, active-window confidence, and damage comparability, but the
   page shows candidate clouds before telling the user how little of it is
   measured-damage-supported.
5. **Sheer length** — ~25 panels, many with Superset heights of 44–76 units.
   The page is several screens of scroll before the first physics chart.

---

## 8. Recommended target — phased, additive

Consistent with the repo's preferred style (additive phased rollout over
big-bang rebuilds), do this in order rather than as one rewrite:

**Phase 0 — stop the bleeding (no redesign).**
- Reconcile the live 11:21 rework with the script: pick canonical, commit it.
  Fix or remove the `soa_axis_score` chart and the "Waiting on Models" panel.
- Cut or rebuild the broken log–log energy scatter (§6.1).
- Fix native-filter scope: include planning + energy-density + event-feature +
  destruction-boundary charts in the right groups, or mark them intentionally
  unfiltered (§6.4).
- Turn off global cell bars (§6.5).

**Phase 1 — lead with the answer.**
- Replace the 4 KPI cards with one textual gate-zero status card + a small
  breakdown, stating the bottleneck in words (§4).
- Move the Experiment Planning Queue up to right under the status card.
- Add a "top blocker" and "next unlock" callout sourced from the plan view.

**Phase 2 — restructure with tabs (cheaper than a second dashboard).**
- Tab 1 "Readiness & Actions": status card, blocker matrix (trimmed), planning
  queue (trimmed).
- Tab 2 "Candidate Triage": compact summary, trimmed best-proxy table, the
  energy-vs-damage signature scatter (faceted by status), waveform-vs-damage scatter.
- Tab 3 "Method Diagnostics": the normalized V/I, bias/energy/power, deposited
  energy, amplification, and Figure 1(b) charts.
- Tab 4 "Raw / QA" (or drill-through links): stress-context and event-feature
  tables, candidate evidence detail.

**Phase 3 — optional view-side cleanup.**
- Add thin dashboard views (`…_readiness_dashboard_view`,
  `…_candidate_triage_view`, `…_planning_dashboard_view`) selecting only the
  display columns, instead of trimming column lists in Python.
- Precompute log columns for any chart that needs a log x-axis.

This keeps the careful data model untouched and changes only presentation,
incrementally.

---

## 9. Comparison against the existing audit

After completing the above, I read
`docs/proxy_readiness_dashboard_audit_2026-06-17.md`.

**Strong agreement** (independently reached the same conclusions):
- The dashboard serves too many jobs and reads as a data dump; it should lead
  with the gate-zero verdict and "what closes the gap."
- Demote the raw tables (evidence detail, stress context, event coverage).
- Split operational from diagnostics; trim the wide tables; disable global cell
  bars; rename charts around user questions.
- `log_x` is broken for this chart type; native-filter scope is inconsistent
  (it also flags planning, energy-density, event-feature, destruction-boundary).
- The candidate logic, gate-zero `>=3` rule, two-tier targets, avalanche
  normalized-Vds exclusion, and the four-source plan view are described
  consistently with my read of the SQL.

**What this review adds that the existing audit does not have:**
1. **The repo-vs-live divergence (§0).** The existing audit treats the
   committed script as the dashboard. The 11:21 screenshots show a reworked
   dashboard that exists in no source file — the live dashboard has drifted from
   its builder. That is a reproducibility/governance issue and should be the
   first thing fixed.
2. **Two concrete runtime errors visible in the screenshots (§6.2, §6.6)** — the
   Figure 1(b) `showMarkers` annotation error (broken at 09:56, fixed by 10:04;
   now correct in source) and the live `soa_axis_score` "column does not exist"
   error. The existing audit only flags `log_x` as "suspect"; it does not note
   that charts were actually erroring on screen.
3. **Decoding the live numbers (§4).** The existing audit notes "0 gate-zero
   families" from prior docs. This review reads `0 / 0 / 1 / 7` off the
   screenshot and identifies the *binding constraint*: the single
   electrical-proxy family does not overlap the seven irradiation families.
4. **A usefulness (not just legibility) critique of the candidate scatters
   (§6.3):** the dense `damage_signature_distance ≈ 0.77` band is cross-device
   avalanche screening rows, so the plots visually overstate readiness.
5. **Concrete thresholds (§3.4):** the actual `stress_proxy_distance_settings`
   default constants (energy log delta 5.0, damage signature mismatch 2.50, waveform
   maxima 1.25–3.00, etc.) that explain the status ladder.

**Where I'd reprioritize differently:**
- The existing audit's recommendation jumps to a fairly heavy rebuild: split
  into two separate dashboards, add three new SQL views, precompute three log
  columns. I agree with the destination but would **sequence it** (Phase 0–3,
  §8): fix the live errors and reconcile the script *first*, then restructure
  with **tabs inside one dashboard** (lighter than maintaining two dashboards
  and two builders), and treat the new SQL views as an optional Phase 3 rather
  than a prerequisite. This matches the repo's documented preference for
  additive/phased change and gets the dashboard usable sooner.
- I weight the **runtime breakage and script/live drift** as higher priority
  than the cosmetic restructuring; the existing audit leads with structure.

**Net:** the two reviews are consistent on diagnosis. The existing audit is
stronger on the detailed target column lists per table; this review is stronger
on what is actually broken on screen, the repo/live divergence, the decoded
gate numbers, and a cheaper phased path to the same destination.
