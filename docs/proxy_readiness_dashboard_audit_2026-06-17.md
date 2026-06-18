# Proxy Readiness Dashboard Audit

Date reviewed: 2026-06-17

Dashboard: `Proxy Readiness - Waveform Failure Features`

Primary implementation:

- `data_processing_scripts/create_proxy_readiness_dashboard.py`
- `schema/025_proxy_readiness_waveforms.sql`

Screenshot set reviewed:

- `docs/readiness_screenshots/`

Related prior context:

- `docs/proxy_stress_equivalence_review.md`
- `docs/proxy_method_review_2026-06-10.md`

Important caveat: I did not read credential-bearing database configuration. A
default `psql` query failed because the local role was not available, so live
row counts were not re-queried for this review. Count examples below are taken
from the checked-in June 2026 review documents and are treated as prior
verified state, not a fresh database query.

## Executive Assessment

The dashboard contains valuable engineering work, but it is trying to serve too
many jobs at once:

1. Coverage gate: do we have enough waveform plus post-IV evidence to attempt
   proxy matching?
2. Candidate triage: which SC or avalanche stress records are the best proxy
   candidates for irradiation events?
3. Measurement planning: what experiment or data recovery closes the next
   blocker?
4. Research diagnostics: how do energy, deposited radiation dose, stress time,
   normalized voltage, and damage distance behave?
5. Raw audit table: expose the underlying feature columns for debugging.

Those are all legitimate products, but they should not all appear as one long
dashboard with full-width plots and wide raw tables. The current result reads
as a data dump rather than a decision interface.

The most important current message is simple: gate-zero readiness fails. Prior
review state reported 0 gate-zero candidate device families, with many
waveforms but little overlap where both electrical proxy records and
irradiation records have post-IV damage evidence. The dashboard should lead
with that fact and then answer "what closes the gap?" Instead, it quickly moves
into many candidate and context plots, which can imply the method is more ready
than the evidence supports.

## How The Dashboard Is Built

The builder script registers these Superset datasets:

| dataset key | view/table | role |
| --- | --- | --- |
| `gate_zero` | `stress_proxy_gate_zero_view` | one-row go/no-go readiness summary |
| `readiness` | `stress_proxy_readiness_view` | per-device coverage and blocker matrix |
| `file_features` | `stress_waveform_file_features` | file-level waveform phenotype extraction |
| `event_features` | `stress_waveform_event_features` | event-level waveform phenotype extraction |
| `basis_features` | `stress_waveform_basis_feature_view` | unnested feature basis flags |
| `context` | `stress_test_context_view` | normalized stress, dose, energy, and Figure 1(b) context |
| `destruction_boundary` | `stress_destruction_boundary_view` | empirical destructive/survived voltage rollup |
| `candidates` | `stress_proxy_candidate_view` | top-10 proxy candidates per target event |
| `candidate_summary` | `stress_proxy_candidate_summary_view` | grouped rank-1 candidate summary |
| `experiment_plan` | `stress_proxy_experiment_plan_view` | proposed measurements/data-recovery actions |

The dashboard layout is generated in one packed grid. The first four charts are
KPI cards, and almost everything after that is a 12-column full-width table or
scatter plot. There are no tabs, no collapsible diagnostic sections, and no
markdown explanation blocks in the generated layout. Superset chart
descriptions exist for a few plots, but the default dashboard reading path is
still just a long sequence of charts.

## Computation Lineage

### 1. File-Level Waveform Features

View: `stress_waveform_file_features`

This view pulls SC, avalanche, and irradiation waveform files from
`baselines_metadata` and `baselines_measurements`.

For each file it computes:

- `energy_vds_id_j`: trapezoidal integration of positive `Vds * Id` over
  adjacent waveform samples.
- `energy_abs_j`: trapezoidal integration of `abs(Vds * Id)`.
- `peak_abs_id_a`, `peak_abs_ig_a`, `peak_abs_power_w`: maxima over waveform
  samples.
- `duration_s`: `max(time) - min(time)`.
- `duration_above_half_peak_power_s`: total time above half peak power.
- `vds_collapse_fraction`: drop from max Vds to final Vds, normalized by max
  Vds.
- `gate_peak_fraction`: peak gate current divided by peak gate plus drain
  current.
- avalanche stored/commanded energy from metadata, or `0.5 * L * I^2` when
  inductance and current are available.
- condition, family, and exact-sample post-IV companion counts from
  `damage_equivalence_view` and post-stress metadata.

Its readiness classification is conservative:

- `metadata_blocked`: missing device type.
- `waveform_blocked`: fewer than two valid power points.
- `no_condition_post_iv_context`: waveform exists but no matching post-IV
  damage fingerprint for the condition.
- `ready_for_descriptive_matching`: waveform and condition-level post-IV
  context exist.

This is a good foundation. The issue is not the computation; it is that the
dashboard exposes too many intermediate columns directly.

### 2. Event-Level Features

View: `stress_waveform_event_features`

SC and avalanche waveform files are represented as `file_as_event` records.
Irradiation records come from `irradiation_single_event_energy_view` as
`detected_single_event` records.

For irradiation detected events the view carries:

- event type: `SEB`, `SELCI`, `SELCII`, `MIXED`, or unknown;
- event path type;
- event window start, peak, and end;
- integrated event terminal energy where comparable;
- rectangular proxy energy where only proxy energy exists;
- energy window basis, censoring reason, confidence, and energy level;
- Vds collapse and gate coupling;
- post-IV companion evidence inherited from the source irradiation file.

This layer matters because many irradiation events are censored or not
event-level comparable. The dashboard should make that caveat prominent before
showing proxy-candidate plots.

### 3. Per-Device Readiness And Gate-Zero

Views:

- `stress_proxy_readiness_view`
- `stress_proxy_gate_zero_view`

`stress_proxy_readiness_view` groups waveform files, detected events, and
post-IV damage fingerprints by `device_type`.

A device family becomes a `gate_zero_candidate` only when all of these are
true:

- `device_type` is not null;
- at least one SC or UID/UIS avalanche waveform has waveform plus post-IV
  overlap;
- at least one irradiation waveform or irradiation event has waveform plus
  post-IV overlap.

`stress_proxy_gate_zero_view` then counts candidate families. Gate-zero passes
only when `candidate_device_families >= 3`.

This is the dashboard's central decision. It should be the main first-screen
message, not just one KPI among many.

### 4. Stress Context

View: `stress_test_context_view`

This view normalizes and enriches event records:

- terminal stress energy from integrated event/file energy or commanded/stored
  energy;
- radiation deposited energy and dose from
  `radiation_stress_dose_summary_view`;
- device voltage and current ratings from `device_library`;
- normalized Vds and normalized current;
- active-volume energy density from `device_material_layers`;
- stress duration and effective stress time;
- stress regime labels: robustness, reliability, radiation, unknown;
- Figure 1(b) fields such as `figure1_regime_family`,
  `response_reversibility`, `soa_relation`, and `test_timescale_class`;
- context flags for missing ratings, missing energy, censored irradiation
  energy, missing radiation deposition, missing gate/collapse, and known
  avalanche normalized-Vds artifacts.

This view is useful for methodology and diagnostics. It is not the same as
readiness. Mixing these context plots into the readiness workflow is a major
source of dashboard confusion.

### 5. Candidate Generation And Ranking

View: `stress_proxy_candidate_view`

Targets are irradiation detected events in two tiers:

- `energy_comparable`: event-level comparable integrated terminal energy is
  available.
- `energy_censored_phenotype_only`: energy is censored or not comparable, so
  matching falls back to phenotype-only evidence and an optional floor.

Candidates are SC and avalanche stress records with positive terminal energy.

Candidate links are created in two scopes:

- `same_device`: candidate `device_type` matches target `device_type`.
- `cross_device`: no same-device candidate exists, but target and candidate
  have the same voltage class and enough comparable phenotype axes.

For energy-comparable targets, candidates are prefiltered to
`abs(ln(candidate_energy / target_energy)) <= 5.0`.

Distance terms:

- `log_energy_delta`: absolute natural-log energy ratio for comparable
  targets.
- `collapse_delta`: absolute Vds collapse mismatch.
- `gate_delta`: absolute gate-coupling mismatch.
- `normalized_vds_delta`: absolute normalized Vds mismatch, excluded for
  avalanche candidates because avalanche normalized Vds has a known clamp or
  scaling artifact.
- `duration_log_delta`: absolute natural-log duration mismatch.
- `path_penalty`: seeded mechanism/path compatibility penalty.

Phenotype distance is a normalized Euclidean distance over available phenotype
axes plus path penalty. Waveform distance adds log-energy distance for
energy-comparable targets plus a small duration term.

Damage evidence is joined from measured damage matches first, then predicted
damage matches. Candidates are classified into statuses such as:

- `measured_damage_candidate`
- `predicted_damage_candidate`
- `device_run_measured_candidate`
- `weak_measured_candidate`
- `waveform_only_candidate`
- `missing_damage_context`
- `phenotype_mismatch`
- `cross_device_screening_only`
- `analog_questionable`
- `inspect_manually`

The view keeps only the top 10 candidates per target event.

This logic is meaningful, but the dashboard makes users inspect the raw
mechanics instead of presenting a compact triage decision.

### 6. Candidate Summary

View: `stress_proxy_candidate_summary_view`

This view keeps rank-1 candidates only and groups by:

- target match tier;
- match scope;
- candidate source;
- target event type and path type;
- mechanism class;
- candidate status;
- replacement confidence.

It reports target-event counts, device counts, evidence-tier counts, median
screening distances, and device lists.

This should be a top-level triage summary, but the current table is still too
wide for quick reading.

### 7. Experiment Planning Queue

View: `stress_proxy_experiment_plan_view`

This view turns blockers into actions:

- same-device SC post-IV measurements when candidate records lack condition
  post-IV evidence;
- same-device avalanche post-IV measurements keyed by sample group;
- same-device electrical stress ladders when only cross-device screening
  exists;
- irradiation data recovery or campaigns for families that have electrical
  proxy evidence but no irradiation target coverage.

This is probably the most actionable section of the dashboard. It should be
near the top, immediately after the readiness matrix, and should have fewer
columns.

## Section-By-Section Usefulness Review

### Gate-Zero KPI Cards

Current charts:

- Gate Zero Pass KPI
- Gate Zero Candidate Families KPI
- Gate Zero Electrical Proxy Post-IV KPI
- Gate Zero Irradiation Post-IV KPI

Use: tell whether the database has enough device-family overlap to treat proxy
matching as more than descriptive screening.

Computation: one-row aggregate from `stress_proxy_gate_zero_view`. Pass means
at least three device families satisfy waveform plus post-IV overlap on both
electrical proxy and irradiation sides.

Assessment: keep, but make the status textual and explicit. A `0`/`1` pass KPI
is easy to miss. The first screen should say something like:

`Gate-zero fails: 0 of 3 required candidate device families. Main gap:
electrical proxy waveform plus post-IV overlap.`

### Device Coverage / Blocker Matrix

Use: identify which device families are blocked by missing SC/UID/UIS
waveforms, missing irradiation waveforms/events, missing electrical proxy
post-IV overlap, missing irradiation post-IV overlap, or missing device type.

Computation: per-device rollup in `stress_proxy_readiness_view`.

Assessment: keep, but reduce. The current table mixes decision columns with
many counts. A user needs:

- device type;
- readiness status;
- gate-zero candidate true/false;
- SC waveform count;
- UID/UIS waveform count;
- irradiation event count;
- electrical proxy waveform plus post-IV count;
- irradiation waveform/event plus post-IV count;
- comparable damage axis count;
- next required action.

Everything else should be hidden or moved to a detail table.

### Candidate Summary

Use: tell what the top-ranked proxy candidates look like in aggregate.

Computation: `stress_proxy_candidate_summary_view`, using only
`candidate_rank = 1`.

Assessment: useful but overloaded. It should be a compact count table or
stacked bar by `candidate_status`, `target_event_type`, and
`candidate_source`. Long `device_types` and `candidate_device_types` strings
are poor dashboard columns.

### Censored SEB Candidate Coverage

Use: isolate SEB events where energy cannot be compared directly and matching
is phenotype-only.

Computation: same summary view filtered to
`target_match_tier = 'energy_censored_phenotype_only'` and
`target_event_type = 'SEB'`.

Assessment: conceptually important, because SEB energy censoring is a known
failure mode. As implemented it duplicates the candidate summary table shape.
Replace it with a small diagnostic panel:

- count of censored SEB targets;
- count with any candidate;
- count with same-device candidates;
- top blockers;
- top candidate mechanism classes.

### Experiment Planning Queue

Use: decide what measurement, post-IV work, or data recovery should happen
next.

Computation: blocker-driven union in `stress_proxy_experiment_plan_view`.

Assessment: keep and move up. This is one of the few sections that directly
answers "what should we do next?" The current table has too many fields for
dashboard use. Show planning rank, priority tier, action type, measurement
device, measurement plan, affected target count, expected unlock, and
rationale. Hide recipe keys and repeated candidate-condition metadata by
default.

### Best Proxy Candidates

Use: inspect the best candidate for each target event.

Computation: `stress_proxy_candidate_view` filtered to `candidate_rank = 1`.

Assessment: keep as a triage table, but rebuild the column set. The current
table exposes dozens of radiation, energy, pulse-history, geometry, evidence,
and blocker fields. That is useful for export, not for a dashboard.

Recommended visible columns:

- target event key;
- target device;
- target event type;
- target match tier;
- target energy or censor reason;
- candidate source;
- candidate device;
- candidate condition;
- candidate status;
- replacement confidence;
- match scope;
- waveform distance;
- damage distance;
- combined distance;
- blockers.

### Candidate Evidence Detail

Use: debug why top-10 candidates received their statuses.

Computation: `stress_proxy_candidate_view` filtered to `candidate_rank <= 10`.

Assessment: demote to an audit/export table. It is too wide and too detailed
for the main dashboard. Keep it available as a diagnostic table or separate
Explore link, not as a main-scroll section.

### Target Vs Best Proxy Terminal Energy

Use: compare target event terminal energy to selected proxy terminal energy.

Computation: scatter from `stress_proxy_candidate_view`, top-ranked candidates
only, positive target and candidate energies.

Assessment: currently risky. The chart is configured with `log_x=True` and
`log_y=True`, but the script comment states that
`echarts_timeseries_scatter` only supports a log y-axis and that `"x"` or
`"both"` are treated as truthy y-axis settings. That means this chart is likely
not a true log-log energy comparison. For data spanning orders of magnitude,
that can make the plot visually misleading.

Recommendation: precompute `log10_target_energy_j` and
`log10_candidate_energy_j` in SQL, then plot those as linear axes, or use a
chart type that supports both log axes correctly.

### Energy Mismatch Vs Phenotype Mismatch

Use: show whether candidates are close in energy and waveform phenotype at the
same time.

Computation: x is `log_energy_delta`; y is `phenotype_distance`; top-ranked
candidates only.

Assessment: keep one version of this plot. It is the most interpretable
candidate scatter because it maps directly to the matching logic. Add visual
threshold lines for energy-out-of-range and phenotype-mismatch thresholds, and
filter or facet by candidate status so blocked/manual rows do not dominate the
reading.

### Waveform Vs Damage Distance

Use: compare waveform similarity to post-IV damage similarity.

Computation: x is `waveform_distance`; y is `best_damage_distance`;
top-ranked candidates only.

Assessment: useful as a validation diagnostic, not a primary readiness chart.
It should be shown only after the summary says whether measured or predicted
damage evidence exists. When damage evidence is missing, this plot becomes
sparse or misleading.

### Energy Density Ratio Vs Phenotype Mismatch

Use: explore whether local active-volume energy-density ratio explains
phenotype mismatch.

Computation: x is `energy_density_ratio`; y is `phenotype_distance`;
top-ranked candidates only.

Assessment: demote. This is a research diagnostic. It also has two
implementation issues:

- it uses `log_x=True`, which the script comments say is not actually a log
  x-axis for this chart type;
- it is missing from `candidate_chart_names`, so candidate native filters are
  not scoped to it.

### Normalized Observed V/I Stress Scatter

Use: show normalized stress space by test type.

Computation: x is observed `abs(Vds)` divided by device voltage rating; y is
peak drain current divided by current rating. Avalanche rows with
`normalized_vds > 1.60` are excluded by quality filter.

Assessment: useful context, but not readiness. Move to a separate "Stress
Context" or "Method Diagnostics" tab/dashboard.

### Blocking Bias Vs Terminal Electrical Energy

Use: show how terminal energy changes with normalized blocking bias.

Computation: x is normalized Vds; y is `stress_energy_j` on log y. Energy is
integrated `Vds * Id` where available or source-specific stored/commanded
energy.

Assessment: useful diagnostic, but it competes with the candidate energy plot.
Keep only if the dashboard has a diagnostics tab. Otherwise remove from the
primary readiness page.

### Blocking Bias Vs Average Terminal Power

Use: show terminal power severity versus normalized Vds.

Computation: x is normalized Vds; y is `stress_energy_j / stress_duration_s`
on log y.

Assessment: duplicative with the energy plot for most readiness decisions.
Demote to diagnostics.

### Irradiation Deposited Energy Vs Blocking Bias

Use: show radiation deposited energy by LET bin and blocking bias.

Computation: irradiation detected events only; deposited energy from
`radiation_stress_dose_summary_view`; positive deposited-energy rows only.

Assessment: valuable for scientific context, but not a proxy-readiness control.
Move to a radiation-energy context dashboard or tab.

### Irradiation Energy Amplification

Use: show terminal electrical energy divided by ion deposited energy.

Computation: irradiation detected events with positive terminal and deposited
energy; y is `electrical_terminal_energy_j / radiation_deposited_energy_j`.

Assessment: good explanatory plot. It helps users understand that the ion acts
as a trigger and the destructive energy comes from the bias circuit. It should
not sit in the middle of the operational readiness workflow.

### Figure 1(b) Stress Landscapes

Current charts:

- Stress vs Timescale Landscape
- Effective Stress-Time Landscape
- Destructive Outcomes
- Destruction Boundary by Device

Use: recreate or support the Kozak et al. style stress-regime landscape using
database records.

Computation: `stress_test_context_view` maps records to normalized Vds,
stress duration, effective cumulative stress time, stress regime family, and
response reversibility. `stress_destruction_boundary_view` groups destructive
and non-destructive normalized Vds by device and voltage class.

Assessment: move out of this dashboard. These charts are methodology and
publication-context material, not proxy-readiness operations. Keeping them in
the same scroll path makes the dashboard feel like a paper workspace rather
than a decision tool.

### Stress Test Context

Use: raw enriched stress-event table.

Computation: direct table over `stress_test_context_view`.

Assessment: remove from main dashboard. Keep as an export/detail table. It has
too many columns to be legible in Superset dashboard mode.

### Event Feature Coverage

Use: raw event feature audit table.

Computation: direct table over `stress_waveform_event_features`.

Assessment: remove from main dashboard. Keep as a data-quality drill-through
or separate QA dashboard.

## Cross-Cutting Legibility Problems

### 1. The Dashboard Has No Information Hierarchy

After the KPI row, almost every chart is full width. Users must infer which
sections are operational, diagnostic, or raw audit views. This is the main
reason the dashboard feels out of hand.

### 2. Wide Tables Are Doing Too Much Work

The dashboard includes multiple tables with dozens of columns. Superset table
rendering is not a good place to inspect radiation dose, waveform phenotype,
damage evidence, geometry, pulse history, blockers, and ranking details all at
once.

### 3. Global Table Cell Bars Add Visual Noise

`table_params()` enables `show_cell_bars=True` for every table. Cell bars can
help on a small numeric summary table, but they are noisy in wide mixed
semantic tables. They should be disabled by default and enabled only for
specific numeric summary views.

### 4. Native Filters Are Inconsistently Scoped

The generated chart groups omit some charts:

- `Proxy Readiness - Candidate Pairs: Energy Density Ratio vs Phenotype
  Mismatch` is not included in `candidate_chart_names`, so candidate filters
  do not apply to it.
- `Proxy Readiness - Event Feature Coverage` is not included in the context
  or readiness chart groups.
- `Proxy Readiness - Figure 1(b): Destruction Boundary by Device` is not in
  the context chart group.
- `Proxy Readiness - Experiment Planning Queue` has its own chart group, but
  the native filters do not target planning charts.

This creates a subtle usability bug: users can apply a filter and not realize
some charts did not update.

### 5. Log-X Scatter Settings Are Known To Be Wrong

The builder script explicitly comments that the selected Superset scatter
plugin only supports a log y-axis, and that attempted log x-axis values are
treated as the y-axis checkbox. Any chart relying on `log_x=True` should be
considered suspect until replaced with precomputed log columns or another chart
type.

### 6. Diagnostic Context Overwhelms The Readiness Story

The normalized V/I, energy, power, deposited-energy, amplification, and Figure
1(b) plots are not useless. They are just not part of the same user task as
"what blocks proxy readiness?" They should live in a diagnostics tab or
separate stress-context dashboard.

### 7. Candidate Confidence Is Not Visually Front-And-Center

The dashboard colors candidate points by status/evidence tier, but users still
have to know what statuses mean. A readiness dashboard should foreground:

- how many top candidates are measured damage supported;
- how many are predicted only;
- how many are waveform only;
- how many are blocked/manual;
- which blocker dominates.

### 8. The Current Page Understates Censoring And Evidence Limits

The SQL carefully carries energy-censoring, active-window confidence,
post-IV-evidence scope, and damage comparability. The dashboard does not make
these caveats prominent enough before showing candidate plots.

## Recommended Target Dashboard

Create a smaller operational dashboard with four sections.

### Section 1: Readiness Status

Keep:

- gate-zero status card, textual;
- candidate family count;
- electrical proxy plus post-IV family count;
- irradiation plus post-IV family count.

Add:

- "top blocker" card;
- "next experiment unlock" card from the planning queue.

### Section 2: Coverage And Blockers

Keep:

- reduced Device Coverage / Blocker Matrix.

Add:

- blocker-count bar chart by `proxy_readiness_status`;
- optional device filter scoped to all charts in this section.

### Section 3: Action Queue

Keep:

- reduced Experiment Planning Queue.

Recommended visible columns:

- planning rank;
- priority tier;
- action type;
- measurement device type;
- measurement plan;
- affected target count;
- expected unlock;
- planning rationale.

### Section 4: Candidate Triage

Keep:

- compact Candidate Summary;
- reduced Best Proxy Candidates table;
- one scatter: Energy Mismatch vs Phenotype Mismatch.

Optional:

- Waveform vs Damage Distance as a diagnostic chart when measured or predicted
  damage evidence exists.

Remove from this operational dashboard:

- raw Candidate Evidence Detail;
- raw Stress Test Context;
- raw Event Feature Coverage;
- normalized V/I context plots;
- blocking bias vs energy/power context plots;
- radiation deposited energy and amplification plots;
- Figure 1(b) stress landscape plots;
- destruction boundary table.

Those removed charts should move to a separate "Proxy Method Diagnostics" or
"Stress Context" dashboard.

## Concrete Implementation Recommendations

1. Split the current dashboard into two dashboards:
   - `Proxy Readiness - Operational`
   - `Proxy Readiness - Method Diagnostics`

2. Add simplified SQL views for dashboard tables instead of selecting raw
   feature columns directly:
   - `stress_proxy_readiness_dashboard_view`
   - `stress_proxy_candidate_triage_view`
   - `stress_proxy_planning_dashboard_view`

3. Fix log-axis plots by adding explicit log columns in SQL:
   - `target_energy_log10_j`
   - `candidate_energy_log10_j`
   - `energy_density_ratio_log10`

4. Fix native filter scope:
   - include all candidate charts in `candidate_chart_names`;
   - either scope planning charts to relevant filters or make them explicitly
     unfiltered;
   - include destruction boundary and event feature coverage in a diagnostics
     group if they remain on any dashboard.

5. Disable global table cell bars. Enable them only for compact numeric
   summary tables.

6. Add explicit threshold annotations to candidate-distance plots:
   - energy out-of-range threshold;
   - phenotype mismatch threshold;
   - waveform status thresholds where meaningful.

7. Rename chart titles around user questions rather than implementation nouns:
   - "Which device families block gate-zero?"
   - "What measurement unlocks the most targets?"
   - "Which top candidates are evidence-supported?"
   - "Where do top candidates fail: energy, phenotype, or damage?"

8. Put raw audit tables behind drill-through links or in a diagnostics
   dashboard. They should not be first-order dashboard charts.

## Bottom Line

The dashboard should not be judged as "bad data." The data model is much more
careful than the current visual presentation. The problem is that the dashboard
is presenting raw lineage, candidate ranking, physics diagnostics, and planning
actions in one undifferentiated scroll.

The highest-value redesign is to make the dashboard a gate-zero and planning
tool first. It should say whether proxy readiness passes, why it fails, what
measurement closes the largest gap, and which candidates are worth inspecting.
Everything else belongs in a diagnostics surface.
