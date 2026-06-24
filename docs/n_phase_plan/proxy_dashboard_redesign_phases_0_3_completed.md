# Proxy Dashboard Redesign - Phases 0-3 Completed

Date completed: 2026-06-17

Plan: `docs/proxy_readiness_dashboard_redesign_plan_2026-06-17.md`
Reviews: `docs/proxy_readiness_dashboard_review_2026-06-17.md`,
`docs/proxy_readiness_dashboard_audit_2026-06-17.md`

Implemented in: `data_processing_scripts/create_proxy_readiness_dashboard.py`

Dashboard: `Proxy Readiness - Waveform Failure Features`
Slug: `proxy-readiness-waveforms`
Live dashboard id: 32

## Executive Summary

The redesign was a presentation and dashboard-governance fix, not a data-model
rewrite. The underlying proxy-readiness SQL was already doing the hard work:
extracting waveform/event features, computing readiness blockers, ranking proxy
candidates, and building an experiment-planning queue. The dashboard had become
difficult to use because it placed all of those outputs into one flat scroll of
KPIs, very wide tables, and diagnostics plots without a clear reading order.

The main change was to make the dashboard answer the operational question first:
whether the project has enough overlapping electrical-proxy and irradiation
evidence to treat SC/UIS stress as a proxy for irradiation single-event
failures. Only after that does the dashboard expose candidate triage,
method-diagnostic plots, and raw QA tables.

No SQL views were changed for Phases 0-3. That was intentional. The audit found
that the data lineage was coherent, while the dashboard surface had lost
hierarchy and legibility. Keeping the change in the Superset builder avoided
mixing a UX cleanup with modeling changes such as new SOA scores or new
dashboard-only SQL views.

## Why This Was Needed

The dashboard was trying to serve several jobs at the same time:

| Job | User question | Type |
| --- | --- | --- |
| Gate readiness | Do we have enough overlapping evidence to attempt proxy matching? | Operational |
| Blocker matrix | Which device families are blocked, and why? | Operational |
| Action queue | What measurement should be done next to unlock readiness? | Operational |
| Candidate triage | Which electrical proxy best matches each irradiation event? | Operational / analytical |
| Method diagnostics | How do stress energy, dose, bias, timescale, and Figure 1(b) context behave? | Research / diagnostic |
| Raw QA | What rows and fields back the charts? | Audit / export |

Before the redesign, these jobs were interleaved in one long dashboard. That
made the first screen misleading: four lookalike KPI cards appeared before a
large sequence of scatter plots and raw tables. The most important fact - the
gate-zero result - was visually just one small number among many. The later
plots gave the impression that the method was ready for detailed proxy
selection even though the gate-zero evidence did not yet pass.

The screenshots and source review also showed a governance problem: the live
Superset dashboard had drifted from the committed builder. Some 11:21 live
charts were manual Superset changes that did not exist in the repository. A
builder redeploy would have overwritten them. The redesign therefore made the
committed builder script canonical again.

## What Was Deliberately Left Unchanged

- `schema/025_proxy_readiness_waveforms.sql` was not changed. Phases 0-3 only
  reorganized and clarified existing outputs.
- The dashboard slug stayed `proxy-readiness-waveforms`, so existing links keep
  working.
- Existing chart titles were mostly preserved. Renaming charts in Superset can
  create duplicate/orphan chart records depending on how the builder resolves
  chart names. The readability work was done through tabs, descriptions, and
  ordering instead.
- `soa_axis_score` was not added. The live dashboard had a manual chart that
  queried this missing column, but adding a numeric SOA severity axis is a
  modeling decision. The available view has categorical `soa_relation`; it does
  not currently define a defensible numeric severity score.
- Thin dashboard-specific SQL views were deferred to optional Phase 4. They may
  still be useful later, but they were not necessary to make the dashboard
  usable.

## Data Context: What The Dashboard Is Showing

The dashboard is built from the materialized views registered in
`DATASET_TABLES` in `create_proxy_readiness_dashboard.py`:

| Dataset key | View | Dashboard role |
| --- | --- | --- |
| `gate_zero` | `stress_proxy_gate_zero_view` | One-row gate-zero verdict: pass/fail, candidate-family count, electrical overlap count, irradiation overlap count. |
| `readiness` | `stress_proxy_readiness_view` | Per-device-family blocker matrix and coverage rollup. |
| `experiment_plan` | `stress_proxy_experiment_plan_view` | Ranked next measurements/actions that unlock the largest blocker gaps. |
| `candidates` | `stress_proxy_candidate_view` | Per-target proxy candidates, distances, statuses, blockers, evidence tiers, and ranks. |
| `candidate_summary` | `stress_proxy_candidate_summary_view` | Aggregated rank-1 candidate counts and medians for triage. |
| `context` | `stress_test_context_view` | Normalized stress, energy, timescale, radiation, and Figure 1(b) diagnostic context. |
| `destruction_boundary` | `stress_destruction_boundary_view` | Per-device destructive/survived normalized-VDS boundary rollup. |
| `event_features` | `stress_waveform_event_features` | Raw event-level feature coverage for QA/export. |

The key computation behind the first screen is `stress_proxy_gate_zero_view`.
Gate-zero passes only when at least three device families have both sides of the
evidence bridge:

- electrical-proxy side: SC or UID/UIS avalanche waveform evidence plus post-IV
  damage overlap;
- irradiation side: irradiation waveform/event evidence plus post-IV damage
  overlap.

At the time of the redesign, the live dashboard showed the important asymmetry:
0 candidate families, 1 electrical-proxy-plus-post-IV family, and 7
irradiation-plus-post-IV families. That means the dashboard should not lead with
candidate scatter plots. It should lead with the fact that the gate fails
because the two evidence pools do not overlap enough yet.

## Phase 0 - Reconcile Drift And Remove Broken Panels

### What changed

- Removed `Candidate Pairs: Target vs Best Proxy Terminal Energy`.
- Rebuilt `Candidate Pairs: Energy Density Ratio vs Damage Signature Mismatch` so the
  energy-density ratio is on the working log y-axis.
- Did not port the live-only `soa_axis_score` chart.
- Confirmed the Figure 1(b) FORMULA annotation layers include the fields
  Superset requires (`showMarkers` and `hideLine`).
- Treated the committed builder as the canonical dashboard definition. Manual
  live-only charts were not preserved as hidden Superset state.

### Why

The removed terminal-energy scatter used `echarts_timeseries_scatter` as if it
could render a log x-axis. In this Superset chart type, the `logAxis` setting
effectively works for the y-axis; `log_x=True` did not produce the intended
axis. A chart that appears quantitative but renders on the wrong scale is worse
than no chart because it can cause the viewer to reason from a false visual
relationship.

The energy-density chart had a related problem. Its information was potentially
useful, but the log transform was assigned to the ineffective axis. The fix was
to swap axes so damage signature distance is on x and energy-density ratio is on y,
where the chart can use a real log scale.

The live `soa_axis_score` panel was removed instead of repaired because the
column was not part of the SQL view. The review treated that as a boundary
between dashboard cleanup and modeling work. A severity score may be useful,
but it needs an explicit definition and validation, not an ad hoc dashboard
field.

The Figure 1(b) annotation check was included because an earlier screenshot
showed a Superset annotation error. The builder already had the required
annotation-layer fields, so the safe action was to preserve that known-good
definition and avoid adding new formula lines for unrelated thresholds.

## Phase 1 - Lead With The Gate-Zero Answer

### What changed

- Added Markdown component support to `build_dashboard_layout()`.
- Added a top-of-dashboard markdown panel explaining the question, the gate
  rule, and the tab order.
- Replaced the old row of four similar KPI cards with:
  - one `Candidate Families` big-number KPI;
  - one `Gate Zero Status` table with the bottleneck counts and device types.
- Added `Next Measurements (Top 3)` from `stress_proxy_experiment_plan_view`.
- Moved the full `Experiment Planning Queue` directly under the verdict block.

### Why

The old KPI row showed the right numbers but did not explain their relationship.
The viewer had to know that the candidate-family count, electrical overlap
count, and irradiation overlap count were different parts of the same readiness
gate. The redesign makes that relationship explicit: the big number states how
many candidate families exist, while the adjacent table shows why the gate does
or does not pass.

The next-measurement callout was promoted because the dashboard should not stop
at "fail." If the gate fails, the useful operational question is what data
collection would unlock progress. That answer already exists in
`stress_proxy_experiment_plan_view`, so the redesign moved it into the first
screen rather than leaving it below candidate and diagnostic material.

The markdown panel was added because the dashboard needs a small amount of
domain framing. This is not explanatory decoration; it prevents a common
misread where users jump straight into candidate plots before confirming that
the underlying evidence bridge exists.

## Phase 2 - Add Workflow Tabs And Trim Wide Tables

### What changed

- Rewrote `build_dashboard_layout()` to emit Superset `TABS` and `TAB` layout
  nodes.
- Each chart definition now carries `tab` and `group` metadata.
- Added four tabs:
  - `Readiness & Actions`
  - `Candidate Triage`
  - `Method Diagnostics`
  - `Raw / QA`
- Trimmed the operational tables:
  - blocker matrix: 14 columns to 9;
  - best-proxy table: roughly 95 columns to 16;
  - planning queue: 27 columns to 8;
  - candidate summary: 19 columns to 12;
  - censored-SEB coverage: compact 6-column panel.
- Changed `table_params()` so `show_cell_bars` defaults to `False`.

### Why

Tabs separate different user workflows. The readiness tab answers "can we move
forward and what should happen next?" The candidate tab answers "what proxies
look viable?" The diagnostics tab answers "what method and physics context
support or challenge the proxy approach?" The raw tab preserves auditability
without forcing every user to scroll through export-grade tables.

The table trimming was the largest legibility improvement. Many old tables were
technically rich but operationally unreadable: dozens of heterogeneous columns
mixed IDs, derived distances, evidence flags, radiation quantities, and
debugging fields in the same viewport. The redesign keeps the columns needed
for the tab's workflow and moves full-detail rows to `Raw / QA`.

Cell bars were disabled by default for the same reason. In wide mixed-semantic
tables, cell bars make the table visually busier without adding a reliable
ranking cue. Numeric magnitude is still available in the cells, but the
dashboard no longer implies that every numeric column should be visually
compared in the same way.

## Phase 3 - Make Candidate Triage Honest And Fix Filter Scope

### What changed

- The operational `Energy Mismatch vs Damage Signature Mismatch` scatter now shows
  rank-1, decision-driving candidates and genuine failure modes.
- The full all-status version moved to `Method Diagnostics` as
  `Energy vs Damage Signature (All Statuses, Diagnostic)`.
- Thresholds are described in chart descriptions instead of drawn as formula
  annotation lines:
  - damage signature mismatch cutoff = 2.50;
  - energy out-of-range cutoff = `|log(proxy/target energy)| = 4.0`.
- `Waveform vs Damage Distance` now explicitly says that rows require measured
  or predicted post-IV damage evidence.
- `select_filter()` now supports multiple dataset targets.
- Native filters now set `tabsInScope`, so filters appear only on tabs where
  their scoped charts live.
- The Device Type filter now targets:
  - `candidates.device_type`;
  - `context.device_type`;
  - `readiness.device_type`;
  - `experiment_plan.measurement_device_type`;
  - `destruction_boundary.device_type`.

### Why

The original candidate scatter mixed operational candidates with a large
cross-device and waveform-only screening cloud. That cloud is diagnostically
important, but it visually overstated readiness when shown as the main triage
plot. The redesigned operational scatter keeps statuses that can drive a
replacement decision or expose a real top-candidate failure, and moves the full
cloud to diagnostics where it can be interpreted as screening behavior.

The threshold lines were kept in descriptions rather than drawn on the chart
because the observed damage signature range was well below the 2.50 cutoff. Drawing a
line far outside the visible data would distort the autoscaled axis and reduce
legibility. Descriptions preserve the decision rule without damaging the plot.
This also avoids reintroducing Superset annotation-layer fragility.

The filter change fixed a real dashboard correctness problem. Before this work,
a single native Device Type filter could not apply cleanly across datasets that
used different column names for the same concept. The planning queue uses
`measurement_device_type`, while most other views use `device_type`. The
multi-target filter support makes one visible filter behave consistently across
readiness, planning, candidate, context, and device-boundary panels.

## Post-Review Fixes

Two issues were found after the first Phases 0-3 implementation review and were
fixed.

### Destruction Boundary now responds to Device Type

`Figure 1(b): Destruction Boundary by Device` is a per-device diagnostic table.
It sits on `Method Diagnostics`, where the Device Type filter is visible. In
the first implementation pass it had `group=None`, so it silently ignored the
filter. That was misleading: a visible filter should not leave a per-device
panel unchanged unless the dashboard clearly marks the panel as global.

The fix added a dedicated `device_only` group, scoped the destruction-boundary
chart into that group, and added `destruction_boundary.device_type` as a Device
filter target. The Device Type filter now has five dataset targets and includes
the destruction-boundary chart in scope.

### Energy failures are included in the operational scatter

The first implementation filtered the operational energy-vs-damage signature scatter
to decision statuses but accidentally left out `energy_out_of_range`, even
though the chart description cited the energy cutoff. That would have made the
operational view look cleaner than the ranking logic if a top-ranked candidate
failed specifically on energy.

The fix added `energy_out_of_range` to `DECISION_STATUS_SQL`. Current data had
zero rank-1 rows with that status at the time of verification, so the point
count did not change, but the chart is now semantically correct for future
data.

## How To Use The Redesigned Dashboard

### Readiness & Actions

Start here. This tab answers whether proxy readiness currently passes and what
to do next.

- The markdown panel states the gate rule so the numbers have context.
- `Candidate Families` shows the count that must reach 3.
- `Gate Zero Status` shows the full one-row evidence balance: current status,
  candidate-family count, electrical-proxy overlap count, irradiation overlap
  count, and candidate device types.
- `Next Measurements (Top 3)` is the short action list. It is intended for the
  next lab or data-recovery decisions.
- `Experiment Planning Queue` keeps the broader ranked backlog.
- `Device Coverage / Blocker Matrix` explains which families are blocked and
  which coverage category is missing.

### Candidate Triage

Use this tab after checking gate readiness. It answers which proxy candidates
look most plausible and why.

- `Candidate Summary` gives grouped counts and median distances for rank-1
  candidates.
- `Censored SEB Candidate Coverage` isolates energy-censored SEB rows, where
  damage-signature-only matching is the available comparison mode.
- `Best Proxy Candidates` shows the compact rank-1 candidate table with the
  fields needed to triage status, confidence, scope, waveform distance, damage
  distance, combined distance, and blockers.
- `Energy Mismatch vs Damage Signature Mismatch` is the operational scatter. It is not
  a complete screening cloud; it is the candidate-decision view.
- `Waveform vs Damage Distance` only contains candidates with measured or
  predicted damage evidence, so sparsity means missing damage context rather
  than absence of proxy candidates.

### Method Diagnostics

Use this tab to inspect the method and physics context, not to make the first
readiness decision.

- The all-status energy-vs-damage signature scatter preserves the full screening cloud.
- The energy-density ratio plot remains available with a working log y-axis.
- Normalized V/I, bias-vs-energy, bias-vs-power, deposited-energy, and
  amplification plots show how electrical and radiation stress quantities
  compare.
- Figure 1(b) plots preserve the stress-vs-timescale framing and reference
  lines.
- The destruction-boundary table summarizes empirical destructive/survived
  normalized-VDS boundaries by device and voltage class.

### Raw / QA

Use this tab for drill-through and export. It intentionally contains the wide
tables that were removed from the operational tabs.

- `Candidate Evidence Detail` keeps top-10 candidate evidence fields.
- `Stress Test Context` keeps the full normalized context rows.
- `Event Feature Coverage` keeps event-level coverage and quality flags.

The Raw / QA tab is deliberately excluded from the Device Type filter's
`tabsInScope`. Those tables are full export/audit surfaces, and the filter is
not shown on that tab. This avoids a visible-but-inert filter and preserves
full-row QA access.

## Implementation Notes For Future Maintainers

- The builder remains the source of truth. Manual Superset edits should be
  treated as temporary unless they are ported back to
  `create_proxy_readiness_dashboard.py`.
- Chart definitions now return an 8-field tuple:
  `name`, `dataset_id`, `viz_type`, `params`, `width`, `height`, `tab`, `group`.
- Layout placement comes from `tab`; filter scope comes from `group`.
- `MARKDOWN_PANELS` are layout-only components. They are not Superset charts
  and should not be passed through chart creation.
- Native filter scopes are derived from chart groups, not chart titles. This is
  more robust because chart titles may eventually be renamed.
- `select_filter()` accepts multiple `(dataset_id, column)` targets. Use this
  when a single user concept appears under different column names across views.
- Avoid new `log_x=True` scatter charts with `echarts_timeseries_scatter`.
  Reshape the chart so the log-scaled quantity is on y, or add a precomputed
  log column in optional Phase 4.
- Keep threshold markers in descriptions unless the threshold sits inside the
  useful plotted range and the Superset annotation layer has been verified.
- Add new raw-detail columns to `Raw / QA` before adding them back to
  operational tables.

## Deployment Evidence

The dashboard was redeployed with Superset metadata updates only during the
iteration loop:

```bash
cd data_processing_scripts
APS_DASHBOARD_EXPORT=0 /home/arodrigues/aps_venv/bin/python \
    create_proxy_readiness_dashboard.py --skip-schema
```

Verified live state after implementation:

- `position_json` contains one `TABS` node, four `TAB` nodes, one `MARKDOWN`
  node, 24 `CHART` nodes, and 24 `ROW` nodes.
- Tab labels are `Readiness & Actions`, `Candidate Triage`,
  `Method Diagnostics`, and `Raw / QA`.
- Fourteen native filters are saved.
- Device Type is configured as a multi-target filter across five dataset
  columns and scopes the readiness, planning, candidate, context, and
  destruction-boundary panels.
- Gate numbers were unchanged by the dashboard redesign:
  `gate_zero_fail_current_state`, 0 candidate families,
  1 electrical-proxy-plus-post-IV family, and
  7 irradiation-plus-post-IV families.
- The operational energy-vs-damage signature scatter had 39 decision points.
- The all-status diagnostic scatter had 819 points.

## Orphaned Chart Cleanup

After confirming they were no longer referenced by any dashboard other than id
32, the removed KPIs, the removed energy scatter, and the uncommitted live 11:21
rework charts were deleted from Superset. Live `Proxy Readiness -` charts now
match the 24 charts defined by the builder, with zero known orphans.

This cleanup matters because orphan charts create ambiguity. If unused charts
remain in Superset with similar titles, future maintainers can accidentally
edit the wrong chart or assume a manual chart is still part of the live
dashboard. The builder and live chart inventory now agree.

## Remaining Work

- Capture fresh browser screenshots for each tab and replace
  `docs/readiness_screenshots/` with the current redesigned state.
- Optional Phase 4: add thin dashboard SQL views or precomputed log columns only
  if the builder-side trims become hard to maintain or if additional chart
  types require derived display fields.
- Optional future cleanup: rename chart titles into a clearer tab-specific
  naming scheme after deciding how to handle Superset chart identity and orphan
  cleanup in the same deployment.
