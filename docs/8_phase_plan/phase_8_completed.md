# Phase 8 Completed - Experiment Planning Queue

Date completed: 2026-06-11

Commit: `Add proxy experiment planning queue` (this commit)

## Scope

Phase 8 implemented the F7 experiment-planning artifact from the proxy-method
plan. This phase does not change candidate scoring, status thresholds, or target
admission. It adds a planning view and dashboard table that rank which lab or
data-recovery action removes the most important current blockers.

The work added:

1. `stress_proxy_experiment_plan_view` in `schema/025_proxy_readiness_waveforms.sql`.
2. A new Superset dataset and table chart, `Proxy Readiness - Experiment Planning Queue`.
3. Planning rows that separate strict same-device evidence upgrades from
   cross-device screening follow-ups and irradiation/data-recovery gaps.

## Planning Arms

The view is materialized and rebuilt with the rest of schema 025. It has three
main planning arms:

| Planning arm | Source evidence | Why it exists |
| --- | --- | --- |
| `same_device_candidate_post_iv` | Same-device candidates with `candidate_missing_condition_post_iv`. | Finds existing SC/avalanche conditions where post-IV IdVg/IdVd/blocking would add measured damage evidence. |
| `same_device_uis_ladder_post_iv` / `same_device_sc_ladder_post_iv` / `same_device_electrical_ladder_post_iv` | Cross-device rows capped by `cross_device_voltage_class_screening_only`. | Identifies target families that need their own electrical stress ladder plus post-IV to remove the cross-device ceiling. |
| `irradiation_data_recovery` | `stress_proxy_readiness_view` families with electrical proxy + post-IV but no irradiation targets. | Surfaces the SCT2080KE-style gap: proxy evidence exists, but there is no irradiation side to compare against. |

Two details matter for correctness:

- Same-device SC post-IV rows are grouped by device + SC voltage/duration,
  because damage matching keys SC exact condition by those fields.
- Same-device avalanche post-IV rows are grouped by device + avalanche sample
  group, because the avalanche damage join treats sample group as the exact
  post-IV companion scope.

Cross-device candidate post-IV is not treated as a strict evidence upgrade. For
cross-device rows, the useful measurement is same-device stress on the target
family, because Phase 6 deliberately keeps cross-device rows capped at
`cross_device_screening_only`.

## Columns Added

`stress_proxy_experiment_plan_view` exposes:

| Column | Meaning |
| --- | --- |
| `planning_rank` | Overall queue order. |
| `planning_priority_tier` | Coarse action class priority before row counts. |
| `plan_source` | `candidate_blocker` or `readiness_gap`. |
| `plan_action_type` | Action family, e.g. `same_device_candidate_post_iv`. |
| `primary_blocker` | Blocker or readiness gap the action addresses. |
| `measurement_device_type` | Device family to measure or recover. |
| `measurement_plan` | Human-readable lab/data action. |
| `measurement_recipe_key` | Stable grouping key for the action. |
| `candidate_source` | SC, avalanche, or irradiation/data-recovery source. |
| `candidate_*` fields | Candidate condition fields when the action is tied to an existing candidate. |
| `pair_count` | Candidate rows directly addressed by the action. |
| `affected_target_count` | Distinct target records affected. |
| `affected_*` fields | Target families, event types, ions, statuses, and mechanisms summarized. |
| `cross_device_pair_count` | Cross-device screening rows addressed. |
| `potential_proxy_record_count` | Readiness-gap proxy records for no-target-family actions. |
| `expected_unlock` | What the measurement unlocks. |
| `planning_rationale` | Why this action is the right interpretation of the blocker. |

Indexes were added on planning rank, action type, measurement device, and primary
blocker.

## Rebuild and Dashboard Update

Schema rebuild:

```bash
/home/arodrigues/aps_venv/bin/python data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
```

Dashboard metadata update:

```bash
/home/arodrigues/aps_venv/bin/python data_processing_scripts/create_proxy_readiness_dashboard.py --skip-schema
```

The dashboard update succeeded. It created Superset dataset
`stress_proxy_experiment_plan_view` with id `119`, created chart
`Proxy Readiness - Experiment Planning Queue` with id `418`, updated dashboard
id `32`, and associated all 21 charts with the dashboard.

## Verification SQL Results

Planning view shape:

| Metric | Value |
| --- | ---: |
| Rows | 61 |
| First rank | 1 |
| Last rank | 61 |
| Rows missing required plan fields | 0 |

Planning action summary:

| Action | Primary blocker | Plan rows | Pair count | Affected targets | Cross-device pairs | Potential proxy records |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `same_device_candidate_post_iv` | `candidate_missing_condition_post_iv` | 30 | 1116 | 1111 | 0 | 0 |
| `same_device_uis_ladder_post_iv` | `cross_device_voltage_class_screening_only` | 5 | 184 | 33 | 184 | 0 |
| `same_device_sc_ladder_post_iv` | `cross_device_voltage_class_screening_only` | 6 | 147 | 23 | 147 | 0 |
| `same_device_electrical_ladder_post_iv` | `cross_device_voltage_class_screening_only` | 19 | 10353 | 2106 | 10353 | 0 |
| `irradiation_data_recovery` | `missing_irradiation_waveforms_or_events` | 1 | 0 | 0 | 0 | 2 |

Count reconciliation against source blockers:

| Check | Source rows | Planned rows |
| --- | ---: | ---: |
| Same-device actionable post-IV rows | 1116 | 1116 |
| Cross-device screening rows | 10684 | 10684 |

Top planning rows match the F7 shortlist:

| Rank | Action | Measurement plan | Pair count | Targets |
| ---: | --- | --- | ---: | ---: |
| 1 | `same_device_candidate_post_iv` | Post-IV after existing `C2M0080120D` SC 600 V / 6 us | 178 | 178 |
| 2 | `same_device_candidate_post_iv` | Post-IV after existing `C2M0080120D` SC 600 V / 7 us | 177 | 177 |
| 3 | `same_device_candidate_post_iv` | Post-IV after existing `C2M0080120D` SC 600 V / 8 us | 177 | 177 |
| 7 | `same_device_candidate_post_iv` | Post-IV after existing `C2M0080120D` SC 600 V / 2 us | 39 | 39 |
| 9 | `same_device_candidate_post_iv` | Post-IV after existing `C2M0080120D` UIS avalanche sample `d3` | 4 | 3 |
| 31 | `same_device_uis_ladder_post_iv` | UIS ladder + post-IV on `IFX-Trench` for SEB targets | 110 | 14 |
| 32 | `same_device_uis_ladder_post_iv` | UIS ladder + post-IV on `CPM3-1200-0075A` for SEB targets | 40 | 4 |
| 45 | `irradiation_data_recovery` | Irradiation campaign or archived irradiation data recovery for `SCT2080KE` | 0 | 0 |

The IFX-Trench UIS row covers Au/C/Ca/Fe/Kr SEB targets and references the
current cross-device avalanche families (`C2M0080120D`, `C3M0075120K`,
`SCT2080KE`, `SCT3080KL`). The SCT2080KE irradiation row carries
`potential_proxy_record_count = 2`, matching the readiness view's electrical
proxy + post-IV coverage.

Reporting-only invariant after Phase 8: candidate status distribution is
unchanged from Phase 7.

| Match scope | Status | Rows | Targets |
| --- | --- | ---: | ---: |
| `cross_device` | `cross_device_screening_only` | 10684 | 1115 |
| `cross_device` | `phenotype_mismatch` | 486 | 297 |
| `same_device` | `analog_questionable` | 1182 | 119 |
| `same_device` | `device_run_measured_candidate` | 8 | 1 |
| `same_device` | `inspect_manually` | 238 | 25 |
| `same_device` | `measured_damage_candidate` | 2 | 1 |
| `same_device` | `missing_damage_context` | 178 | 19 |
| `same_device` | `phenotype_mismatch` | 12 | 2 |
| `same_device` | `waveform_only_candidate` | 191 | 20 |
| `same_device` | `weak_measured_candidate` | 19 | 3 |

## Checks

Passed:

```bash
/home/arodrigues/aps_venv/bin/python -m py_compile \
  data_processing_scripts/create_proxy_readiness_dashboard.py \
  data_processing_scripts/extract_stress_pulse_history.py
/home/arodrigues/aps_venv/bin/python -m pytest tests/ -q
git diff --check
```

Full pytest result: `20 passed`.

## Known Follow-Ups

- `same_device_electrical_ladder_post_iv` rows are numerous because cumulative
  SELC-style cross-device rows dominate the candidate pool. They are deliberately
  lower priority than SEB/UIS rows because the mechanism table already marks
  cumulative leakage analogs as questionable.
- The dashboard now contains the planning table, but there is still no separate
  dashboard tab system in this script; layout remains a single packed dashboard
  with the planning queue inserted after candidate-summary coverage.

## Final State

- Branch: `master`
- Code commit: `Add proxy experiment planning queue` (this commit)
- Working tree after commit: clean
