# Phase 5 Completed - Calibration Harness and Avalanche Bias-Axis Fix

Date completed: 2026-06-11

Commit: `Add proxy distance calibration harness` (this commit)

## Scope

Phase 5 implemented the calibration harness from the proxy-method plan and
fixed the Phase 4 physics regression where avalanche clamp voltage was treated
as a knob-like normalized-bias axis. The change keeps Phase 4 normalized VDS
scoring for SC candidates, but omits `normalized_vds_delta` for avalanche
candidates because UIS clamp voltage is device breakdown physics rather than a
chosen test condition.

The phase also resolves two carried-over housekeeping items:

1. `ingestion_avalanche.py --remap-only --dry-run` now skips schema writes,
   row updates, commits, and avalanche-view refreshes while reporting would-be
   update counts.
2. `docs/8_phase_plan/` is now intended to be committed as the rollout docs of
   record.

## Distance Settings Table

`schema/025_proxy_readiness_waveforms.sql` now owns a one-row
`stress_proxy_distance_settings` table. The seeded `default` row mirrors the
pre-Phase-5 hard-coded constants:

| Setting | Default |
| --- | ---: |
| `max_energy_log_delta` | 5.00 |
| `collapse_delta_scale` | 0.25 |
| `gate_delta_scale` | 0.20 |
| `normalized_vds_delta_scale` | 0.15 |
| `energy_log_weight` | 1.00 |
| `same_path_penalty` | 0.00 |
| `path_unknown_penalty` | 0.25 |
| `path_mismatch_penalty` | 0.75 |
| `duration_log_weight` | 0.01 |
| `best_damage_distance_fallback` | 2.50 |
| `energy_out_of_range_log_delta` | 4.00 |
| `phenotype_mismatch_distance` | 2.50 |
| `measured_exact_waveform_max` | 1.75 |
| `predicted_waveform_max` | 1.75 |
| `device_run_waveform_max` | 2.25 |
| `weak_waveform_max` | 3.00 |
| `waveform_only_max` | 1.25 |
| `high_confidence_combined_max` | 1.50 |

`stress_proxy_candidate_view` cross joins the default row and exposes
`distance_setting_name` so candidate rows can be audited against the settings
used to score them. The Proxy Readiness candidate tables now display that
column.

## Avalanche Normalized-VDS Fix

For avalanche candidates, the candidate view now sets:

```text
normalized_vds_delta = NULL
```

Those rows receive the explicit blocker:

```text
normalized_vds_axis_excluded_avalanche_clamp
```

Avalanche rows with `normalized_vds > 1.60` are also tagged for quality review
with:

```text
avalanche_normalized_vds_above_quality_limit
candidate_avalanche_normalized_vds_above_quality_limit
```

This preserves the pairwise axis-omission design instead of forcing avalanche
clamp voltage into the same axis as irradiation bias and SC bus voltage.

## Candidate Distribution

Before applying Phase 5, the live database had the Phase 4 regression:

| Candidate status | Rows |
| --- | ---: |
| `analog_questionable` | 1183 |
| `inspect_manually` | 237 |
| `missing_damage_context` | 180 |
| `phenotype_mismatch` | 21 |
| `waveform_only_candidate` | 180 |
| `weak_measured_candidate` | 29 |

After Phase 5:

| Candidate status | Rows |
| --- | ---: |
| `analog_questionable` | 1182 |
| `device_run_measured_candidate` | 8 |
| `inspect_manually` | 238 |
| `measured_damage_candidate` | 2 |
| `missing_damage_context` | 178 |
| `phenotype_mismatch` | 12 |
| `waveform_only_candidate` | 191 |
| `weak_measured_candidate` | 19 |

Candidate row preservation held: `1830` displayed candidate rows across `183`
targets before and after.

## SEB Regression Recovery

Phase 4 rank-1 SEB candidates were all SC rows:

| Candidate source | Status | Rank-1 rows |
| --- | --- | ---: |
| `sc` | `waveform_only_candidate` | 18 |
| `sc` | `weak_measured_candidate` | 4 |

After Phase 5:

| Candidate source | Status | Rank-1 rows |
| --- | --- | ---: |
| `avalanche` | `measured_damage_candidate` | 1 |
| `sc` | `waveform_only_candidate` | 18 |
| `sc` | `weak_measured_candidate` | 3 |

The named calibration regression check also reports:

| Check | Passed | SEB avalanche measured | Rank-1 SEB avalanche measured | Avalanche VDS delta omitted | Omission explained |
| --- | ---: | ---: | ---: | ---: | ---: |
| `phase4_avalanche_vds_axis_regression` | yes | 2 | 1 | 41/41 | 41/41 |

## Calibration Harness

Added `data_processing_scripts/calibrate_proxy_distance.py`. It loads the
seeded default settings, builds a small grid over collapse, gate, normalized
VDS, energy-weight, phenotype-threshold, and weak-threshold constants, and
evaluates same-device retrieval against rank-1 `strong`/`usable` damage
equivalence rows.

Generated artifacts:

```text
out/proxy_distance_calibration/report.md
out/proxy_distance_calibration/results.json
out/proxy_distance_calibration/best_settings.json
```

The `out/` tree is ignored by git, so the artifacts are generated locally but
not committed. The report numbers from the live run were:

| Metric | Value |
| --- | ---: |
| Truth pairs with target events | 1 |
| Target-event retrieval cases | 84 |
| Candidate-pool rows evaluated | 31668 |
| Default top-1 hit rate | 0.012 |
| Default top-3 hit rate | 0.012 |
| Default mean truth rank | 17.714 |
| Best-grid top-1 hit rate | 0.012 |
| Best-grid top-3 hit rate | 0.012 |
| Best-grid mean truth rank | 15.417 |

Best grid candidate:

```text
grid_c0.3_g0.15_n0.1_ew0.5_pt2.25_ww2.5
```

The report intentionally recommends keeping the database `default` row. The
truth corpus is still too small to treat this as fitted calibration.

## End-to-End Verification

Plan query after Phase 5:

| Target tier | Event | Candidate | Mechanism | Status | Rank-1 rows |
| --- | --- | --- | --- | --- | ---: |
| `energy_censored_phenotype_only` | `SEB` | `sc` | `thermal_runaway_pair_secondary` | `waveform_only_candidate` | 18 |
| `energy_censored_phenotype_only` | `SEB` | `sc` | `thermal_runaway_pair_secondary` | `weak_measured_candidate` | 3 |
| `energy_censored_phenotype_only` | `SEB` | `avalanche` | `thermal_runaway_pair` | `measured_damage_candidate` | 1 |
| `energy_censored_phenotype_only` | `SELCI` | `sc` | `gate_oxide_pair_repetitive_only` | `analog_questionable` | 98 |
| `energy_censored_phenotype_only` | `SELCI` | `sc` | `gate_oxide_pair_repetitive_only` | `inspect_manually` | 21 |
| `energy_censored_phenotype_only` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `inspect_manually` | 2 |
| `energy_censored_phenotype_only` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `analog_questionable` | 1 |
| `energy_comparable` | `MIXED` | `sc` | `cumulative_defect_no_electrical_analog` | `missing_damage_context` | 1 |
| `energy_comparable` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `analog_questionable` | 20 |
| `energy_comparable` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `missing_damage_context` | 18 |

Mapped avalanche pool:

```text
stress_test_context_view source='avalanche' with device_type IS NOT NULL = 1087
```

## Checks

Passed:

```bash
/home/arodrigues/aps_venv/bin/python -m py_compile \
  data_processing_scripts/calibrate_proxy_distance.py \
  data_processing_scripts/ingestion_avalanche.py \
  data_processing_scripts/create_proxy_readiness_dashboard.py
/home/arodrigues/aps_venv/bin/python -m pytest tests/test_calibrate_proxy_distance.py -q
/home/arodrigues/aps_venv/bin/python data_processing_scripts/ingestion_avalanche.py --remap-only --dry-run
/home/arodrigues/aps_venv/bin/python data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
/home/arodrigues/aps_venv/bin/python data_processing_scripts/calibrate_proxy_distance.py
/home/arodrigues/aps_venv/bin/python -m pytest tests/ -q
```

Full pytest result: `15 passed in 0.15s`.

Note: the remap-only dry-run was used only to verify no-write behavior. In
this live DB session it loaded `0` avalanche-scope mapping rules, so its
matched/unmapped counts were not used as Phase 1 mapping evidence.

## Final State

- Branch: `master`
- Code commit: `Add proxy distance calibration harness` (this commit)
- `docs/8_phase_plan/` should be committed with this phase so Phase 1 through
  Phase 5 rollout notes become tracked project artifacts.
