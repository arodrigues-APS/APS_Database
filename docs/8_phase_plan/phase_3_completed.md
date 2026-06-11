# Phase 3 Completed - Censored SEB Proxy Tier and Mechanism Gates

Date completed: 2026-06-10

Commit: `35165dc Add censored SEB proxy tier and mechanism gates`

## Scope

Phase 3 implemented the censored-SEB target tier and mechanism-class wiring
from the proxy-method plan. The goal was to make censored single-event targets
evaluable without pretending their terminal electrical energy is fully
comparable, and to replace the hand-tuned path-penalty CASE expression with an
auditable mechanism compatibility table.

The work stayed additive at the schema and dashboard boundaries:

1. Added a target tier for censored irradiation events:
   `energy_censored_phenotype_only`.
2. Added a pipeline-owned `stress_mechanism_compatibility` table with
   idempotent seeds.
3. Added `mechanism_match_class`, status ceilings, and rationale fields to
   proxy candidates.
4. Added `analog_questionable` as a final candidate status for weak electrical
   analogs.
5. Surfaced the new tiers, mechanism classes, and censored-SEB coverage in the
   Proxy Readiness dashboard.

Phase 2 had already landed immediately before this work:
`fc045fa Fix proxy matching correctness gates`.

## Target Tiers

Before Phase 3, `stress_proxy_candidate_view` only admitted irradiation
targets that passed the existing event-energy comparability gates. That kept
censored SEB events out of the candidate view entirely, even when their
collapse phenotype was strongly comparable to avalanche or SC stress.

Phase 3 splits target admission into two explicit tiers:

| Target tier | Meaning |
| --- | --- |
| `energy_comparable` | Existing behavior: event-level integrated `VDS*ID` energy is comparable and positive. |
| `energy_censored_phenotype_only` | Detected single event fails the energy gates but has an `energy_censored_reason`; ranking excludes log-energy distance and relies on phenotype/mechanism evidence. |

For censored targets with:

```text
energy_censored_reason = 'failure_cutoff'
```

the view carries:

```text
target_energy_floor_j = event_energy_vds_id_j
```

That value is treated as a lower bound. Candidates below the floor receive the
`candidate_energy_below_censored_floor` blocker and an ordering penalty. In
the current rebuilt top-10 candidate set, no rows triggered that blocker, so
the code path is present but not active on the current data.

## Mechanism Compatibility Table

Phase 3 added `stress_mechanism_compatibility` to
`schema/025_proxy_readiness_waveforms.sql`.

The table is pipeline-owned and idempotently seeded during schema rebuild:

| Target event | Candidate source | Min collapse | Mechanism class | Path penalty | Status ceiling |
| --- | --- | ---: | --- | ---: | --- |
| `SEB` | `avalanche` | 0.30 | `thermal_runaway_pair` | 0.15 | none |
| `SEB` | `sc` | 0.00 | `thermal_runaway_pair_secondary` | 0.25 | none |
| `SELCI` | `sc` | 0.00 | `gate_oxide_pair_repetitive_only` | 0.50 | `analog_questionable` |
| `SELCII` | `any` | 0.00 | `cumulative_defect_no_electrical_analog` | 0.75 | `analog_questionable` |
| `MIXED` | `any` | 0.00 | `cumulative_defect_no_electrical_analog` | 0.75 | `analog_questionable` |

The candidate view joins the table with a lateral match. Exact
`candidate_source` rows outrank `any`, and higher `min_candidate_collapse`
thresholds outrank lower ones. If no seeded mechanism rule matches, the view
falls back to the earlier path-type behavior:

| Fallback condition | Mechanism class | Path penalty |
| --- | --- | ---: |
| Same extracted path type | `same_path_type` | 0.00 |
| Missing path type on either side | `path_unknown` | 0.25 |
| No matching path rule | `mechanism_unknown` | 0.75 |

## Status Ceiling

The mechanism table can cap candidate statuses through `status_ceiling`.

Phase 3 adds `analog_questionable` as a final candidate status. The cap applies
only when the row would otherwise be a positive candidate:

- `measured_damage_candidate`
- `predicted_damage_candidate`
- `device_run_measured_candidate`
- `weak_measured_candidate`
- `waveform_only_candidate`

Hard blockers remain hard blockers. For example, a row that is already
`phenotype_mismatch` or `missing_damage_context` is not converted to
`analog_questionable`.

This keeps the distinction between:

- rows that are blocked because evidence is missing or mismatched, and
- rows that have usable screening evidence but an electrical analog that is
  scientifically questionable.

## Code Changes

- Added `stress_mechanism_compatibility` table creation and seed rows in
  `schema/025_proxy_readiness_waveforms.sql`.
- Replaced the single `targets` CTE filter with tiered target admission:
  `energy_comparable` and `energy_censored_phenotype_only`.
- Added `target_energy_floor_j` for `failure_cutoff` censored targets.
- Excluded `log_energy_delta` from `waveform_distance` for
  `energy_censored_phenotype_only` targets.
- Added mechanism-aware `path_penalty`, `mechanism_match_class`,
  `mechanism_status_ceiling`, and `mechanism_rationale`.
- Added `candidate_rank_penalty` for candidates below censored target energy
  floors.
- Added `uncapped_candidate_status` so the final status ceiling is auditable.
- Added `analog_questionable` priority and
  `analog_questionable_screening_confidence`.
- Added tier/mechanism columns to `stress_proxy_candidate_view`.
- Extended `stress_proxy_candidate_summary_view` grouping by target tier and
  mechanism class.
- Updated `create_proxy_readiness_dashboard.py` with:
  - colors for `analog_questionable`, target tiers, and mechanism classes;
  - native filters for target tier and mechanism class;
  - tier/mechanism columns in candidate and evidence tables;
  - a censored-SEB candidate coverage table.

## Columns Added

Proxy candidate view:

| Column | Meaning |
| --- | --- |
| `target_match_tier` | Whether the target uses event-comparable energy or censored phenotype-only matching. |
| `target_energy_floor_j` | Lower-bound terminal energy for `failure_cutoff` censored targets, when available. |
| `mechanism_match_class` | Mechanism compatibility class selected from the seeded table or fallback logic. |
| `mechanism_status_ceiling` | Optional final-status ceiling from the mechanism rule. |
| `mechanism_rationale` | Human-readable reason for the mechanism rule or fallback. |
| `candidate_rank_penalty` | Ordering penalty for candidates below a censored target energy floor. |
| `uncapped_candidate_status` | Status before mechanism ceiling is applied. |

Candidate summary view:

| Column | Meaning |
| --- | --- |
| `target_match_tier` | Target tier for top-ranked candidate summary rows. |
| `mechanism_match_class` | Mechanism class for top-ranked candidate summary rows. |

Dashboard metadata:

| Surface | Addition |
| --- | --- |
| Color map | `analog_questionable`, target tiers, and mechanism classes |
| Native filters | Target tier and mechanism class |
| Candidate tables | Tier, mechanism, uncapped status, rank penalty |
| Summary chart | Censored-SEB candidate coverage |

## Rebuild Command

Command run:

```bash
python3 data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
```

Proxy-readiness rebuild result:

- Proxy-readiness SQL views rebuilt successfully.
- `stress_mechanism_compatibility` was created/updated idempotently.
- `stress_proxy_candidate_view` was recreated with censored target tiers,
  mechanism classes, status ceilings, and rank penalties.
- `stress_proxy_candidate_summary_view` was recreated with tier/mechanism
  grouping.

## Verification SQL Results

SEB targets are now evaluable:

| Metric | Result |
| --- | ---: |
| Distinct `target_stress_record_key` where `target_event_type = 'SEB'` | 22 |

C2M0080120D Ni SEB events now receive avalanche candidates with the intended
mechanism class:

| Target tier | Event | Ion | Energy | Candidate source | Mechanism class | Example status | Example rank |
| --- | --- | --- | ---: | --- | --- | --- | ---: |
| `energy_censored_phenotype_only` | `SEB` | `Ni` | 62.0 | `avalanche` | `thermal_runaway_pair` | `waveform_only_candidate` | 4 |
| `energy_censored_phenotype_only` | `SEB` | `Ni` | 62.0 | `avalanche` | `thermal_runaway_pair` | `waveform_only_candidate` | 5 |

The top-ranked grouped candidate distribution after Phase 3:

| Target tier | Event | Candidate source | Mechanism class | Status | Rows |
| --- | --- | --- | --- | --- | ---: |
| `energy_censored_phenotype_only` | `SEB` | `sc` | `thermal_runaway_pair_secondary` | `waveform_only_candidate` | 18 |
| `energy_censored_phenotype_only` | `SEB` | `sc` | `thermal_runaway_pair_secondary` | `weak_measured_candidate` | 3 |
| `energy_censored_phenotype_only` | `SEB` | `avalanche` | `thermal_runaway_pair` | `measured_damage_candidate` | 1 |
| `energy_censored_phenotype_only` | `SELCI` | `sc` | `gate_oxide_pair_repetitive_only` | `analog_questionable` | 119 |
| `energy_censored_phenotype_only` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `analog_questionable` | 3 |
| `energy_comparable` | `MIXED` | `sc` | `cumulative_defect_no_electrical_analog` | `missing_damage_context` | 1 |
| `energy_comparable` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `analog_questionable` | 20 |
| `energy_comparable` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `missing_damage_context` | 18 |

SELCII x SC status ceiling check:

| Target tier | Event | Candidate source | Mechanism class | Ceiling | Uncapped status | Final status | Rows |
| --- | --- | --- | --- | --- | --- | --- | ---: |
| `energy_comparable` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `analog_questionable` | `weak_measured_candidate` | `analog_questionable` | 20 |
| `energy_comparable` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `analog_questionable` | `missing_damage_context` | `missing_damage_context` | 18 |
| `energy_censored_phenotype_only` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `analog_questionable` | `waveform_only_candidate` | `analog_questionable` | 2 |
| `energy_censored_phenotype_only` | `SELCII` | `sc` | `cumulative_defect_no_electrical_analog` | `analog_questionable` | `weak_measured_candidate` | `analog_questionable` | 1 |

Candidate status distribution by target tier:

| Target tier | Event | Status | Rows | Targets |
| --- | --- | --- | ---: | ---: |
| `energy_censored_phenotype_only` | `SEB` | `waveform_only_candidate` | 194 | 20 |
| `energy_censored_phenotype_only` | `SEB` | `weak_measured_candidate` | 16 | 3 |
| `energy_censored_phenotype_only` | `SEB` | `device_run_measured_candidate` | 8 | 1 |
| `energy_censored_phenotype_only` | `SEB` | `measured_damage_candidate` | 2 | 1 |
| `energy_censored_phenotype_only` | `SELCI` | `analog_questionable` | 1190 | 119 |
| `energy_censored_phenotype_only` | `SELCII` | `analog_questionable` | 30 | 3 |
| `energy_comparable` | `MIXED` | `missing_damage_context` | 10 | 1 |
| `energy_comparable` | `SELCII` | `analog_questionable` | 196 | 20 |
| `energy_comparable` | `SELCII` | `missing_damage_context` | 168 | 18 |
| `energy_comparable` | `SELCII` | `phenotype_mismatch` | 12 | 2 |
| `energy_comparable` | `SELCII` | `inspect_manually` | 4 | 1 |

Censored-floor blocker check:

| Metric | Result |
| --- | ---: |
| Rows with `candidate_energy_below_censored_floor` | 0 |
| Distinct targets with `candidate_energy_below_censored_floor` | 0 |

This is expected for the current top-10 candidate set. The floor blocker and
rank penalty are present for future `failure_cutoff` rows with a non-null
energy floor and lower-energy candidate.

Avalanche pool check:

| Query | Result |
| --- | ---: |
| `stress_test_context_view WHERE source = 'avalanche' AND device_type IS NOT NULL` | 1087 |

Mechanism seed rows after rebuild:

| Event | Source | Min collapse | Mechanism class | Penalty | Ceiling |
| --- | --- | ---: | --- | ---: | --- |
| `MIXED` | `any` | 0.0 | `cumulative_defect_no_electrical_analog` | 0.75 | `analog_questionable` |
| `SEB` | `avalanche` | 0.3 | `thermal_runaway_pair` | 0.15 | none |
| `SEB` | `sc` | 0.0 | `thermal_runaway_pair_secondary` | 0.25 | none |
| `SELCI` | `sc` | 0.0 | `gate_oxide_pair_repetitive_only` | 0.50 | `analog_questionable` |
| `SELCII` | `any` | 0.0 | `cumulative_defect_no_electrical_analog` | 0.75 | `analog_questionable` |

## Checks

Passed:

```bash
python3 -m py_compile data_processing_scripts/create_proxy_readiness_dashboard.py
python3 data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
git diff --check
```

Not run:

```bash
python3 -m pytest
```

`pytest` was not installed in the available Python environment:
`/usr/bin/python3: No module named pytest`.

## Final State

- Branch: `master`
- Latest code commit: `35165dc Add censored SEB proxy tier and mechanism gates`
- Branch status after Phase 3 commit: ahead of `origin/master` by 2 commits
  before the user applied/pushed the changes.
- Current branch status when this note was added: synced with `origin/master`.
- `docs/8_phase_plan/` was untracked before this documentation note was added.
- This completion note documents the Phase 3 code commit; it can be committed
  together with the existing Phase 1 and Phase 2 completion notes if the
  phase-plan docs are meant to become tracked project artifacts.
