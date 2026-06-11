# Phase 2 Completed - Matching Correctness Gates

Date completed: 2026-06-10

Commit: `fc045fa Fix proxy matching correctness gates`

## Scope

Phase 2 implemented the two matching-correctness fixes from the proxy-method
plan:

1. Damage equivalence now checks whether compared damage axes move in the same
   direction. Sign-opposed axes are surfaced and demote match quality.
2. Proxy candidate waveform phenotype distance now uses only overlapping
   collapse/gate phenotype axes instead of scoring missing axes with fixed
   imputation constants.

The work stayed additive at the schema and dashboard boundaries. Existing
candidate categories remain, with one new blocked/manual status for rows that
would otherwise have no phenotype-axis overlap:
`missing_phenotype_overlap`.

Phase 1 had already landed immediately before this work:
`50a910d Map Selam avalanche sample codes`.

## Correctness Gate 1 - Damage Sign Agreement

The damage-equivalence matcher previously ranked by normalized distance alone.
That could make a small numeric distance look usable even when the irradiation
fingerprint and stress candidate moved in opposite physical directions, for
example negative irradiation `ΔVth` versus positive SC `ΔVth`.

Phase 2 adds per-axis sign mismatch checks for:

- `ΔVth`
- `ΔRds(on)`
- `ΔV(BR)DSS`

Each axis uses a small deadband before sign comparison:

```text
abs(axis_value) < axis_scale / 10 => sign-neutral
```

This avoids penalizing near-zero noise. If both sides are outside the deadband
and their signs differ, that axis is recorded in `sign_mismatch_axes`.

Final rank demotion is:

| Condition | Final rank behavior |
| --- | --- |
| All compared axes are sign mismatches | `4` / `inspect manually` |
| Some compared axes are sign mismatches | `max(base_rank, 3)` / at best `weak` |
| No sign mismatches | Keep base distance rank |

`base_comparability_rank` is surfaced alongside the final
`comparability_rank` so the demotion is auditable.

## Correctness Gate 2 - Pairwise Phenotype Distance

The proxy candidate view previously used fixed fallback penalties when
collapse or gate phenotype overlap was missing:

```sql
COALESCE(collapse_delta, 0.75)
COALESCE(gate_delta, 0.25)
```

That meant a candidate could receive a finite phenotype distance even when the
score was driven by constants rather than real overlapping phenotype evidence.

Phase 2 changes the distance calculation to:

- Count available phenotype axes in `phenotype_axes_used`.
- Sum only available collapse/gate terms.
- Normalize by the number of available phenotype axes.
- Return `NULL` waveform/phenotype distances when zero phenotype axes overlap.
- Classify zero-overlap rows as `missing_phenotype_overlap`.

The existing blockers are still emitted when applicable:

- `missing_collapse_overlap`
- `missing_gate_overlap`
- `missing_duration_overlap`

## Code Changes

- Updated `damage_equivalence_match_view` in
  `data_processing_scripts/ml_sc_irrad_equivalence.py`.
- Updated `damage_equivalence_prediction_match_view` with the same sign
  agreement and demotion logic.
- Added `sign_mismatch_axis_count`, `sign_mismatch_axes`, and
  `base_comparability_rank` to the match views.
- Updated the Python CSV/CLI helper in `ml_sc_irrad_equivalence.py` so
  regenerated CSV output uses the same sign-demotion logic as SQL.
- Updated `schema/025_proxy_readiness_waveforms.sql` to compute pairwise
  phenotype distance and expose `phenotype_axes_used`.
- Propagated measured and predicted sign-mismatch evidence into
  `stress_proxy_candidate_view`.
- Added the `missing_phenotype_overlap` candidate status to the proxy dashboard
  color map and table columns.
- Added sign-mismatch columns to the SC/irradiation damage-equivalence
  dashboard table.

## Columns Added

Damage match views:

| Column | Meaning |
| --- | --- |
| `dvth_sign_mismatch` | `1` when compared `ΔVth` signs disagree outside deadband |
| `drds_sign_mismatch` | `1` when compared `ΔRds(on)` signs disagree outside deadband |
| `dbv_sign_mismatch` | `1` when compared `ΔV(BR)DSS` signs disagree outside deadband |
| `sign_mismatch_axis_count` | Number of sign-opposed compared axes |
| `sign_mismatch_axes` | Human-readable axis list |
| `base_comparability_rank` | Distance-only rank before sign demotion |

Proxy candidate view:

| Column | Meaning |
| --- | --- |
| `phenotype_axes_used` | Count of overlapping collapse/gate phenotype axes |
| `measured_sign_mismatch_axis_count` | Sign mismatch count from measured damage evidence |
| `measured_sign_mismatch_axes` | Sign mismatch axis list from measured damage evidence |
| `prediction_sign_mismatch_axis_count` | Sign mismatch count from predicted damage evidence |
| `prediction_sign_mismatch_axes` | Sign mismatch axis list from predicted damage evidence |

CSV export:

| Column | Meaning |
| --- | --- |
| `nearest_comparability_rank` | Final demoted nearest-match rank |
| `nearest_comparability_status` | Final nearest-match status |
| `nearest_sign_mismatch_axes` | Sign-opposed axes for the nearest match |

## Rebuild Commands

Commands run:

```bash
python3 data_processing_scripts/ml_sc_irrad_equivalence.py --rebuild
python3 data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
```

Damage-equivalence rebuild result:

- Loaded fingerprints: 101
- SC fingerprints: 76
- Avalanche fingerprints: 1
- Irradiation fingerprints: 24
- Nearest-SC matches computed for irradiation fingerprints: 10
- CSV regenerated:
  `out/sc_irrad_equivalence/irrad_to_sc_equivalents.csv`
- Scatter plots regenerated under `out/sc_irrad_equivalence/`

Proxy-readiness rebuild result:

- Proxy-readiness SQL views rebuilt successfully.
- `stress_proxy_candidate_view` was recreated with the new pairwise phenotype
  distance logic and sign-mismatch evidence columns.

## Verification SQL Results

The hard-gate C-ion example now demotes below usable:

| Device | Run | Energy | Right `ΔVth` | Left `ΔVth` | Distance | Axes | Sign mismatch | Base rank | Final rank | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| `C2M0080120D` | 11 | 12.0 | -1.21 | 0.35 | 1.141 | 1 | `ΔVth` | 3 | 4 | `inspect manually` |

The closest two-axis C-ion match also demotes because both compared axes are
sign-opposed:

| Device | Run | Energy | Right `ΔVth` | Left `ΔVth` | Distance | Axes | Sign mismatch | Base rank | Final rank | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| `C2M0080120D` | 11 | 12.0 | -1.21 | 0.835 | 1.507 | 2 | `ΔVth, ΔV(BR)DSS` | 3 | 4 | `inspect manually` |
| `C2M0080120D` | 12 | 6.0 | -0.91 | 0.835 | 1.365 | 2 | `ΔVth, ΔV(BR)DSS` | 2 | 4 | `inspect manually` |

Damage matches with sign mismatches after rebuild:

| Pair type | Final status | Rows |
| --- | --- | ---: |
| `avalanche_vs_irradiation` | `inspect manually` | 1 |
| `avalanche_vs_irradiation` | `weak` | 2 |
| `sc_vs_avalanche` | `inspect manually` | 1 |
| `sc_vs_avalanche` | `weak` | 5 |
| `sc_vs_irradiation` | `inspect manually` | 46 |
| `sc_vs_irradiation` | `weak` | 8 |

Candidate-view phenotype-axis coverage:

| `phenotype_axes_used` | Rows | Min phenotype distance | Max phenotype distance |
| ---: | ---: | ---: | ---: |
| 1 | 390 | 0.519 | 4.071 |

Zero-axis candidate check:

| Query | Result |
| --- | --- |
| `stress_proxy_candidate_view WHERE phenotype_axes_used = 0` | no rows |

This means no displayed top-10 candidate row is currently scored purely from
phenotype imputation constants. The new `missing_phenotype_overlap` status is
still present for future zero-overlap rows that pass into the candidate set.

Candidate-view status distribution after Phase 2:

| Status | Count |
| --- | ---: |
| `weak_measured_candidate` | 198 |
| `missing_damage_context` | 177 |
| `phenotype_mismatch` | 12 |
| `inspect_manually` | 2 |
| `waveform_only_candidate` | 1 |

The status movement from Phase 1 is expected: sign-opposed damage evidence is
no longer counted as usable/device-run evidence, and pairwise phenotype
distance changes the waveform ordering without admitting zero-axis imputation
matches.

## Checks

Passed:

```bash
python3 -m py_compile \
  data_processing_scripts/ml_sc_irrad_equivalence.py \
  data_processing_scripts/create_sc_irrad_dashboard.py \
  data_processing_scripts/create_proxy_readiness_dashboard.py

python3 data_processing_scripts/ml_sc_irrad_equivalence.py --rebuild
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
- Latest code commit: `fc045fa Fix proxy matching correctness gates`
- Branch status after commit: ahead of `origin/master` by 1 commit
- `docs/8_phase_plan/` was untracked before this documentation note was added.
- This completion note documents the Phase 2 code commit; it can be committed
  together with the existing Phase 1 completion note if the phase-plan docs are
  meant to become tracked project artifacts.
