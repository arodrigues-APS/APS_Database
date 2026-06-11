# Phase 7 Completed - Repetition and Cumulative Stress Context

Date completed: 2026-06-11

Commit: `Add stress pulse history descriptors` (this commit)

## Scope

Phase 7 implemented the repetition/cumulative descriptors from the proxy-method
plan. The key design choice is that pulse history is reporting context only: it
is exposed beside SELC/SELCII/MIXED irradiation targets, but it does not enter
`waveform_distance`, `phenotype_distance`, candidate status thresholds, or the
Phase 5 calibration constants.

The work added:

1. A persistent additive table, `stress_pulse_history`, keyed by
   `baselines_metadata.id`.
2. `data_processing_scripts/extract_stress_pulse_history.py`, which extracts
   explicit pulse counters from SC/avalanche metadata and filenames.
3. Pulse-history columns in `stress_test_context_view`.
4. Target dose/fluence versus candidate repetition columns in
   `stress_proxy_candidate_view`.
5. Dashboard table-column wiring and unit tests for parser/grouping behavior.

## Implementation Notes

`stress_pulse_history` stores one row per waveform metadata record when an
explicit sequence counter exists:

| Column | Meaning |
| --- | --- |
| `metadata_id` | Source `baselines_metadata.id`. |
| `pulse_index` | Counter extracted from metadata or filename. |
| `pulse_count_in_sequence` | Deterministic cumulative row count within the physical-sample sequence. |
| `sequence_key` | `source|device_type|physical_sample_key`. |
| `cumulative_energy_j` | Running sum of explicit per-pulse metadata energy when complete. |
| `basis` | Evidence source for the counter, e.g. `avalanche_shot_index_metadata`. |
| `provenance` | Extractor/version-style trace string. |

The sequence key is deliberately per physical sample, not per stress condition.
For cumulative damage, a sample that experiences changing avalanche energy or
gate bias is still one stress history. `pulse_index` keeps the original counter
for traceability; `pulse_count_in_sequence` is the ordered cumulative count used
for reporting.

The extractor is conservative. It emits rows only when a counter is explicit in
metadata or a known filename/path pattern. Current avalanche ingestion already
populates `avalanche_shot_index` for nearly all rows, so Phase 7 immediately
loads Selam avalanche sequences. Current SC waveform files do not carry explicit
`sc_sequence_num` or `pulseN` counters, so the extractor supports SC repetition
patterns but does not infer SC cumulative history from duration sweeps.

For candidate rows, `candidate_repetition_cumulative_energy_j` prefers
`stress_pulse_history.cumulative_energy_j` when available and only falls back to
`candidate_pulse_count_in_sequence * candidate_energy_j` when metadata energy is
missing. This avoids letting waveform integration artifacts dominate UIS
cumulative-context reporting.

## Columns Added

Context view:

| Column | Meaning |
| --- | --- |
| `stress_pulse_index` | Raw extracted pulse counter. |
| `pulse_count_in_sequence` | Cumulative pulse count within the sample sequence. |
| `prior_pulse_count` | Count before the current pulse. |
| `pulse_sequence_key` | Physical-sample sequence key. |
| `cumulative_pulse_energy_j` | Running metadata-energy sum through the current pulse. |
| `cumulative_prior_energy_j` | Running metadata-energy sum before the current pulse when computable. |
| `pulse_history_basis` | Counter evidence basis. |
| `pulse_history_provenance` | Extractor provenance string. |

Candidate view:

| Column | Meaning |
| --- | --- |
| `target_repetition_fluence_cm2` | Target irradiation fluence context. |
| `target_repetition_dose_gy` | Target radiation dose context. |
| `candidate_stress_pulse_index` | Candidate raw pulse counter. |
| `candidate_pulse_count_in_sequence` | Candidate cumulative pulse count. |
| `candidate_prior_pulse_count` | Candidate prior pulse count. |
| `candidate_pulse_sequence_key` | Candidate physical-sample sequence key. |
| `candidate_cumulative_pulse_energy_j` | Candidate cumulative metadata energy. |
| `candidate_cumulative_prior_energy_j` | Candidate prior cumulative metadata energy. |
| `candidate_pulse_history_basis` | Candidate pulse counter evidence basis. |
| `candidate_pulse_history_provenance` | Candidate pulse-history provenance string. |
| `candidate_repetition_pulse_count` | Alias for the reporting pulse count. |
| `candidate_repetition_single_pulse_energy_j` | Candidate terminal energy for the current pulse. |
| `candidate_repetition_cumulative_energy_j` | Metadata cumulative energy, or count x pulse energy fallback. |
| `repetition_context_available` | True for SELC/SELCII/MIXED rows with target dose/fluence and candidate pulse context. |

New context/blocker tags:

- `pulse_history_available`
- `missing_repetition_context_for_cumulative_target`

The blocker tag is informational; it does not alter candidate status ordering.

## Rebuild Commands

The schema creates the table and view definitions. The extractor populates the
table. The dependent materialized views must then be refreshed so they can see
the loaded rows.

```bash
/home/arodrigues/aps_venv/bin/python data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
/home/arodrigues/aps_venv/bin/python data_processing_scripts/extract_stress_pulse_history.py --rebuild
PGPASSWORD=APSLab psql -h localhost -p 5435 -U postgres -d mosfets -X -v ON_ERROR_STOP=1 \
  -c "REFRESH MATERIALIZED VIEW stress_test_context_view; REFRESH MATERIALIZED VIEW stress_proxy_candidate_view; REFRESH MATERIALIZED VIEW stress_proxy_candidate_summary_view;"
```

All three steps completed successfully.

## Verification SQL Results

Pulse-history table population:

| Metric | Value |
| --- | ---: |
| `stress_pulse_history` rows | 1245 |
| Distinct sequences | 160 |
| Max `pulse_count_in_sequence` | 54 |
| Basis rows: `avalanche_shot_index_metadata` | 1245 |

Context-view coverage:

| Source | Context rows | Rows with pulse history | Sequences | Max pulse count | Max cumulative energy J |
| --- | ---: | ---: | ---: | ---: | ---: |
| `avalanche` | 1258 | 1245 | 160 | 54 | 54.94 |
| `sc` | 26 | 0 | 0 | null | null |

Cumulative-target candidate context:

| Target event | Candidate source | Rows | Targets | Rows with repetition context |
| --- | --- | ---: | ---: | ---: |
| `MIXED` | `avalanche` | 315 | 37 | 307 |
| `MIXED` | `sc` | 65 | 38 | 0 |
| `SELCI` | `avalanche` | 445 | 56 | 432 |
| `SELCI` | `sc` | 1315 | 176 | 0 |
| `SELCII` | `avalanche` | 5657 | 968 | 5266 |
| `SELCII` | `sc` | 4483 | 974 | 0 |

Example SELCII row with repeated avalanche context:

| Target | Ion | Candidate | Sample | Pulse count | Single-pulse energy J | Cumulative energy J | Target fluence | Target dose Gy | Status |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `SELCII` | Au | `SCT3080KL` avalanche | `RT75` | 41 | 109810.6287 | 12.46 | 5608 | 1.5557e-05 | `cross_device_screening_only` |

The large single-pulse terminal energy in this example is from waveform
integration; the reported cumulative energy uses the metadata pulse-energy
history, which is the intended Phase 7 behavior.

Reporting-only invariant after Phase 7: candidate status distribution is
unchanged from Phase 6.

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

Phase 5/6 regression invariants still hold:

| Invariant | Result |
| --- | ---: |
| SEB avalanche `measured_damage_candidate` rows | 2 |
| Rank-1 SEB avalanche measured rows | 1 |
| Avalanche candidate rows with non-null `normalized_vds_delta` | 0 |

Candidate pulse-history provenance in `stress_proxy_candidate_view`:

| Basis | Candidate rows |
| --- | ---: |
| `avalanche_shot_index_metadata` | 6371 |

## Checks

Passed:

```bash
/home/arodrigues/aps_venv/bin/python -m py_compile \
  data_processing_scripts/extract_stress_pulse_history.py \
  data_processing_scripts/create_proxy_readiness_dashboard.py
/home/arodrigues/aps_venv/bin/python -m pytest tests/test_stress_pulse_history.py -q
/home/arodrigues/aps_venv/bin/python -m pytest tests/ -q
git diff --check
```

Full pytest result: `20 passed`.

## Known Follow-Ups

- Current SC waveform rows have no explicit sequence counters, so SC repetition
  context remains null until future ingestion populates `sc_sequence_num` or
  filenames include supported `pulseN` / `{N}_after...` patterns.
- `stress_pulse_history` is data, not a materialized view. Re-run
  `extract_stress_pulse_history.py --rebuild` after re-ingesting SC/avalanche
  waveform metadata, then refresh the dependent proxy materialized views.
- UIS waveform-integrated terminal energy can still be artifact-heavy. Phase 7
  avoids using that value for cumulative metadata-energy sums when explicit
  avalanche energy metadata exists, but the outlier waveforms remain worth
  investigating separately.

## Final State

- Branch: `master`
- Code commit: `Add stress pulse history descriptors` (this commit)
- Working tree after commit: clean
