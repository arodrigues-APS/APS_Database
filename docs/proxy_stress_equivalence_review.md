# Proxy Stress Equivalence Review

Date reviewed: 2026-05-25

## Executive Conclusion

The codebase now has the right building blocks for a proxy-stress workflow, but
it does not yet have enough overlapping evidence to claim that short-circuit
or avalanche stress is equivalent to irradiation stress.

The defensible path is a gated workflow:

1. Use waveform/event features to generate candidates.
2. Reject candidates with mismatched failure phenotype.
3. Require measured or validation-gated predicted post-IV damage agreement
   before calling anything an equivalent stress.

Energy alone should not be used as the equivalence metric. The current proton
SEB diagnostic shows why: matching on Joules alone selects mostly avalanche
waveforms, but adding Vds collapse moves almost every nearest match to
short-circuit waveforms. That means energy is useful for candidate retrieval,
not for equivalence.

## Existing Pieces

### Source-Aware Irradiation Event Extraction

Files:

- `data_processing_scripts/extract_single_event_effects.py`
- `docs/single_event_detection.md`
- `data_processing_scripts/create_irradiation_dashboard.py`

This layer provides the irradiation event labels needed before any proxy work
is meaningful:

- `event_type` is the radiation-effect label: `SEB`, `SELCI`, `SELCII`,
  `MIXED`, or `UNKNOWN`.
- `path_type` is the mechanism-neutral electrical path: `DRAIN_SOURCE`,
  `DRAIN_GATE`, `MIXED`, or `UNKNOWN`.
- High-energy proton events are labeled as SEB when source-aware criteria pass.
- Proton events are no longer mislabeled as SELC-I or SELC-II.

This is important because a proxy comparison should not compare "SELC-II-like"
leakage to proton SEB just because both have drain-current movement.

### Measured Damage Fingerprint Matching

Files:

- `data_processing_scripts/ml_sc_irrad_equivalence.py`
- `data_processing_scripts/create_sc_irrad_dashboard.py`
- `out/sc_irrad_equivalence/irrad_to_sc_equivalents.csv`

Views:

- `damage_equivalence_view`
- `damage_equivalence_match_view`
- `damage_equivalence_coverage_view`
- `damage_equivalence_match_segment_view`

This layer compares stress conditions by post-IV electrical response:

- delta Vth
- delta Rds(on)
- delta BV

Current verified fingerprint coverage:

| source | fingerprints | with delta Vth | with delta Rds(on) | with delta BV |
| --- | ---: | ---: | ---: | ---: |
| avalanche | 1 | 1 | 1 | 1 |
| irradiation | 24 | 20 | 2 | 17 |
| short-circuit | 76 | 36 | 43 | 18 |

Current rank-1 match status:

| pair | status |
| --- | --- |
| SC vs irradiation | 6 usable, 4 weak |
| SC vs avalanche | 1 usable |
| avalanche vs irradiation | 2 usable, 2 weak, 2 inspect manually |

Interpretation:

- SC vs irradiation has some usable damage-space candidates.
- Avalanche is underdetermined because there is only one avalanche post-IV
  damage fingerprint.
- Most matches use only one or two damage axes, so they are useful as ranked
  candidates, not proof of equivalence.

### Predicted Irradiation Damage Layer

Files:

- `data_processing_scripts/ml_post_iv_physical_prediction.py`
- `data_processing_scripts/create_iv_physical_prediction_dashboard.py`
- `data_processing_scripts/create_sc_irrad_prediction_dashboard.py`

Views:

- `damage_equivalence_prediction_fingerprint_view`
- `damage_equivalence_prediction_match_view`
- `damage_equivalence_prediction_coverage_view`
- `damage_equivalence_prediction_match_segment_view`
- `damage_equivalence_prediction_validation_view`
- `damage_equivalence_prediction_support_reason_view`

This is the right place to use predicted irradiation response when measured
irradiation is sparse. It should remain a separate evidence tier:

- measured SC/avalanche damage compared to measured irradiation damage is the
  strongest evidence;
- measured SC/avalanche damage compared to predicted irradiation damage is an
  exploratory bridge;
- prediction confidence, donor count, donor distance, validation support, and
  reference tier must travel with every match.

This layer is valuable for prioritizing which SC or avalanche stress condition
to try next, but it should not overwrite measured damage-equivalence views.

### Waveform Proxy Readiness Layer

Files:

- `schema/025_proxy_readiness_waveforms.sql`
- `data_processing_scripts/create_proxy_readiness_dashboard.py`
- `data_processing_scripts/plot_proton_proxy_match_shift.py`

Views:

- `stress_waveform_file_features`
- `stress_waveform_event_features`
- `stress_waveform_basis_feature_view`
- `stress_proxy_readiness_view`
- `stress_proxy_gate_zero_view`

This layer extracts stress-exposure and failure-phenotype features:

- integrated Vds times Id energy;
- absolute energy;
- commanded or stored avalanche energy;
- peak current;
- peak power;
- duration;
- Vds collapse fraction;
- gate-current fraction;
- post-IV companion coverage.

Current verified proxy-readiness state:

| metric | value |
| --- | ---: |
| waveform file feature rows | 1800 |
| event feature rows | 3095 |
| basis feature rows | 14752 |
| device family rows | 23 |
| gate-zero candidate device families | 0 |
| gate-zero status | gate_zero_fail_current_state |
| SC waveform files | 22 |
| UID/UIS avalanche waveform files | 167 |
| irradiation waveform files | 516 |
| irradiation events | 1811 |
| post-IV damage fingerprints | 101 |
| electrical proxy waveform plus post-IV files | 2 |
| irradiation waveform plus post-IV files | 333 |
| irradiation events with waveform plus post-IV | 612 |

The gate-zero failure is the most important current result. The database has
many irradiation events and many avalanche waveforms, but it lacks enough
device-family overlap where electrical proxy waveforms also have post-IV damage
labels.

Top readiness blockers:

- 20 device families are missing SC or UID/UIS avalanche waveforms.
- `C2M0080120D` has SC/avalanche waveforms and irradiation events, but is
  blocked by missing electrical-proxy post-IV overlap.
- `SCT2080KE` has electrical proxy post-IV overlap, but lacks irradiation
  waveform/event coverage.
- One family is blocked by missing device type.

### C2M0080120D Avalanche/Irradiation Pilot

Files:

- `data_processing_scripts/pilot_avalanche_irradiation_compare.py`
- `out/avalanche_irrad_pilot/README.md`

Current case-study result:

- C2M0080120D D3 UIS avalanche waveforms: 5 files.
- High-energy proton SEB events: 18 events across 6 files.
- Heavy-ion SEB contrast: 4 events across 4 files.
- Avalanche D3 median current amplitude: 53.9103 A.
- Avalanche D3 median Vds collapse fraction: 1.01275.
- High-energy proton SEB median delta Id: 0.000366 A.
- High-energy proton SEB median Vds collapse fraction: 0.
- Heavy-ion SEB median delta Id: 0.020998 A.
- Heavy-ion SEB median Vds collapse fraction: 0.993892.

Interpretation:

- The proton subset does not look avalanche-UIS-like in instantaneous collapse.
- The heavy-ion SEB contrast is much closer to avalanche UIS in collapse
  phenotype.
- Avalanche may be a better proxy candidate for hard-collapse heavy-ion SEB
  than for the current 200 MeV proton SEB subset.

### Proton Energy-Only vs Energy-Plus-Collapse Diagnostic

Files:

- `data_processing_scripts/plot_proton_proxy_match_shift.py`
- `out/proxy_matching_shift/proton_proxy_match_shift_summary.md`
- `out/proxy_matching_shift/proton_proxy_match_shift.png`

Current result for 44 proton SEB events:

| match basis | nearest proxy distribution |
| --- | --- |
| energy only | 15 avalanche UIS, 15 avalanche UID, 12 avalanche RT, 1 avalanche Test, 1 SC waveform |
| energy plus collapse | 43 SC waveform, 1 avalanche RT |

Median context:

- Proton event energy: 0.263839 J.
- Proton collapse fraction: 0.
- Energy-only proxy collapse fraction: 0.97875.
- Energy-plus-collapse proxy collapse fraction: 0.0985366.
- 42 of 44 events change nearest proxy class when collapse is added.

Interpretation:

Energy overlap exists, but it is not enough. The failure phenotype changes the
answer almost completely.

## Recommended Combined Workflow

### 1. Keep Measured, Predicted, and Waveform Evidence Separate

Use three evidence tiers:

- measured damage equivalence: post-IV damage fingerprints from
  `damage_equivalence_*`;
- predicted damage equivalence: validation-gated predicted irradiation
  fingerprints from `damage_equivalence_prediction_*`;
- waveform phenotype similarity: energy, collapse, gate, current, duration, and
  path metrics from `stress_waveform_*`.

Do not merge these into one opaque score yet. A candidate should show which
evidence tier passed, failed, or was missing.

### 2. Generate Candidates With Energy, Not Final Equivalence

Candidate retrieval can start with:

- same `device_type`, or a controlled device-family grouping;
- similar integrated energy on log scale;
- similar duration or pulse-width class;
- similar peak current or peak power envelope;
- same broad stress class where applicable.

This finds plausible nearby stress points without claiming physics equivalence.

### 3. Gate Candidates by Failure Phenotype

Before looking at damage-space distance, reject candidates with incompatible
phenotype:

- Vds collapse fraction;
- gate-current fraction;
- `path_type`;
- event catastrophic flag;
- avalanche outcome;
- trace abort or hard-failure evidence;
- current step magnitude class.

For the current 200 MeV proton SEB subset, collapse is decisive: avalanche
waveforms often match energy but not phenotype.

### 4. Validate With Post-IV Damage Fingerprints

A proxy candidate becomes an equivalence candidate only if the post-IV damage
fingerprint also agrees:

- at least two comparable axes should be present when possible;
- distance should pass a threshold such as the existing `strong` or `usable`
  bands;
- sample counts and IQR should be reported;
- measured matches should outrank predicted matches;
- one-axis matches should remain "weak" unless backed by other evidence.

The current SC/irradiation damage matches are promising, but not broad enough
for a general rule.

### 5. Use Predictions as an Experiment Planner

The predicted irradiation layer should be used to answer:

> If this device family were irradiated under condition X, which SC or
> avalanche stress condition is expected to land closest in post-IV response?

This is most useful for choosing the next cheap electrical stress experiment,
not for replacing irradiation qualification.

### 6. Report an Equivalence Envelope, Not a Single Number

The final useful output should be a table like:

| field | example |
| --- | --- |
| target irradiation condition | proton 200 MeV, device family, bias state |
| proxy stress | SC 400 V / 17 us or avalanche UID/UIS condition |
| waveform basis | energy plus collapse plus gate metrics |
| damage basis | delta Vth, delta Rds(on), delta BV |
| evidence tier | measured, predicted, or mixed |
| status | ready, candidate, weak, blocked |
| blockers | missing post-IV, missing waveform, missing device mapping |

This is more defensible than a single "equivalent Joules" value.

## What Is Needed Next

### Required Code/Product Additions

1. Replace the `.pyc` dashboard wrapper with source code.

   `data_processing_scripts/create_proxy_readiness_dashboard.py` currently
   loads `create_proxy_readiness_dashboard_impl.pyc`. That is not maintainable
   enough for a production dashboard builder. The source implementation should
   be restored or rewritten so dashboard contents, filters, and chart params
   can be reviewed and versioned.

2. Add a production proxy-candidate view.

   A new view should join waveform candidates to damage matches and emit one
   row per target irradiation condition and candidate SC/avalanche condition.
   It should include energy distance, phenotype distance, damage distance,
   evidence tier, status, and blockers.

3. Add an experiment-planning dashboard.

   The current readiness dashboard says why equivalence is blocked. A planning
   view should say which measurement would unblock the most useful comparison:
   for example, "post-IV after C2M0080120D electrical proxy waveform stress" or
   "irradiation waveform/event coverage for SCT2080KE".

4. Add explicit energy calibration and uncertainty.

   Energy should be normalized by measurement window, sign convention, measured
   vs commanded source, instrument coverage, and waveform completeness. The
   dashboard already distinguishes integrated and proxy energy, but a production
   model should expose uncertainty or confidence for each energy basis.

5. Add validation gates for predicted irradiation matching.

   The predicted layer already tracks support and validation. The proxy workflow
   should require those fields and should fail closed when support is missing.

6. Improve physical sample mapping.

   Avalanche and SC post-IV data need stronger `sample_group` and
   `physical_sample_key` resolution. Current C2M0080120D D3/d3 mapping is a
   useful case study, but broad equivalence needs reliable sample identity.

7. Add cumulative irradiation descriptors.

   Low-energy proton TID/DD behavior should not be forced into single-event
   Joule matching. Add dose/fluence/displacement-damage descriptors and keep
   those separate from pulse-energy proxies.

### Required Data/Measurement Additions

1. For `C2M0080120D`, add post-IV measurements after the relevant SC or
   avalanche waveform stresses. This family has rich irradiation and waveform
   data but currently lacks electrical-proxy post-IV overlap.

2. For `SCT2080KE`, add irradiation waveform/event coverage if that family is
   still important. It has the electrical proxy post-IV overlap that most other
   families are missing.

3. Add more avalanche post-IV damage fingerprints. One avalanche fingerprint is
   not enough to generalize avalanche-to-irradiation equivalence.

4. Record exact stress recipes for electrical proxy tests:

   - SC bus voltage;
   - pulse width;
   - gate drive;
   - temperature;
   - number of pulses;
   - avalanche mode;
   - inductance;
   - commanded and measured energy;
   - failure outcome.

## Practical Near-Term Recommendation

For the current dataset, short-circuit is the more credible immediate proxy
candidate for the high-energy proton SEB subset when waveform phenotype is
included, because proton events have near-zero Vds collapse and the
energy-plus-collapse diagnostic moves 43 of 44 events to SC waveforms.

Avalanche remains a credible candidate for hard-collapse heavy-ion SEB-like
phenotypes, but the codebase currently has too little avalanche post-IV damage
coverage to validate that broadly.

The next best implementation step is not a more complex model. It is a
proxy-candidate table that combines:

- energy and waveform phenotype similarity;
- measured damage-space nearest neighbors;
- predicted irradiation damage where measured damage is missing;
- explicit blockers and confidence flags.

That table should power the next dashboard and should keep all candidates in a
"descriptive/readiness" status until gate-zero coverage passes.
