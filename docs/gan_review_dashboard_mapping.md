# GaN Stability/Reliability/Robustness Review: Dashboard Mapping

Source reviewed:

- `relevant_papers/Stability_Reliability_and_Robustness_of_GaN_Power_Devices_A_Review.pdf`
- Paper metadata from local PDF: Kozak et al., IEEE Transactions on Power
  Electronics, 2023, 30 pages.

Scope of this note:

- Review the introduction and the paper's effective methodology.
- Break down the comparison types used in the review.
- Map those comparison types onto the current APS dashboards and proxy-stress
  workflow.

Important caveat:

The paper is about GaN power devices, while this database currently contains
SC, avalanche, irradiation, and post-IV workflows that are mostly being used for
SiC power-device stress comparison. The directly useful part is the comparison
methodology, not the GaN-specific numeric limits.

## What The Paper Is Trying To Compare

The introduction frames GaN reliability around three practical engineering
questions:

1. How much extra conduction or switching loss is created by parametric
   instability?
2. What do device-level and circuit-level reliability tests actually tell a
   power-electronics designer?
3. How resilient is the device to surge energy, overvoltage, overcurrent, and
   combinations of those stressors?

The paper then separates stress studies into three regimes:

| regime | stress relation to SOA | response type | typical timescale |
| --- | --- | --- | --- |
| stability | inside the safe operating area | recoverable parameter shifts | switching transient to sustained operation |
| reliability | near the SOA boundary | degradation or lifetime statistics | hours to years |
| robustness | outside the SOA | destructive or near-destructive withstand | single event to short repetitive sequences |

That separation is the most useful idea for these dashboards. It says we should
not compare stresses only by energy. We should also label whether the stress is
recoverable, cumulative, near-boundary, or destructive, and whether the test
represents application-like circuit operation.

## Methodology Used By The Review

The paper is a literature review, not a new experimental study. Its methodology
is a structured comparison across:

- device architecture;
- stress stimulus;
- test method;
- timescale;
- SOA relation;
- measured response;
- application relevance;
- failure or recovery mechanism.

It repeatedly warns that the same nominal measurement can mean different things
when the test method changes. For example, static dc stress, pulsed I-V, double
pulse, and steady-state circuit testing can all report a shift in Rds(on), but
they sample different trapping/recovery time constants and are not directly
interchangeable.

## Comparison Types In The Paper

### 1. Material And Device-Architecture Comparisons

The introduction compares Si, SiC, and GaN at the material/device level, then
compares GaN device architectures:

- Schottky p-gate HEMT;
- hybrid-drain gate-injection transistor;
- cascode GaN HEMT;
- direct-drive/module devices;
- vertical GaN devices.

The comparison is not just performance rating. It links architecture to likely
failure locations and mechanisms:

- gate stack;
- passivation/interface traps;
- buffer traps;
- substrate/transition layers;
- package and multichip interconnects.

Dashboard translation:

- Add or strengthen a `device_architecture` or `device_family_physics` field in
  the device library when known.
- Treat manufacturer/part number as insufficient by itself. A proxy-equivalence
  table should be able to group by device structure or blocking technology when
  that metadata exists.
- For the current dashboards, `device_type` is the practical grouping key, but
  the paper argues for adding a physics-aware grouping above or beside it.

### 2. Stress-Regime Comparisons

The paper's central organizing comparison is:

- stability vs reliability vs robustness;
- inside-SOA vs near-SOA vs outside-SOA;
- recoverable shift vs permanent degradation vs catastrophic failure;
- short transient vs long lifetime stress.

Dashboard translation:

Create a common stress taxonomy used by all dashboards:

| proposed field | example values |
| --- | --- |
| `stress_regime` | stability, reliability, robustness |
| `soa_relation` | inside_soa, near_boundary, outside_soa, unknown |
| `response_reversibility` | recoverable, partially_recoverable, permanent, catastrophic, unknown |
| `test_timescale_class` | transient, single_pulse, repetitive_pulse, steady_state, long_duration |
| `test_method_class` | dc_bias, pulse_iv, double_pulse, circuit, uis, radiation, temperature |

This would let the proxy-readiness dashboard show whether an SC waveform, an
avalanche waveform, and an irradiation event are comparable as stress tests
before comparing their damage response.

### 3. Traditional Qualification Test Comparisons

The paper reviews qualification-style tests such as:

- high-temperature reverse bias;
- high-temperature gate bias;
- power cycling;
- automotive qualification tests;
- vendor-specific extended reliability reports.

The key comparison is not "pass/fail"; it is what each test stresses:

- drain-source blocking field;
- gate stack;
- package/interconnect thermal cycling;
- operating mission profile.

Dashboard translation:

- The current database has post-IV deltas and waveform data, but not a broad
  qualification-test abstraction.
- A future "Qualification/Stress Taxonomy" dashboard could group data by stress
  stimulus instead of source folder:
  - drain bias;
  - gate bias;
  - current;
  - self-heating/temperature;
  - radiation dose/fluence/LET;
  - surge energy.
- This is directly relevant to proxy equivalence because SC, avalanche, and
  irradiation should only be compared when the stimulated failure mechanisms are
  plausible counterparts.

### 4. Test-Method Comparisons For Dynamic Parameters

The paper compares three broad ways to characterize dynamic parameter shifts:

- dc stress;
- pulsed I-V;
- circuit tests.

It then compares methods for dynamic Rds(on), including:

- pulsed I-V;
- double-pulse testing;
- steady-state continuous switching.

The methodological point is that faster or more application-like tests capture
short-lived trapping states that slow post-stress measurements can miss.

Dashboard translation:

- Existing post-IV damage dashboards mainly capture before/after electrical
  response, not recovery dynamics.
- The waveform dashboards can capture transient behavior, but they need a
  `measurement_latency` or `post_stress_delay_s` field to know whether a
  parameter shift is fast/recoverable or permanent.
- For equivalence, a measured `delta_rds` from a slow curve-tracer file should
  not be treated as identical to a dynamic Rds(on) shift measured during
  switching.

Current relevant dashboard pieces:

- `damage_equivalence_view` compares delta Vth, delta Rds(on), and delta BV.
- `stress_waveform_file_features` and `stress_waveform_event_features` capture
  energy, collapse, gate, duration, and peak-current features.
- A missing bridge is the time between stress and post-IV measurement.

### 5. Dynamic Loss / Parameter Shift Comparisons

The paper's stability sections compare:

- dynamic Rds(on);
- output capacitance loss;
- dynamic threshold voltage;
- switching frequency;
- duty cycle;
- voltage/current overlap;
- temperature;
- hard vs soft switching.

Dashboard translation:

- The existing dashboards already have the right post-IV response axes for the
  static part: Vth, Rds(on), and breakdown voltage.
- The waveform layer already has a partial dynamic-stress basis:
  - energy;
  - peak current;
  - peak power;
  - duration;
  - Vds collapse;
  - gate-current fraction.
- To mimic the paper's method more closely, add these derived axes where data
  allows:
  - normalized stress voltage, such as `vds_peak / rated_vds`;
  - normalized current, such as `peak_current / rated_current`;
  - frequency or repetition count for repetitive tests;
  - duty cycle or pulse spacing;
  - hard-switching vs soft-switching label;
  - temperature at stress.

### 6. Short-Circuit Robustness Comparisons

The paper compares SC robustness by:

- short-circuit type;
- bus voltage;
- gate-drive condition;
- SC withstand time;
- single-event vs repetitive SC;
- failure mechanism;
- parametric shifts after repeated nonfatal pulses.

The practical finding for dashboard design is that SC robustness is not one
number. It depends strongly on bus voltage, gate drive, protection timing, and
whether the event is single or repetitive.

Dashboard translation:

Current SC-relevant fields already exist or are implied:

- `sc_voltage_v`;
- `sc_duration_us`;
- `sc_vgs_on_v`;
- `sc_vgs_off_v`;
- waveform peak current;
- waveform energy;
- post-SC Vth/Rds/BV deltas.

Recommended dashboard additions:

- SC withstand or outcome status: survived, degraded, failed, aborted, unknown.
- Repetition count and pulse index.
- Protection/detection delay if known.
- Normalized bus voltage relative to device rating.
- A plot of `delta_rds`, `delta_vth`, and `delta_bv` vs SC voltage/duration.
- A proxy-specific plot of SC energy/collapse/peak-power vs irradiation event
  energy/collapse/path type.

### 7. Surge Energy, Avalanche, And Overvoltage Comparisons

The paper separates conventional avalanche ruggedness from GaN nonavalanche
overvoltage/surge behavior. It compares:

- UIS;
- clamped inductive switching;
- active-clamped high-frequency overvoltage switching;
- static breakdown voltage vs dynamic breakdown voltage;
- single-pulse vs repetitive overvoltage;
- energy withstand vs overvoltage margin;
- Coss/energy-storage tradeoffs.

Dashboard translation:

The Avalanche dashboard already captures:

- avalanche mode;
- avalanche family;
- energy;
- peak current;
- inductance;
- gate bias;
- temperature;
- outcome;
- pre/post IV curves.

Recommended additions:

- Treat avalanche `mode` as a `test_method_class` rather than only a filter.
- Derive `vds_peak / rated_vds` and dynamic overvoltage margin where waveform
  Vds and device rating are known.
- Track single-pulse vs repetitive avalanche shots.
- Add a "surge/overvoltage phenotype" view:
  - energy;
  - collapse fraction;
  - peak Vds;
  - peak current;
  - post-IV deltas;
  - outcome.

For proxy equivalence:

- Avalanche energy can be compared with irradiation event energy only after
  phenotype gating.
- The existing proton diagnostic already shows that energy-only matching points
  to avalanche, while energy plus collapse points to SC for the current proton
  SEB subset.
- Avalanche may still be a strong proxy candidate for hard-collapse heavy-ion
  SEB-like behavior, where collapse phenotype is closer.

### 8. Radiation Comparisons

The paper groups radiation effects into:

- total ionizing dose;
- displacement damage;
- single-event effects.

It then compares radiation susceptibility by:

- dose or fluence;
- particle species;
- LET;
- range;
- off-state blocking bias;
- device architecture;
- single-event burnout voltage or derating boundary;
- post-event leakage/parametric degradation.

Dashboard translation:

The Irradiation dashboard already has many required dimensions:

- ion species;
- beam energy;
- LET;
- fluence;
- event type;
- path type;
- event energy;
- Vds collapse;
- post-IV deltas through the damage-equivalence layer.

Recommended additions:

- Separate radiation mechanism class explicitly:
  - `tid`;
  - `displacement_damage`;
  - `single_event_effect`;
  - `mixed_or_unknown`.
- Add bias-state fields to event/radiation views when available:
  - off-state Vds;
  - gate bias;
  - drain current compliance;
  - blocking ratio vs rated voltage.
- Add a derating-oriented plot:
  - event/failure occurrence vs LET and Vds/rated_Vds;
  - post-IV damage vs fluence/dose;
  - event class vs bias state.

For proxy equivalence:

- TID/DD-like cumulative damage should not be forced into the same Joule-based
  proxy model as SC or avalanche pulses.
- Single-event burnout-like traces can be compared to electrical pulse stresses,
  but only through waveform phenotype plus post-IV damage agreement.

### 9. Extreme-Temperature Comparisons

The paper compares cryogenic and elevated-temperature behavior by:

- static Rds(on);
- Vth;
- breakdown voltage;
- dynamic Rds(on);
- switching loss;
- package/interconnect reliability.

Dashboard translation:

- The current proxy workflow should treat temperature as a first-class stress
  context, not only metadata.
- If temperature is present in avalanche or SC metadata, it should be included
  in matching distance or at least in candidate filters.
- For irradiation, temperature during exposure and post-IV measurement should be
  tracked separately if available.

## How This Could Be Applied To The Current Dashboards

### Irradiation Dashboard

Apply the paper's framework by adding mechanism-specific views:

- `radiation_mechanism_class`: TID, displacement damage, SEE, unknown.
- LET/range/fluence/dose panels separated by mechanism class.
- OFF-state bias derating panel: event rate or damage vs normalized blocking
  voltage.
- Event phenotype panel: event type, path type, Vds collapse, gate fraction,
  energy, and post-IV damage availability.

This would make the irradiation dashboard less label-only and more comparable
to SC/avalanche stress tests.

### SC Dashboard

Apply the SC robustness framework:

- show withstand time or pulse duration vs bus voltage;
- separate single-pulse from repetitive stress;
- add gate-drive conditions and protection timing;
- show post-SC Vth/Rds/BV deltas against SC voltage and duration;
- expose whether the stress was inside-SOA, near-boundary, or outside-SOA.

This would make SC data usable as a controlled robustness map, not just a set of
waveforms and pre/post curves.

### Avalanche Dashboard

Apply the surge/overvoltage framework:

- separate UIS, UID, RT, and other modes as test methods;
- compare commanded energy, integrated energy, peak Vds, peak current, and
  outcome;
- add dynamic overvoltage margin where peak Vds and rating are known;
- separate single-shot and repetitive shots;
- compare post-avalanche Vth/Rds/BV deltas to stress energy and overvoltage
  margin.

This would make avalanche usable as a surge/overvoltage robustness dashboard
and not only an energy dashboard.

### SC/Avalanche/Irradiation Damage Equivalence Dashboard

This dashboard is closest to the paper's "response comparison" logic because it
already compares post-IV shifts:

- delta Vth;
- delta Rds(on);
- delta BV.

Recommended changes:

- Add stress-regime and method-class filters.
- Show whether each match is measured, predicted, or waveform-only.
- Add a warning/status column when a match compares different regimes, such as
  cumulative irradiation damage to single-pulse electrical stress.
- Require at least two shared damage axes for "usable" equivalence candidates
  unless waveform phenotype is also strong.

### Predicted Irradiation Damage Dashboard

Apply the paper's vendor/test-method warning here:

- Predictions should carry the test-method context used to produce the response.
- A predicted irradiation fingerprint should not be silently treated as
  equivalent to measured irradiation.
- Confidence should include support status, donor count, validation coverage,
  and whether the stress regime is represented in the training data.

This dashboard is a good place to plan the next cheap SC/avalanche test, but
not a place to declare qualification replacement.

### Proxy Readiness Dashboard

This is the natural home for the paper's methodology.

Recommended new panels:

1. Stress-regime coverage matrix:

   Rows: device type.
   Columns: SC, avalanche, irradiation, post-IV, prediction.
   Cells: stability/reliability/robustness coverage.

2. Method-comparability matrix:

   Rows: target irradiation condition.
   Columns: SC and avalanche candidates.
   Metrics: energy distance, collapse distance, gate/path compatibility,
   damage distance, evidence tier.

3. Readiness blockers:

   Missing SC waveform, missing avalanche post-IV, missing irradiation bias,
   missing temperature, missing device rating, missing post-stress delay.

4. Application-likeness score:

   Score whether the candidate proxy was measured in a circuit-like stress
   state rather than a static or low-fidelity test.

## Practical Takeaways For The Proxy-Equivalence Goal

1. The paper supports the current conclusion that energy alone is too weak.

   Energy must be combined with stress regime, timescale, waveform phenotype,
   and post-IV damage.

2. The paper suggests a better proxy-candidate model:

   ```text
   candidate_score =
       waveform_similarity
     + phenotype_compatibility
     + post_iv_damage_similarity
     + application_likeness
     - missing_context_penalties
   ```

   This should remain transparent as separate columns, not hidden in one number.

3. SC and avalanche should be used for different irradiation analogs.

   - SC may be more plausible for non-collapse high-energy proton events in the
     current dataset when energy plus collapse is considered.
   - Avalanche may be more plausible for hard-collapse heavy-ion SEB-like
     events.
   - TID/DD-like cumulative proton damage needs dose/fluence/damage-axis
     comparison, not pulse-energy matching.

4. Add a stress-taxonomy layer before adding more ML.

   The paper's main lesson is that test method and stress regime decide what a
   result means. A clean stress taxonomy will make the existing dashboards more
   useful than a more complex nearest-neighbor score alone.

5. Add or recover missing context fields.

   Highest-value additions:

   - normalized Vds and current relative to rating;
   - stress temperature;
   - post-stress measurement delay;
   - pulse count and repetition rate;
   - failure/outcome status;
   - radiation mechanism class;
   - bias state during irradiation;
   - device architecture/family physics.

## Proposed Implementation Path

1. Add a `stress_test_context_view`.

   One row per metadata/stress record with common fields:

   - source;
   - device type;
   - stress regime;
   - method class;
   - SOA relation;
   - timescale class;
   - normalized voltage/current if ratings exist;
   - energy basis;
   - response reversibility;
   - outcome.

2. Join `stress_test_context_view` into:

   - `damage_equivalence_view`;
   - `stress_waveform_file_features`;
   - `stress_waveform_event_features`;
   - prediction fingerprint views.

3. Build a `stress_proxy_candidate_view`.

   This should combine:

   - waveform similarity;
   - phenotype compatibility;
   - damage fingerprint distance;
   - prediction support;
   - blockers.

4. Add dashboard filters/panels based on the paper's axes:

   - stress regime;
   - method class;
   - timescale;
   - SOA relation;
   - radiation mechanism;
   - application-likeness.

5. Keep the current gate-zero rule.

   Until device families have overlap among electrical proxy waveforms,
   irradiation events/waveforms, and post-IV damage, the dashboards should keep
   results in a readiness/candidate state.
