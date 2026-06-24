# Proxy-Finding Methods Review and Improvement Plan

Date reviewed: 2026-06-10
Scope: all code paths that propose SC/avalanche stress as a proxy for
irradiation stress, the live database state behind them, and the local
literature in `docs/relevant_papers/`.
Previous review: `docs/proxy_stress_equivalence_review.md` (2026-05-25).

## 1. Inventory of Current Methods

Six layers participate in proxy finding today.

### 1.1 Source-aware single-event extraction (foundation labels)

- `data_processing_scripts/extract_single_event_effects.py`
- `docs/single_event_detection.md`

Produces `event_type` (SEB / SELCI / SELCII / MIXED) and mechanism-neutral
`path_type`. 516 files, 1811 events: 76 SEB, 177 SELC-I, 1520 SELC-II,
38 MIXED. High-energy-proton events can only be SEB or UNKNOWN; low-energy
proton steps are TID/DD diagnostics. This is the layer that prevents
damage signature matching across incompatible radiation mechanisms.

### 1.2 Measured damage-fingerprint matching

- `data_processing_scripts/ml_sc_irrad_equivalence.py`
- `damage_equivalence_view`, `damage_equivalence_match_view`,
  `damage_equivalence_coverage_view`, `damage_equivalence_match_segment_view`
- `out/sc_irrad_equivalence/irrad_to_sc_equivalents.csv`

Median (ΔVth, ΔRds(on), ΔBV) per stress condition vs a source-aware pristine
baseline; reliability-weighted Euclidean nearest neighbor with pairwise axis
dropping. Rank-1 today: SC↔irradiation 6 usable + 4 weak,
avalanche↔irradiation 2 usable + 2 weak + 2 inspect, SC↔avalanche 1 usable.
Most matches still rest on one comparable axis.

### 1.3 Validation-gated predicted damage

- `data_processing_scripts/ml_post_iv_physical_prediction.py`
- `damage_equivalence_prediction_*` views

Predicts irradiation post-IV physical parameters (Δvth, log Rds ratio) where
measured irradiation is missing, with donor counts, validation gates, and
reference tiers carried on every match. Used as a separate, lower evidence
tier — correct design.

### 1.4 Waveform damage signature + energy candidate retrieval (production method)

- `schema/025_proxy_readiness_waveforms.sql`
- `stress_waveform_file_features`, `stress_waveform_event_features`,
  `stress_test_context_view`, `stress_proxy_candidate_view`,
  `stress_proxy_candidate_summary_view`
- `data_processing_scripts/create_proxy_readiness_dashboard.py`
  (now real source, not `.pyc` — May-review item resolved)

`stress_proxy_candidate_view` is the production proxy recommender:

- targets: irradiation detected single events with comparable event-level
  integrated Vds·Id energy;
- candidates: SC/avalanche records of the **same `device_type`** within
  |Δln E| ≤ 5;
- waveform distance = log-energy delta + damage signature distance
  (collapse delta /0.25, gate delta /0.20, hand-tuned `path_penalty`)
  + 0.01·(duration log delta)²;
- evidence join: measured damage match (exact-condition vs device-run scope),
  else predicted damage, else waveform-only;
- output: `candidate_status`, `replacement_confidence`, `candidate_blockers`,
  top-10 per target.

This implements the May review's recommended candidate table, including
energy-basis calibration flags (`energy_is_comparable`, censoring reasons,
window basis) and fail-closed prediction gating.

### 1.5 Radiation dose/deposition layer (new since May)

- `schema/027_radiation_stress_dose.sql`
- `data_processing_scripts/radiation_stress_dose.py`
- `data_processing_scripts/seed_radiation_dose_foundation.py`

Per-layer deposited energy and dose (electronic/nuclear/total, Gy) from
fluence × stopping power × geometry, with provenance and confidence.
Populated: 1795 single-particle + 1441 event-window components calculated.
Single-layer SiC active-region model; proton SiC stopping from PSTAR Bragg
additivity; heavy-ion LET copied from `irradiation_runs`. The columns are
carried into `stress_test_context_view` / candidate view as context but are
**not yet part of any matching distance**.

### 1.6 Diagnostics and pilots

- `plot_proton_proxy_match_shift.py`: energy-only vs energy+collapse nearest
  proxy class (43/44 proton SEB events flip from avalanche to SC when
  collapse is added) — the canonical "energy alone is retrieval, not
  equivalence" result.
- `pilot_avalanche_irradiation_compare.py`: C2M0080120D case study; proton
  events are SELCII-like (collapse 0), heavy-ion SEB is avalanche-like
  (collapse ≈ 0.99).
- `stress_proxy_gate_zero_view` / `stress_proxy_readiness_view`: coverage
  gates. Gate-zero still **fails**: 0 candidate device families.

## 2. Live State (queried 2026-06-10)

| fact | value |
| --- | --- |
| candidate pairs in `stress_proxy_candidate_view` | 390 |
| statuses present | weak_measured (180), missing_damage_context (178), inspect_manually (20), damage_signature_mismatch (12) |
| measured_damage / predicted_damage candidates | **0 / 0** |
| rank-1 targets covered | C2M0080120D SELCII + 1 MIXED only |
| rank-1 recommendation today | SC 600V/8µs (12 targets), SC 600V/2µs (6 targets), all vs Ca-ion SELCII, weak 1-axis damage support |
| universal blockers | missing_gate_overlap 390/390, candidate_missing_condition_post_iv 390/390 |
| SEB targets in candidate view | **0** (all 76 SEB events fail energy comparability: failure_cutoff, active_window_unknown) |
| avalanche records usable in join | 176 of 1258 (1082 Selam-experiment files have NULL `device_type`) |
| SC records | 26 (22 with device_type) |
| device overlap (candidates ∩ irradiation) | C2M0080120D only; SCT2080KE has SC+avalanche but no irradiation |
| heavy-ion hard-collapse SEB with post-IV | IFX-Trench/Kr (6), IFX-Trench/Au (4), CPM3-1200-0075A/Au (4), C2M0080120D/Ni (2) |

## 3. Findings and Recommendations

### F1 — The physically strongest proxy pairing (SEB ↔ avalanche) is
### structurally excluded; the pairing the pipeline does recommend
### (SELC-II ↔ SC) is mechanistically weak

The literature chain is consistent:

- SEB in SiC MOSFETs ends in parasitic-BJT latch / thermal runaway initiated
  by ion-induced local avalanche (Grome & Ji review; Ball 2021; Pocaterra &
  Ciappa alphas at 80–93 % V_BD).
- UIS avalanche failure is the same end state — BJT latch above a critical
  current/energy (Nida et al. 2021, this group: latch above ~52 A on 1.2 kV
  parts, with a UIS-based design-limit procedure).
- Wu et al. 2024 model SC withstand time and critical avalanche energy with
  one thermal-runaway criterion — SC and UIS bridge through junction
  temperature.
- SELC-II, by contrast, is cumulative stacking-fault formation from many ion
  strikes (Für/Medeiros et al., group TNS resubmission). No single-pulse
  electrical overstress creates ion-track stacking faults.

Yet today: every SEB event is dropped from targets by the energy gates
(`failure_cutoff` truncation at the moment of failure, unknown active
windows), while 1500+ SELC-II events dominate and draw SC recommendations on
waveform-shape similarity plus 1-axis weak damage matches.

Recommendation (follows the lab's "tag quality, don't widen" rule):

1. Add a **damage-signature-only target tier** for energy-censored events instead of
   excluding them. Keep the energy axis out of the distance for that tier and
   emit `target_energy_censored_*` blockers, which the schema already
   defines but which are currently dead code (targets that would carry them
   never enter the view).
2. Treat destructive-event energy as **right-censored, not missing**: a
   truncated SEB waveform still yields "energy ≥ X". Candidates with energy
   below that bound can be screened out; candidates above it stay. This is a
   survival-analysis framing, not a precision-energy framing.
3. Add a mechanism-compatibility classification, e.g.
   `mechanism_match_class`: `thermal_runaway_pair` (SEB↔UIS, SEB↔SC),
   `cumulative_defect_no_electrical_analog` (SELC-II↔anything single-pulse),
   `gate_oxide_pair` (SELC-I/TID ↔ repetitive SC / gate stress — see F6).
   Statuses for `cumulative_defect_no_electrical_analog` pairs should cap at
   `analog_questionable` no matter how good the waveform distance looks.
   This replaces the opaque hand-tuned `path_penalty` constants with an
   auditable table.

### F2 — One device family carries the whole program; 1082 avalanche files
### are unusable for matching because `device_type` is NULL

The exact join `c.device_type = t.device_type` is correct as the strictest
tier, but the data can't feed it: the AVL_Selam / AVL_RT / AVL_UID_m10V
experiments (1082 files — 86 % of all avalanche waveforms) have no
`device_type`, so they can never match anything. Meanwhile the hard-collapse
heavy-ion SEB events with post-IV sit mostly on IFX-Trench and
CPM3-1200-0075A, which have no electrical-stress data at all under exact
device matching.

Recommendations:

1. **Map the Selam avalanche files to part numbers** (thesis + Nida 2021
   test matrices identify the 1.2 kV commercial parts). This is the single
   cheapest data unlock in the whole pipeline; it multiplies the avalanche
   candidate pool by ~7 and may create new device overlaps.
2. Add a **second retrieval tier across devices within a voltage/technology
   class** (`device_mapping_rules` already exists as infrastructure),
   allowed only on normalized axes (F3) and always labeled
   `cross_device_candidate` with its own confidence ceiling. Without this,
   IFX-Trench SEB targets are permanently unmatchable no matter how much
   data arrives for other families.

### F3 — Matching uses raw terminal Joules; the literature normalizes

Raw energy across devices and mechanisms is not comparable: the FOM-like
papers normalize SEB bias as a fraction of breakdown voltage (alpha SEB at
80–93 % V_BD; heavy-ion SEB commonly ~40–50 % V_BD; Ball 2021 ties tolerance
to epi thickness/doping), avalanche capability as current vs the BJT-latch
limit (Nida 2021), and runaway onset as energy density vs critical
temperature (Wu 2024). The context view already computes `normalized_vds`
and `normalized_current` — they are displayed but unused in the distance.

Recommendations, in increasing order of physics:

1. Add `normalized_vds` delta (V/BV fraction) to the damage signature distance for
   blocking-state targets; for SEB targets this is the dominant axis in the
   literature and is robust to instrument current-scale differences.
2. Use the new `device_material_layers` area/thickness estimates to express
   stress as **energy density** (J/cm³ of active SiC) on the electrical
   side, and per-particle deposited energy (already computed in the dose
   layer) on the radiation side. Carry an explicit localization-mismatch
   term: an ion deposits its energy in a ~µm-scale filament, UIS heats the
   full active area; matched terminal Joules are orders of magnitude apart
   in local energy density. Even a crude modeled ratio (track volume vs
   active volume) is more honest than implicit equivalence.
3. Wire `radiation_deposited_energy_*` / dose columns into the matching
   distance for cumulative targets (F6) — they are currently context-only.

### F4 — Damage comparability can rank an opposite-sign 1-axis match as
### "nearest"

In `out/sc_irrad_equivalence/irrad_to_sc_equivalents.csv`, C-ion runs with
ΔVth = −1.21 V match SC conditions with ΔVth = +0.35 V at distance ≈ 1.1 on
a single axis. Negative Vth shift (TID-like hole trapping / donor-like
defects) vs positive Vth shift (electron trapping from SC stress) are
different physics; magnitude-scaled distance hides that.

Recommendations:

1. Require **sign agreement** on each compared axis (or add a categorical
   sign-mismatch penalty that forces status below `usable`).
2. Keep 1-axis matches capped at `weak` (already the case) and surface the
   axis identity + signs in the dashboards (partially done via
   `comparable_axis_labels`).

### F5 — Distance weights and thresholds are hand-tuned constants with no
### calibration harness

`/0.25`, `/0.20`, `0.01`, missing-value imputations (0.75, 0.25), status
cuts (1.25/1.75/2.25/2.50/3.00/4.0) are all uncalibrated. With so little
ground truth this cannot become a trained model yet — but it can become a
**scored** one:

1. Build a tiny leave-one-out harness over the measured damage matches
   (`damage_equivalence_match_view` usable pairs): does waveform distance
   rank the damage-confirmed candidate first? Report rank correlation per
   weight choice. Store the result with the view version.
2. Move the constants into one SQL/Python constant block (or a settings
   table) with comments tying each to its calibration result, so future
   tuning is reviewable.

### F6 — Cumulative damage (SELC-II accumulation, proton TID/DD) needs a
### repetition axis, not a bigger single pulse

The group's own papers define the target: SELC-II degradation accumulates
over hundreds of strikes and can transition to catastrophic failure
(cumulative TNS paper); 1–3 MeV protons degrade via carrier removal + oxide
trapping with dose (NSREC 2025). The electrical analog of *that* is
repetitive sub-critical stress (N × short SC pulses producing gradual
Vth/Rds drift), not one larger pulse. The schema has no per-file pulse
count / pulse index / cumulative-exposure descriptors for SC and avalanche
tests, so a "100 × 2 µs SC pulses" recipe cannot even be represented.

Recommendations:

1. Add `pulse_count` / `pulse_index` / cumulative-stress descriptors to SC
   and avalanche metadata (the May review's "record exact stress recipes"
   item — still open).
2. Match cumulative radiation targets on (dose or fluence, per-event
   severity, repetition count) against (pulse energy, pulse count), keeping
   the dose layer's Gy axis as the radiation side.

### F7 — Post-IV coverage is still the binding constraint (gate-zero fails)

Every one of the 390 candidate pairs has `candidate_missing_condition_post_iv`;
electrical-proxy waveform+post-IV overlap exists for 2 files (SCT2080KE);
avalanche has one usable damage fingerprint. No algorithmic change fixes
this. The measurement shortlist, in value order:

1. Post-IV (IdVg, IdVd, blocking) after the existing C2M0080120D SC
   600V/2–8µs and D3-class UIS conditions — directly converts today's
   `weak_measured_candidate` rows into testable `measured_damage_candidate`
   rows.
2. A UIS energy ladder with post-IV on a heavy-ion-SEB-rich family
   (IFX-Trench or CPM3-1200-0075A) — creates the first real SEB↔avalanche
   damage comparison.
3. Irradiation (or archived irradiation data recovery) for SCT2080KE — the
   one family that already has electrical-proxy post-IV overlap.

### F8 — Gate-current overlap is missing in 100 % of pairs

`gate_delta` is imputed for all 390 pairs, so the gate axis contributes a
constant — it differentiates nothing while appearing in the formula. Either
extract gate current where traces have it on both sides, or drop the axis
from the distance until it can discriminate (keep it as a flag).

## 4. How to Find a Recommended SC/Avalanche Proxy Test

### Today (works now)

Step 1 — query the production view for the target family/effect:

```sql
SELECT candidate_rank, candidate_source, candidate_stress_condition_label,
       candidate_sc_voltage_v, candidate_sc_duration_us,
       candidate_avalanche_mode, candidate_energy_j,
       waveform_distance, best_damage_distance, damage_evidence_tier,
       candidate_status, replacement_confidence, candidate_blockers
FROM stress_proxy_candidate_view
WHERE device_type = 'C2M0080120D'
  AND target_event_type = 'SELCII'        -- or SEB / MIXED
  AND candidate_rank <= 3
ORDER BY target_stress_record_key, candidate_rank;
```

Step 2 — read `candidate_status` honestly: anything below
`measured_damage_candidate` is a *screening* suggestion. Today's best
answer is "SC 600 V / 8 µs (or 2 µs) resembles C2M0080120D Ca-ion SELC-II
events in waveform damage signature, with weak 1-axis damage support" — and per F1
its mechanism compatibility is questionable; treat it as a hypothesis to
test, not a recommendation to substitute.

Step 3 — check `candidate_blockers` and the gate-zero/readiness dashboards
to see which measurement would upgrade the answer (today: post-IV after the
SC condition, avalanche fingerprints, SEB energy censoring).

Step 4 — for planning a *new* proxy test (no candidate exists), use the
prediction planner (`damage_equivalence_prediction_*`): which SC/avalanche
condition is predicted to land nearest in post-IV space, with validation
gates passing.

### Target workflow (after F1–F3 land) — SEB example

1. Define the target: family, environment (ion/LET or proton energy), bias
   point; SEB targets enter via the censored/damage signature tier.
2. Mechanism gate: SEB → UIS avalanche first (BJT-latch pair), SC second.
3. Match on normalized axes: V_stress/BV near the target's bias fraction
   (literature: heavy-ion SEB ≈ 40–50 % V_BD, alpha SEB ≈ 80–93 % V_BD),
   peak current vs latch limit, energy ≥ censored lower bound, collapse
   damage signature ≥ 0.9.
4. Validate: post-IV fingerprint agreement on ≥ 2 same-sign axes
   (measured > predicted), IQR and n reported.
5. Report the equivalence envelope row (condition, axes, tier, status,
   blockers) — never a single equivalent-Joules number.

## 5. Priority Order

| # | action | type | effort | unlocks |
| --- | --- | --- | --- | --- |
| 1 | Map Selam avalanche files to part numbers | data curation | low | ~7× avalanche pool, new overlaps |
| 2 | Censored/damage-signature-only SEB target tier | schema/SQL | low | the physically right proxy question |
| 3 | Sign agreement in damage comparability | SQL | low | stops false "nearest" matches |
| 4 | Mechanism-compatibility class table | schema/SQL | low | auditable replacement for path_penalty |
| 5 | Post-IV after existing C2M0080120D SC/UIS conditions | lab | medium | first measured_damage_candidate rows |
| 6 | Normalized axes (V/BV, I/Irated, energy density) in distance | SQL/Python | medium | cross-condition comparability |
| 7 | Leave-one-out weight calibration harness | Python | medium | defensible thresholds |
| 8 | UIS ladder + post-IV on IFX-Trench/CPM3 | lab | high | real SEB↔avalanche evidence |
| 9 | Cross-device retrieval tier | SQL | medium | unmatchable targets get candidates |
| 10 | Pulse-count/cumulative descriptors + dose-axis matching | schema | medium | honest SELC-II/TID proxies |

## 6. Literature Anchors (docs/relevant_papers)

- `Linking stress types/electronics-13-00996.pdf` (Wu 2024): one
  thermal-runaway model spans SC withstand time and critical avalanche
  energy — the SC↔UIS bridge.
- `Avalanche_papers/Analysis_of_Current_Capability...` (Nida 2021, APS):
  UIS procedure identifying BJT-latch design limits — the avalanche-side
  test recipe and normalization.
- `Linking stress types/electronics-13-01414-v3.pdf` (Grome & Ji 2024):
  SEB mechanism review + radiation/electrical merit system — FOM framing.
- `Radiation-FOM-like-papers/Effects_of_Breakdown_Voltage...` (Ball 2021):
  SEB tolerance scales with epi design; power dissipation along the ion
  track core — energy-density argument.
- `Linking stress types/1-s2.0-S0026271423002275-main.pdf` (Pocaterra &
  Ciappa 2023, ETH): alpha SEB at 80–93 % V_BD — bias-fraction axis.
- `SELC Statistics/Cumulative_Effects_TNS...` (APS group): SELC-II is
  cumulative stacking-fault damage — why single-pulse proxies are wrong
  for it.
- `NSREC2025_TNS_FINAL.pdf` (APS/Padova): proton TID/DD carrier removal +
  oxide trapping — why proton degradation needs dose axes, not Joules.
- `Linking stress types/Huang_2024...`: SC failure mode flips between bus
  voltages (metal cracking vs gate-oxide runaway) — SC condition choice
  changes mechanism, so SC proxies must record bus voltage and pulse
  recipe, not just energy.
