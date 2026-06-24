# Implementation Plan: Proxy-Method Review Findings (F1–F8)

Companion to `docs/proxy_method_review_2026-06-10.md`. Eight phases, ordered
by value/effort; each phase is independently committable and additive
(never widen the energy gates — tag, don't widen). Lab measurements (F7)
are out of scope except for the planning artifact in Phase 8.

Key files:

- `schema/025_proxy_readiness_waveforms.sql` — context + candidate views (main work site)
- `data_processing_scripts/ml_sc_irrad_equivalence.py` — damage equivalence SQL (sign agreement)
- `data_processing_scripts/create_proxy_readiness_dashboard.py` — schema apply + dashboard
- `schema/device_mapping_rules.sql` + `seed_device_mapping_rules.py` + `common.match_device()` — mapping infra (exists, reuse)
- `data_processing_scripts/ingestion_avalanche.py` — already re-maps `device_type` on re-run (lines ~1041–1089)
- `schema/027_radiation_stress_dose.sql` — dose columns to wire in (F3)

DB: Postgres `mosfets` on localhost:5435 (`db_config.get_connection()`).

---

## Phase 0 — Land in-flight work

Commit the current working tree as-is (stress-landscape changes to 025 +
dashboard + `radiation_stress_dose.py` fluence fix + `superset_api.py` +
`seed_radiation_dose_foundation.py` + `dashboard_png_export.py` + the review
doc). One commit so review-driven changes to the same files stay separable.
Verify `git status` clean.

## Phase 1 — Selam avalanche device mapping (F2a, priority 1)

1. Mine the SELAM thesis
   (`docs/relevant_papers/SELAM_THESIS_Excess Carrier Generation_SiC.pdf`,
   `pdftotext` it) + Nida 2021
   (`docs/relevant_papers/Avalanche_papers/Analysis_of_Current_Capability...pdf`)
   for sample-code → part-number evidence. Thesis ground truth (ch. 5):
   C2M0080120D (Wolfspeed planar), SCT2080KE (Rohm planar), SCT3080KL (Rohm
   trench); 3rd-gen 4-pin C3M0075120K appears for the `_4pin` experiments.
   Working hypothesis to verify against thesis figures/tables:
   `C*` → C2M0080120D, `RP*` → SCT2080KE, `RT*` → SCT3080KL,
   `C4P*` → C3M0075120K.
2. Build the proposed mapping table (sample-code pattern, experiment scope,
   part number, thesis page/section evidence, confidence). **Hard gate:
   confirm the mapping before any backfill runs.**
3. After confirmation:
   - Ensure parts exist in `device_library` (likely missing: SCT3080KL,
     C3M0075120K → extend `seed_device_library.py`).
   - Add `device_mapping_rules` rows, `scope='avalanche'`,
     `pattern_type='regex'` anchored on `/Selam/` path + code prefix (e.g.
     `SELAM.*[/\\]RP\d`), `source_reference` = thesis citation, notes =
     `inferred_from_thesis_confirmed_by_user`. Extend
     `seed_device_mapping_rules.py` with an `AVALANCHE_RULES` block
     (idempotent ON CONFLICT pattern already there).
   - Backfill via `ingestion_avalanche.py` re-run (it updates `device_type`
     on existing rows through `match_device()`); if a full re-ingest is
     slow, add a `--remap-only` flag that re-runs only the match/UPDATE pass.
   - Rebuild 025 views (`create_proxy_readiness_dashboard.py` applies the
     schema file, which DROP/CREATEs all matviews).

Verify: `SELECT COUNT(*) FROM stress_test_context_view WHERE
source='avalanche' AND device_type IS NOT NULL` (expect ~1258);
candidate-pool/device-overlap query from the review; record before/after in
the commit message.

## Phase 2 — Matching correctness fixes (F4 sign agreement, F8 gate axis)

1. `ml_sc_irrad_equivalence.py` `DAMAGE_VIEW_SQL` (measured pairs CTE ~line
   440 and prediction copy ~line 1336): add per-axis sign columns
   (`SIGN(right_dvth)=SIGN(left_dvth)` etc. with a small dead-band, e.g.
   |Δ| < scale/10 counts as sign-neutral), aggregate `sign_mismatch_axes`;
   demote `comparability_rank`: all-axes-mismatch → 4 ('inspect manually'),
   any-mismatch → max(rank, 3) ('weak'). Surface `sign_mismatch_axes` in
   match views + CSV export + `create_sc_irrad_dashboard.py` tooltip columns.
2. `schema/025` candidate-view `distances` CTE: make damage signature distance
   pairwise like the damage matcher — sum only available terms, normalize by
   available weight; remove fixed imputations
   (`COALESCE(collapse_delta, 0.75)`, `COALESCE(gate_delta, 0.25)`). Keep
   `missing_collapse_overlap` / `missing_gate_overlap` blockers; add
   `damage_signature_axes_used` count column; require ≥1 damage signature axis else status
   `missing_damage_signature_overlap`.

Verify: the C-ion ΔVth −1.21 vs SC +0.35 pair drops below 'usable'; no
candidate-view row has distance driven purely by imputation constants
(query `damage_signature_axes_used = 0`).

## Phase 3 — Censored SEB target tier + mechanism classes (F1, priority 2+4)

All in `schema/025` `stress_proxy_candidate_view` (+ dashboard):

1. **Target tiers**: replace the single `targets` CTE filter with
   `target_match_tier`:
   - `energy_comparable` — current filter, unchanged behavior.
   - `energy_censored_damage_signature_only` — detected single events failing the
     energy gates; carry `energy_censored_reason`; compute
     `target_energy_floor_j` = `event_energy_vds_id_j` when
     `energy_censored_reason = 'failure_cutoff'` (right-censored lower
     bound), else NULL.
   - For the censored tier: exclude `log_energy_delta` from
     `waveform_distance`; add blocker `candidate_energy_below_censored_floor`
     when `candidate_energy_j < target_energy_floor_j` and demote those
     candidates' rank. This makes the existing dead-code `target_energy_*`
     blocker CASEs live for tier-B rows (keep them).
2. **Mechanism table**: new pipeline-owned table
   `stress_mechanism_compatibility` in 025 (CREATE TABLE IF NOT EXISTS +
   idempotent seed INSERTs): columns (target_event_type, candidate_source,
   min_candidate_collapse, mechanism_match_class, path_penalty,
   status_ceiling, rationale). Seed per review:
   - SEB × avalanche (collapse ≥ 0.30) → `thermal_runaway_pair`,
     penalty 0.15, no ceiling
   - SEB × sc → `thermal_runaway_pair_secondary`, 0.25
   - SELCI × sc → `gate_oxide_pair_repetitive_only`, 0.50, ceiling
     `analog_questionable`
   - SELCII/MIXED × any → `cumulative_defect_no_electrical_analog`, 0.75,
     ceiling `analog_questionable`
   - fallback for path_type equality → 0.0 as today.
   Join it in `pairs` to replace the hand-tuned `path_penalty` CASE; emit
   `mechanism_match_class`.
3. **Status**: add `analog_questionable` to `candidate_status` (applied as
   ceiling), extend the priority CASE + `replacement_confidence` +
   `CANDIDATE_COLORS` and filters in `create_proxy_readiness_dashboard.py`;
   add tier/mechanism columns to the candidate table chart + a censored-SEB
   coverage chart.

Verify: SEB targets present
(`SELECT COUNT(DISTINCT target_stress_record_key) FROM
stress_proxy_candidate_view WHERE target_event_type='SEB'` > 0);
C2M0080120D Ni SEB events (2, collapse ≈ 1.0) get avalanche candidates with
`mechanism_match_class = 'thermal_runaway_pair'`; SELCII × sc rows now
`analog_questionable` (not `weak_measured_candidate`); previous tier-A rows
otherwise unchanged (diff counts).

## Phase 4 — Normalized axes + dose wiring (F3, priority 6)

1. Add `normalized_vds_delta = |candidate.normalized_vds −
   target.normalized_vds|` to the damage signature distance (pairwise, weight
   ~/0.15 — literature: SEB bias fraction is the dominant axis) — matters
   most for the censored SEB tier where energy is excluded.
2. Context view: add `stress_energy_density_j_cm3` for sc/avalanche =
   `stress_energy_j / (exposed_area_cm2 × thickness_um × 1e-4)` from
   `device_material_layers` (layer_order 0; LEFT JOIN, NULL-safe, carry
   geometry confidence); add `energy_localization_class`
   ('ion_track_localized' for irradiation events, 'bulk_active_region' for
   sc/avalanche). Report `energy_density_ratio` on candidate pairs as a
   column (NOT in distance — honesty about locality mismatch).
3. Wire `target_radiation_dose_total_gy` / deposited energy into reported
   columns for SELC/cumulative targets (already selected; add per-pair
   `dose_context_available` flag) — no distance term yet.

Verify: spot-check one SEB target: candidate ranking responds to
normalized_vds; energy_density_ratio populated where geometry exists; no row
loses candidates solely from the new join (LEFT JOIN check).

## Phase 5 — Calibration harness (F5, priority 7)

New `data_processing_scripts/calibrate_proxy_distance.py`:

- Ground truth: rank-1 `usable`+ rows from `damage_equivalence_match_view`
  (and avalanche pairs).
- Leave-one-out: for each damage-confirmed (target-run, candidate-condition)
  pair, ask whether `stress_proxy_candidate_view`'s waveform distance ranks
  that condition first among same-device candidates; report top-1/top-3 hit
  rate and Spearman rank correlation per weight configuration (small grid
  over collapse/gate/normalized-vds/energy weights + status thresholds).
- Output `out/proxy_distance_calibration/report.md` + JSON of best
  constants. Honest caveat in report: n is tiny (~10 pairs); this is a
  sanity harness, not training.
- Consolidate constants: move all candidate-view weights/thresholds into a
  one-row `stress_proxy_distance_settings` table (created + seeded in 025,
  joined via CROSS JOIN in the view) so the calibration script can report
  against named settings and future tuning is a seeded-row change,
  reviewable in diff. Mirror defaults in a single commented block.

Verify: script runs against the live DB, report generated; rebuilt views
produce identical results with settings-table defaults (count + checksum
query before/after).

## Phase 6 — Cross-device retrieval tier (F2b, priority 9)

1. Derive `voltage_class` (650/900/1200/1700) and `technology_class`
   (planar/trench from `device_library.device_category`/notes) in the
   context view (reuse the `rated_voltage_v` parsing already there).
2. Candidate view: add a second candidate pool where `device_type` differs
   but voltage_class matches and the target has no same-device candidate;
   require normalized axes present; label `match_scope = 'cross_device'`,
   status ceiling `cross_device_screening_only` (new status below
   waveform_only), rank after all same-device candidates.

Verify: IFX-Trench heavy-ion SEB targets (Kr/Au, post-IV-rich) now receive
cross-device avalanche candidates labeled `cross_device_screening_only`;
same-device results unchanged.

## Phase 7 — Repetition/cumulative descriptors (F6, priority 10)

1. New additive table in 025 (or new `028_stress_pulse_history.sql` if
   cleaner ownership): `stress_pulse_history(metadata_id, pulse_index,
   pulse_count_in_sequence, sequence_key, cumulative_energy_j, basis,
   provenance)`.
2. Extraction script `extract_stress_pulse_history.py`: parse sequence
   counters from avalanche/SC filenames (Selam files encode them:
   `RP10_1.4J_Vg-1000001.h5` trailing counter; SC `*_pulseN` patterns where
   present) grouped per physical sample; emit per-sample cumulative pulse
   count + energy.
3. Context view: join per-sample `prior_pulse_count` /
   `cumulative_prior_energy_j` for sc/avalanche records; candidate view: for
   SELC/cumulative targets, report (target fluence/dose from 027) vs
   (candidate pulse_count × pulse energy) as columns +
   `repetition_context_available` flag.

Verify: pulse counts populated for Selam sequences; a SELCII target row
shows dose-vs-repetition context columns.

## Phase 8 — Experiment-planning artifact (F7 surface)

Add `stress_proxy_experiment_plan_view` (025): aggregate
`candidate_blockers` to rank which single measurement unblocks the most
pairs (e.g. "post-IV after C2M0080120D SC 600V/8µs", "UIS ladder + post-IV
on IFX-Trench", "irradiation for SCT2080KE"), with pair counts and affected
targets. Add a dashboard tab/chart in `create_proxy_readiness_dashboard.py`.

Verify: the view's top rows match the review's §F7 shortlist (sanity),
counts correct against blocker tallies.

---

## Cross-cutting rules

- Additive only: never widen the energy gates (tag, don't widen); tier-A
  behavior must remain reproducible — every phase records before/after
  counts of the `candidate_status` distribution in its commit message.
- 025 is DROP/CREATE; each phase ends with: apply schema via
  `create_proxy_readiness_dashboard.py` (or its apply function), refresh
  Superset datasets (`refresh_dataset_columns` helper), run
  `pytest tests/ -q`, run the verification SQL.
- One commit per phase, following repo message style.
- Hard gate in Phase 1: confirm the sample-code → part-number table before
  any backfill.

## End-to-end verification (after Phase 3, repeat at end)

```sql
-- SEB targets now evaluable, with mechanism-aware candidates
SELECT target_match_tier, target_event_type, candidate_source,
       mechanism_match_class, candidate_status, COUNT(*)
FROM stress_proxy_candidate_view
WHERE candidate_rank = 1
GROUP BY 1,2,3,4,5 ORDER BY 1,2,6 DESC;

-- Avalanche pool usable
SELECT COUNT(*) FILTER (WHERE device_type IS NOT NULL)
FROM stress_test_context_view WHERE source='avalanche';
```

Then rebuild the Proxy Readiness dashboard and confirm charts render with
the new statuses/tiers (dashboard script exits 0; spot-check via the
Superset API helpers).
