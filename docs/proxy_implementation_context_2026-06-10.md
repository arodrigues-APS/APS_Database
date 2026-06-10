# Session Context Handoff — Proxy Implementation (2026-06-10)

Purpose: everything a new conversation needs to execute
`docs/proxy_implementation_plan_2026-06-10.md` without re-deriving this
session's exploration. Read alongside:

- `docs/proxy_method_review_2026-06-10.md` — findings F1–F8, literature
  anchors, the "how to find a proxy" procedure (the *why*).
- `docs/proxy_implementation_plan_2026-06-10.md` — phases 0–8 (the *what*).
- `docs/proxy_stress_equivalence_review.md` — earlier 2026-05-25 review.
- This file — environment, code anchors, live-DB numbers, decisions, gotchas
  (the *where and how much*).

## 1. Decisions already made by the user

1. Scope: implement **all** code findings F1–F8, phased per the plan. Lab
   measurements out of scope except Phase 8 planning view.
2. Phase 1 Selam mapping: extract sample-code → part-number evidence from
   the SELAM thesis, **stage and present for user confirmation before any
   backfill** (hard gate).
3. Sequencing: **Phase 0 commits the current uncommitted working tree
   first** (stress-landscape work) so plan changes stay separable.
4. The user wants to drive implementation; do not start phases unprompted.

Lab working rules (from prior feedback, apply throughout):

- Pristine data layering: provenance (`data_source`), role/state
  (`irrad_role`/`test_condition`), curation status, `is_likely_irradiated`
  are orthogonal — never collapse them.
- Phased, additive rollouts for schema changes touching live data.
- "Tag quality, don't widen": when a constraint excludes legitimate data,
  add a quality flag/tier and include it — never silently loosen the gate.

## 2. Environment

- Repo: `/home/arodrigues/APS_Database/APS_Database`, branch `master`,
  HEAD `1773e8d` "Checkpoint before stress landscape implementation".
- Postgres: `PGPASSWORD=APSLab psql -h localhost -p 5435 -U postgres -d mosfets`
  (defaults in `data_processing_scripts/db_config.py`, env-overridable).
- Superset on `localhost:8088` (helpers in
  `data_processing_scripts/superset_api.py`; dashboard slug
  `proxy-readiness-waveforms`; dataset map `DATASET_TABLES` in
  `create_proxy_readiness_dashboard.py`).
- `pdftotext` is installed (papers in `docs/relevant_papers/`).
- Tests: `pytest tests/ -q`.
- Schema application pattern: `common.apply_schema(conn, include_pipeline={...})`;
  `create_proxy_readiness_dashboard.py::apply_proxy_schema()` applies
  pipeline schemas `022_irradiation_single_events.sql` +
  `027_radiation_stress_dose.sql`, then executes
  `schema/025_proxy_readiness_waveforms.sql` (which DROP/CREATEs all its
  matviews).

## 3. Uncommitted working tree (Phase 0 commits all of this)

Modified: `schema/025_proxy_readiness_waveforms.sql` (+87: adds
`stress_observed_abs_vds_v` to file/event features, event-window dose rollup
guard, `average_terminal_power_w`, `figure1_panel_label` +
`figure1_regime_family` + index — "stress landscape" / Figure 1 work),
`data_processing_scripts/create_proxy_readiness_dashboard.py` (+347/−95,
landscape panels, `big_number_params` helper),
`data_processing_scripts/radiation_stress_dose.py` (NULL-safe
`fluence_delta_cm2`), `data_processing_scripts/superset_api.py` (+8).
Untracked: `data_processing_scripts/seed_radiation_dose_foundation.py`,
`data_processing_scripts/dashboard_png_export.py`,
`docs/proxy_method_review_2026-06-10.md`,
`docs/proxy_implementation_plan_2026-06-10.md`, this file.

## 4. Code map with line anchors (pre-Phase-0 working tree)

### schema/025_proxy_readiness_waveforms.sql (~2350 lines)

Dependency chain: `stress_waveform_file_features` (L17) →
`stress_waveform_event_features` (L698) → `stress_waveform_basis_feature_view`
(L1048), `stress_proxy_readiness_view` (L1086), `stress_proxy_gate_zero_view`
(L1249), `stress_test_context_view` (L1307) → `stress_proxy_candidate_view`
(L1748) → `stress_proxy_candidate_summary_view` (L2296).

Candidate view internals:

- `targets` CTE L1749–1760: irradiation detected events, requires
  `energy_is_comparable AND energy_level='event' AND
  electrical_terminal_energy_basis='integrated_event_vds_id' AND energy>0`.
  **This is what excludes all SEB events** (Phase 3 changes this).
- `candidates` CTE L1761–1768: sc/avalanche with device_type + energy.
- `pairs` L1769–1904: join on `c.device_type = t.device_type`, prefilter
  `|Δln E| ≤ 5.0`; hand-tuned `path_penalty` CASE at L1887–1900
  (SEB×avalanche collapse≥0.30 → 0.15; SEB×sc → 0.25; SELC×sc → 0.50;
  NULL path → 0.25; else 0.75) — replaced by mechanism table in Phase 3.
- `distances` L1905–1926: phenotype = sqrt((COALESCE(collapse_delta,0.75)/0.25)²
  + (COALESCE(gate_delta,0.25)/0.20)² + path_penalty²); waveform = sqrt(
  log_energy_delta² + phenotype² + 0.01·duration_log_delta²). The fixed
  imputations are removed in Phase 2 (pairwise normalization instead).
- `evidence` L1927–2036: LATERAL joins to `damage_equivalence_match_view`
  (match_scope exact_condition vs device_run_best_damage) and
  `damage_equivalence_prediction_match_view` (sc only, latest model run).
- `classified` L2038–2096: status thresholds (energy_out_of_range >4.0,
  phenotype_mismatch >2.50, measured ≤1.75, device-run ≤2.25, weak ≤3.00,
  waveform_only ≤1.25; best_damage default 2.50 in combined distance);
  `candidate_blockers` array — the `target_energy_*` blocker CASEs at
  L2083–2094 are currently dead code (targets pre-filtered); they become
  live for the censored tier in Phase 3.
- `ranked` L2098–2147 (status priority + combined distance), top-10 filter
  L2283.

Context view internals: terminal energy/basis selection L1320–1396
(irradiation events only get energy when comparable; bases:
integrated_event_vds_id / non_comparable_integrated_event /
proxy_event_rectangular_excluded / commanded_or_stored / missing);
dose rollup join L1308–1318 + L1397–1446 (event_window + single_particle
scopes from `radiation_stress_dose_summary_view`); `rated` voltage/current
parsing from `device_library` L1463–1479; `normalized_vds` /
`normalized_current` / `average_terminal_power_w` L1480–1520 (computed but
NOT in any distance — Phase 4 wires normalized_vds in);
regime/taxonomy/context_flags L1633–1735.

Event features: sc/avalanche files are `file_as_event` rows, always
`energy_is_comparable` when energy exists, basis `full_file_waveform`,
censored 'none' (L752–757); irradiation events carry
`energy_is_comparable/energy_window_basis/energy_censored_reason/
active_window_confidence/energy_level` from
`irradiation_single_event_energy_view` (L857–861), which is fed by
`irradiation_energy_windows.py` (+ schema 026).

### data_processing_scripts/ml_sc_irrad_equivalence.py (~2050 lines)

`DAMAGE_VIEW_SQL` from ~L70. Pristine pool/baseline ~L84–120 (source-aware,
`NOT is_likely_irradiated`). Measured pairs CTE ~L440–585: per-axis
reliability weights sqrt(sqrt(n)/(1+|IQR|) cross products), per-device robust
`axis_scales`, distance = weighted RMS over available axes;
`comparability_rank`: ≥3 axes & d≤0.75 → 1 (strong), ≥2 & ≤1.5 → 2 (usable),
≤2.5 → 3 (weak), else 4 (inspect manually); **no sign-agreement check —
Phase 2 adds it here and in the prediction copy ~L1336–1423**. Match
ranking ~L562–575. CSV export ~L1988–2017 →
`out/sc_irrad_equivalence/irrad_to_sc_equivalents.csv`.

Concrete sign-mismatch evidence (current CSV): C-ion 12 MeV run
irrad ΔVth −1.21 V matched to SC 800V/2.5µs ΔVth +0.35 V at distance 1.141
on 1 axis; C 6 MeV similar (0.921). Counter-example (legit): Ca 344 MeV →
SC 400V/18µs d=0.416 on ΔBV axis, same sign (+193 vs +149).

### Mapping infrastructure (Phase 1 reuses, doesn't build)

- `schema/device_mapping_rules.sql`: pattern (substring/regex), scope
  (all/baselines/sc/irradiation/avalanche), priority DESC then
  LENGTH(pattern) DESC, part_number FK → `device_library`,
  UNIQUE(pattern, pattern_type, scope).
- `data_processing_scripts/seed_device_mapping_rules.py`: idempotent
  ON CONFLICT seeding; docstring documents the per-scope matcher-ordering
  trap (library-substring vs rules precedence differs by scope).
- `data_processing_scripts/ingestion_avalanche.py` L1041–1089: calls
  `common.match_device()` and **UPDATEs device_type/manufacturer on existing
  rows when the mapping changes** — re-running ingestion is the backfill
  path (plan allows adding `--remap-only` if full re-ingest is slow).
- `device_library`: C2M0080120D, SCT2080KE, SCT3030KL exist; SCT3080KL and
  C3M0075120K likely missing → extend `seed_device_library.py`.

## 5. Live DB state (queried 2026-06-10 — re-verify before relying on)

### stress_proxy_candidate_view (390 pairs)

| candidate_status | replacement_confidence | n |
| --- | --- | ---: |
| weak_measured_candidate | low_measured_damage_screening_confidence | 180 |
| missing_damage_context | blocked_or_manual_review | 178 |
| inspect_manually | blocked_or_manual_review | 20 |
| phenotype_mismatch | blocked_or_manual_review | 12 |

Zero measured/predicted damage candidates. Rank-1 exists only for
C2M0080120D SELCII (38) + 1 MIXED, all SC candidates; top conditions:
600V_8us (12 targets), 600V_2us (6), avg waveform distance ~2.0, measured
damage distance 0.42 (weak, device-run scope).

Blockers: missing_gate_overlap 390/390, candidate_missing_condition_post_iv
390/390, damage_context_device_run_not_exact_candidate 200,
target_missing_condition_post_iv 190, missing_damage_context 190,
phenotype_distance_high 12.

### Why no SEB targets (event energy breakdown, irradiation events)

Comparable (enter targets): SELCII 983 + SELCI 18 + MIXED 9 with
`integrated_event_vds_id`. All 76 SEB rows non-comparable:
57 `active_window_unknown` (missing), 11 `non_comparable_integrated_event`
(failure_cutoff), 5 `proxy_event_rectangular_excluded` (failure_cutoff),
2 heuristic_current_plateau, 1 event_outside_active_window. The 11+5
failure_cutoff rows have a usable **right-censored energy floor**
(`event_energy_vds_id_j`) for Phase 3.

### Candidate pools

- avalanche: 1258 records, **only 176 with device_type**
  (C2M0080120D 142, SCT2080KE 34).
- sc: 26 records (C2M0080120D 15, SCT2080KE 7 with device_type).
- Device overlap with irradiation: **C2M0080120D only**. SCT2080KE has
  SC+avalanche but no irradiation data.

### Unmapped avalanche files (Phase 1 payload, 1082 rows)

By experiment/family/mode: AVL_Selam/Selam/UIS 504, AVL_Selam/Selam/RT 212,
AVL_UIS_selam_4pin/UIS 114, AVL_RT/RT 109, AVL_UID_m10V/UID 98,
AVL_UIDSelam/UID 43, AVL_Selam/Avalanche 2. `device_id` == `sample_group`
== sample code. Path examples (under `.../Measurements/Selam/`):
`C7_1.14J_Vg-1000000.h5`, `RP10_1.4J_Vg-1000001.h5`, `RT35_0.1JG00017.h5`,
`UID_m10V/RT62_0.2J_Vg-1000010.h5`, `UIDSelam/RT20_0.4500035_ch3.h5`,
`UIS_selam_4pin/C4P3_0JG00005.h5`,
`UIS 24.07.2019/Series2_1-47mH/series2_00026.mat`. Note trailing counters in
filenames = pulse-sequence indices (Phase 7 exploits this).

### SEB/MIXED target profile (collapse fraction ≈ phenotype)

| device | ion | type | n | post-IV | avg collapse |
| --- | --- | --- | ---: | ---: | ---: |
| IFX-Trench | Ni | MIXED | 25 | 25 | 0.00 |
| C2M0080120D | proton | SEB | 18 | 0 | 0.00 |
| SCT3030KL | proton | SEB | 14 | 0 | 0.00 |
| C2M0025120D | proton | SEB | 9 | 0 | 0.00 |
| IFX-Trench | Kr | SEB | 6 | 6 | 0.32 |
| IFX-Trench | Au | SEB/MIXED | 4/6 | 4/6 | 0.99/0.00 |
| CPM3-1200-0075A | Au | SEB | 4 | 4 | 0.98 |
| IFX-Diode-3x3 | Au | SEB | 3 | 0 | 0.98 |
| C2M0080120D | Ni | SEB | 2 | 2 | 1.00 |

Phase 3 acceptance case: the 2 C2M0080120D Ni SEB events (hard collapse,
post-IV) should receive avalanche candidates labeled `thermal_runaway_pair`.
IFX-Trench/CPM3 SEB targets stay candidate-less until Phase 6 (cross-device).

### Other state

- Gate-zero: **fail**, 0 candidate families. Families: sc waveforms 2,
  UID/UIS 2, irradiation 17, post-IV 13, electrical-proxy post-IV overlap 1
  (SCT2080KE), irrad post-IV overlap 7. Files: SC 22, UIS 167, irradiation
  516; events 1811; fingerprints 101; proxy waveform+post-IV files 2;
  irrad waveform+post-IV 333; events with both 262.
- Dose components (`radiation_stress_dose_components`): single_particle
  1795 calculated; event_window 1441 calculated + 354 per_particle_only;
  file 250+250; campaign 60+45; all basis
  `layer_residual_energy_integrated_stopping_power`.
- Measured damage rank-1: sc_vs_irradiation 6 usable + 4 weak;
  avalanche_vs_irradiation 2 usable + 2 weak + 2 inspect; sc_vs_avalanche
  1 usable.
- Event detection totals: 516 files, 1811 events (76 SEB, 177 SELCI,
  1520 SELCII, 38 MIXED); 200 MeV proton subset = 44 SEB across 13 files.

## 6. Phase-1 ground truth status (SELAM thesis)

Extract with: `pdftotext "docs/relevant_papers/SELAM_THESIS_Excess Carrier
Generation_SiC.pdf" /tmp/selam_thesis.txt` (7787 lines).

Confirmed in text: devices used are commercial 1.2 kV 80 mΩ planar
C2M0080120D (Wolfspeed) and SCT2080KE (Rohm) plus trench SCT3080KL (Rohm)
(~L3062–3063 of the text dump); per-device avalanche energies/currents
~L3070–3142 (C2M 1.04 J / 20.9 A; SCT2080KE 1.31 J / 24.8 A; SCT3080KL 11 A);
UIS specifically on C2M0080120D ~L4016–4036 (energy capability drop above
~52 A, max ~104.9 A); 3rd-gen Wolfspeed C3M0075120K mentioned ~L5013.

**Unverified**: the sample-code prefix semantics (C*/RP*/RT*/C4P*). The
plausible mapping — C→C2M0080120D (Cree), RP→SCT2080KE (Rohm planar),
RT→SCT3080KL (Rohm trench), C4P→C3M0075120K (4-pin Kelvin package) — was
NOT found stated in extractable text; it is likely in figure captions,
tables, or lab notes. Check those (or ask the user) before seeding rules.
Watch the conflict: `AVL_RT` experiment + `RT*` codes appear under both
"room temperature" and "Rohm trench" readings — resolve explicitly.
`series2` (.mat, UIS 24.07.2019) fits none of the prefixes; leave unmapped
unless evidence found.

## 7. Verification queries (used this session, reuse per phase)

```sql
-- status distribution (record before/after in each phase commit message)
SELECT candidate_status, replacement_confidence, COUNT(*)
FROM stress_proxy_candidate_view GROUP BY 1,2 ORDER BY 3 DESC;

-- blocker tallies
SELECT unnest(candidate_blockers) AS blocker, COUNT(*)
FROM stress_proxy_candidate_view GROUP BY 1 ORDER BY 2 DESC;

-- target energy-comparability breakdown
SELECT event_type, energy_is_comparable, energy_level,
       electrical_terminal_energy_basis, energy_censored_reason, COUNT(*)
FROM stress_test_context_view
WHERE source='irradiation' AND event_record_type='detected_single_event'
GROUP BY 1,2,3,4,5 ORDER BY 6 DESC;

-- candidate pool / device overlap
WITH t AS (SELECT DISTINCT device_type FROM stress_test_context_view
           WHERE source='irradiation' AND device_type IS NOT NULL),
c AS (SELECT device_type, source, COUNT(*) n FROM stress_test_context_view
      WHERE source IN ('sc','avalanche') AND device_type IS NOT NULL
      GROUP BY 1,2)
SELECT c.*, (t.device_type IS NOT NULL) AS has_irrad
FROM c LEFT JOIN t USING (device_type) ORDER BY has_irrad DESC, n DESC;

-- end-to-end after Phase 3 (and at completion)
SELECT target_match_tier, target_event_type, candidate_source,
       mechanism_match_class, candidate_status, COUNT(*)
FROM stress_proxy_candidate_view
WHERE candidate_rank = 1
GROUP BY 1,2,3,4,5 ORDER BY 1,2,6 DESC;
```

## 8. Review→plan crosswalk and gotchas

- F1 (SEB exclusion + mechanism classes) → Phase 3. F2a (Selam mapping) →
  Phase 1; F2b (cross-device) → Phase 6. F3 (normalized axes, energy
  density, dose wiring) → Phase 4. F4 (sign agreement) + F8 (gate axis) →
  Phase 2. F5 (calibration + settings table) → Phase 5. F6 (repetition
  descriptors) → Phase 7. F7 (lab work) → Phase 8 planning view only.
- 025 is wholesale DROP/CREATE — any phase touching it rebuilds all
  matviews; expect minutes-scale rebuild (1800 file features, 14752 basis
  rows). Refresh Superset dataset columns afterwards
  (`refresh_dataset_columns` in superset_api.py).
- Adding statuses (`analog_questionable`, `cross_device_screening_only`,
  `missing_phenotype_overlap`) requires touching: status CASE, priority
  CASE, `replacement_confidence` CASE, ranking ORDER BY, summary view, and
  `CANDIDATE_COLORS` + filters in `create_proxy_readiness_dashboard.py`.
- The proton SEB cohort (44 events, collapse 0) will NOT match avalanche
  even in the censored tier — phenotype distance correctly pushes them to
  SC (`thermal_runaway_pair_secondary`); that is expected behavior, per the
  energy+collapse diagnostic (43/44 flip,
  `out/proxy_matching_shift/proton_proxy_match_shift_summary.md`).
- Energy-window logic lives in `irradiation_energy_windows.py` +
  schema 026 — Phase 3 must NOT touch it (tag, don't widen); the censored
  tier consumes its existing outputs.
- The candidate view's LATERAL damage joins key on
  `right_irrad_run_id` = target's run — when adding tiers keep that join
  intact or measured evidence silently disappears.
- `stress_record_key` format: `source:metadata_id:event_id|file`.
- Literature anchors with one-line takeaways are in review §6; the
  mechanism-table seeds in Phase 3 encode exactly those (Wu 2024 SC/UIS
  thermal-runaway bridge; Nida 2021 UIS latch limit ~52 A; Ball 2021 epi
  design vs SEB; Pocaterra & Ciappa alpha SEB at 80–93% V_BD; group TNS
  SELC-II cumulative stacking faults; NSREC2025 proton TID/DD).
