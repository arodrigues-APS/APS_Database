# Phase 6 Completed - Cross-Device Retrieval Tier

Date completed: 2026-06-11

Commit: `Add cross-device proxy retrieval tier` (this commit)

## Scope

Phase 6 implemented the cross-device retrieval tier from the proxy-method
plan. Targets on device families with no same-device SC/avalanche data (most
importantly the heavy-ion-SEB-rich IFX-Trench family) can now receive
screening-level candidates from other families in the same voltage class,
without contaminating same-device evidence tiers.

The work stayed additive at the schema and dashboard boundaries:

1. Added `voltage_class` and `technology_class` to
   `stress_test_context_view`.
2. Added a `candidate_links` CTE to `stress_proxy_candidate_view` with
   explicit `same_device` and `cross_device` link arms.
3. Cross-device rows always classify as the new hard status
   `cross_device_screening_only` and always rank after every same-device
   candidate for the same target.
4. Added cross-device blockers, summary-view grouping, dashboard colors,
   filters, and table columns (plus an LET label/filter convenience for the
   context dataset).

Phase 5 had already landed immediately before this work:
`44a7a85 Add proxy distance calibration harness`.

## Implementation Notes

Cross-device links are only created when all of these hold:

- candidate `device_type` differs from the target's;
- both sides have a non-null, equal `voltage_class` (650/900/1200/1700
  buckets derived from `device_library.voltage_rating` with part-number
  fallbacks);
- both sides carry `normalized_vds` and `vds_collapse_fraction`;
- the target has **no** same-device candidate at all (`NOT EXISTS` over the
  same-device link condition), so cross-device never competes with
  same-device evidence;
- the tier-aware energy prefilter holds for `energy_comparable` targets.

During implementation the link condition initially also required
`gate_delta_fraction` on both sides. That over-constrained the tier to zero
rows: no SC/avalanche candidate currently has a gate axis (the same fact
Phase 2 surfaced as `missing_gate_overlap` on 100 % of pairs). The two gate
requirements were removed; gate overlap remains visible per-pair through the
existing `missing_gate_overlap` blocker.

`technology_class` (planar/trench) is deliberately a blocker tag, not a link
gate: `cross_device_technology_class_not_matched` marks pairs that cross the
planar/trench boundary.

Status semantics: `cross_device_screening_only` is assigned before any
damage-evidence status, so a cross-device row can never present itself as a
measured/predicted damage candidate. Energy/phenotype hard gates still apply
first (cross-device rows can still be `phenotype_mismatch` or
`energy_out_of_range`). Ranking adds a match-scope prefix so same-device rows
always outrank cross-device rows within a target.

## Columns Added

Context view:

| Column | Meaning |
| --- | --- |
| `voltage_class` | 650/900/1200/1700 bucket from rated voltage with part-number fallbacks. |
| `technology_class` | planar/trench from device-library category/notes/part naming, where known. |
| `let_label` | Zero-padded irradiation LET legend/filter label. |

Candidate view:

| Column | Meaning |
| --- | --- |
| `match_scope` | `same_device` or `cross_device` link arm. |
| `candidate_device_type` / `candidate_device_label` / `candidate_manufacturer` | Candidate-side identity (now distinct from the target family). |
| `target_voltage_class` / `candidate_voltage_class` | Voltage-class evidence for the link. |
| `target_technology_class` / `candidate_technology_class` | Technology-class context. |

New blockers: `cross_device_voltage_class_screening_only`,
`cross_device_technology_class_not_matched`.

New status/confidence: `cross_device_screening_only` /
`cross_device_screening_confidence`.

Summary view: grouped by `match_scope`, plus `candidate_device_type_count`
and `candidate_device_types`.

## Rebuild Command

```bash
python3 data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
```

Rebuild completed successfully (exit 0).

## Verification SQL Results

Candidate distribution by match scope after Phase 6:

| Match scope | Status | Rows | Targets |
| --- | --- | ---: | ---: |
| `cross_device` | `cross_device_screening_only` | 10684 | 1115 |
| `cross_device` | `phenotype_mismatch` | 486 | 297 |
| `same_device` | `analog_questionable` | 1182 | 119 |
| `same_device` | `inspect_manually` | 238 | 25 |
| `same_device` | `waveform_only_candidate` | 191 | 20 |
| `same_device` | `missing_damage_context` | 178 | 19 |
| `same_device` | `weak_measured_candidate` | 19 | 3 |
| `same_device` | `phenotype_mismatch` | 12 | 2 |
| `same_device` | `device_run_measured_candidate` | 8 | 1 |
| `same_device` | `measured_damage_candidate` | 2 | 1 |

The same-device distribution is identical to the Phase 5 baseline row for
row: cross-device admission changed nothing in the same-device evidence
tiers.

Plan criterion - IFX-Trench heavy-ion SEB targets now receive cross-device
avalanche candidates:

| Ion | Candidate source | Candidate device | Status | Rows | Targets |
| --- | --- | --- | --- | ---: | ---: |
| Kr | avalanche | SCT3080KL | `cross_device_screening_only` | 46 | 6 |
| Kr | avalanche | C2M0080120D | `cross_device_screening_only` | 5 | 5 |
| Kr | sc | C2M0080120D | `cross_device_screening_only` | 7 | 5 |
| Au | avalanche | SCT3080KL | `cross_device_screening_only` | 20 | 4 |
| Au | avalanche | C2M0080120D | `cross_device_screening_only` | 14 | 4 |
| Au | avalanche | C3M0075120K | `cross_device_screening_only` | 4 | 2 |
| Au | avalanche | SCT2080KE | `cross_device_screening_only` | 2 | 2 |

The Kr/Au trench targets matching the SCT3080KL trench avalanche pool is the
intended headline: trench-to-trench, same voltage class, hard-collapse SEB
phenotype - the best currently possible screening lead for those events.

Phase 5 named-regression invariants after Phase 6:

| Invariant | Result |
| --- | ---: |
| SEB avalanche `measured_damage_candidate` rows | 2 |
| Rank-1 SEB avalanche measured rows | 1 |
| Avalanche candidate rows with non-null `normalized_vds_delta` | 0 |

Classification sanity:

| Device | Voltage class | Technology class |
| --- | ---: | --- |
| `IFX-Trench` | 1200 | trench |
| `SCT3080KL` | 1200 | trench |
| `SCT2080KE` | 1200 | planar |
| `C2M0080120D` | 1200 | (null - library notes carry no planar/trench text) |
| `CPW5-1700-Z050A/B` | 1700 | (null) |
| `Cree-Diode`, `IFX-Diode-3x3` | (null - excluded from cross-device) | (null) |

Device mapping rules remain intact: avalanche 4, baselines 7, irradiation 31,
sc 19. (The avalanche rules were re-seeded on 2026-06-11 after the Phase 5
review found them deleted by an older-checkout seed run; see the Phase 5
review notes.)

## Checks

Passed:

```bash
/home/arodrigues/aps_venv/bin/python -m py_compile \
  data_processing_scripts/create_proxy_readiness_dashboard.py
/home/arodrigues/aps_venv/bin/python data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
/home/arodrigues/aps_venv/bin/python -m pytest tests/ -q
```

Full pytest result: `15 passed`.

## Known Follow-Ups

- `technology_class` is null for several families whose library notes lack
  planar/trench wording (e.g. C2M0080120D is planar per the SELAM thesis);
  seeding those notes would tighten the technology-mismatch blocker.
- Diode families have no `voltage_class` from part naming and are excluded
  from cross-device retrieval until their library voltage ratings are filled.
- The remap-on-zero-rules hardening and the score_row/SQL parity check from
  the Phase 5 review remain open.

## Final State

- Branch: `master`
- Code commit: `Add cross-device proxy retrieval tier` (this commit)
- Working tree after commit: clean
