# Phase 4 Completed - Normalized Bias and Energy-Density Context

Date completed: 2026-06-11

Commit: `b7d9dc4 Add normalized bias and energy-density proxy context`

## Scope

Phase 4 implemented the normalized-axis and dose-context wiring from the
proxy-method plan. The goal was to add one more physically meaningful
phenotype-matching axis while keeping energy-density and dose information
honest as reporting context rather than silently folding locality-mismatched
quantities into the distance score.

The work stayed additive at the schema and dashboard boundaries:

1. Added `normalized_vds_delta` as a pairwise phenotype axis.
2. Added layer-0 geometry-derived stress energy density to
   `stress_test_context_view`.
3. Added per-pair `energy_density_ratio` as a reporting-only field.
4. Added `dose_context_available` for SELC/cumulative target reporting.
5. Surfaced the new fields and an energy-density scatter chart in the Proxy
   Readiness dashboard.

Phase 3 had already landed immediately before this work:
`35165dc Add censored SEB proxy tier and mechanism gates`.

## Normalized Bias Axis

Before Phase 4, proxy phenotype distance used overlapping collapse and gate
coupling axes. That left censored SEB rows especially dependent on collapse
and mechanism penalties because Phase 3 intentionally removed log-energy
distance for censored targets.

Phase 4 adds:

```text
normalized_vds_delta = abs(candidate_normalized_vds - target_normalized_vds)
```

The axis participates in the same pairwise phenotype-distance framework as
collapse and gate deltas:

| Axis | Scale used in distance |
| --- | ---: |
| `collapse_delta` | 0.25 |
| `gate_delta` | 0.20 |
| `normalized_vds_delta` | 0.15 |

The distance still uses only overlapping axes. If normalized blocking-bias
evidence is missing for either side, that axis is omitted and the row receives
the `missing_normalized_vds_overlap` blocker. In the rebuilt current dataset,
all displayed candidate rows had normalized-bias overlap.

## Energy-Density Context

Phase 4 adds a left join from `stress_test_context_view` to
`device_material_layers` for `layer_order = 0`. The join is intentionally
optional so missing geometry cannot drop waveform/event rows.

For SC and avalanche rows, the context view computes:

```text
stress_energy_density_j_cm3 =
  stress_energy_j / (exposed_area_cm2 * thickness_um * 1e-4)
```

For irradiation rows, it computes a comparable reporting field from deposited
radiation energy when available:

```text
stress_energy_density_j_cm3 =
  COALESCE(radiation_deposited_energy_total_j,
           radiation_deposited_energy_j)
  / active_volume_cm3
```

The field is deliberately contextual. It is not added to waveform distance.
Electrical stress energy is spread through a bulk active region, while
single-ion deposited energy is highly localized. Treating those as a direct
distance axis would overstate comparability.

## Energy Localization

The context view now labels energy locality:

| Source | `energy_localization_class` |
| --- | --- |
| `irradiation` | `ion_track_localized` |
| `sc` | `bulk_active_region` |
| `avalanche` | `bulk_active_region` |

Candidate pairs also report:

```text
energy_density_ratio =
  candidate_stress_energy_density_j_cm3
  / target_stress_energy_density_j_cm3
```

That ratio is surfaced for inspection and dashboard plotting, not for scoring.

## Dose Context Flag

Phase 4 adds:

```text
dose_context_available
```

The flag is true when the target carries any of:

- `target_radiation_dose_total_gy`
- `target_radiation_deposited_energy_j`
- `target_radiation_deposited_energy_total_j`

The dose/deposited-energy columns were already selected into the candidate
view. This phase adds the per-pair boolean so SELC and cumulative targets can
be filtered or audited quickly without adding a dose distance term yet.

## Code Changes

- Added `material_geometry` CTE in `schema/025_proxy_readiness_waveforms.sql`.
- Added context-view columns:
  - `stress_energy_density_j_cm3`
  - `energy_density_basis`
  - `energy_density_active_volume_cm3`
  - `energy_density_geometry_confidence`
  - `energy_density_geometry_provenance`
  - `energy_localization_class`
- Added candidate-pair columns:
  - `target_stress_energy_density_j_cm3`
  - `target_energy_density_basis`
  - `target_energy_density_active_volume_cm3`
  - `target_energy_density_geometry_confidence`
  - `target_energy_localization_class`
  - `candidate_stress_energy_density_j_cm3`
  - `candidate_energy_density_basis`
  - `candidate_energy_density_active_volume_cm3`
  - `candidate_energy_density_geometry_confidence`
  - `candidate_energy_localization_class`
  - `dose_context_available`
  - `energy_density_ratio`
  - `normalized_vds_delta`
- Added `normalized_vds_delta` to pairwise phenotype-distance scoring.
- Added `missing_normalized_vds_overlap` blocker.
- Updated `create_proxy_readiness_dashboard.py` with:
  - new context/candidate/evidence table columns;
  - a `Proxy Readiness - Candidate Pairs: Energy Density Ratio vs Phenotype
    Mismatch` scatter chart;
  - candidate-filter scope for the new chart.

## Rebuild Command

Command run:

```bash
python3 data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
```

Proxy-readiness rebuild result:

- Proxy-readiness SQL views rebuilt successfully.
- `stress_test_context_view` was recreated with geometry-derived energy
  density and localization columns.
- `stress_proxy_candidate_view` was recreated with normalized-bias phenotype
  scoring, `energy_density_ratio`, and `dose_context_available`.

## Verification SQL Results

Left-join preservation check:

| Source | Rows before | Rows after | Rows with density | Rows with geometry |
| --- | ---: | ---: | ---: | ---: |
| `avalanche` | 1258 | 1258 | 598 | 599 |
| `irradiation` | 1811 | 1811 | 1433 | 1811 |
| `sc` | 26 | 26 | 22 | 22 |

No context rows were lost from the new geometry join.

Candidate row preservation check:

| Target tier | Rows before | Rows after | Targets after |
| --- | ---: | ---: | ---: |
| `energy_censored_phenotype_only` | 1440 | 1440 | 144 |
| `energy_comparable` | 390 | 390 | 39 |

Phenotype-axis usage after adding normalized bias:

| `phenotype_axes_used` | Rows | Rows with `normalized_vds_delta` |
| ---: | ---: | ---: |
| 2 | 1830 | 1830 |

Energy-density-ratio coverage:

| Target tier | Event | Rows | Rows with ratio | Min ratio | Max ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| `energy_censored_phenotype_only` | `SEB` | 220 | 220 | 30033695095.240200 | 358118557299319.000000 |
| `energy_censored_phenotype_only` | `SELCI` | 1190 | 1040 | 51724697108.469200 | 358635312054.044000 |
| `energy_censored_phenotype_only` | `SELCII` | 30 | 20 | 155174091325.408000 | 358635312054.044000 |
| `energy_comparable` | `MIXED` | 10 | 10 | 352248533.049309 | 1181708221.800300 |
| `energy_comparable` | `SELCII` | 380 | 380 | 2745513.240240 | 1083232536650.280000 |

Dose-context availability:

| Event | Dose context available | Rows |
| --- | --- | ---: |
| `MIXED` | true | 10 |
| `SEB` | true | 220 |
| `SELCI` | false | 150 |
| `SELCI` | true | 1040 |
| `SELCII` | true | 410 |

## SEB Ranking Spot Check

For the C2M0080120D Ni SEB targets, the new normalized-bias axis is active in
the top-ranked candidates:

| Target | Ion | Source | Rank | Target norm VDS | Candidate norm VDS | Delta | Axes | Phenotype distance | Status |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `irradiation:12974:9890` | `Ni` | `sc` | 1 | 0.842 | 0.590 | 0.251 | 2 | 1.210 | `weak_measured_candidate` |
| `irradiation:12974:9890` | `Ni` | `sc` | 2 | 0.842 | 0.613 | 0.229 | 2 | 1.906 | `weak_measured_candidate` |
| `irradiation:13224:9900` | `Ni` | `sc` | 1 | 0.917 | 0.590 | 0.326 | 2 | 1.558 | `weak_measured_candidate` |
| `irradiation:13224:9900` | `Ni` | `sc` | 2 | 0.917 | 0.613 | 0.304 | 2 | 2.129 | `weak_measured_candidate` |

After Phase 4, the materialized top-10 candidates for these two Ni SEB targets
are SC rows rather than avalanche rows. This is a real ranking response to the
new normalized-bias axis and existing damage evidence, not a dropped-row
problem: both context and candidate row counts stayed exactly at baseline.

Top SEB candidates after Phase 4:

| Candidate source | Mechanism class | Status | Rows | Targets |
| --- | --- | --- | ---: | ---: |
| `sc` | `thermal_runaway_pair_secondary` | `waveform_only_candidate` | 18 | 18 |
| `sc` | `thermal_runaway_pair_secondary` | `weak_measured_candidate` | 4 | 4 |

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
- Latest code commit: `b7d9dc4 Add normalized bias and energy-density proxy context`
- Branch status after Phase 4 commit: ahead of `origin/master` by 1 commit.
- `docs/8_phase_plan/` was already untracked before this documentation note
  was added.
- This completion note documents the Phase 4 code commit; it can be committed
  together with the existing Phase 1, Phase 2, and Phase 3 completion notes if
  the phase-plan docs are meant to become tracked project artifacts.
