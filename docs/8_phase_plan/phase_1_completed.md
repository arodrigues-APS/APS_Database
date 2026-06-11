# Phase 1 Completed - Selam Avalanche Device Mapping

Date completed: 2026-06-10

Commit: `50a910d Map Selam avalanche sample codes`

## Scope

Phase 1 implemented the Selam avalanche device mapping unlock from the
proxy-method plan. The work stayed additive: it added verified avalanche-scope
mapping rules, seeded missing device-library rows, added a lightweight
remap-only path for existing avalanche metadata, rebuilt the proxy-readiness
views, and recorded the evidence used to pass the mapping hard gate.

Phase 0 was already landed before this work started:
`509a56d Add stress-landscape proxy readiness foundation and review docs`.
The working tree was clean, so no empty Phase 0 commit was created.

## Evidence Gate

The mapping hypothesis was checked before any backfill/remap. The thesis text
confirmed the devices used in the relevant Selam chapters:

- Chapter 5 / Figure 5.4: `C2M0080120D`, `SCT2080KE`, and `SCT3080KL`.
- Chapter 6 / Table 6.1: `C2M0080120D`, `C3M0075120K`, and `SCT3080KL`.
- Nida 2021 confirmed the C2M0080120D destructive UIS/current-capability
  study context.

The missing bridge was the lab sample-code prefix vocabulary. That was found
in the existing local datasheet seeds:

- `data_processing_scripts/datasheetrdson.py`
- `data_processing_scripts/datasheetgm.py`

Those seeds encode:

| Selam code | Device |
| --- | --- |
| `C*` excluding `C4P*` | `C2M0080120D` |
| `RP*` | `SCT2080KE` |
| `RT*` | `SCT3080KL` |
| `C4P*` | `C3M0075120K` |

The detailed evidence table was added in:
`docs/selam_avalanche_device_mapping_2026-06-10.md`.

## Code Changes

- Added `SCT3080KL` and `C3M0075120K` to `seed_device_library.py`.
- Corrected existing `SCT2080KE` metadata from trench to planar when it still
  has the old incorrect seed note.
- Added four `scope='avalanche'` regex rules to `seed_device_mapping_rules.py`.
- Added `ingestion_avalanche.py --remap-only` so existing avalanche rows can
  be remapped from stored `csv_path` values without reopening HDF5/MAT files.
- Made `h5py` and `scipy.io.loadmat` lazy for full ingestion, so `--remap-only`
  can run in the DB environment without waveform-parser packages installed.

## Rules Added

| Regex | Part | Priority |
| --- | --- | ---: |
| `SELAM.*[/\\]C4P\d` | `C3M0075120K` | 250 |
| `SELAM.*[/\\]C(?!4P)\d` | `C2M0080120D` | 200 |
| `SELAM.*[/\\]RP\d` | `SCT2080KE` | 200 |
| `SELAM.*[/\\]RT\d` | `SCT3080KL` | 200 |

`C4P` intentionally outranks the generic `C` rule. The `C` rule also uses a
negative lookahead so future priority changes do not accidentally map 4-pin
samples to the 3-pin C2M part.

## Backfill / Remap Result

Commands run:

```bash
python3 data_processing_scripts/seed_device_library.py
python3 data_processing_scripts/seed_device_mapping_rules.py
python3 data_processing_scripts/ingestion_avalanche.py --remap-only
python3 data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
```

Device library seed result:

- Inserted: 2
- Skipped existing: 31
- Corrected metadata: 1
- Total devices: 43

Mapping-rule seed result:

- `avalanche`: 4
- `baselines`: 7
- `irradiation`: 31
- `sc`: 19

Remap result:

- Rows scanned: 1258
- Matched: 1087
- Updated: 911
- Unchanged: 176
- Still unmapped: 171

## Verification SQL Results

Avalanche context mapping:

| Total | Mapped | Unmapped |
| ---: | ---: | ---: |
| 1258 | 1087 | 171 |

Mapped avalanche devices:

| Device | Rows |
| --- | ---: |
| `SCT3080KL` | 462 |
| `C2M0080120D` | 363 |
| `SCT2080KE` | 236 |
| `C3M0075120K` | 26 |

Device overlap after remap:

| Device | Source | Rows | Has irradiation |
| --- | --- | ---: | --- |
| `C2M0080120D` | avalanche | 363 | yes |
| `C2M0080120D` | sc | 15 | yes |
| `SCT3080KL` | avalanche | 462 | no |
| `SCT2080KE` | avalanche | 236 | no |
| `C3M0075120K` | avalanche | 26 | no |
| `SCT2080KE` | sc | 7 | no |

Candidate-view status distribution stayed unchanged:

| Status | Count |
| --- | ---: |
| `weak_measured_candidate` | 180 |
| `missing_damage_context` | 178 |
| `inspect_manually` | 20 |
| `phenotype_mismatch` | 12 |

The candidate view still has 390 rows because Phase 1 only maps source data.
Phase 3 is the phase that admits censored SEB targets, which is where the
newly mapped avalanche pool becomes more important. After Phase 1, the current
exact-device candidate view includes 12 `C2M0080120D` avalanche candidate rows.

## Intentionally Left Unmapped

Two residual buckets were left unmapped because the hard gate was not satisfied
for them:

| Residual bucket | Rows | Reason |
| --- | ---: | --- |
| `series1_*`, `series2_*`, `series3_*` under `UIS 24.07.2019` | 83 | The files fit the Selam avalanche campaign but do not carry a sample prefix tying each series to a device. |
| `I*` under `UIS_selam_4pin/IFX_IMZ120R045M1/` | 88 | The folder names an Infineon part outside the Phase 1 thesis mapping hypothesis and that part is not currently in `device_library`. |

These should only be mapped in a later additive change if lab notes or another
source tie the residual rows to concrete part numbers.

## Checks

Passed:

```bash
python3 -m py_compile \
  data_processing_scripts/seed_device_library.py \
  data_processing_scripts/seed_device_mapping_rules.py \
  data_processing_scripts/ingestion_avalanche.py

python3 data_processing_scripts/create_proxy_readiness_dashboard.py --schema-only
git diff --cached --check
```

Not run:

```bash
pytest tests/ -q
```

`pytest` was not installed in the available Python environment:
`pytest: command not found` and `python3: No module named pytest`.

## Final State

- Branch: `master`
- Latest commit: `50a910d Map Selam avalanche sample codes`
- Working tree after commit: clean
- Branch status after commit: ahead of `origin/master` by 1 commit
