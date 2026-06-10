# Selam Avalanche Device Mapping Evidence (2026-06-10)

This note records the Phase 1 hard-gate evidence for mapping Selam avalanche sample-code prefixes through `device_mapping_rules` with `scope='avalanche'`.

## Confirmed Mappings

| Path/sample code | Part number | Evidence | Confidence | Rows before remap |
| --- | --- | --- | --- | ---: |
| `C*` excluding `C4P*` under `/Selam/` | `C2M0080120D` | SELAM thesis ch. 5 names C2M0080120D as the Wolfspeed 1.2 kV 80 mOhm planar MOSFET used in the UIS studies; local datasheet seeds use `C2M` for C2M0080120D. | High | 221 |
| `RP*` under `/Selam/` | `SCT2080KE` | SELAM thesis ch. 5 names SCT2080KE as the Rohm 1.2 kV 80 mOhm planar MOSFET; local datasheet seeds use `RP` for SCT2080KE. | High | 202 |
| `RT*` under `/Selam/` | `SCT3080KL` | SELAM thesis ch. 5/6 names SCT3080KL as the Rohm trench MOSFET; thesis figure text shows `RTxx` labels in trench post-analysis figures; local datasheet seeds use `RT` for SCT3080KL. | High | 462 |
| `C4P*` under `/Selam/UIS_selam_4pin/` | `C3M0075120K` | SELAM thesis ch. 6 Table 6.1 names C3M0075120K as the 4-pin Wolfspeed planar device; local datasheet seeds use `C3M` for C3M0075120K. | High | 26 |

## Left Unmapped

| Pattern/folder | Rows | Reason |
| --- | ---: | --- |
| `series1_*`, `series2_*`, `series3_*` under `UIS 24.07.2019` | 83 | The files fit the Selam avalanche campaign but do not carry a device prefix; leave unmapped until lab notes or a table ties each series to a part. |
| `I*` under `UIS_selam_4pin/IFX_IMZ120R045M1/` | 88 | The folder names an Infineon part outside the thesis mapping hypothesis and not currently in `device_library`; leave for a separate evidence-backed rule. |

## Source Anchors

- `docs/relevant_papers/SELAM_THESIS_Excess Carrier Generation_SiC.pdf`, ch. 5: C2M0080120D, SCT2080KE, and SCT3080KL are the commercial 1.2 kV 80 mOhm devices used for the avalanche ruggedness comparison.
- Same thesis, ch. 6/Table 6.1: C2M0080120D, C3M0075120K 4-pin planar, and SCT3080KL 3-pin trench are compared.
- `data_processing_scripts/datasheetrdson.py` and `data_processing_scripts/datasheetgm.py` encode the existing local prefix vocabulary: `C2M`, `C3M`, `RP`, and `RT` map to the corresponding commercial part numbers above.
