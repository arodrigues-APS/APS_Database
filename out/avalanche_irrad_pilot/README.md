# Avalanche vs Irradiation Pilot

Scope: C2M0080120D only.

This pilot intentionally does not claim UIS/proton failure equivalence. The
available proton C2M0080120D events are SELCII leakage events, not SEB.

## Cohorts

- Avalanche D3 UIS waveforms: 5 files.
- Proton irradiation SELCII events: 18 events across 6 files.
- Heavy-ion irradiation SEB contrast: 4 events across 4 files.

## Key Descriptors

- Avalanche D3 median pulse/current amplitude: 53.9103 A.
- Avalanche D3 median Vds collapse fraction: 1.01275.
- Proton SELCII median delta |Id|: 0.000366055 A.
- Proton SELCII median Vds collapse fraction: 0.
- Heavy-ion SEB median delta |Id|: 0.0209982 A.
- Heavy-ion SEB median Vds collapse fraction: 0.993892.

## Damage Case Study

The probable C2M0080120D D3/d3 mapping has one pre/post avalanche IV pair:

| device_type | sample_mapping | pre_vth_from_vth | post_vth_from_vth | delta_vth_from_vth | pre_vth_from_idvg | post_vth_from_idvg | delta_vth_from_idvg | pre_rds_mohm | post_rds_mohm | delta_rds_mohm |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C2M0080120D | D3/d3/IV/3 probable sample mapping | 5.41 | 4.91 | -0.5 | 12.59 | 12.28 | -0.31 | 72.3416 | 1111.53 | 1039.18 |

## Interpretation

The proton subset does not show a UIS-burnout analogue in this pilot: it has
small drain-source leakage steps and essentially no Vds collapse. The heavy-ion
SEB contrast is the irradiation cohort that shares the hard-collapse descriptor
with the UIS waveforms, although its current scale is still instrument- and
stress-regime dependent.

Treat this as a capability check and case study, not population-level evidence.
