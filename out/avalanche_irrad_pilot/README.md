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

The C2M0080120D D3 curve-tracer IV companions are curated by exact file
list. The pre files are explicit sample-3 APEC_2018 files; the post files
use M3 naming and remain an inferred D3 correspondence until a lab note or
other source confirms the physical sample identity.

| metadata_id | device_type | sample_group | test_condition | curve_tracer_measured_at | filename | measurement_category | measurement_type | identity_status | identity_confidence | vth_v | rdson_mohm | bvdss_v | duplicate_equivalent_files | curation_note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16219 | C2M0080120D | D3 | pre_avalanche | 2018-03-02 | T150_3_IdVd_StepVg.csv | IdVd | T150_3_IdVd_StepVg | explicit_sample_3 | high |  | 133.622 |  | ["T150_3_IdVd_StepVg_R.csv"] | sample-3 IV (XML carries C2M0080120D_3); 29d before D3 UIS waveforms |
| 16218 | C2M0080120D | D3 | pre_avalanche | 2018-03-02 | T150_3_IdVg_StepVd.csv | IdVg | T150_3_IdVg_StepVd | explicit_sample_3 | high | 8.41 |  |  | ["T150_3_IdVg_StepVd_R.csv"] | sample-3 IV (XML carries C2M0080120D_3); 29d before D3 UIS waveforms |
| 16215 | C2M0080120D | D3 | pre_avalanche | 2018-03-02 | T25_3_IdVd_StepVg.csv | IdVd | T25_3_IdVd_StepVg | explicit_sample_3 | high |  | 107.335 |  |  | sample-3 IV (XML carries C2M0080120D_3); 29d before D3 UIS waveforms |
| 16217 | C2M0080120D | D3 | pre_avalanche | 2018-03-02 | T25_3_IdVd_StepVg_R.csv | IdVd | T25_3_IdVd_StepVg_R | explicit_sample_3 | high |  | 77.0081 |  |  | sample-3 IV (XML carries C2M0080120D_3); 29d before D3 UIS waveforms |
| 16214 | C2M0080120D | D3 | pre_avalanche | 2018-03-02 | T25_3_IdVg_StepVd.csv | IdVg | T25_3_IdVg_StepVd | explicit_sample_3 | high | 15 |  |  |  | sample-3 IV (XML carries C2M0080120D_3); 29d before D3 UIS waveforms |
| 16216 | C2M0080120D | D3 | pre_avalanche | 2018-03-02 | T25_3_IdVg_StepVd_R.csv | IdVg | T25_3_IdVg_StepVd_R | explicit_sample_3 | high | 15.01 |  |  |  | sample-3 IV (XML carries C2M0080120D_3); 29d before D3 UIS waveforms |
| 16220 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdV01_Vg.csv | IdVd | M3_IdV01_Vg | inferred_m3_to_d3 | medium |  | 72.8988 |  |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |
| 16228 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdVdss_Vg0.csv | Blocking | M3_IdVdss_Vg0 | inferred_m3_to_d3 | medium |  |  | 1681.96 |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |
| 16229 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdVdss_Vgn5.csv | Blocking | M3_IdVdss_Vgn5 | inferred_m3_to_d3 | medium |  |  |  |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |
| 16224 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdVd_Vg.csv | IdVd | M3_IdVd_Vg | inferred_m3_to_d3 | medium |  | 78.1534 |  |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |
| 16225 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdVdVg.csv | IdVd | M3_IdVdVg | inferred_m3_to_d3 | medium |  |  |  |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |
| 16226 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdVd_Vg_off.csv | IdVd | M3_IdVd_Vg_off | inferred_m3_to_d3 | medium |  |  |  |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |
| 16227 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdVd_Vg_on.csv | IdVd | M3_IdVd_Vg_on | inferred_m3_to_d3 | medium |  |  |  |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |
| 16223 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdVEqVg.csv | IdVd | M3_IdVEqVg | inferred_m3_to_d3 | medium |  |  |  |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |
| 16221 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdVg20_Vd0_05.csv | IdVg | M3_IdVg20_Vd0_05 | inferred_m3_to_d3 | medium | 3.5 |  |  |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |
| 16222 | C2M0080120D | D3 | post_avalanche | 2018-07-31 | M3_IdVg20_Vd20.csv | IdVg | M3_IdVg20_Vd20 | inferred_m3_to_d3 | medium | 19.96 |  |  |  | M3 device naming (no later 2018 ref folder); first post-UIS IV available |

## Interpretation

The proton subset does not show a UIS-burnout analogue in this pilot: it has
small drain-source leakage steps and essentially no Vds collapse. The heavy-ion
SEB contrast is the irradiation cohort that shares the hard-collapse descriptor
with the UIS waveforms, although its current scale is still instrument- and
stress-regime dependent.

Treat this as a capability check and case study, not population-level evidence.
