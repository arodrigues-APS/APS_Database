# Irradiation Single-Event Detection

This note documents the source-aware SEB / SELC-I / SELC-II extraction implemented in
`data_processing_scripts/extract_single_event_effects.py`.

## Output Tables

- `irradiation_single_event_file_summary`
  One row per irradiation monitor file (`baselines_metadata.id`). Stores
  detector settings, noise thresholds, event counts, current extrema, fluence
  span, and per-file rates.

- `irradiation_single_events`
  One row per detected event. Stores the radiation-effect label (`event_type`),
  mechanism-neutral leakage path (`path_type`), severity, catastrophic flag,
  confidence, point/time/fluence location, before/after Vds, before/after Id/Ig,
  delta Id/Ig, slope and delta ratios, gate-current fraction, thresholds, and
  JSON evidence flags.

## Plotting Views

- `irradiation_single_event_view`
  Event-level rows joined to campaign, run, LET, device, and filename metadata.

- `irradiation_single_event_file_frequency_view`
  One row per file and event type. Includes zero-event rows, which is useful
  when plotting rates without dropping no-event files.

- `irradiation_single_event_let_frequency_view`
  Aggregates event counts and rates by campaign, ion, beam energy, LET,
  device type, manufacturer, and event type. This is the main view for plotting
  event frequency against LET.

Example:

```sql
SELECT
    let_mev_cm2_mg,
    event_type,
    device_type,
    n_events,
    event_rate_per_1e5_fluence,
    event_rate_per_s
FROM irradiation_single_event_let_frequency_view
WHERE event_type IN ('SEB', 'SELCI', 'SELCII')
ORDER BY let_mev_cm2_mg, event_type;
```

## Algorithm

For each irradiation waveform file:

1. Sort points by `point_index`.
2. Clean Keithley overflow sentinels (`abs(value) >= 1e30`) to `NULL`.
3. Work on leakage magnitudes `abs(Id)` and `abs(Ig)` while also storing signed
   deltas for audit.
4. Estimate per-file noise using robust MAD on adjacent current differences.
5. Set detection thresholds:
   - `Id threshold = max(1e-6 A, 8 * robust_sigma(delta Id))`
   - `Ig threshold = max(1e-8 A, 8 * robust_sigma(delta Ig))`
6. Detect candidate samples from positive current steps or short local ramps.
   Candidate events are drain-current anchored: gate-only excursions are not
   treated as SEB/SELC unless Id has a weak simultaneous rise.
7. Merge adjacent candidates into one event cluster.
8. Compute event evidence from median before/after windows:
   - `delta_id_abs_a`
   - `delta_ig_abs_a`
   - `id_to_ig_delta_ratio`
   - `vds_before_v`, `vds_after_v`, `vds_delta_v`
   - current slopes in A/s
   - fluence and time location
9. Classify the electrical leakage path first, then map that path to a
   radiation-effect label only when the source supports that mechanism.

## Source-Aware Taxonomy

`path_type` is mechanism-neutral:

- `DRAIN_GATE`: Id and Ig rise together with comparable slope or delta.
- `DRAIN_SOURCE`: Id rises far more strongly than Ig, or gate current is
  absent/flat.
- `MIXED`: both Id and Ig rise, but the ratio is between the drain-gate and
  drain-source bands.
- `UNKNOWN`: diagnostic event with insufficient source/path evidence.

`event_type` is the radiation-effect label used by dashboards:

- Heavy ions: `DRAIN_GATE` maps to `SELCI`, `DRAIN_SOURCE` maps to `SELCII`,
  and `MIXED` maps to `MIXED`. A heavy-ion event is promoted to `SEB` only when
  there is independent hard-failure evidence beyond mA-scale leakage alone,
  such as Vds collapse, trace abort, or 10 mA-scale drain current.
- Low-energy protons: detected current steps are treated as TID/DD diagnostics,
  not SELC or SEB. They become `UNKNOWN` and are dropped by default unless
  `--include-unknown` is used.
- High-energy protons: proton campaigns at or above
  `--proton-seb-energy-min-mev` (default 100 MeV) never emit SELC labels. A
  robust high-field drain-current step at `|Vds| >= --proton-seb-min-vds-v`
  (default 20 V) is labeled `SEB`; otherwise it is `UNKNOWN` and dropped by
  default.

This separation prevents the dashboard from turning proton drain-current steps
into SELC-II merely because Ig is flat.

## Classification Rules

- `SELCI`
  Heavy-ion only. Id and Ig rise together. The default ratio band is
  `0.33 <= delta |Id| / delta |Ig| <= 3.0`. If the gate signal is just below the
  adaptive noise threshold, it can still count as weak gate coupling when it is
  at least half threshold and the gate fraction is SELC-I-like
  (`delta Ig / (delta Id + delta Ig) >= 0.25`).

- `SELCII`
  Heavy-ion only. Id rises and Ig is absent/flat, or Id rises far more strongly
  than Ig. The default ratio threshold is `delta |Id| / delta |Ig| >= 10.0`.

- `MIXED`
  Heavy-ion event where both Id and Ig rise, but the ratio falls between the
  SELC-I and SELC-II bands.

- `SEB`
  High-energy-proton SEB is source-defined: a robust high-field drain-current
  step in a proton campaign at or above the configured high-energy threshold.
  Heavy-ion SEB remains a catastrophic override on top of the path classifier and
  requires a mA-scale drain jump plus hard-failure evidence. Gate-coupled
  heavy-ion leakage remains `SELCI` unless it also has Vds collapse, trace abort,
  and a 10 mA-scale drain signature.

- `UNKNOWN`
  Low-confidence or source-suppressed diagnostic events. These are not stored by
  default; pass `--include-unknown` when auditing detector thresholds.

The event row keeps both `event_type` and `path_type`. For example, high-energy
proton SEB events can have `event_type = 'SEB'` and
`path_type = 'DRAIN_SOURCE'`, while a heavy-ion drain-source leakage event can
have `event_type = 'SELCII'` and `path_type = 'DRAIN_SOURCE'`.

## Literature Basis

- Low-energy proton folders are treated as TID/DD, consistent with the local
  Padova proton papers under `relevant_papers/Padova_papers/`, including
  `Displacement_Damage_and_Total_Ionizing_Dose_Induced_by_3-MeV_Protons_in_SiC_Vertical_Power_MOSFETs.pdf`
  and `Total-Ionizing-Dose_and_Displacement_Damage_Effects_in_Trench_SiC_Power_MOSFETs.pdf`.
- SELC labels are heavy-ion labels, matching the local heavy-ion SELC papers
  `relevant_papers/Heavy-Ion_Microbeam_Studies_of_Single-Event_Leakage_Current_Induced_by_Long-_and_Short-Range_Particles_in_SiC_Power_Devices.pdf`
  and `relevant_papers/SELC Statistics/Analysis_of_Heavy-Ion-Induced_Leakage_Current_in_SiC_Power_Devices.pdf`.
- The PSI 200 MeV proton campaign is represented as high-energy proton SEB,
  matching the campaign audit entry for Martinella et al. 2023,
  "High-Energy Proton and Atmospheric-Neutron Irradiations of SiC Power MOSFETs:
  SEB Study and Impact on Channel and Drift Resistances" (`docs/irradiation_paper_audit.md`).

## Rate Caveats

`event_rate_per_1e5_fluence` is emitted only when the file has a meaningful
fluence span (`fluence_span >= 1`). Some campaigns have no fluence column or
near-zero placeholder-like fluence values; use `event_rate_per_s` or raw
`n_events` for those until a campaign-specific ion-count normalization is added.

## Rebuilding

```bash
python3 data_processing_scripts/extract_single_event_effects.py --rebuild
```

Useful filters:

```bash
python3 data_processing_scripts/extract_single_event_effects.py --campaign GSI_March_2025 --rebuild
python3 data_processing_scripts/extract_single_event_effects.py --device-type IFX-Trench --rebuild
python3 data_processing_scripts/extract_single_event_effects.py --metadata-id 11138 --rebuild
```

Thresholds can be overridden from the CLI. The detector stores the full settings
JSON in `irradiation_single_event_file_summary.settings` for reproducibility.

The current default rebuild analyzed 516 irradiation files and stored 1,811
named events: 76 SEB, 177 SELC-I, 1,520 SELC-II, 38 MIXED, and 0 UNKNOWN.
The 200 MeV proton subset contributes 44 SEB events across 13 files and no
SELC-I/SELC-II events.
