# Irradiation Single-Event Detection

This note documents the SEB / SELC-I / SELC-II extraction implemented in
`data_processing_scripts/extract_single_event_effects.py`.

## Output Tables

- `irradiation_single_event_file_summary`
  One row per irradiation monitor file (`baselines_metadata.id`). Stores
  detector settings, noise thresholds, event counts, current extrema, fluence
  span, and per-file rates.

- `irradiation_single_events`
  One row per detected event. Stores event type, confidence, point/time/fluence
  location, before/after Vds, before/after Id/Ig, delta Id/Ig, ratio, slopes,
  thresholds, and JSON evidence flags.

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
9. Classify the event.

## Classification Rules

- `SEB`
  Requires the event itself to have a mA-scale drain jump. It is promoted to
  SEB when the jump is accompanied by a mA-scale gate jump/level, Vds collapse,
  trace abort, or a hard 10 mA-scale drain jump.

- `SELCI`
  Id and Ig rise together. Default ratio band:
  `0.33 <= delta |Id| / delta |Ig| <= 3.0`.

- `SELCII`
  Id rises and Ig is absent/flat, or Id rises far more strongly than Ig.
  Default ratio threshold:
  `delta |Id| / delta |Ig| >= 10.0`.

- `MIXED`
  Both Id and Ig rise, but the ratio falls between the SELC-I and SELC-II
  bands.

- `UNKNOWN`
  Low-confidence diagnostic events. These are logged for audit but should be
  excluded from primary SEB/SELC plots.

## Rate Caveats

`event_rate_per_1e5_fluence` is emitted only when the file has a meaningful
fluence span (`fluence_span >= 1`). Some campaigns have no fluence column or
near-zero placeholder-like fluence values; use `event_rate_per_s` or raw
`n_events` for those until a campaign-specific ion-count normalization is
added.

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
