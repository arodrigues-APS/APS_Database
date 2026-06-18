# Figure 1(b) Phase 2 Completed - Effective Time and Destruction Boundary

Date completed: 2026-06-11

## Scope

Phase 2 made additive SQL changes for Figure 1(b). No existing context columns
were renamed or repurposed. The core reason for this phase is that single-shot
plots are useful, but repetitive avalanche sequences also need an honest
effective-time view that exposes the scaling assumption.

Implemented in `schema/025_proxy_readiness_waveforms.sql`:

1. `effective_stress_time_s` in `stress_test_context_view`.
   Repetitive sequences with `pulse_count_in_sequence > 1` are scaled as
   `pulse_count_in_sequence * stress_duration_s`; all other rows keep
   `stress_duration_s`.
2. `figure1b_time_basis` in `stress_test_context_view`, with explicit values
   for `single_pulse_or_event`, `repetitive_sequence_scaled`, `file_window`,
   and `unknown_no_duration`.
3. Materialized view `stress_destruction_boundary_view`, with per-device
   lower-bound destruction information: `destructive_count`,
   `min_destructive_normalized_vds`, `max_survived_normalized_vds`, and
   `record_count`.
4. Indexes on the new boundary view by `device_type` and `voltage_class`.

Implemented in `data_processing_scripts/create_proxy_readiness_dashboard.py`:

1. Registered dataset key `destruction_boundary` for
   `stress_destruction_boundary_view`.
2. Added `stress_duration_s`, `effective_stress_time_s`, and
   `figure1b_time_basis` to the Stress Test Context table.
3. Added chart `Proxy Readiness - Figure 1(b): Effective Stress-Time
   Landscape`.
4. Added table chart `Proxy Readiness - Figure 1(b): Destruction Boundary by
   Device`.

Implemented in `tests/test_stress_context_figure1b.py`:

1. Effective time equals `pulse_count_in_sequence * stress_duration_s` for
   repetitive rows.
2. `figure1b_time_basis` is populated whenever effective time is populated.
3. The Figure 1(b) chart filter excludes the known avalanche artifact family.
4. Destructive marker rows remain plottable.
5. The boundary view has at least one destructive lower-bound row.
6. Source-level plottable counts remain in expected lower-bound ranges.

## Deployment Evidence

Schema rebuild completed with:

```bash
/home/arodrigues/aps_venv/bin/python create_proxy_readiness_dashboard.py --schema-only
```

Dashboard metadata update completed with:

```bash
/home/arodrigues/aps_venv/bin/python create_proxy_readiness_dashboard.py --skip-schema
```

Superset results:

| Artifact | Result |
| --- | --- |
| `stress_destruction_boundary_view` dataset | Existing/registered as dataset id `120` |
| Effective stress-time chart | Updated chart id `420` |
| Destruction boundary table | Updated chart id `422` |

## Verification SQL

Source-level plottability after rebuild:

| Source | Total rows | Has duration | Has normalized VDS | Has both | Destructive with both | Sequenced with both | Duration range | Max effective time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| avalanche | 1258 | 1258 | 1087 | 1087 | 0 | 1074 | 2.539e-7 s to 0.0029994 s | 0.0819836 s |
| irradiation | 1811 | 1128 | 1632 | 981 | 11 | 0 | 0.15773 s to 50.748 s | 50.748 s |
| sc | 26 | 23 | 22 | 22 | 0 | 0 | 1.2999e-5 s to 2.5998e-5 s | 2.5998e-5 s |

Time-basis population:

| Basis | Rows |
| --- | ---: |
| `single_pulse_or_event` | 1186 |
| `repetitive_sequence_scaled` | 1085 |
| `unknown_no_duration` | 686 |
| `file_window` | 138 |

Boundary rollup:

| Metric | Value |
| --- | ---: |
| Boundary rows | 16 |
| Rows with non-null destructive boundary | 10 |
| Destructive records contributing to boundary view | 73 |
| Minimum destructive normalized VDS | 0.2916441667 |
| Maximum survived normalized VDS | 1.5969681933 |

## Test Evidence

Final test command:

```bash
/home/arodrigues/aps_venv/bin/python -m pytest tests/ -q
```

Result: `26 passed in 0.19s`.
