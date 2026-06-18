# Figure 1(b) Phase 1 Completed - Stress vs Timescale Charts

Date completed: 2026-06-11

## Scope

Phase 1 added the dashboard-only recreation of Kozak et al. Figure 1(b) using
the existing `stress_test_context_view` fields. The intent is not to invent
reliability data, but to place the available SC, avalanche, and irradiation
stress records in the paper's stress-severity vs timescale plane and show that
the current database is concentrated in the robustness/single-event region.

Implemented in `data_processing_scripts/create_proxy_readiness_dashboard.py`:

1. Stable colors for lowercase `robustness`, `reliability`, `radiation`, and
   `unknown` Figure 1 regime families.
2. Chart `Proxy Readiness - Figure 1(b): Stress vs Timescale Landscape` using
   `normalized_vds` vs `stress_duration_s`, grouped by
   `figure1_regime_family`.
3. Chart `Proxy Readiness - Figure 1(b): Destructive Outcomes (Destruction
   Limit Markers)` using the same axes, filtered to
   `response_reversibility = 'destructive_or_catastrophic'`.
4. Native-filter scoping for the new context charts.
5. The avalanche quality guardrail from the plan is enforced in both charts:
   `NOT (source = 'avalanche' AND normalized_vds > 1.60)`.

## Deployment Evidence

The final dashboard metadata update completed with:

```bash
/home/arodrigues/aps_venv/bin/python create_proxy_readiness_dashboard.py --skip-schema
```

Superset results:

| Artifact | Result |
| --- | --- |
| Dashboard | Updated dashboard id `32`, slug `proxy-readiness-waveforms` |
| Landscape chart | Updated chart id `419` |
| Destructive marker chart | Updated chart id `421` |
| Total dashboard charts | `25` |

Exported files were regenerated under
`out/superset_charts/proxy-readiness/`:

| Export | Size / shape check |
| --- | --- |
| `proxy-readiness-figure-1-b-stress-vs-timescale-landscape.png` | 1680 x 980 PNG, non-white pixel fraction 0.0557 |
| `proxy-readiness-figure-1-b-destructive-outcomes-destruction-limit-markers.png` | 1680 x 980 PNG, non-white pixel fraction 0.0243 |

## Verification SQL

Chart-population query after schema rebuild:

| Metric | Rows |
| --- | ---: |
| Raw rows with `normalized_vds` and `stress_duration_s` | 2090 |
| Excluded avalanche artifact rows, `normalized_vds > 1.60` | 676 |
| Filtered landscape rows | 1414 |

Destructive marker query:

| Source | Event type | Rows | Normalized VDS range | Duration range |
| --- | --- | ---: | ---: | ---: |
| irradiation | SEB | 11 | 0.39581875 to 0.8333016667 | 0.18063 s to 50.748 s |

## Notes

Phase 1 was deployed as part of the combined Figure 1(b) rollout after the
Phase 2 schema rebuild, but the measured-duration landscape chart remains a
Phase 1 chart: it uses the original `stress_duration_s` field and does not
depend on the derived effective-time column.
