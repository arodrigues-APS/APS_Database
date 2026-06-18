# Figure 1(b) Phase 3 Completed - Reference Lines and Presentation

Date completed: 2026-06-11

## Scope

Phase 3 adds the presentation pieces that make the Figure 1(b) recreation read
like the paper: the database points remain at short timescales, while the
qualification and lifetime references sit far above them. That vertical gap is
the intended message: this database currently documents robustness and
single-event stress windows, not long reliability qualification campaigns.

Implemented in `data_processing_scripts/create_proxy_readiness_dashboard.py`:

1. Extended `scatter_params()` with optional `annotation_layers`,
   `x_axis_bounds`, `y_axis_bounds`, and chart-description passthrough.
2. Added reference-line annotations for:
   - `3.6e6` s, labeled `Acceptable test time: 1000 h`;
   - `4.73e8` s, labeled `Specified lifetime: 15 y`.
3. Set Figure 1(b) landscape y-axis bounds to `[1e-12, 1e9]` so the
   reliability gap remains visible on the log axis.
4. Added chart descriptions that cite the Kozak et al. Figure 1(b) mapping and
   document the key caveats: irradiation durations are measurement windows,
   avalanche `normalized_vds > 1.60` rows are quality-excluded, and current
   destructive markers are irradiation SEB only.

Implemented in `data_processing_scripts/superset_api.py`:

1. Added backward-compatible optional `description` support to `create_chart()`.
2. Existing callers are unaffected; the new Figure 1(b) charts pass
   descriptions through a private `_description` key that is stripped before
   chart params are sent to Superset.

## Deployment Evidence

Dashboard metadata update completed after the description support was added:

```bash
/home/arodrigues/aps_venv/bin/python create_proxy_readiness_dashboard.py --skip-schema
```

Superset reported:

| Artifact | Result |
| --- | --- |
| Dashboard | Updated dashboard id `32` |
| Landscape chart | Updated chart id `419` |
| Effective stress-time chart | Updated chart id `420` |
| Destructive marker chart | Updated chart id `421` |
| Boundary table | Updated chart id `422` |
| Total dashboard charts | `25` |

## Export Verification

The local export helper regenerated the Figure 1(b) artifacts:

| Export | Check |
| --- | --- |
| `proxy-readiness-figure-1-b-stress-vs-timescale-landscape.png` | 1680 x 980 PNG, non-white pixel fraction 0.0557 |
| `proxy-readiness-figure-1-b-effective-stress-time-landscape.png` | 1680 x 980 PNG, non-white pixel fraction 0.0638 |
| `proxy-readiness-figure-1-b-destructive-outcomes-destruction-limit-markers.png` | 1680 x 980 PNG, non-white pixel fraction 0.0243 |
| `proxy-readiness-figure-1-b-destruction-boundary-by-device.csv` | 16 device/voltage rows exported |

The sandbox image viewer was unavailable because the environment failed during
`bwrap` setup, so verification used file metadata and pixel checks rather than
manual image inspection.

## Test Evidence

Final checks:

```bash
/home/arodrigues/aps_venv/bin/python -m py_compile \
  data_processing_scripts/create_proxy_readiness_dashboard.py \
  data_processing_scripts/superset_api.py \
  tests/test_stress_context_figure1b.py
/home/arodrigues/aps_venv/bin/python -m pytest tests/ -q
```

Result: `26 passed in 0.19s`.

## Post-Deployment Fix (2026-06-12)

In-browser the Figure 1(b) landscape charts failed with a Superset data error:

```text
Request is incorrect: {'queries': {0: {'annotation_layers':
  {0: {'showMarkers': ['Missing data for required field.']},
   1: {'showMarkers': ['Missing data for required field.']}}}}}
```

Superset's annotation-layer API schema requires `showMarkers` (and expects
the other explore-UI keys) even for FORMULA layers. Two fixes in
`create_proxy_readiness_dashboard.py`:

1. `FIGURE1B_REFERENCE_LINES` now carries the full explore-UI payload:
   `sourceType`, `opacity`, `width`, `showMarkers`, `hideLine`, `overrides`.
2. `scatter_params()` sets `truncateYAxis = True` whenever explicit
   `y_axis_bounds` are passed, because Superset ignores the bounds otherwise.
   This makes the `[1e-12, 1e9]` reliability-gap axis effective.

Verified after a `--skip-schema` redeploy: chart 419 stores the complete
layers with `truncateYAxis: true`, and a replayed `/api/v1/chart/data`
request including the annotation layers returns HTTP 200 / query success
(the previous request failed marshmallow validation with HTTP 400).
