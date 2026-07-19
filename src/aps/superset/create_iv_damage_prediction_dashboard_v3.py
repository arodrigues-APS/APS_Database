"""Deploy the complete V3 dashboard, including derived and learned curves.

The base builder keeps the learned full-curve surface independently testable;
this composition adds the explicitly lower-claim scalar projection tab and is
the production deployment entry point.
"""

from __future__ import annotations

from aps.superset import create_iv_damage_prediction_dashboard_v3_full_curve as dashboard


dashboard.DATASETS["curve_projection"] = "iv_damage_curve_projection_view"
dashboard.DATASETS["curve_projection_gate"] = (
    "iv_damage_curve_projection_release_gate_view"
)
dashboard.TABS["curve_projection"] = (
    "Derived Curve Projections", "TAB-v3-curve-projections",
)
dashboard.GUIDANCE["TAB-v3-curve-projections"] = (
    "### Shape-constrained scalar projection\n\nThese curves apply a certified scalar ΔVth shift or "
    "RDS(on) scale to the immutable pre-curve. They are transparent engineering projections, "
    "not learned shape predictions, and do not inherit the functional full-curve release claim."
)

_base_definitions = dashboard.definitions


def _complete_definitions() -> list[dict]:
    return [
        *_base_definitions(),
        dict(
            name="V3 Curve — Projection Certification Gate",
            ds="curve_projection_gate", tab="curve_projection",
            viz="table", width=12, height=34,
            params=dashboard.table([
                "method_version", "projection_kind", "target_type",
                "curve_family", "method_approved",
                "external_certification_passed", "certified_by",
                "certified_at",
            ]),
        ),
        dict(
            name="V3 Curve — Shape-Constrained Scalar Projection",
            ds="curve_projection", tab="curve_projection",
            viz="echarts_timeseries_line", width=12, height=58,
            params=dashboard.line(
                "x_value_v",
                [
                    dashboard.metric("pre current (A)", "MAX(pre_i_drain_a)"),
                    dashboard.metric("projected post (A)", "MAX(predicted_i_drain_a)"),
                    dashboard.metric("lower (A)", "MAX(predicted_lower_a)"),
                    dashboard.metric("upper (A)", "MAX(predicted_upper_a)"),
                ],
                ["request_key", "model_version", "projection_kind", "curve_family"],
            ),
        ),
        dict(
            name="V3 Curve — Projection Provenance",
            ds="curve_projection", tab="curve_projection",
            viz="table", width=12, height=34,
            params=dashboard.table([
                "request_key", "model_version", "stress_type", "target_type",
                "projection_kind", "method_version", "curve_family",
                "measurement_protocol_id", "projection_status",
                "evidence_status", "decision_eligible", "created_at",
            ]),
        ),
    ]


dashboard.definitions = _complete_definitions


def main() -> None:
    dashboard.main()


if __name__ == "__main__":
    main()
