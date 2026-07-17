from aps.ml.iv_damage_policy import AcceptancePolicy
from aps.ml.iv_damage_training import _metrics_meet_policy
from aps.ml.iv_damage_validation import ValidationMetrics


def metrics(**changes):
    values = {
        "total_units": 30,
        "supported_units": 30,
        "supported_fraction": 1.0,
        "mae": 0.1,
        "median_absolute_error": 0.1,
        "p90_absolute_error": 0.2,
        "bias": 0.01,
        "baseline_mae": 0.2,
        "baseline_improvement": 0.5,
        "interval_coverage": 0.8,
        "mean_interval_width": 0.4,
        "catastrophic_error_rate": 0.0,
    }
    values.update(changes)
    return ValidationMetrics(**values)


def policy():
    return AcceptancePolicy(
        "approved-v1",
        approved=True,
        max_median_abs_error=0.2,
        max_p90_abs_error=0.4,
        max_abs_bias=0.1,
        max_catastrophic_error_rate=0.05,
        max_mean_interval_width=0.5,
    )


def test_every_required_grouped_diagnostic_must_meet_full_policy():
    assert _metrics_meet_policy(metrics(), policy())
    assert not _metrics_meet_policy(metrics(p90_absolute_error=0.5), policy())
    assert not _metrics_meet_policy(metrics(baseline_improvement=0.0), policy())
    assert not _metrics_meet_policy(metrics(interval_coverage=0.5), policy())
