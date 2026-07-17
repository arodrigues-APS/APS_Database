"""Leakage-safe validation primitives for the prospective IV damage model.

The unit of evidence is an independent physical-device/stress-session/target
response, never an individual curve point.  Group construction is deliberately
conservative: units connected by any protected identifier remain in the same
fold, even when this makes a proposed validation experiment impossible.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
import math
from statistics import median
from typing import Iterable, Mapping, Sequence


SPLIT_SCHEMES = frozenset(
    {"leave_device", "leave_condition", "leave_run", "leave_campaign", "leave_ion"}
)


@dataclass(frozen=True)
class ValidationUnit:
    response_unit_key: str
    physical_device_key: str
    stress_session_key: str
    target_type: str
    observed_response: float
    stress_condition_key: str | None = None
    run_key: str | None = None
    campaign_key: str | None = None
    ion_species: str | None = None
    baseline_reference_group_key: str | None = None
    device_type: str | None = None

    def __post_init__(self) -> None:
        for field in (
            "response_unit_key",
            "physical_device_key",
            "stress_session_key",
            "target_type",
        ):
            if not str(getattr(self, field) or "").strip():
                raise ValueError(f"{field} is required")
        if not math.isfinite(float(self.observed_response)):
            raise ValueError("observed_response must be finite")


@dataclass(frozen=True)
class FoldAssignment:
    response_unit_key: str
    fold: int
    component_key: str


@dataclass(frozen=True)
class PredictionRecord:
    response_unit_key: str
    observed: float
    predicted: float | None
    baseline_predicted: float
    interval_lower: float | None = None
    interval_upper: float | None = None
    supported: bool = True


@dataclass(frozen=True)
class ValidationMetrics:
    total_units: int
    supported_units: int
    supported_fraction: float
    mae: float | None
    median_absolute_error: float | None
    p90_absolute_error: float | None
    bias: float | None
    baseline_mae: float | None
    baseline_improvement: float | None
    interval_coverage: float | None
    mean_interval_width: float | None
    catastrophic_error_rate: float | None


class _DisjointSet:
    def __init__(self, keys: Iterable[str]) -> None:
        self.parent = {key: key for key in keys}

    def find(self, key: str) -> str:
        root = key
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[key] != key:
            parent = self.parent[key]
            self.parent[key] = root
            key = parent
        return root

    def union(self, left: str, right: str) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self.parent[max(left_root, right_root)] = min(left_root, right_root)


def protected_tokens(unit: ValidationUnit, scheme: str) -> tuple[str, ...]:
    """Return identifiers that are forbidden from crossing folds."""

    if scheme not in SPLIT_SCHEMES:
        raise ValueError(f"unsupported split scheme: {scheme}")
    tokens = [
        f"device:{unit.physical_device_key}",
        f"session:{unit.stress_session_key}",
    ]
    if unit.baseline_reference_group_key:
        tokens.append(f"baseline:{unit.baseline_reference_group_key}")
    scheme_values = {
        "leave_device": unit.physical_device_key,
        "leave_condition": unit.stress_condition_key,
        "leave_run": unit.run_key,
        "leave_campaign": unit.campaign_key,
        "leave_ion": unit.ion_species,
    }
    value = scheme_values[scheme]
    if value:
        tokens.append(f"{scheme}:{value}")
    return tuple(sorted(set(tokens)))


def _components(
    units: Sequence[ValidationUnit], scheme: str
) -> dict[str, list[ValidationUnit]]:
    keys = [unit.response_unit_key for unit in units]
    if len(keys) != len(set(keys)):
        raise ValueError("response_unit_key must be unique; aggregate replicates first")
    dsu = _DisjointSet(keys)
    first_by_token: dict[str, str] = {}
    for unit in units:
        for token in protected_tokens(unit, scheme):
            previous = first_by_token.setdefault(token, unit.response_unit_key)
            dsu.union(previous, unit.response_unit_key)
    grouped: dict[str, list[ValidationUnit]] = defaultdict(list)
    for unit in units:
        grouped[dsu.find(unit.response_unit_key)].append(unit)
    return dict(grouped)


def assign_grouped_folds(
    units: Sequence[ValidationUnit],
    scheme: str,
    *,
    n_splits: int = 5,
    seed: int = 0,
) -> list[FoldAssignment]:
    """Assign linked evidence units to deterministic, approximately balanced folds."""

    if n_splits < 2:
        raise ValueError("n_splits must be at least 2")
    if not units:
        raise ValueError("at least one response unit is required")
    components = _components(units, scheme)
    if len(components) < n_splits:
        raise ValueError(
            f"{scheme} has only {len(components)} independent components for "
            f"{n_splits} folds"
        )

    def order_key(item: tuple[str, list[ValidationUnit]]) -> tuple[int, str]:
        root, members = item
        digest = hashlib.sha256(f"{seed}:{root}".encode()).hexdigest()
        return (-len(members), digest)

    fold_sizes = [0] * n_splits
    rows: list[FoldAssignment] = []
    for root, members in sorted(components.items(), key=order_key):
        fold = min(range(n_splits), key=lambda index: (fold_sizes[index], index))
        component_key = hashlib.sha256(
            "\n".join(sorted(member.response_unit_key for member in members)).encode()
        ).hexdigest()
        rows.extend(
            FoldAssignment(member.response_unit_key, fold, component_key)
            for member in members
        )
        fold_sizes[fold] += len(members)
    return sorted(rows, key=lambda row: row.response_unit_key)


def assert_no_group_leakage(
    units: Sequence[ValidationUnit],
    assignments: Sequence[FoldAssignment],
    scheme: str,
) -> None:
    assignment_by_key = {row.response_unit_key: row.fold for row in assignments}
    expected = {unit.response_unit_key for unit in units}
    if set(assignment_by_key) != expected or len(assignments) != len(expected):
        raise ValueError("assignments must contain every response unit exactly once")
    folds_by_token: dict[str, set[int]] = defaultdict(set)
    for unit in units:
        for token in protected_tokens(unit, scheme):
            folds_by_token[token].add(assignment_by_key[unit.response_unit_key])
    leaked = sorted(token for token, folds in folds_by_token.items() if len(folds) > 1)
    if leaked:
        raise ValueError(f"protected groups cross folds: {', '.join(leaked[:5])}")


class BaselinePredictor:
    """A training-fold-only damage baseline with explicit fallback behavior."""

    STRATEGIES = frozenset({"zero", "global_median", "device_median", "device_ion_median"})

    def __init__(self, strategy: str) -> None:
        if strategy not in self.STRATEGIES:
            raise ValueError(f"unsupported baseline strategy: {strategy}")
        self.strategy = strategy
        self._global_by_target: dict[str, float] = {}
        self._group_values: dict[tuple[str, ...], float] = {}
        self._fitted = False

    def _key(self, unit: ValidationUnit) -> tuple[str, ...] | None:
        if self.strategy == "device_median" and unit.device_type:
            return (unit.target_type, unit.device_type)
        if self.strategy == "device_ion_median" and unit.device_type and unit.ion_species:
            return (unit.target_type, unit.device_type, unit.ion_species)
        return None

    def fit(self, training_units: Sequence[ValidationUnit]) -> "BaselinePredictor":
        if not training_units:
            raise ValueError("baseline requires non-empty training data")
        by_target: dict[str, list[float]] = defaultdict(list)
        by_group: dict[tuple[str, ...], list[float]] = defaultdict(list)
        for unit in training_units:
            by_target[unit.target_type].append(float(unit.observed_response))
            key = self._key(unit)
            if key is not None:
                by_group[key].append(float(unit.observed_response))
        self._global_by_target = {
            target: float(median(values)) for target, values in by_target.items()
        }
        self._group_values = {
            key: float(median(values)) for key, values in by_group.items()
        }
        self._fitted = True
        return self

    def predict(self, unit: ValidationUnit) -> float:
        if not self._fitted:
            raise RuntimeError("baseline must be fitted before prediction")
        if self.strategy == "zero":
            return 0.0
        key = self._key(unit)
        if key is not None and key in self._group_values:
            return self._group_values[key]
        try:
            return self._global_by_target[unit.target_type]
        except KeyError as exc:
            raise ValueError(f"unseen target type: {unit.target_type}") from exc


def _quantile(values: Sequence[float], fraction: float) -> float:
    if not values:
        raise ValueError("quantile requires values")
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def evaluate_predictions(
    records: Sequence[PredictionRecord],
    *,
    catastrophic_error_limit: float | None = None,
) -> ValidationMetrics:
    """Evaluate one record per independent response unit without silent dropping."""

    keys = [record.response_unit_key for record in records]
    if len(keys) != len(set(keys)):
        raise ValueError("validation records must be unique by response_unit_key")
    supported = [
        record
        for record in records
        if record.supported and record.predicted is not None
    ]
    total = len(records)
    fraction = len(supported) / total if total else 0.0
    if not supported:
        return ValidationMetrics(
            total, 0, fraction, None, None, None, None, None, None, None, None, None
        )

    residuals = [float(row.predicted) - row.observed for row in supported]
    abs_errors = [abs(value) for value in residuals]
    baseline_errors = [abs(row.baseline_predicted - row.observed) for row in supported]
    mae = sum(abs_errors) / len(abs_errors)
    baseline_mae = sum(baseline_errors) / len(baseline_errors)
    improvement = None if baseline_mae == 0 else 1.0 - mae / baseline_mae

    interval_rows = [
        row
        for row in supported
        if row.interval_lower is not None and row.interval_upper is not None
    ]
    if interval_rows:
        if any(row.interval_lower > row.interval_upper for row in interval_rows):
            raise ValueError("interval lower bound exceeds upper bound")
        coverage = sum(
            row.interval_lower <= row.observed <= row.interval_upper
            for row in interval_rows
        ) / len(interval_rows)
        mean_width = sum(
            row.interval_upper - row.interval_lower for row in interval_rows
        ) / len(interval_rows)
    else:
        coverage = mean_width = None

    catastrophic_rate = None
    if catastrophic_error_limit is not None:
        if catastrophic_error_limit <= 0:
            raise ValueError("catastrophic_error_limit must be positive")
        catastrophic_rate = sum(
            error > catastrophic_error_limit for error in abs_errors
        ) / len(abs_errors)

    return ValidationMetrics(
        total_units=total,
        supported_units=len(supported),
        supported_fraction=fraction,
        mae=mae,
        median_absolute_error=float(median(abs_errors)),
        p90_absolute_error=_quantile(abs_errors, 0.9),
        bias=sum(residuals) / len(residuals),
        baseline_mae=baseline_mae,
        baseline_improvement=improvement,
        interval_coverage=coverage,
        mean_interval_width=mean_width,
        catastrophic_error_rate=catastrophic_rate,
    )


def fold_manifest(
    assignments: Sequence[FoldAssignment], *, scheme: str, seed: int
) -> list[Mapping[str, object]]:
    """Return persistence-ready split assignments in deterministic order."""

    return [
        {
            "response_unit_key": row.response_unit_key,
            "fold": row.fold,
            "component_key": row.component_key,
            "split_scheme": scheme,
            "seed": seed,
        }
        for row in sorted(assignments, key=lambda item: item.response_unit_key)
    ]
