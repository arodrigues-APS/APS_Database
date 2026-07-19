"""Prospective V3 request and immutable dataset-snapshot contracts."""

from __future__ import annotations

import hashlib
import json
import math
from numbers import Real
from dataclasses import asdict, dataclass
from typing import Mapping, Sequence


PROHIBITED_REQUEST_FIELDS = frozenset(
    {
        "post_value", "post_metadata_id", "post_feature_id", "observed_value",
        "observed_response", "response_value", "abs_residual", "residual",
    }
)


@dataclass(frozen=True)
class PredictionRequest:
    physical_device_key: str
    device_type: str
    measurement_protocol_id: str
    stress_type: str
    target_type: str
    pre_value: float
    pre_uncertainty: float | None
    reference_policy: str
    stress_features: Mapping[str, object]
    request_source: str
    manufacturer: str | None = None
    requested_by: str | None = None
    requested_prediction_horizon_s: float | None = None

    def __post_init__(self) -> None:
        if self.stress_type not in {"sc", "irradiation"}:
            raise ValueError(f"unsupported stress_type: {self.stress_type}")
        if self.target_type not in {"delta_vth_v", "log_rdson_ratio"}:
            raise ValueError(f"unsupported target_type: {self.target_type}")
        if self.reference_policy not in {"same_device", "library_screening"}:
            raise ValueError(f"unsupported reference_policy: {self.reference_policy}")
        prohibited = PROHIBITED_REQUEST_FIELDS.intersection(self.stress_features)
        if prohibited:
            raise ValueError(
                "prospective request contains post-outcome field(s): "
                + ", ".join(sorted(prohibited))
            )
        if not all(
            value and str(value).strip()
            for value in (
                self.physical_device_key, self.device_type,
                self.measurement_protocol_id, self.request_source,
            )
        ):
            raise ValueError("request identity and protocol fields are required")
        if (
            not isinstance(self.pre_value, Real)
            or isinstance(self.pre_value, bool)
            or not math.isfinite(float(self.pre_value))
        ):
            raise ValueError("pre_value must be finite")
        if self.pre_uncertainty is not None and (
            not isinstance(self.pre_uncertainty, Real)
            or isinstance(self.pre_uncertainty, bool)
            or not math.isfinite(float(self.pre_uncertainty))
            or self.pre_uncertainty < 0
        ):
            raise ValueError("pre_uncertainty must be finite and nonnegative")
        if self.target_type == "log_rdson_ratio" and self.pre_value <= 0:
            raise ValueError("pre-stress Rds(on) must be positive")
        features = dict(self.stress_features)
        horizon = self.requested_prediction_horizon_s
        feature_horizon = features.get("post_measurement_delay_s")
        if self.stress_type == "irradiation":
            if horizon is None and feature_horizon is None:
                raise ValueError(
                    "irradiation requests require prediction_horizon_s "
                    "(post_measurement_delay_s)"
                )
            if horizon is None:
                horizon = feature_horizon
            elif feature_horizon is None:
                features["post_measurement_delay_s"] = horizon
            else:
                try:
                    conflict = float(horizon) != float(feature_horizon)
                except (TypeError, ValueError):
                    conflict = True
                if conflict:
                    raise ValueError(
                        "prediction_horizon_s must equal "
                        "stress_features.post_measurement_delay_s"
                    )
        if horizon is not None and (
            isinstance(horizon, bool)
            or not isinstance(horizon, (int, float))
            or not math.isfinite(float(horizon))
            or float(horizon) <= 0
        ):
            raise ValueError("requested_prediction_horizon_s must be finite and positive")
        object.__setattr__(self, "stress_features", features)
        object.__setattr__(
            self,
            "requested_prediction_horizon_s",
            None if horizon is None else float(horizon),
        )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def request_key(request: PredictionRequest) -> str:
    """Stable idempotency key for the complete prospective input contract."""
    payload = asdict(request)
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def dataset_snapshot_hash(
    *,
    unit_records: Sequence[Mapping[str, object]],
    extraction_versions: Mapping[str, str],
    source_query: str,
    source_code_sha: str,
) -> str:
    """Hash a sorted immutable dataset representation and its provenance."""
    normalized_units = sorted(
        (dict(record) for record in unit_records),
        key=lambda record: str(record.get("unit_key", "")),
    )
    payload = {
        "units": normalized_units,
        "extraction_versions": dict(sorted(extraction_versions.items())),
        "source_query": source_query,
        "source_code_sha": source_code_sha,
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def post_value_from_response(
    target_type: str,
    pre_value: float,
    predicted_response: float,
) -> float:
    if target_type == "delta_vth_v":
        return float(pre_value) + float(predicted_response)
    if target_type == "log_rdson_ratio":
        if pre_value <= 0.0:
            raise ValueError("pre-stress Rds(on) must be positive")
        return float(pre_value) * math.exp(float(predicted_response))
    raise ValueError(f"unsupported target_type: {target_type}")
