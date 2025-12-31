from __future__ import annotations

from typing import Literal, TypedDict

Severity = Literal["info", "warning", "critical"]
Direction = Literal["high", "low"]


class NormalizedAnomaly(TypedDict):
    id: str
    policy: str
    metric: str
    severity: Severity
    direction: Direction
    value: float
    threshold: dict[str, float]
    unit: str
    evidence_keys: list[str]
    summary: str


def make_normalized_anomaly(
    *,
    anomaly_id: str,
    policy: str,
    metric: str,
    severity: Severity,
    direction: Direction,
    value: float,
    threshold: dict[str, float],
    unit: str,
    evidence_keys: list[str],
    summary: str,
) -> NormalizedAnomaly:
    if severity not in ("info", "warning", "critical"):
        raise ValueError(f"Invalid severity: {severity}")
    if direction not in ("high", "low"):
        raise ValueError(f"Invalid direction: {direction}")
    if not isinstance(evidence_keys, list) or not all(isinstance(e, str) for e in evidence_keys):
        raise ValueError("evidence_keys must be a list of strings")
    if not isinstance(threshold, dict):
        raise ValueError("threshold must be a dict")
    if "warning" not in threshold or "critical" not in threshold:
        raise ValueError("threshold must contain 'warning' and 'critical'")

    return {
        "id": anomaly_id,
        "policy": policy,
        "metric": metric,
        "severity": severity,
        "direction": direction,
        "value": float(value),
        "threshold": {
            "warning": float(threshold["warning"]),
            "critical": float(threshold["critical"]),
        },
        "unit": unit,
        "evidence_keys": list(evidence_keys),
        "summary": summary,
    }
