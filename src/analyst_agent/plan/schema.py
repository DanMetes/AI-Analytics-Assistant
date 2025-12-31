from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

ALLOWED_STEP_TYPES: set[str] = {"distribution", "trend", "concentration", "quality", "segmentation"}

_REQUIRED_BY_TYPE: dict[str, tuple[str, ...]] = {
    "distribution": ("metric",),
    "trend": ("metric", "time_axis"),
    "concentration": ("metric", "entity"),
    "quality": ("metric",),
    "segmentation": ("metric", "by"),
}


class PlanValidationError(ValueError):
    """Raised when analysis_plan.json violates the contract."""


def _is_empty_plan(obj: Any) -> bool:
    if obj is None:
        return True
    if obj == {}:
        return True
    if obj == []:
        return True
    if isinstance(obj, Mapping) and obj.get("steps") in (None, [], {}):
        # steps missing or empty list
        return True
    return False


def normalize_empty_plan() -> dict[str, Any]:
    return {"steps": []}


def validate_plan_obj(obj: Any) -> dict[str, Any]:
    """Validate and normalize an analysis plan object.

    Returns a normalized dict representation. Raises PlanValidationError on violations.

    Empty plans are accepted and normalized to {'steps': []}.
    """
    if _is_empty_plan(obj):
        return normalize_empty_plan()

    if not isinstance(obj, Mapping):
        raise PlanValidationError("analysis_plan.json must be a JSON object (mapping).")

    steps = obj.get("steps")
    if steps is None:
        # treat missing as empty
        return normalize_empty_plan()

    if not isinstance(steps, list):
        raise PlanValidationError("'steps' must be a list.")

    normalized_steps: list[dict[str, Any]] = []
    for i, step in enumerate(steps):
        if not isinstance(step, Mapping):
            raise PlanValidationError(f"step[{i}] must be an object.")
        step_id = step.get("id")
        if not isinstance(step_id, str) or not step_id.strip():
            raise PlanValidationError(f"step[{i}].id must be a non-empty string.")
        step_type = step.get("type")
        if not isinstance(step_type, str) or step_type not in ALLOWED_STEP_TYPES:
            raise PlanValidationError(
                f"step[{i}].type must be one of {sorted(ALLOWED_STEP_TYPES)}."
            )

        rationale = step.get("rationale")
        if not isinstance(rationale, str):
            raise PlanValidationError(f"step[{i}].rationale must be a string (can be empty).")

        required = _REQUIRED_BY_TYPE.get(step_type, ())
        missing: list[str] = []
        for field in required:
            v = step.get(field)
            if v is None or (isinstance(v, str) and not v.strip()):
                missing.append(field)
        if missing:
            raise PlanValidationError(f"step[{i}] missing required fields for type '{step_type}': {missing}")

        # Keep original fields; ensure id/type/rationale present
        norm = dict(step)
        norm["id"] = step_id
        norm["type"] = step_type
        norm["rationale"] = rationale
        normalized_steps.append(norm)

    # optional: stable ordering by id for determinism
    normalized_steps = sorted(normalized_steps, key=lambda s: str(s.get("id", "")))

    normalized: dict[str, Any] = dict(obj)
    normalized["steps"] = normalized_steps
    return normalized


def load_and_validate_plan(path: Path) -> dict[str, Any]:
    """Load analysis_plan.json from disk and validate."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return normalize_empty_plan()
    except json.JSONDecodeError as e:
        raise PlanValidationError(f"analysis_plan.json is not valid JSON: {e}") from e

    return validate_plan_obj(raw)


def write_plan(path: Path, plan: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # stable JSON output
    path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
