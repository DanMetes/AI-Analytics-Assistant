from __future__ import annotations

import pytest

from analyst_agent.plan.schema import PlanValidationError, validate_plan_obj


def test_empty_plan_is_accepted() -> None:
    assert validate_plan_obj({}) == {"steps": []}
    assert validate_plan_obj({"steps": []}) == {"steps": []}
    assert validate_plan_obj({"steps": None}) == {"steps": []}


def test_invalid_step_type_is_rejected() -> None:
    bad = {"steps": [{"id": "s1", "type": "badtype", "metric": "sales", "rationale": ""}]}
    with pytest.raises(PlanValidationError):
        validate_plan_obj(bad)


def test_missing_required_field_is_rejected_deterministically() -> None:
    # trend requires time_axis
    bad = {"steps": [{"id": "t1", "type": "trend", "metric": "sales", "rationale": "x"}]}
    with pytest.raises(PlanValidationError) as ei:
        validate_plan_obj(bad)
    assert "missing required fields" in str(ei.value)
