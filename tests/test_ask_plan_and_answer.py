from __future__ import annotations

import json
from pathlib import Path

from analyst_agent.ask import answer_or_plan


def _write_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # minimal artifacts
    (run_dir / "metrics.csv").write_text(
        "section,key,value\n"
        "overall,row_count,10\n"
        "time_summary,year=2023:sum_units,999\n",
        encoding="utf-8",
    )
    (run_dir / "data_profile.json").write_text(
        json.dumps({"_status": "ok", "row_count": 10, "column_count": 3, "time_candidates": ["year"]}),
        encoding="utf-8",
    )
    (run_dir / "anomalies_normalized.json").write_text(
        json.dumps({"_status": "ok", "anomalies": [{"id": "a1", "summary": "sum_units spike", "severity": "critical"}]}),
        encoding="utf-8",
    )
    return run_dir


def test_answer_mode_deterministic(tmp_path: Path) -> None:
    run_dir = _write_run_dir(tmp_path)
    res = answer_or_plan(run_dir=run_dir, question="What is the row count?", use_llm=False)
    assert res.mode == "answer"
    assert "row_count" in res.answer_md
    assert any("data_profile:row_count" in r or "metric:overall:row_count" in r for r in res.evidence_refs)


def test_plan_mode_deterministic(tmp_path: Path) -> None:
    run_dir = _write_run_dir(tmp_path)
    res = answer_or_plan(run_dir=run_dir, question="Which sub_category drives the spike?", use_llm=False)
    assert res.mode == "plan"
    assert res.plan_path is not None and res.plan_path.exists()
    assert res.code_path is not None and res.code_path.exists()
