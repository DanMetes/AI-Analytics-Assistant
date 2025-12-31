from __future__ import annotations

import json
from pathlib import Path

from analyst_agent.synth.llm_interpretation import generate_llm_interpretation


def test_llm_interpretation_fallback_writes_cache(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()

    dp = run_dir / "data_profile.json"
    pl = run_dir / "analysis_plan.json"
    mc = run_dir / "metrics.csv"
    an = run_dir / "anomalies_normalized.json"

    dp.write_text(json.dumps({"_status": "ok", "row_count": 1, "column_count": 1, "columns": []}), encoding="utf-8")
    pl.write_text(json.dumps({"_status": "ok", "steps": []}), encoding="utf-8")
    mc.write_text("section,key,value\noverall,row_count,1\n", encoding="utf-8")
    an.write_text(json.dumps({"_status": "ok", "anomalies": []}), encoding="utf-8")

    obj = generate_llm_interpretation(
        run_dir=run_dir,
        data_profile_path=dp,
        plan_path=pl,
        metrics_csv_path=mc,
        anomalies_path=an,
        plots_dir=run_dir / "plots",
        metrics_compact=[{"section": "overall", "key": "row_count", "value": "1"}],
    )

    assert obj.generated_by in ("fallback", "openai")
    cache_dir = run_dir / "cache"
    assert cache_dir.exists()
    assert any(p.name.startswith("llm_interpretation_") for p in cache_dir.iterdir())
