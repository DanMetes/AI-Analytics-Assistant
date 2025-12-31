from __future__ import annotations

from pathlib import Path

import pandas as pd

from analyst_agent.pipeline.context import RunContext
from analyst_agent.profile.profiler import profile_dataset_to_html
from analyst_agent.utils import write_json


def test_fallback_eda_html_is_information_dense(tmp_path: Path, monkeypatch) -> None:
    """If ydata-profiling is unavailable, we still must write a useful HTML EDA.

    This test intentionally runs in environments without ydata-profiling
    installed and asserts that the fallback report contains key sections.
    """

    monkeypatch.chdir(tmp_path)

    # Create a small CSV with mixed types.
    csv_path = tmp_path / "sample.csv"
    df = pd.DataFrame(
        {
            "order_date": ["2023-01-01", "2023-01-02", "2023-02-01", None],
            "region": ["West", "West", "East", "East"],
            "units": [1, 2, 9999, 4],
            "sales": [10.0, 20.0, 99999.0, 40.0],
        }
    )
    df.to_csv(csv_path, index=False)

    # Arrange project dataset fingerprint so profiler can locate source CSV.
    project_id = "p1"
    dataset_id = "d1"
    ds_dir = tmp_path / "projects" / project_id / "datasets" / dataset_id
    ds_dir.mkdir(parents=True, exist_ok=True)
    write_json(ds_dir / "fingerprint.json", {"source_path": str(csv_path)})

    run_dir = tmp_path / "projects" / project_id / "runs" / "r1"
    run_dir.mkdir(parents=True, exist_ok=True)
    ctx = RunContext.create(project_root=tmp_path, run_dir=run_dir, dataset_hash="x", run_id="r1")

    outcome = profile_dataset_to_html(ctx=ctx, project_id=project_id, dataset_id=dataset_id, analysis_log_path=None)
    assert outcome.ok is True

    html_path = ctx.eda_report_path()
    assert html_path.exists()
    content = html_path.read_text(encoding="utf-8")

    # Key sections we require for usefulness.
    assert "Dataset Overview" in content
    assert "Column Summary" in content
    assert "Missingness Overview" in content
    assert "Numeric Columns" in content
    assert "Categorical Columns" in content
    assert "Correlation Overview" in content
    assert "Limitations" in content