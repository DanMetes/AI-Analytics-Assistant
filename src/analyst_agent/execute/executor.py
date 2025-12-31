from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from ..artifacts import ArtifactWriter
from ..pipeline.context import RunContext

from .primitives.concentration import run_concentration
from .primitives.distribution import run_distribution
from .primitives.quality import run_quality
from .primitives.segmentation import run_segmentation
from .primitives.trend import run_trend


@dataclass(frozen=True)
class MetricRow:
    """A single metric row in the *contract* metrics.csv format.

    The project-wide contract for metrics.csv is a three-column CSV:
      section,key,value

    Batch E1 is best-effort and must not break the existing metrics
    produced by the legacy analysis engine; therefore this executor
    appends rows using the same schema.
    """

    section: str
    key: str
    value: str

    def as_list(self) -> list[str]:
        return [self.section, self.key, self.value]


_METRICS_HEADER = ["section", "key", "value"]


def _load_source_csv(project_root: Path, project_id: str, dataset_id: str) -> Path:
    fp = project_root / "projects" / project_id / "datasets" / dataset_id / "fingerprint.json"
    if not fp.exists():
        raise FileNotFoundError(f"Missing dataset fingerprint.json at {fp}")
    obj = json.loads(fp.read_text(encoding="utf-8"))
    source_path = obj.get("source_path")
    if not isinstance(source_path, str) or not source_path:
        raise ValueError("fingerprint.json missing source_path")
    return Path(source_path)


def _append_metrics(path: Path, rows: Iterable[MetricRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    # Never overwrite existing metrics.csv. If the file does not exist,
    # write the contract header first.
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(_METRICS_HEADER)
        for r in rows:
            w.writerow(r.as_list())


def _append_analysis_log(analysis_log_path: Path, payload: dict[str, Any]) -> None:
    """Best-effort merge into analysis_log.json under a 'stages' list."""

    try:
        if analysis_log_path.exists():
            existing = json.loads(analysis_log_path.read_text(encoding="utf-8"))
            if not isinstance(existing, dict):
                existing = {}
        else:
            existing = {}

        stages = existing.get("stages")
        if not isinstance(stages, list):
            stages = []
        stages.append(payload)
        existing["stages"] = stages
        analysis_log_path.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except Exception:
        return


def execute_plan(
    *,
    ctx: RunContext,
    project_id: str,
    dataset_id: str,
    analysis_log_path: Path,
) -> None:
    """Execute plan primitives and write metrics/plots.

    Batch E1 scope:
    - deterministic
    - no LLM
    - best-effort execution (should not abort the run)

    This stage must not break existing metrics generation; it appends
    additional rows to metrics.csv.
    """

    aw = ArtifactWriter(ctx)

    plan_path = aw.path_analysis_plan()
    if not plan_path.exists():
        return
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if not isinstance(plan, dict):
        return
    steps = plan.get("steps")
    if not isinstance(steps, list) or not steps:
        return

    source_csv = _load_source_csv(ctx.project_root, project_id, dataset_id)
    df = pd.read_csv(source_csv)

    plots_dir = aw.plots_dir()
    plots_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[MetricRow] = []
    ran: list[dict[str, Any]] = []

    for step in steps:
        if not isinstance(step, dict):
            continue
        step_id = str(step.get("id", ""))
        step_type = str(step.get("type", ""))
        if not step_id or not step_type:
            continue

        try:
            if step_type == "quality":
                rows, plot_paths = run_quality(df=df, step=step, plots_dir=plots_dir)
            elif step_type == "distribution":
                rows, plot_paths = run_distribution(df=df, step=step, plots_dir=plots_dir)
            elif step_type == "trend":
                rows, plot_paths = run_trend(df=df, step=step, plots_dir=plots_dir)
            elif step_type == "concentration":
                rows, plot_paths = run_concentration(df=df, step=step, plots_dir=plots_dir)
            elif step_type == "segmentation":
                rows, plot_paths = run_segmentation(df=df, step=step, plots_dir=plots_dir)
            else:
                continue

            for r in rows:
                metric = str(r.get("metric", ""))
                stat = str(r.get("stat", ""))
                value = str(r.get("value", ""))
                # Maintain the contract schema: section,key,value.
                # Use section=step_id and a stable key incorporating the
                # primitive metric + stat.
                key = ".".join([p for p in [metric, stat] if p])
                all_rows.append(MetricRow(section=step_id, key=key, value=value))

            ran.append(
                {
                    "id": step_id,
                    "type": step_type,
                    "metric": step.get("metric"),
                    "plots": [p.name for p in plot_paths],
                    "rows_in": int(len(df)),
                }
            )
        except Exception as e:
            ran.append(
                {
                    "id": step_id,
                    "type": step_type,
                    "metric": step.get("metric"),
                    "error": f"{type(e).__name__}: {e}",
                    "rows_in": int(len(df)),
                }
            )

    if all_rows:
        _append_metrics(aw.path_metrics_csv(), all_rows)

    _append_analysis_log(
        analysis_log_path,
        {
            "stage": "execute_primitives",
            "steps_ran": ran,
            "source_csv": str(source_csv),
        },
    )
