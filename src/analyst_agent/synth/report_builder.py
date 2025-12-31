from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReportInputs:
    run_dir: Path
    ingest_meta: Path
    data_profile: Path
    analysis_plan: Path
    metrics_csv: Path
    anomalies_normalized: Path
    eda_report: Path
    plots_dir: Path


def _safe_load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _read_metrics(metrics_csv: Path, *, max_rows: int = 25) -> list[dict[str, str]]:
    if not metrics_csv.exists():
        return []
    rows: list[dict[str, str]] = []
    try:
        with metrics_csv.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if not r:
                    continue
                rows.append({k: ("" if v is None else str(v)) for k, v in r.items()})
    except Exception:
        return []

    # Deterministic ordering for reporting: section -> key -> value.
    rows_sorted = sorted(rows, key=lambda r: (r.get("section", ""), r.get("key", ""), r.get("value", "")))
    return rows_sorted[:max_rows]


def _summarize_anomalies(anoms_path: Path, *, max_rows: int = 15) -> list[str]:
    obj = _safe_load_json(anoms_path)
    status = obj.get("_status")
    if isinstance(status, str) and status and status != "ok":
        return [f"- Anomaly detection unavailable (_status={status})."]
    anoms = obj.get("anomalies")
    if not isinstance(anoms, list):
        return ["- No anomalies file found or file was invalid."]
    if not anoms:
        return ["- No anomalies detected under configured policy thresholds."]

    def _sev_rank(sev: str) -> int:
        return {"critical": 3, "warning": 2, "info": 1}.get(str(sev).lower(), 0)

    sorted_anoms = sorted(
        [a for a in anoms if isinstance(a, dict)],
        key=lambda a: (
            -_sev_rank(str(a.get("severity", "info"))),
            str(a.get("metric", "")),
            str(a.get("id", "")),
        ),
    )

    lines: list[str] = []
    for a in sorted_anoms[:max_rows]:
        sev = str(a.get("severity", "info")).lower()
        sev_label = sev.capitalize() if sev else "Info"
        metric = str(a.get("metric", ""))
        summary = str(a.get("summary", "")).strip()
        val = a.get("value")
        if isinstance(val, float):
            val_s = f"{val:.4g}"
        else:
            val_s = "" if val is None else str(val)
        tail = f" ({metric}={val_s})" if metric and val_s else (f" ({metric})" if metric else "")
        text = summary if summary else "Anomaly"
        lines.append(f"- {sev_label} — {text}{tail}")
    if len(sorted_anoms) > max_rows:
        lines.append(f"- … ({len(sorted_anoms) - max_rows} more)")
    return lines


def _plan_lines(plan_path: Path, *, max_rows: int = 20) -> list[str]:
    obj = _safe_load_json(plan_path)
    steps = obj.get("steps")
    if not isinstance(steps, list) or not steps:
        return ["- No analyses planned."]
    lines: list[str] = []
    for s in steps[:max_rows]:
        if not isinstance(s, dict):
            continue
        t = str(s.get("type", ""))
        metric = str(s.get("metric", ""))
        parts = [t] if t else ["analysis"]
        if metric:
            parts.append(f"metric={metric}")
        if s.get("time_axis"):
            parts.append(f"time_axis={s.get('time_axis')}")
        if s.get("entity"):
            parts.append(f"entity={s.get('entity')}")
        if s.get("by"):
            parts.append(f"by={s.get('by')}")
        rid = str(s.get("id", ""))
        prefix = f"[{rid}] " if rid else ""
        lines.append(f"- {prefix}" + ", ".join(parts))
    if isinstance(steps, list) and len(steps) > max_rows:
        lines.append(f"- … ({len(steps) - max_rows} more)")
    return lines


def _executed_query_lines(analysis_log_path: Path, *, max_rows: int = 10) -> list[str]:
    obj = _safe_load_json(analysis_log_path)
    q = obj.get("queries_executed")
    if not isinstance(q, list) or not q:
        return ["- No queries recorded."]
    queries = [str(x) for x in q if isinstance(x, (str, int, float))]
    lines = [f"- {qq}" for qq in queries[:max_rows]]
    if len(queries) > max_rows:
        lines.append(f"- … ({len(queries) - max_rows} more)")
    return lines


def _warnings_lines(analysis_log_path: Path, *, max_rows: int = 10) -> list[str]:
    obj = _safe_load_json(analysis_log_path)
    w = obj.get("warnings")
    if not isinstance(w, list) or not w:
        return ["- None."]
    items = [str(x) for x in w if isinstance(x, (str, int, float))]
    lines = [f"- {s}" for s in items[:max_rows]]
    if len(items) > max_rows:
        lines.append(f"- … ({len(items) - max_rows} more)")
    return lines


def _plots_lines(plots_dir: Path, *, max_rows: int = 25) -> list[str]:
    if not plots_dir.exists() or not plots_dir.is_dir():
        return ["- No plots produced."]
    plots = sorted([p for p in plots_dir.glob("*.png") if p.is_file()], key=lambda p: p.name)
    if not plots:
        return ["- No plots produced."]
    lines = [f"- plots/{p.name}" for p in plots[:max_rows]]
    if len(plots) > max_rows:
        lines.append(f"- … ({len(plots) - max_rows} more)")
    return lines


def _limitations_lines(ingest: dict[str, Any], profile: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    sampling = ingest.get("sampling")
    if isinstance(sampling, dict) and sampling.get("applied"):
        method = sampling.get("method") or "sample"
        rows = sampling.get("rows")
        lines.append(f"- Sampling applied for profiling ({method}; rows={rows}).")
    # Missingness: report top 3 columns by missing fraction.
    cols = profile.get("columns")
    if isinstance(cols, dict):
        miss = []
        for name, meta in cols.items():
            if not isinstance(meta, dict):
                continue
            frac = meta.get("missing_fraction")
            if isinstance(frac, (int, float)) and frac > 0:
                miss.append((float(frac), str(name)))
        miss_sorted = sorted(miss, key=lambda x: (-x[0], x[1]))[:3]
        for frac, name in miss_sorted:
            lines.append(f"- Missing data: {name} missing_fraction={frac:.3f}.")
    if not lines:
        lines.append("- No notable limitations detected from profiling artifacts.")
    return lines


def build_report(*, inputs: ReportInputs, output_path: Path) -> None:
    """Build report.md deterministically.

    This function must not rely on any non-deterministic system state.
    It only reads artifacts already present in the run directory.
    """

    ingest_obj = _safe_load_json(inputs.ingest_meta)
    profile_obj = _safe_load_json(inputs.data_profile)

    # Executive summary: top anomaly lines (deterministic sort in helper).
    anomaly_lines = _summarize_anomalies(inputs.anomalies_normalized)
    metrics_rows = _read_metrics(inputs.metrics_csv)
    plan_lines = _plan_lines(inputs.analysis_plan)
    analysis_log_path = inputs.run_dir / "analysis_log.json"
    executed_lines = _executed_query_lines(analysis_log_path)
    warnings_lines = _warnings_lines(analysis_log_path)
    plots_lines = _plots_lines(inputs.plots_dir)
    limitations_lines = _limitations_lines(ingest_obj, profile_obj)

    # Dataset overview (facts only)
    ds_name = str(ingest_obj.get("dataset_id") or "")
    row_count = profile_obj.get("row_count")
    col_count = profile_obj.get("column_count")
    time_candidates = profile_obj.get("time_candidates")
    time_candidates_s = ""
    if isinstance(time_candidates, list) and time_candidates:
        time_candidates_s = ", ".join(sorted([str(x) for x in time_candidates])[:5])

    lines: list[str] = []
    lines.append("# Analyst Agent Report\n")

    # 1. Executive Summary
    lines.append("\n## Executive Summary\n")
    lines.extend([l + "\n" for l in anomaly_lines])

    # 2. Dataset Overview
    lines.append("\n## Dataset Overview\n")
    if ds_name:
        lines.append(f"- Dataset: {ds_name}\n")
    if isinstance(row_count, int) and isinstance(col_count, int):
        lines.append(f"- Shape: {row_count} rows × {col_count} columns\n")
    if time_candidates_s:
        lines.append(f"- Time candidates: {time_candidates_s}\n")
    lines.append(f"- EDA report: eda_report.html\n")

    # 3. Executed Queries
    lines.append("\n## Executed Queries\n")
    lines.extend([l + "\n" for l in executed_lines])

    # 3b. Execution Warnings
    lines.append("\n## Execution Warnings\n")
    lines.extend([l + "\n" for l in warnings_lines])

    # 4. Planned Analyses
    lines.append("\n## Planned Analyses\n")
    lines.extend([l + "\n" for l in plan_lines])

    # 5. Key Metrics
    lines.append("\n## Key Metrics\n")
    if metrics_rows:
        for r in metrics_rows:
            sec = r.get("section", "")
            key = r.get("key", "")
            val = r.get("value", "")
            if sec or key:
                label = "/".join([p for p in [sec, key] if p])
                lines.append(f"- {label}: {val}\n")
    else:
        lines.append("- No metrics produced.\n")

    # 6. Anomalies
    lines.append("\n## Anomalies\n")
    lines.extend([l + "\n" for l in anomaly_lines])

    # 7. Limitations & Caveats
    lines.append("\n## Limitations & Caveats\n")
    lines.extend([l + "\n" for l in limitations_lines])

    # Links
    lines.append("\n## Artifacts\n")
    lines.append("- eda_report.html\n")
    lines.append("- data_profile.json\n")
    lines.append("- analysis_plan.json\n")
    lines.append("- metrics.csv\n")
    lines.append("- anomalies_normalized.json\n")
    lines.append("\n### Plots\n")
    lines.extend([l + "\n" for l in plots_lines])

    output_path.write_text("".join(lines), encoding="utf-8")
