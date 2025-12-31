from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from ._util import quantiles, safe_filename, save_matplotlib


def run_distribution(*, df: pd.DataFrame, step: dict, plots_dir: Path):
    metric = str(step.get("metric", ""))
    if not metric or metric not in df.columns:
        return [], []

    series = pd.to_numeric(df[metric], errors="coerce").dropna()
    if series.empty:
        return [], []

    q = quantiles(series)
    rows = [
        {"metric": metric, "stat": "count", "value": str(int(series.size)), "details_json": ""},
        {"metric": metric, "stat": "mean", "value": f"{float(series.mean()):.6f}", "details_json": ""},
        {"metric": metric, "stat": "std", "value": f"{float(series.std(ddof=0)):.6f}", "details_json": ""},
        {"metric": metric, "stat": "min", "value": f"{float(series.min()):.6f}", "details_json": ""},
        {"metric": metric, "stat": "max", "value": f"{float(series.max()):.6f}", "details_json": ""},
        {"metric": metric, "stat": "p05", "value": f"{q['p05']:.6f}", "details_json": ""},
        {"metric": metric, "stat": "p50", "value": f"{q['p50']:.6f}", "details_json": ""},
        {"metric": metric, "stat": "p95", "value": f"{q['p95']:.6f}", "details_json": ""},
    ]

    fig = plt.figure()
    plt.hist(series, bins=30)
    plt.title(f"Distribution: {metric}")
    plt.xlabel(metric)
    plt.ylabel("Count")

    fname = safe_filename(f"{step.get('id','distribution')}_{metric}_hist") + ".png"
    out = plots_dir / fname
    save_matplotlib(fig, out)

    return rows, [out]
