from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from ._util import safe_filename, save_matplotlib


def run_quality(*, df: pd.DataFrame, step: dict, plots_dir: Path):
    """Compute basic data quality stats.

    Returns (rows, plot_paths)
    rows: list[dict(metric, stat, value, details_json)]
    """
    rows: list[dict[str, str]] = []
    plot_paths: list[Path] = []

    # Dataset-level quality stats.
    n_rows = int(len(df))
    n_cols = int(df.shape[1])
    missing_total = int(df.isna().sum().sum())
    cells = max(1, n_rows * n_cols)
    missing_rate = missing_total / cells

    rows.append({"metric": "__dataset__", "stat": "rows", "value": str(n_rows), "details_json": ""})
    rows.append({"metric": "__dataset__", "stat": "columns", "value": str(n_cols), "details_json": ""})
    rows.append({"metric": "__dataset__", "stat": "missing_total", "value": str(missing_total), "details_json": ""})
    rows.append({"metric": "__dataset__", "stat": "missing_rate", "value": f"{missing_rate:.6f}", "details_json": ""})

    # Top missing columns chart.
    miss = df.isna().mean().sort_values(ascending=False).head(10)
    if not miss.empty and float(miss.iloc[0]) > 0:
        fig = plt.figure()
        miss.plot(kind="bar")
        plt.title("Top Missingness (rate)")
        plt.ylabel("Missing rate")
        fname = safe_filename(f"{step.get('id','quality')}_missingness") + ".png"
        out = plots_dir / fname
        save_matplotlib(fig, out)
        plot_paths.append(out)

    return rows, plot_paths
