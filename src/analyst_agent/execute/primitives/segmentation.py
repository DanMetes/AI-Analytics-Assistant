from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from ._util import safe_filename, save_matplotlib


def run_segmentation(*, df: pd.DataFrame, step: dict, plots_dir: Path):
    metric = str(step.get("metric", ""))
    by = str(step.get("by", ""))
    if not metric or not by:
        return [], []
    if metric not in df.columns or by not in df.columns:
        return [], []

    m = pd.to_numeric(df[metric], errors="coerce")
    g = df[by].astype(str)
    tmp = pd.DataFrame({"g": g, "m": m}).dropna()
    if tmp.empty:
        return [], []

    grouped = tmp.groupby("g", as_index=False)["m"].mean().sort_values("m", ascending=False)
    top = grouped.head(10)

    rows = []
    if not top.empty:
        rows.append({"metric": metric, "stat": f"segmentation_groups_{by}", "value": str(int(grouped.shape[0])), "details_json": ""})
        rows.append({"metric": metric, "stat": f"segmentation_top_group_{by}", "value": str(top['g'].iloc[0]), "details_json": ""})

    fig = plt.figure()
    plt.bar(top["g"], top["m"])
    plt.title(f"Top 10 {by} by {metric} (mean)")
    plt.xlabel(by)
    plt.ylabel(metric)
    plt.xticks(rotation=45, ha="right")

    fname = safe_filename(f"{step.get('id','segmentation')}_{metric}_by_{by}") + ".png"
    out = plots_dir / fname
    save_matplotlib(fig, out)

    return rows, [out]
