from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from ._util import safe_filename, save_matplotlib


def run_trend(*, df: pd.DataFrame, step: dict, plots_dir: Path):
    metric = str(step.get("metric", ""))
    time_axis = str(step.get("time_axis", ""))
    if not metric or not time_axis:
        return [], []
    if metric not in df.columns or time_axis not in df.columns:
        return [], []

    t = pd.to_datetime(df[time_axis], errors="coerce")
    m = pd.to_numeric(df[metric], errors="coerce")
    tmp = pd.DataFrame({"t": t, "m": m}).dropna()
    if tmp.empty:
        return [], []

    tmp["period"] = tmp["t"].dt.to_period("M").dt.to_timestamp()
    grouped = tmp.groupby("period", as_index=False)["m"].mean().sort_values("period")

    rows = []
    if not grouped.empty:
        first = float(grouped["m"].iloc[0])
        last = float(grouped["m"].iloc[-1])
        rows.append({"metric": metric, "stat": "trend_first", "value": f"{first:.6f}", "details_json": ""})
        rows.append({"metric": metric, "stat": "trend_last", "value": f"{last:.6f}", "details_json": ""})
        rows.append({"metric": metric, "stat": "trend_periods", "value": str(int(len(grouped))), "details_json": ""})

    fig = plt.figure()
    plt.plot(grouped["period"], grouped["m"])
    plt.title(f"Trend (monthly mean): {metric}")
    plt.xlabel(time_axis)
    plt.ylabel(metric)

    fname = safe_filename(f"{step.get('id','trend')}_{metric}_trend") + ".png"
    out = plots_dir / fname
    save_matplotlib(fig, out)

    return rows, [out]
