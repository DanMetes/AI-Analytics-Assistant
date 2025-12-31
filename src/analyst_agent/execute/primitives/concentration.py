from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from ._util import safe_filename, save_matplotlib


def run_concentration(*, df: pd.DataFrame, step: dict, plots_dir: Path):
    metric = str(step.get("metric", ""))
    entity = str(step.get("entity", ""))
    if not metric or not entity:
        return [], []
    if metric not in df.columns or entity not in df.columns:
        return [], []

    m = pd.to_numeric(df[metric], errors="coerce")
    e = df[entity].astype(str)
    tmp = pd.DataFrame({"e": e, "m": m}).dropna()
    if tmp.empty:
        return [], []

    grouped = tmp.groupby("e", as_index=False)["m"].sum().sort_values("m", ascending=False)
    total = float(grouped["m"].sum())
    top_n = grouped.head(10)

    rows = []
    if total > 0 and not top_n.empty:
        top1 = float(top_n["m"].iloc[0])
        rows.append({"metric": metric, "stat": "concentration_total", "value": f"{total:.6f}", "details_json": ""})
        rows.append({"metric": metric, "stat": "concentration_top1_share", "value": f"{top1/total:.6f}", "details_json": ""})
        rows.append({"metric": metric, "stat": "concentration_top10_share", "value": f"{float(top_n['m'].sum())/total:.6f}", "details_json": ""})

    fig = plt.figure()
    plt.bar(top_n["e"], top_n["m"])
    plt.title(f"Top 10 {entity} by {metric} (sum)")
    plt.xlabel(entity)
    plt.ylabel(metric)
    plt.xticks(rotation=45, ha="right")

    fname = safe_filename(f"{step.get('id','concentration')}_{metric}_top10") + ".png"
    out = plots_dir / fname
    save_matplotlib(fig, out)

    return rows, [out]
