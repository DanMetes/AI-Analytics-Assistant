from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def is_id_like(col: pd.Series) -> bool:
    """Heuristic to exclude ID-like fields from numeric metric selection."""
    name = str(col.name).lower()
    if name.endswith("id") or name in {"id", "order_id", "patient_id", "record_id"}:
        return True

    # High cardinality integers with near-unique values.
    try:
        s = col.dropna()
        if s.empty:
            return False
        if pd.api.types.is_integer_dtype(s):
            nunique = int(s.nunique())
            if nunique / max(1, int(len(s))) > 0.95:
                return True
    except Exception:
        return False

    return False


def safe_filename(name: str) -> str:
    keep = []
    for ch in name:
        if ch.isalnum() or ch in {"-", "_", "."}:
            keep.append(ch)
        elif ch in {" ", "/", "\\", ":"}:
            keep.append("_")
    out = "".join(keep).strip("_")
    return out or "plot"


def save_matplotlib(fig: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, bbox_inches="tight")
    fig.clf()


def quantiles(series: pd.Series) -> dict[str, float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {"p05": float("nan"), "p50": float("nan"), "p95": float("nan")}
    qs = s.quantile([0.05, 0.5, 0.95])
    return {"p05": float(qs.loc[0.05]), "p50": float(qs.loc[0.5]), "p95": float(qs.loc[0.95])}
