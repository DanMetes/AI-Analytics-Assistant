from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ..paths import dataset_dir
from ..utils import read_json, write_json
from ..pipeline.context import RunContext


_DATE_NAME_RE = re.compile(r"(date|time|dt|timestamp|created|updated)", re.IGNORECASE)


@dataclass(frozen=True)
class ProfileSummaryOutcome:
    ok: bool
    rows: int
    cols: int
    sampled: bool
    error: Optional[str] = None


def summarize_dataset_to_json(
    *,
    ctx: RunContext,
    project_id: str,
    dataset_id: str,
    analysis_log_path: Optional[Path] = None,
    max_rows: int = 200_000,
    corr_threshold: float = 0.7,
    skew_threshold: float = 1.0,
) -> ProfileSummaryOutcome:
    """Generate deterministic machine-readable dataset summary.

    Batch F2 requirements:
    - Compute from DataFrame deterministically (do NOT scrape HTML)
    - Include row/col counts and per-column summary
    - Detect time-candidate columns
    - Flag high correlations among numeric columns
    - Write stable JSON (key ordering and list ordering)

    Failure behavior:
    - Do not abort the run; write a deterministic error payload instead.
    """

    out_path = ctx.run_dir / "data_profile.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Locate source CSV path from fingerprint.json.
    fp_path = dataset_dir(project_id, dataset_id) / "fingerprint.json"
    source_csv: Optional[Path] = None
    if fp_path.exists():
        try:
            fp = read_json(fp_path)
            p = fp.get("source_path")
            if isinstance(p, str) and p:
                source_csv = Path(p)
        except Exception:
            source_csv = None

    if source_csv is None or not source_csv.exists():
        msg = (
            "Unable to locate the source CSV for profiling. "
            "Expected fingerprint.json with a valid source_path."
        )
        _write_error_json(out_path, msg)
        _record_summary_log(analysis_log_path, {"profile_summary": {"ok": False, "error": msg}})
        return ProfileSummaryOutcome(ok=False, rows=0, cols=0, sampled=False, error=msg)

    try:
        df = pd.read_csv(source_csv)
        rows, cols = int(df.shape[0]), int(df.shape[1])
        sampled = False

        if rows > max_rows:
            df = df.sample(n=max_rows, random_state=42)
            sampled = True

        payload = _build_profile_payload(
            df=df,
            source_csv=str(source_csv),
            sampled=sampled,
            max_rows=max_rows,
            corr_threshold=corr_threshold,
            skew_threshold=skew_threshold,
        )

        # Stable ordering.
        out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        _record_summary_log(
            analysis_log_path,
            {
                "profile_summary": {
                    "ok": True,
                    "source_csv": str(source_csv),
                    "rows_loaded": rows,
                    "cols": cols,
                    "rows_summarized": int(df.shape[0]),
                    "sampled": sampled,
                    "max_rows": max_rows,
                }
            },
        )
        return ProfileSummaryOutcome(ok=True, rows=rows, cols=cols, sampled=sampled)
    except Exception as e:  # noqa: BLE001
        err = f"{type(e).__name__}: {e}"
        _write_error_json(out_path, err)
        _record_summary_log(analysis_log_path, {"profile_summary": {"ok": False, "error": err}})
        return ProfileSummaryOutcome(ok=False, rows=0, cols=0, sampled=False, error=err)


def _build_profile_payload(
    *,
    df: pd.DataFrame,
    source_csv: str,
    sampled: bool,
    max_rows: int,
    corr_threshold: float,
    skew_threshold: float,
) -> dict[str, Any]:
    rows, cols = int(df.shape[0]), int(df.shape[1])

    columns: dict[str, Any] = {}
    time_candidates: list[str] = []

    for col in df.columns:
        s = df[col]
        info: dict[str, Any] = {}

        dtype_norm = _normalize_dtype(s.dtype)
        missing_count = int(s.isna().sum())
        missing_frac = float(missing_count / rows) if rows > 0 else 0.0
        cardinality = int(s.nunique(dropna=True))

        info["dtype"] = dtype_norm
        info["missing_count"] = missing_count
        info["missing_fraction"] = _round(missing_frac, 6)
        info["cardinality"] = cardinality

        if _is_numeric(s):
            stats = _numeric_stats(s, skew_threshold=skew_threshold)
            info.update(stats)

        columns[str(col)] = info

        if _is_time_candidate(col_name=str(col), series=s):
            time_candidates.append(str(col))

    # Correlations: numeric-numeric only.
    corr_flags = _correlation_flags(df, threshold=corr_threshold)

    # Deterministic list ordering.
    time_candidates = sorted(set(time_candidates))

    return {
        "_schema": "analyst_agent.data_profile.v1",
        "source_csv": source_csv,
        "rows": rows,
        "cols": cols,
        "sampled": sampled,
        "max_rows": max_rows,
        "columns": columns,
        "time_candidates": time_candidates,
        "correlations": corr_flags,
    }


def _normalize_dtype(dtype: Any) -> str:
    # pandas dtype to a stable, coarse label
    s = str(dtype)
    if s.startswith("datetime"):
        return "datetime"
    if s.startswith("int") or s.startswith("Int"):
        return "int"
    if s.startswith("float"):
        return "float"
    if s == "bool" or s.startswith("boolean"):
        return "bool"
    if s == "category":
        return "category"
    if s == "object" or s == "string":
        return "string"
    return s


def _is_numeric(series: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(series)


def _numeric_stats(series: pd.Series, *, skew_threshold: float) -> dict[str, Any]:
    s = pd.to_numeric(series, errors="coerce")
    s = s.dropna()
    if s.empty:
        return {
            "mean": None,
            "std": None,
            "min": None,
            "max": None,
            "p05": None,
            "p50": None,
            "p95": None,
            "skew": None,
            "skew_flag": False,
        }

    q = s.quantile([0.05, 0.50, 0.95], interpolation="linear")
    mean = float(s.mean())
    std = float(s.std(ddof=0))
    vmin = float(s.min())
    vmax = float(s.max())
    skew = float(s.skew()) if s.shape[0] >= 3 else 0.0

    return {
        "mean": _round(mean, 6),
        "std": _round(std, 6),
        "min": _round(vmin, 6),
        "max": _round(vmax, 6),
        "p05": _round(float(q.loc[0.05]), 6),
        "p50": _round(float(q.loc[0.50]), 6),
        "p95": _round(float(q.loc[0.95]), 6),
        "skew": _round(skew, 6),
        "skew_flag": bool(abs(skew) >= skew_threshold),
    }


def _is_time_candidate(*, col_name: str, series: pd.Series) -> bool:
    if _DATE_NAME_RE.search(col_name) is not None:
        return True
    if pd.api.types.is_datetime64_any_dtype(series):
        return True

    # Attempt parse on a bounded, deterministic sample.
    s = series.dropna()
    if s.empty:
        return False
    # Cast to string for parsing (deterministic head).
    sample = s.astype(str).head(200)
    parsed = pd.to_datetime(sample, errors="coerce")
    success = int(parsed.notna().sum())
    rate = success / float(sample.shape[0]) if sample.shape[0] > 0 else 0.0
    return rate >= 0.9


def _correlation_flags(df: pd.DataFrame, *, threshold: float) -> list[dict[str, Any]]:
    num_df = df.select_dtypes(include=["number"]).copy()
    if num_df.shape[1] < 2:
        return []

    corr = num_df.corr(method="pearson")
    cols = list(corr.columns)

    flags: list[dict[str, Any]] = []
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            a, b = cols[i], cols[j]
            r = corr.loc[a, b]
            if pd.isna(r):
                continue
            rv = float(r)
            if abs(rv) >= threshold:
                flags.append({"a": str(a), "b": str(b), "r": _round(rv, 4)})

    # Deterministic ordering: strongest first, then name tie-breakers.
    flags.sort(key=lambda x: (-abs(float(x["r"])), str(x["a"]), str(x["b"])))
    return flags


def _round(x: float, ndigits: int) -> float:
    return float(round(x, ndigits))


def _write_error_json(path: Path, error: str) -> None:
    payload = {
        "_schema": "analyst_agent.data_profile.v1",
        "_status": "error",
        "error": error,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _record_summary_log(analysis_log_path: Optional[Path], payload: dict[str, Any]) -> None:
    """Best-effort merge into analysis_log.json without failing the run."""

    if analysis_log_path is None:
        return

    try:
        if analysis_log_path.exists():
            existing = read_json(analysis_log_path)
            if isinstance(existing, dict):
                existing.update(payload)
                write_json(analysis_log_path, existing)
                return
        write_json(analysis_log_path, payload)
    except Exception:
        return
