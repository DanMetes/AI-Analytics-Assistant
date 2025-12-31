from __future__ import annotations

import html
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from ..paths import dataset_dir
from ..utils import read_json, write_json
from ..pipeline.context import RunContext
from .fallback_eda import generate_fallback_eda_html


@dataclass(frozen=True)
class ProfileOutcome:
    """Result of the EDA profiling step."""

    ok: bool
    rows_loaded: int
    rows_profiled: int
    sampled: bool
    error: Optional[str] = None


def profile_dataset_to_html(
    *,
    ctx: RunContext,
    project_id: str,
    dataset_id: str,
    analysis_log_path: Optional[Path] = None,
) -> ProfileOutcome:
    """Generate eda_report.html for the dataset.

    Batch F1 requirements:
    - Prefer ydata-profiling
    - If dataset is large, deterministically sample
    - If profiling fails (including missing dependency), do not fail the run
    - Always write eda_report.html (real report or error banner)
    - Record sampling / failure in analysis_log.json or ingest_meta.json

    Notes:
    - Raw CSVs are not stored in the project folder. We locate the original
      source path via projects/<project_id>/datasets/<dataset_id>/fingerprint.json.
    """

    out_html = ctx.eda_report_path()
    out_html.parent.mkdir(parents=True, exist_ok=True)

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
        _write_error_html(out_html, msg)
        _record_profile_log(
            analysis_log_path,
            {
                "profile_eda": {
                    "ok": False,
                    "error": msg,
                    "source_csv": str(source_csv) if source_csv else None,
                }
            },
        )
        return ProfileOutcome(ok=False, rows_loaded=0, rows_profiled=0, sampled=False, error=msg)

    # Load dataset.
    df = pd.read_csv(source_csv)
    rows_loaded = int(df.shape[0])

    max_rows = _get_eda_max_rows()
    sampled = False
    rows_profiled = rows_loaded

    if rows_loaded > max_rows:
        # Deterministic sampling.
        df = df.sample(n=max_rows, random_state=42)
        sampled = True
        rows_profiled = int(df.shape[0])

    try:
        # Lazy import: profiling dependency is allowed to be missing without
        # breaking the run.
        from ydata_profiling import ProfileReport  # type: ignore

        report = ProfileReport(
            df,
            title="Analyst Agent — EDA Profile",
            minimal=True,
            progress_bar=False,
        )
        report.to_file(str(out_html))

        _record_profile_log(
            analysis_log_path,
            {
                "profile_eda": {
                    "ok": True,
                    "source_csv": str(source_csv),
                    "rows_loaded": rows_loaded,
                    "rows_profiled": rows_profiled,
                    "sampled": sampled,
                    "eda_max_rows": max_rows,
                }
            },
        )
        return ProfileOutcome(
            ok=True,
            rows_loaded=rows_loaded,
            rows_profiled=rows_profiled,
            sampled=sampled,
        )
    except Exception as e:  # noqa: BLE001
        # Fallback EDA: generate a deterministic, information-dense HTML report
        # even when ydata-profiling is unavailable or fails at runtime.
        err = f"{type(e).__name__}: {e}"
        generate_fallback_eda_html(
            df=df,
            out_path=out_html,
            note=(
                "Full ydata-profiling report unavailable; generated fallback EDA instead. "
                f"Reason: {err}"
            ),
        )
        _record_profile_log(
            analysis_log_path,
            {
                "profile_eda": {
                    "ok": True,
                    "method": "fallback",
                    "fallback_reason": err,
                    "source_csv": str(source_csv),
                    "rows_loaded": rows_loaded,
                    "rows_profiled": rows_profiled,
                    "sampled": sampled,
                    "eda_max_rows": max_rows,
                }
            },
        )
        return ProfileOutcome(
            ok=True,
            rows_loaded=rows_loaded,
            rows_profiled=rows_profiled,
            sampled=sampled,
            error=err,
        )


def _get_eda_max_rows(default: int = 100_000) -> int:
    raw = os.environ.get("EDA_MAX_ROWS")
    if raw is None or raw.strip() == "":
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except ValueError:
        return default


def _write_error_html(path: Path, message: str) -> None:
    escaped = html.escape(message)
    content = (
        "<!doctype html>\n"
        "<html><head><meta charset='utf-8'><title>EDA Report Error</title></head>\n"
        "<body style='font-family: sans-serif;'>\n"
        "<h1>EDA report generation failed</h1>\n"
        "<p>This run continued, but the automated EDA report could not be generated.</p>\n"
        f"<pre style='white-space: pre-wrap; background:#f6f8fa; padding:12px; border-radius:6px;'>{escaped}</pre>\n"
        "</body></html>\n"
    )
    path.write_text(content, encoding="utf-8")


def _record_profile_log(analysis_log_path: Optional[Path], payload: dict[str, Any]) -> None:
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
        # If missing or not a dict, just write the payload.
        write_json(analysis_log_path, payload)
    except Exception:
        # Never fail the run due to logging.
        return
