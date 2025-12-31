from __future__ import annotations

import base64
import html
import io
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import matplotlib.pyplot as plt
import pandas as pd


@dataclass(frozen=True)
class FallbackEdaConfig:
    """Configuration for deterministic fallback EDA generation."""

    max_categories: int = 10
    max_numeric_plots: int = 20
    max_categorical_plots: int = 20
    max_datetime_plots: int = 10
    max_examples: int = 3
    corr_threshold: float = 0.7


def generate_fallback_eda_html(
    *,
    df: pd.DataFrame,
    out_path: Path,
    title: str = "Analyst Agent — EDA (Fallback)",
    note: str | None = None,
    config: FallbackEdaConfig | None = None,
) -> None:
    """Generate a deterministic single-file HTML EDA report.

    This is used when ydata-profiling is unavailable (e.g., unsupported Python
    versions). The report is intended to be information-dense and useful while
    remaining lightweight and deterministic.
    """

    cfg = config or FallbackEdaConfig()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Deterministic column ordering.
    cols = list(df.columns)

    # Infer basic column types.
    numeric_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    datetime_cols: list[str] = []
    categorical_cols: list[str] = []
    other_cols: list[str] = []

    for c in cols:
        if c in numeric_cols:
            continue
        s = df[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            datetime_cols.append(c)
        elif pd.api.types.is_bool_dtype(s):
            categorical_cols.append(c)
        elif pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s) or pd.api.types.is_categorical_dtype(s):
            categorical_cols.append(c)
        else:
            other_cols.append(c)

    # Attempt to identify datetime-like object columns deterministically.
    for c in list(categorical_cols):
        s = df[c]
        if s.dropna().empty:
            continue
        sample = s.dropna().astype(str).head(50)
        # Pandas can emit noisy warnings when it cannot infer a date format.
        # For fallback typing heuristics, we intentionally treat this as
        # best-effort and silence the warning to keep CLI output clean.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            parsed = pd.to_datetime(sample, errors="coerce", utc=False)
        ok_rate = float(parsed.notna().mean())
        if ok_rate >= 0.9:
            datetime_cols.append(c)
            categorical_cols.remove(c)

    html_parts: list[str] = []
    html_parts.append("<!doctype html>")
    html_parts.append("<html><head><meta charset='utf-8'>")
    html_parts.append(f"<title>{html.escape(title)}</title>")
    html_parts.append(
        "<style>"
        "body{font-family:system-ui,Segoe UI,Arial,sans-serif;margin:24px;line-height:1.35;}"
        "h1,h2{margin:0.6em 0 0.2em 0;}"
        "table{border-collapse:collapse;margin:12px 0;width:100%;}"
        "th,td{border:1px solid #ddd;padding:6px 8px;font-size:13px;vertical-align:top;}"
        "th{background:#f6f8fa;text-align:left;}"
        "code,pre{background:#f6f8fa;padding:2px 4px;border-radius:4px;}"
        ".note{background:#fff6d6;border:1px solid #f3d27a;padding:10px 12px;border-radius:8px;}"
        ".img{max-width:100%;height:auto;border:1px solid #eee;border-radius:6px;}"
        "</style>"
    )
    html_parts.append("</head><body>")
    html_parts.append(f"<h1>{html.escape(title)}</h1>")

    if note:
        html_parts.append(f"<div class='note'><b>Note:</b> {html.escape(note)}</div>")

    # Dataset overview
    html_parts.append("<h2>Dataset Overview</h2>")
    html_parts.append("<ul>")
    html_parts.append(f"<li><b>Rows:</b> {len(df):,}</li>")
    html_parts.append(f"<li><b>Columns:</b> {df.shape[1]:,}</li>")
    mem = int(df.memory_usage(deep=True).sum())
    html_parts.append(f"<li><b>Approx. memory:</b> {mem:,} bytes</li>")
    html_parts.append("</ul>")

    # Column summary table
    html_parts.append("<h2>Column Summary</h2>")
    html_parts.append("<table><thead><tr>")
    html_parts.append("<th>Column</th><th>Inferred Type</th><th>% Missing</th><th>Cardinality</th><th>Examples</th>")
    html_parts.append("</tr></thead><tbody>")
    for c in cols:
        s = df[c]
        inferred = (
            "numeric" if c in numeric_cols else "datetime" if c in datetime_cols else "categorical" if c in categorical_cols else "other"
        )
        miss = float(s.isna().mean() * 100.0)
        card = int(s.nunique(dropna=True))
        examples = _examples(s, cfg.max_examples)
        html_parts.append(
            "<tr>"
            f"<td><code>{html.escape(str(c))}</code></td>"
            f"<td>{html.escape(inferred)}</td>"
            f"<td>{miss:.2f}%</td>"
            f"<td>{card:,}</td>"
            f"<td>{html.escape(examples)}</td>"
            "</tr>"
        )
    html_parts.append("</tbody></table>")

    # Missingness overview
    html_parts.append("<h2>Missingness Overview</h2>")
    miss_counts = df.isna().sum().sort_values(ascending=False)
    miss_tbl = pd.DataFrame({"missing": miss_counts, "pct_missing": (miss_counts / max(len(df), 1)) * 100.0})
    html_parts.append(_dataframe_table(miss_tbl.head(100)))
    miss_plot = _plot_bar(
        x=miss_tbl.index.tolist()[: min(len(miss_tbl), 30)],
        y=miss_tbl["pct_missing"].tolist()[: min(len(miss_tbl), 30)],
        xlabel="Column",
        ylabel="% missing",
        title="Top Missingness (%), first 30 columns",
        rotate_xticks=True,
    )
    html_parts.append(_img_tag(miss_plot))

    # Numeric section
    html_parts.append("<h2>Numeric Columns</h2>")
    if not numeric_cols:
        html_parts.append("<p>No numeric columns detected.</p>")
    else:
        for c in numeric_cols[: cfg.max_numeric_plots]:
            s = pd.to_numeric(df[c], errors="coerce")
            html_parts.append(f"<h3><code>{html.escape(str(c))}</code></h3>")
            desc = s.describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]).to_frame().T
            html_parts.append(_dataframe_table(desc))
            hist = _plot_hist(s.dropna(), title=f"Histogram — {c}", xlabel=str(c))
            html_parts.append(_img_tag(hist))

    # Categorical section
    html_parts.append("<h2>Categorical Columns</h2>")
    if not categorical_cols:
        html_parts.append("<p>No categorical columns detected.</p>")
    else:
        for c in categorical_cols[: cfg.max_categorical_plots]:
            s = df[c]
            html_parts.append(f"<h3><code>{html.escape(str(c))}</code></h3>")
            vc = s.astype(str).fillna("<NA>").value_counts().head(cfg.max_categories)
            tbl = pd.DataFrame({"value": vc.index, "count": vc.values})
            tbl["pct"] = (tbl["count"] / max(len(df), 1)) * 100.0
            html_parts.append(_dataframe_table(tbl))

    # Datetime section
    html_parts.append("<h2>Datetime Columns</h2>")
    if not datetime_cols:
        html_parts.append("<p>No datetime columns detected.</p>")
    else:
        for c in datetime_cols[: cfg.max_datetime_plots]:
            raw = df[c]
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                parsed = pd.to_datetime(raw, errors="coerce")
            ok_rate = float(parsed.notna().mean() * 100.0)
            html_parts.append(f"<h3><code>{html.escape(str(c))}</code></h3>")
            html_parts.append(f"<p><b>Parse success:</b> {ok_rate:.2f}%</p>")
            if parsed.notna().any():
                html_parts.append(
                    f"<ul><li><b>Min:</b> {html.escape(str(parsed.min()))}</li><li><b>Max:</b> {html.escape(str(parsed.max()))}</li></ul>"
                )
                # Count by year as a stable overview.
                years = parsed.dropna().dt.year
                vc = years.value_counts().sort_index()
                yr_plot = _plot_bar(
                    x=[str(x) for x in vc.index.tolist()[:50]],
                    y=vc.values.tolist()[:50],
                    xlabel="Year",
                    ylabel="Count",
                    title=f"Counts by Year — {c}",
                    rotate_xticks=True,
                )
                html_parts.append(_img_tag(yr_plot))
            else:
                html_parts.append("<p>No parseable datetime values.</p>")

    # Correlations
    html_parts.append("<h2>Correlation Overview</h2>")
    if len(numeric_cols) < 2:
        html_parts.append("<p>Not enough numeric columns for correlations.</p>")
    else:
        num_df = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        corr = num_df.corr(numeric_only=True)
        html_parts.append(_dataframe_table(corr.round(4).head(50)))
        heat = _plot_corr_heatmap(corr, title="Numeric Correlation Heatmap")
        html_parts.append(_img_tag(heat))
        strong = _strong_correlations(corr, threshold=cfg.corr_threshold)
        if strong:
            html_parts.append(f"<p><b>Strong correlations (|r| ≥ {cfg.corr_threshold:.2f}):</b></p>")
            html_parts.append("<ul>")
            for a, b, r in strong[:50]:
                html_parts.append(f"<li><code>{html.escape(a)}</code> vs <code>{html.escape(b)}</code>: {r:.3f}</li>")
            html_parts.append("</ul>")
        else:
            html_parts.append(f"<p>No correlations above |r| ≥ {cfg.corr_threshold:.2f} detected.</p>")

    # Limitations
    html_parts.append("<h2>Limitations</h2>")
    html_parts.append(
        "<ul>"
        "<li>This report was generated by the built-in fallback EDA generator because the full ydata-profiling report was unavailable.</li>"
        "<li>The fallback is deterministic and focuses on core summaries (missingness, distributions, categorical counts, and correlations).</li>"
        "<li>Advanced checks (duplicates, interactions, detailed warnings) are not included.</li>"
        "</ul>"
    )

    html_parts.append("</body></html>")

    out_path.write_text("\n".join(html_parts), encoding="utf-8")


def _examples(series: pd.Series, n: int) -> str:
    vals = series.dropna().astype(str).head(n).tolist()
    return ", ".join(vals) if vals else ""


def _dataframe_table(df: pd.DataFrame) -> str:
    # Deterministic HTML table rendering.
    buf: list[str] = []
    buf.append("<table><thead><tr>")
    for c in df.columns:
        buf.append(f"<th>{html.escape(str(c))}</th>")
    buf.append("</tr></thead><tbody>")
    for _, row in df.iterrows():
        buf.append("<tr>")
        for c in df.columns:
            v = row[c]
            buf.append(f"<td>{html.escape(str(v))}</td>")
        buf.append("</tr>")
    buf.append("</tbody></table>")
    return "".join(buf)


def _fig_to_base64_png() -> str:
    bio = io.BytesIO()
    plt.tight_layout()
    plt.savefig(bio, format="png", dpi=120)
    plt.close()
    return base64.b64encode(bio.getvalue()).decode("ascii")


def _img_tag(b64: str) -> str:
    return f"<p><img class='img' src='data:image/png;base64,{b64}' /></p>"


def _plot_hist(series: pd.Series, *, title: str, xlabel: str) -> str:
    plt.figure()
    plt.hist(series, bins=30)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("Count")
    return _fig_to_base64_png()


def _plot_bar(
    *,
    x: list[str],
    y: list[float],
    xlabel: str,
    ylabel: str,
    title: str,
    rotate_xticks: bool = False,
) -> str:
    plt.figure(figsize=(max(6, min(14, 0.3 * max(len(x), 1))), 4))
    plt.bar(range(len(x)), y)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(range(len(x)), x, rotation=60 if rotate_xticks else 0, ha="right" if rotate_xticks else "center")
    return _fig_to_base64_png()


def _plot_corr_heatmap(corr: pd.DataFrame, *, title: str) -> str:
    plt.figure(figsize=(8, 6))
    plt.imshow(corr.values, aspect="auto")
    plt.title(title)
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=90)
    plt.yticks(range(len(corr.index)), corr.index)
    plt.colorbar()
    return _fig_to_base64_png()


def _strong_correlations(corr: pd.DataFrame, *, threshold: float) -> list[tuple[str, str, float]]:
    out: list[tuple[str, str, float]] = []
    cols = list(corr.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            r = corr.iloc[i, j]
            if pd.isna(r):
                continue
            if abs(float(r)) >= threshold:
                out.append((str(cols[i]), str(cols[j]), float(r)))
    # Deterministic ordering: strongest first, then name.
    out.sort(key=lambda t: (-abs(t[2]), t[0], t[1]))
    return out
