from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class LlmInputs:
    """Safe, non-row-level inputs for optional synthesis."""

    data_profile: dict[str, Any]
    analysis_plan: dict[str, Any]
    metrics_compact: list[dict[str, str]]
    anomalies_normalized: dict[str, Any]
    plot_captions: list[str]


def _safe_load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _read_metrics_compact(metrics_csv: Path, *, max_rows: int = 30) -> list[dict[str, str]]:
    if not metrics_csv.exists():
        return []
    rows: list[dict[str, str]] = []
    try:
        with metrics_csv.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if not r:
                    continue
                rows.append({k: "" if v is None else str(v) for k, v in r.items()})
    except Exception:
        return []
    # Deterministic ordering
    rows = sorted(rows, key=lambda r: (r.get("section", ""), r.get("key", ""), r.get("value", "")))
    return rows[:max_rows]


def _plot_captions_from_dir(plots_dir: Path, plan: dict[str, Any], *, max_rows: int = 25) -> list[str]:
    if not plots_dir.exists() or not plots_dir.is_dir():
        return []
    pngs = sorted([p for p in plots_dir.glob("*.png") if p.is_file()], key=lambda p: p.name)
    if not pngs:
        return []

    # Deterministic captions based on filename and plan step hints.
    steps = plan.get("steps") if isinstance(plan.get("steps"), list) else []
    step_by_id = {}
    for s in steps:
        if isinstance(s, dict) and s.get("id"):
            step_by_id[str(s.get("id"))] = s

    captions: list[str] = []
    for p in pngs[:max_rows]:
        base = p.stem
        cap = f"plots/{p.name}"
        # If we encoded step id in filename (common), attach a small hint.
        for sid, s in step_by_id.items():
            if sid in base:
                t = str(s.get("type", "analysis"))
                metric = str(s.get("metric", ""))
                extras: list[str] = []
                if s.get("time_axis"):
                    extras.append(f"time_axis={s.get('time_axis')}")
                if s.get("entity"):
                    extras.append(f"entity={s.get('entity')}")
                if s.get("by"):
                    extras.append(f"by={s.get('by')}")
                tail = ", ".join([x for x in [f"type={t}", f"metric={metric}" if metric else ""] if x] + extras)
                cap = f"plots/{p.name} — {tail}" if tail else cap
                break
        captions.append(f"- {cap}")
    if len(pngs) > max_rows:
        captions.append(f"- … ({len(pngs) - max_rows} more)")
    return captions


def build_llm_inputs(*, data_profile_path: Path, plan_path: Path, metrics_csv: Path, anomalies_path: Path, plots_dir: Path) -> LlmInputs:
    """Collect the only allowed inputs for optional synthesis."""

    profile = _safe_load_json(data_profile_path)
    plan = _safe_load_json(plan_path)
    anomalies = _safe_load_json(anomalies_path)
    metrics = _read_metrics_compact(metrics_csv)
    captions = _plot_captions_from_dir(plots_dir, plan)
    return LlmInputs(
        data_profile=profile,
        analysis_plan=plan,
        metrics_compact=metrics,
        anomalies_normalized=anomalies,
        plot_captions=captions,
    )


def _render_fallback(inputs: LlmInputs) -> str:
    """Deterministic, non-LLM fallback narrative.

    This exists so that `--llm` always produces a labeled section even when
    an LLM provider is not configured.
    """

    row_count = inputs.data_profile.get("row_count")
    col_count = inputs.data_profile.get("column_count")
    time_candidates = inputs.data_profile.get("time_candidates")
    steps = inputs.analysis_plan.get("steps") if isinstance(inputs.analysis_plan.get("steps"), list) else []

    lines: list[str] = []
    lines.append("### Key takeaways (deterministic fallback)\n")
    if isinstance(row_count, int) and isinstance(col_count, int):
        lines.append(f"- Dataset shape: {row_count} rows × {col_count} columns.\n")
    if isinstance(time_candidates, list) and time_candidates:
        lines.append(f"- Time candidates detected: {', '.join(sorted([str(x) for x in time_candidates])[:5])}.\n")
    if isinstance(steps, list) and steps:
        lines.append(f"- Planned analyses executed: {len(steps)} step(s) per analysis_plan.json.\n")
    if inputs.metrics_compact:
        lines.append("- A compact metrics summary is available (see Key Metrics section above).\n")

    lines.append("\n### Suggested next analyses (heuristic)\n")
    lines.append("- Validate whether anomalies persist after stratifying by region/segment/category (if present).\n")
    lines.append("- If time is meaningful, test seasonality and structural breaks around large outliers.\n")
    lines.append("- Consider robustness checks: winsorization vs. log transforms for heavy-tailed metrics.\n")

    lines.append("\n### Proposed Python snippets (illustrative; uses computed artifacts only)\n")
    lines.append("```python\n")
    lines.append("import json\nimport pandas as pd\n\n")
    lines.append("profile = json.load(open('data_profile.json'))\n")
    lines.append("metrics = pd.read_csv('metrics.csv')\n")
    lines.append("print(profile.get('row_count'), profile.get('column_count'))\n")
    lines.append("print(metrics.head(10))\n")
    lines.append("```\n")

    lines.append("\n### Caveats\n")
    lines.append("- This section is generated without access to raw rows; it is limited to computed artifacts.\n")
    lines.append("- Enable an LLM provider (e.g., OpenAI) to replace this fallback with true model-driven synthesis.\n")
    return "".join(lines)


def _try_openai_chat(prompt: str) -> str | None:
    """Attempt OpenAI chat completion if configured.

    This is strictly best-effort and must not fail the run.
    """

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        # Lazy import so tests and offline installs remain unaffected.
        from openai import OpenAI  # type: ignore

        client = OpenAI(api_key=api_key)
        model = os.getenv("ANALYST_AGENT_LLM_MODEL", "gpt-4o-mini")
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a cautious data analysis assistant. Never invent facts."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        text = resp.choices[0].message.content or ""
        return text.strip()
    except Exception:
        return None


def append_llm_interpretation(*, report_md_path: Path, inputs: LlmInputs) -> None:
    """Append the optional LLM interpretation section to report.md.

    Must be safe and must not fail the run.
    """

    # Build a compact, non-row-level prompt.
    prompt_obj = {
        "data_profile": inputs.data_profile,
        "analysis_plan": inputs.analysis_plan,
        "metrics_compact": inputs.metrics_compact,
        "anomalies_normalized": inputs.anomalies_normalized,
        "plot_captions": inputs.plot_captions,
        "constraints": [
            "Do not assume access to raw data rows.",
            "Do not invent metric values.",
            "Be explicit about uncertainty.",
        ],
        "requested_output": {
            "section_title": "LLM Interpretation (Optional)",
            "bullets": ["key takeaways", "suggested next analyses"],
            "include": ["proposed Python snippets", "explicit caveats"],
        },
    }
    prompt = "You will be given computed analysis artifacts as JSON. Produce a concise interpretation." + "\n\n" + json.dumps(
        prompt_obj, indent=2, sort_keys=True
    )

    llm_text = _try_openai_chat(prompt)
    body = llm_text if llm_text else _render_fallback(inputs)

    section = "\n\n## LLM Interpretation (Optional)\n\n" + body.strip() + "\n"
    try:
        existing = report_md_path.read_text(encoding="utf-8") if report_md_path.exists() else ""
        # Avoid duplicating the section on reruns.
        if "## LLM Interpretation (Optional)" in existing:
            return
        report_md_path.write_text(existing.rstrip() + section, encoding="utf-8")
    except Exception:
        # Best-effort only.
        return
