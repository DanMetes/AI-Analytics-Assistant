from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AskResult:
    mode: str  # "answer"|"plan"
    answer_md: str
    evidence_refs: list[str]
    plan_path: Path | None = None
    code_path: Path | None = None


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _safe_load_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _read_metrics(metrics_csv: Path) -> list[dict[str, str]]:
    import csv

    if not metrics_csv.exists():
        return []
    rows: list[dict[str, str]] = []
    with metrics_csv.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            if not r:
                continue
            rows.append({k: "" if v is None else str(v) for k, v in r.items()})
    return rows


def _tokenize(text: str) -> set[str]:
    # Treat underscores as separators so "row_count" matches "row count".
    t = text.lower().replace("_", " ")
    t = re.sub(r"[^a-z0-9]+", " ", t)
    toks = {x for x in t.split() if len(x) >= 3}
    return toks


def _score_overlap(q: set[str], doc: set[str]) -> float:
    if not q or not doc:
        return 0.0
    inter = len(q & doc)
    return inter / max(1, len(q))


def _best_evidence_for_question(
    *,
    question: str,
    metrics: list[dict[str, str]],
    anomalies: list[dict[str, Any]],
    data_profile: dict[str, Any],
    k: int = 6,
) -> tuple[list[str], list[str]]:
    """Return (snippets, evidence_refs) based on simple lexical retrieval.

    This is deterministic and dependency-free.
    """

    q = _tokenize(question)
    candidates: list[tuple[float, str, str]] = []

    # Metrics
    for r in metrics:
        key = f"{r.get('section','')}:{r.get('key','')}"
        text = f"{r.get('section','')} {r.get('key','')} {r.get('value','')}"
        s = _score_overlap(q, _tokenize(text))
        if s > 0:
            candidates.append((s, f"metric:{key}", f"{key} = {r.get('value','')}"))

    # Anomalies
    for a in anomalies:
        if not isinstance(a, dict):
            continue
        aid = str(a.get("id") or "")
        summ = str(a.get("summary") or "")
        text = f"{aid} {summ} {a.get('metric','')} {a.get('severity','')}"
        s = _score_overlap(q, _tokenize(text))
        if s > 0:
            candidates.append((s, f"anomaly:{aid}", summ or aid))

    # Profile high-level
    # Only include safe summary fields.
    for fld in ("row_count", "column_count", "time_candidates"):
        if fld in data_profile:
            text = f"{fld} {data_profile.get(fld)}"
            s = _score_overlap(q, _tokenize(text))
            if s > 0:
                candidates.append((s, f"data_profile:{fld}", f"{fld}: {data_profile.get(fld)}"))

    candidates = sorted(candidates, key=lambda x: (-x[0], x[1]))
    top = candidates[:k]
    snippets = [t[2] for t in top]
    refs = [t[1] for t in top]
    return snippets, refs


def _try_openai_answer(question: str, context: dict[str, Any]) -> dict[str, Any] | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI  # type: ignore

        client = OpenAI(api_key=api_key)
        model = os.getenv("ANALYST_AGENT_LLM_MODEL", "gpt-4o-mini")
        prompt_obj = {
            "question": question,
            "context": context,
            "instructions": [
                "Answer only using the provided context.",
                "If context is insufficient, return mode='plan' with a methodology and code skeleton.",
                "Return ONLY JSON with keys: mode, answer_md, evidence_refs, methodology_plan, python_code.",
            ],
        }
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a cautious data analysis assistant. "
                        "Do not invent facts. "
                        "If you cannot answer, propose a plan and code."),
                },
                {"role": "user", "content": json.dumps(prompt_obj, indent=2)},
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        obj = json.loads(resp.choices[0].message.content or "{}")
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def answer_or_plan(
    *,
    run_dir: Path,
    question: str,
    use_llm: bool = True,
) -> AskResult:
    """Answer a user question from existing artifacts.

    If insufficient information is available, emit a methodology plan and a
    Python script scaffold under run_dir/ask/.
    """

    metrics = _read_metrics(run_dir / "metrics.csv")
    anomalies_obj = _safe_load_json(run_dir / "anomalies_normalized.json")
    anomalies = anomalies_obj.get("anomalies") if isinstance(anomalies_obj.get("anomalies"), list) else []
    profile = _safe_load_json(run_dir / "data_profile.json")

    snippets, refs = _best_evidence_for_question(
        question=question,
        metrics=metrics,
        anomalies=[a for a in anomalies if isinstance(a, dict)],
        data_profile=profile,
    )

    context = {
        "evidence_snippets": snippets,
        "evidence_refs": refs,
        "data_profile": {k: profile.get(k) for k in ("row_count", "column_count", "time_candidates") if k in profile},
    }

    # Use LLM if configured and requested.
    if use_llm:
        obj = _try_openai_answer(question, context)
        if isinstance(obj, dict) and obj.get("mode") in ("answer", "plan"):
            mode = str(obj.get("mode"))
            ans = str(obj.get("answer_md") or "").strip()
            ev = obj.get("evidence_refs")
            ev_list = [str(x) for x in ev] if isinstance(ev, list) else refs

            if mode == "answer" and ans:
                return AskResult(mode="answer", answer_md=ans, evidence_refs=ev_list)

            # Plan mode: write plan + code if provided.
            plan_text = obj.get("methodology_plan")
            code_text = obj.get("python_code")
            return _write_plan_and_code(run_dir=run_dir, question=question, evidence_refs=ev_list, plan_text=plan_text, code_text=code_text)

    # Deterministic decision: if overlap is low, go to plan.
    overlap_score = _score_overlap(_tokenize(question), _tokenize(" ".join(snippets)))
    if overlap_score >= 0.35 and snippets:
        # Answer mode: conservative, evidence-first.
        md = "\n".join(
            [
                f"### Answer (artifact-backed)\n",
                f"Based on the computed artifacts, the most relevant available evidence is:\n",
            ]
            + [f"- {s}" for s in snippets]
            + ["\nIf you need a deeper breakdown, ask a follow-up (e.g., 'by region' or 'top contributors')."]
        )
        return AskResult(mode="answer", answer_md=md.strip() + "\n", evidence_refs=refs)

    return _write_plan_and_code(run_dir=run_dir, question=question, evidence_refs=refs, plan_text=None, code_text=None)


def _write_plan_and_code(
    *,
    run_dir: Path,
    question: str,
    evidence_refs: list[str],
    plan_text: Any,
    code_text: Any,
) -> AskResult:
    ask_dir = run_dir / "ask"
    ask_dir.mkdir(parents=True, exist_ok=True)
    stamp = _utc_stamp()
    plan_path = ask_dir / f"ask_{stamp}_plan.json"
    code_path = ask_dir / f"ask_{stamp}.py"

    # Deterministic default plan
    plan_obj = {
        "_status": "ok",
        "question": question,
        "reason": "Insufficient information in current artifacts to answer confidently.",
        "evidence_refs_considered": evidence_refs,
        "methodology": [
            "Identify the target metric(s) and any grouping/time axis implied by the question.",
            "Compute aggregates (count, sum, mean, percentiles) overall and within relevant groups.",
            "If anomalies are suspected, compute top-k contribution to determine whether results are leverage-driven.",
            "Write derived metrics and a short markdown summary to new artifacts for re-ingestion or review.",
        ],
        "outputs": [
            "derived_metrics.csv",
            "derived_summary.md",
        ],
    }

    if isinstance(plan_text, (str, list, dict)) and plan_text:
        plan_obj["llm_methodology_plan"] = plan_text

    plan_path.write_text(json.dumps(plan_obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # Deterministic default code scaffold
    template = '''\
"""Ad-hoc analysis scaffold generated by Analyst Agent.

Question:
    __QUESTION__

Expected inputs (in the current working directory):
    - metrics.csv (optional)
    - data_profile.json (optional)
    - anomalies_normalized.json (optional)
    - A dataset CSV path you provide when running this script

Outputs:
    - derived_metrics.csv
    - derived_summary.md

Notes:
    - This scaffold is domain-agnostic and safe by default.
    - Modify TARGET_METRICS / GROUP_BY / TIME_COL based on your dataset.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


DATA_PATH = Path("YOUR_DATA.csv")  # <-- set this
TIME_COL = None  # e.g. "order_date" or "year"
GROUP_BY = []    # e.g. ["region", "category"]
TARGET_METRICS = []  # e.g. ["units", "sales", "profit"]


def main() -> None:
    df = pd.read_csv(DATA_PATH)
    cols = set(df.columns)

    # Basic sanity
    summary_lines = []
    summary_lines.append(f"Rows: {len(df)}; Cols: {df.shape[1]}")

    # Choose numeric targets if none provided
    if not TARGET_METRICS:
        numeric = df.select_dtypes(include=["number"]).columns.tolist()
        TARGET_METRICS.extend(numeric[:3])
        summary_lines.append(f"Auto-selected TARGET_METRICS={TARGET_METRICS}")

    use_group = [g for g in GROUP_BY if g in cols]
    use_targets = [m for m in TARGET_METRICS if m in cols]

    if not use_targets:
        raise SystemExit("No numeric target metrics found. Set TARGET_METRICS to numeric columns.")

    agg = {m: ["count", "sum", "mean", "min", "max"] for m in use_targets}
    if use_group:
        out = df.groupby(use_group).agg(agg)
    else:
        out = df.agg(agg)

    # Flatten columns
    if isinstance(out, pd.DataFrame):
        out.columns = ["_".join([str(x) for x in col if x]) for col in out.columns.to_flat_index()]
        out = out.reset_index()
    else:
        out = pd.DataFrame(out)

    out.to_csv("derived_metrics.csv", index=False)

    Path("derived_summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
'''

    # Inject only the question; avoid f-string evaluation of braces inside the scaffold.
    default_code = template.replace("__QUESTION__", question)

    content = str(code_text).strip() if isinstance(code_text, str) and code_text.strip() else default_code
    code_path.write_text(content + "\n", encoding="utf-8")

    answer_md = (
        "### Not enough information in current artifacts\n\n"
        "I cannot answer this reliably using only the existing computed artifacts. "
        "I generated a methodology plan and a Python scaffold you can run to produce the missing derived analysis.\n\n"
        f"- Plan: {plan_path.name}\n"
        f"- Code: {code_path.name}\n"
    )

    return AskResult(mode="plan", answer_md=answer_md, evidence_refs=evidence_refs, plan_path=plan_path, code_path=code_path)


def latest_run_dir_for_project(project_id: str, *, project_root: Path | None = None) -> Path:
    root = project_root or Path(".")
    runs = root / "projects" / project_id / "runs"
    if not runs.exists():
        raise FileNotFoundError(f"No runs directory found for project_id={project_id}")
    candidates = [p for p in runs.iterdir() if p.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No runs found for project_id={project_id}")
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]
