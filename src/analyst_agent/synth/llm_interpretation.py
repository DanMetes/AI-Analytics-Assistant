from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class EvidenceGatedInterpretation:
    """Structured LLM interpretation output.

    This is a *presentation* layer only. It must never be treated as a source of
    computed facts. Every claim must reference evidence present in deterministic
    run artifacts.
    """

    claims: list[dict[str, Any]]
    supporting_evidence: list[str]
    negative_evidence: list[str]
    open_questions: list[str]
    recommended_next_analyses: list[str]
    generated_by: str  # "openai" or "fallback"
    cache_key: str


def _sha256_files(paths: Iterable[Path]) -> str:
    h = hashlib.sha256()
    for p in sorted([Path(x) for x in paths], key=lambda x: x.name):
        if not p.exists() or not p.is_file():
            continue
        h.update(p.name.encode("utf-8"))
        h.update(b"\0")
        h.update(p.read_bytes())
        h.update(b"\0")
    return h.hexdigest()


def _safe_load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _fallback_structured(*, data_profile: dict[str, Any], anomalies: dict[str, Any], metrics_compact: list[dict[str, str]], cache_key: str) -> EvidenceGatedInterpretation:
    """Deterministic fallback that still follows the evidence-gated contract."""

    claims: list[dict[str, Any]] = []
    supporting: list[str] = []
    negative: list[str] = []
    open_q: list[str] = []
    next_analyses: list[str] = []

    anoms = anomalies.get("anomalies") if isinstance(anomalies.get("anomalies"), list) else []
    if anoms:
        # Sort deterministically (severity desc, then id)
        sev_rank = {"critical": 3, "warning": 2, "info": 1}
        anoms_sorted = sorted(
            [a for a in anoms if isinstance(a, dict)],
            key=lambda a: (-sev_rank.get(str(a.get("severity", "info")).lower(), 0), str(a.get("id", ""))),
        )
        top = anoms_sorted[:5]
        for a in top:
            claims.append(
                {
                    "text": str(a.get("summary") or "").strip() or f"Anomaly detected: {a.get('id')}",
                    "confidence": "high",
                    "evidence_refs": [f"anomalies_normalized:{a.get('id')}"]
                    + [f"evidence_key:{k}" for k in (a.get("evidence_keys") or []) if isinstance(k, str)],
                }
            )
        supporting.append("Anomalies were generated deterministically from policy thresholds and written to anomalies_normalized.json.")
    else:
        claims.append(
            {
                "text": "No anomalies were detected under current policy thresholds.",
                "confidence": "high",
                "evidence_refs": ["anomalies_normalized:empty"],
            }
        )

    # Distribution framing if present
    cols = data_profile.get("columns")
    if isinstance(cols, list):
        # Add one generic skew observation if any skew_flag present
        skewed = [c for c in cols if isinstance(c, dict) and c.get("skew_flag")]
        if skewed:
            names = ", ".join(sorted({str(c.get("name", "")) for c in skewed if c.get("name")})[:5])
            supporting.append(f"The profiling summary flags skewed distributions for: {names}.")
            claims.append(
                {
                    "text": "Some numeric fields appear heavy-tailed; year/segment aggregates may be dominated by a small number of extreme records.",
                    "confidence": "medium",
                    "evidence_refs": ["data_profile:columns.skew_flag"],
                }
            )
            next_analyses.append("Compute top-k contribution (e.g., top 1/top 5 rows) to totals within anomalous groups to distinguish leverage vs broad shift.")

    # Negative evidence defaults
    negative.append("This interpretation does not use raw row-level records; it is limited to computed artifacts and profiling summaries.")

    # Open questions
    open_q.append("If an anomaly reflects an extreme value, is it a data error, a synthetic artifact, or a legitimate high-magnitude event?" )

    # Generic next analyses
    next_analyses.extend(
        [
            "Stratify key aggregates by available categorical fields (region/segment/category) to localize drivers.",
            "If time fields exist, compare anomalous periods against baselines using counts + sums + percentiles.",
        ]
    )

    return EvidenceGatedInterpretation(
        claims=claims,
        supporting_evidence=supporting,
        negative_evidence=negative,
        open_questions=open_q,
        recommended_next_analyses=list(dict.fromkeys(next_analyses)),
        generated_by="fallback",
        cache_key=cache_key,
    )


def _try_openai_json(prompt: str) -> dict[str, Any] | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI  # type: ignore

        client = OpenAI(api_key=api_key)
        model = os.getenv("ANALYST_AGENT_LLM_MODEL", "gpt-4o-mini")
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a cautious data analysis assistant. "
                        "You MUST NOT invent facts. "
                        "Every claim must cite evidence_refs that are present in the provided artifacts. "
                        "If a question cannot be answered, put it in open_questions and propose recommended_next_analyses. "
                        "Return ONLY valid JSON."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        text = resp.choices[0].message.content or ""
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def generate_llm_interpretation(
    *,
    run_dir: Path,
    data_profile_path: Path,
    plan_path: Path,
    metrics_csv_path: Path,
    anomalies_path: Path,
    plots_dir: Path,
    metrics_compact: list[dict[str, str]],
) -> EvidenceGatedInterpretation:
    """Generate (and cache) an evidence-gated interpretation.

    - Cache key is a SHA256 over allowed inputs.
    - If OpenAI is not configured or output is invalid, falls back deterministically.
    """

    cache_dir = run_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_key = _sha256_files([data_profile_path, plan_path, metrics_csv_path, anomalies_path])
    cache_path = cache_dir / f"llm_interpretation_{cache_key}.json"
    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and cached.get("_status") == "ok":
                return EvidenceGatedInterpretation(
                    claims=list(cached.get("claims") or []),
                    supporting_evidence=list(cached.get("supporting_evidence") or []),
                    negative_evidence=list(cached.get("negative_evidence") or []),
                    open_questions=list(cached.get("open_questions") or []),
                    recommended_next_analyses=list(cached.get("recommended_next_analyses") or []),
                    generated_by=str(cached.get("generated_by") or "fallback"),
                    cache_key=str(cached.get("cache_key") or cache_key),
                )
        except Exception:
            pass

    profile = _safe_load_json(data_profile_path)
    plan = _safe_load_json(plan_path)
    anomalies = _safe_load_json(anomalies_path)

    # Minimal plot captions (filenames only, safe)
    plot_files = []
    try:
        if plots_dir.exists() and plots_dir.is_dir():
            plot_files = [f"plots/{p.name}" for p in sorted(plots_dir.glob("*.png"))[:25]]
    except Exception:
        plot_files = []

    prompt_obj = {
        "artifacts": {
            "data_profile": profile,
            "analysis_plan": plan,
            "metrics_compact": metrics_compact,
            "anomalies_normalized": anomalies,
            "plots": plot_files,
        },
        "constraints": [
            "You do not have access to raw row-level data.",
            "Do not invent values or drivers not supported by the provided artifacts.",
            "Every claim must include evidence_refs that point to anomalies ids, metric keys, or profile fields present above.",
        ],
        "required_json_schema": {
            "claims": [
                {
                    "text": "...",
                    "confidence": "high|medium|low",
                    "evidence_refs": ["..."]
                }
            ],
            "supporting_evidence": ["..."],
            "negative_evidence": ["..."],
            "open_questions": ["..."],
            "recommended_next_analyses": ["..."],
        },
    }

    prompt = "Produce an evidence-gated interpretation as JSON." + "\n\n" + json.dumps(prompt_obj, indent=2, sort_keys=True)

    llm_obj = _try_openai_json(prompt)
    if isinstance(llm_obj, dict):
        # Basic validation (fail closed)
        claims = llm_obj.get("claims")
        if isinstance(claims, list) and all(isinstance(c, dict) and "text" in c and "evidence_refs" in c for c in claims):
            out = EvidenceGatedInterpretation(
                claims=claims,
                supporting_evidence=list(llm_obj.get("supporting_evidence") or []),
                negative_evidence=list(llm_obj.get("negative_evidence") or []),
                open_questions=list(llm_obj.get("open_questions") or []),
                recommended_next_analyses=list(llm_obj.get("recommended_next_analyses") or []),
                generated_by="openai",
                cache_key=cache_key,
            )
        else:
            out = _fallback_structured(data_profile=profile, anomalies=anomalies, metrics_compact=metrics_compact, cache_key=cache_key)
    else:
        out = _fallback_structured(data_profile=profile, anomalies=anomalies, metrics_compact=metrics_compact, cache_key=cache_key)

    payload = {
        "_status": "ok",
        "generated_by": out.generated_by,
        "cache_key": out.cache_key,
        "claims": out.claims,
        "supporting_evidence": out.supporting_evidence,
        "negative_evidence": out.negative_evidence,
        "open_questions": out.open_questions,
        "recommended_next_analyses": out.recommended_next_analyses,
    }
    try:
        cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except Exception:
        pass
    return out


def render_llm_interpretation_markdown(obj: EvidenceGatedInterpretation) -> str:
    lines: list[str] = []
    lines.append(f"### Output provenance\n")
    lines.append(f"- generated_by: {obj.generated_by}\n")
    lines.append(f"- cache_key: {obj.cache_key[:12]}…\n")

    lines.append("\n### Claims (evidence-gated)\n")
    for c in obj.claims[:10]:
        text = str(c.get("text", "")).strip()
        conf = str(c.get("confidence", "medium")).strip()
        ev = c.get("evidence_refs") or []
        ev_str = ", ".join([str(x) for x in ev][:6])
        lines.append(f"- **{conf}** — {text}\n")
        if ev_str:
            lines.append(f"  - evidence: {ev_str}\n")

    if obj.supporting_evidence:
        lines.append("\n### Supporting evidence\n")
        for s in obj.supporting_evidence[:10]:
            lines.append(f"- {s}\n")

    if obj.negative_evidence:
        lines.append("\n### What this does not show\n")
        for n in obj.negative_evidence[:10]:
            lines.append(f"- {n}\n")

    if obj.open_questions:
        lines.append("\n### Open questions\n")
        for q in obj.open_questions[:10]:
            lines.append(f"- {q}\n")

    if obj.recommended_next_analyses:
        lines.append("\n### Recommended next analyses\n")
        for r in obj.recommended_next_analyses[:10]:
            lines.append(f"- {r}\n")

    return "".join(lines).strip() + "\n"
