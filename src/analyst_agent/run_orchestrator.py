from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analyze import run_analysis as run_analysis_engine
from .interpreters import get_interpreter
from .policy_registry import PolicyRegistry


@dataclass
class OutputManifest:
    """Paths for run artifacts (v1 contract).

    This keeps CLI printing stable and makes outputs explicit.
    """

    run_dir: Path
    report_md: Path
    metrics_csv: Path
    analysis_log_json: Path
    reproduce_sql: Path
    figures_dir: Path | None = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_run_dir(project_id: str) -> tuple[str, Path]:
    run_id = str(uuid.uuid4())
    run_dir = Path("projects") / project_id / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_id, run_dir


def _write_report_stub(report_path: Path, question: str) -> None:
    """Write deterministic report template.

    v1 report is a stub template (no LLM).
    """
    content = (
        "# Analyst Agent Report (v1)\n\n"
        f"**Question:** {question}\n\n"
        "## Summary\n\n"
        "_Stub report. This version writes computed artifacts only; no LLM generation._\n\n"
        "## Artifacts\n\n"
        "- metrics.csv: computed aggregates\n"
        "- analysis_log.json: audit trail (queries, warnings, errors)\n"
        "- reproduce.sql: exact SQL used\n"
        "- interpretation.json: machine-readable interpretation\n"
    )
    report_path.write_text(content, encoding="utf-8")


def _append_interpretation_sections(
    report_path: Path,
    *,
    findings: list[object],
    caveats: list[str],
    policy_selection: dict | None,
    metadata: dict | None = None,
) -> None:
    lines: list[str] = []

    # Build an Executive Summary section derived strictly from normalized anomalies.
    # It is inserted before Findings and provides a deterministic overview of key anomalies.
    exec_lines: list[str] = []
    if metadata and isinstance(metadata, dict):
        anoms_norm = metadata.get("anomalies_normalized") or []
        if isinstance(anoms_norm, list) and anoms_norm:

            def _sev_rank(sev_str: str) -> int:
                return {"critical": 3, "warning": 2, "info": 1}.get(str(sev_str).lower(), 0)

            sorted_anoms = sorted(
                anoms_norm,
                key=lambda a: (
                    -_sev_rank(a.get("severity", "info")),
                    str(a.get("metric", "")),
                    str(a.get("id", "")),
                ),
            )
            for a in sorted_anoms:
                sev = str(a.get("severity", "")).capitalize()
                summary = str(a.get("summary", "")).rstrip(".")
                metric = str(a.get("metric", ""))
                val = a.get("value")
                if isinstance(val, (int, float)):
                    val_str = f"{val:.2f}"
                else:
                    val_str = str(val)
                exec_lines.append(f"- {sev} — {summary} ({metric}={val_str})\n")
        else:
            exec_lines.append("- No anomalies detected under policy thresholds.\n")
    else:
        exec_lines.append("- No anomalies detected under policy thresholds.\n")

    lines.append("\n## Executive Summary\n")
    lines.extend(exec_lines)

    lines.append("\n## Findings\n")
    if findings:
        for f in findings:
            if hasattr(f, "text"):
                lines.append(f"- {getattr(f, 'text')}\n")
            else:
                lines.append(f"- {str(f)}\n")
    else:
        lines.append("- No findings available.\n")

    # Step-6: optional evidence framing sections (domain-agnostic).
    if metadata and isinstance(metadata, dict):
        supp = metadata.get("supporting_evidence") or []
        if isinstance(supp, list) and supp:
            lines.append("\n## Supporting Evidence\n")
            for s in supp:
                lines.append(f"- {str(s)}\n")

        neg = metadata.get("negative_evidence") or []
        if isinstance(neg, list) and neg:
            lines.append("\n## What This Does Not Show\n")
            for n in neg:
                lines.append(f"- {str(n)}\n")

    lines.append("\n## Caveats\n")
    if caveats:
        for c in caveats:
            lines.append(f"- {c}\n")
    else:
        lines.append("- None noted.\n")

    anomalies = []
    if metadata and isinstance(metadata, dict):
        anomalies = metadata.get("anomalies") or []
    if anomalies:
        lines.append("\n## Anomalies\n")
        for a in anomalies:
            lines.append(f"- {a}\n")

    if policy_selection:
        lines.append("\n## Policy selection trace\n")
        selected = policy_selection.get("selected")
        lines.append(f"- Selected policy: {selected}\n")
        candidates = policy_selection.get("candidates") or []
        top_candidates = sorted(
            candidates,
            key=lambda c: (c.get("score", 0), str(c.get("name", ""))),
            reverse=True,
        )[:2]
        for cand in top_candidates:
            name = cand.get("name")
            score = cand.get("score")
            missing = cand.get("missing_required_roles") or []
            lines.append(f"- Candidate {name}: score={score}, missing_required={missing}\n")

    if metadata:
        lines.append("\n## Coverage & Confidence\n")
        coverage = metadata.get("coverage") or {}
        confidence = metadata.get("confidence") or {}
        cov_expected = coverage.get("expected")
        cov_present = coverage.get("present")
        cov_ratio = coverage.get("ratio")
        cov_missing = coverage.get("missing") or []
        lines.append(
            f"- Coverage: {cov_present}/{cov_expected} metrics present (ratio={cov_ratio}). Missing: {cov_missing}\n"
        )
        agg_conf = confidence.get("aggregate")
        trend_conf = confidence.get("trend")
        lines.append(f"- Confidence: aggregate={agg_conf}, trend={trend_conf}\n")

    with report_path.open("a", encoding="utf-8") as f:
        f.writelines(lines)


def run_analysis(
    *,
    project_id: str,
    dataset_id: str,
    db_path: str,
    question: str,
    policy_name: str,
    roles: dict[str, list[str]] | None,
    plots: bool = False,
) -> OutputManifest:
    """Orchestrator wrapper.

    - opens session DB
    - creates run folder
    - calls deterministic analysis engine
    - writes required artifacts
    - returns manifest
    """

    run_id, run_dir = _ensure_run_dir(project_id)

    report_md = run_dir / "report.md"
    metrics_csv = run_dir / "metrics.csv"
    analysis_log_json = run_dir / "analysis_log.json"
    reproduce_sql = run_dir / "reproduce.sql"

    figures_dir: Path | None = None
    if plots:
        figures_dir = run_dir / "figures"
        figures_dir.mkdir(parents=True, exist_ok=True)

    errors: list[str] = []
    resolved_roles: dict[str, str] = {}
    selection_log: dict[str, object] = {}
    selected_policy_name = policy_name

    try:
        conn = sqlite3.connect(db_path)
        try:
            metrics_rows, queries, warnings, resolved_roles, selection_log, selected_policy_name = run_analysis_engine(
                conn=conn,
                question=question,
                run_dir=run_dir,
                policy_name=policy_name,
                roles=roles,
                plots=plots,
            )
        finally:
            conn.close()

        import pandas as pd  # local import to keep module load fast

        df = pd.DataFrame(metrics_rows, columns=["section", "key", "value"])
        df.to_csv(metrics_csv, index=False)

        reproduce_sql.write_text("\n\n".join(q.strip() for q in queries if q.strip()) + "\n", encoding="utf-8")

        _write_report_stub(report_md, question)

        try:
            policy_meta = PolicyRegistry().describe_policy(selected_policy_name) or {}
        except Exception:
            policy_meta = {}

        policy_version = str(policy_meta.get("version") or "unknown")
        severity_thresholds = policy_meta.get("severity_thresholds") or policy_meta.get("thresholds")

        log = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "run_id": run_id,
            "question": question,
            "created_at": _utc_now_iso(),
            "policy": {
                "name": selected_policy_name,
                "version": policy_version,
                "resolved_roles": resolved_roles or None,
                "severity_thresholds": severity_thresholds,
            },
            "queries_executed": queries,
            "warnings": warnings,
            "errors": errors,
            "status": "success",
        }
        if policy_name == "auto":
            log["policy_selection"] = selection_log or None

        # Step-6: make profile evidence available to interpreters (domain-agnostic).
        # This improves explanation quality without affecting deterministic metric computation.
        # NOTE: this reads the run-local data_profile.json created earlier in the pipeline.
        try:
            profile_path = run_dir / "data_profile.json"
            if profile_path.exists():
                profile_obj = json.loads(profile_path.read_text(encoding="utf-8"))
                if isinstance(profile_obj, dict) and profile_obj.get("_status") != "not_implemented":
                    # Keep the full object for auditability; interpreters should only use summary fields.
                    log["data_profile"] = profile_obj
        except Exception:
            # Do not fail the run if profile cannot be read.
            pass

        interpreter = get_interpreter(selected_policy_name)
        interpretation = interpreter.interpret(metrics_rows, log)

        combined_caveats = list(dict.fromkeys((interpretation.caveats or []) + (log.get("warnings") or [])))
        _append_interpretation_sections(
            report_md,
            findings=interpretation.findings,
            caveats=combined_caveats,
            policy_selection=log.get("policy_selection"),
            metadata=getattr(interpretation, "metadata", None),
        )

        interpretation_path = run_dir / "interpretation.json"
        findings_payload = []
        for f in sorted(interpretation.findings, key=lambda x: getattr(x, "title", "") or ""):
            findings_payload.append(
                {
                    "severity": getattr(f, "severity", None),
                    "title": getattr(f, "title", None),
                    "text": getattr(f, "text", None),
                    "evidence_keys": getattr(f, "evidence_keys", None),
                }
            )

        metadata_payload = getattr(interpretation, "metadata", None)
        if isinstance(metadata_payload, dict) and "anomalies" in metadata_payload:
            anomalies = metadata_payload.get("anomalies") or []
            metadata_payload = dict(metadata_payload)
            metadata_payload["anomalies"] = sorted(anomalies, key=lambda a: str(a))

        interp_payload = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "run_id": run_id,
            "question": question,
            "created_at": log.get("created_at"),
            "policy": log.get("policy"),
            "findings": findings_payload,
            "caveats": list(combined_caveats),
            "metadata": metadata_payload,
        }

        interpretation_path.write_text(json.dumps(interp_payload, indent=2, sort_keys=True), encoding="utf-8")
        log.setdefault("artifacts_written", []).append(str(interpretation_path))

        # Write anomalies_normalized.json as a first-class run artifact.
        # For policy-specific interpreters, this comes from interpreter metadata.
        anomalies_path = run_dir / "anomalies_normalized.json"
        anomalies_payload: dict[str, Any] = {"_status": "ok", "anomalies": []}
        try:
            meta = getattr(interpretation, "metadata", None)
            if isinstance(meta, dict):
                an = meta.get("anomalies_normalized")
                if isinstance(an, list):
                    anomalies_payload["anomalies"] = an
            anomalies_path.write_text(json.dumps(anomalies_payload, indent=2, sort_keys=True), encoding="utf-8")
            log.setdefault("artifacts_written", []).append(str(anomalies_path))
        except Exception:
            # Best-effort only: do not fail the run if writing anomalies fails.
            pass

        analysis_log_json.write_text(json.dumps(log, indent=2), encoding="utf-8")

    except Exception as e:
        errors.append(str(e))

        try:
            policy_meta = PolicyRegistry().describe_policy(selected_policy_name) or {}
        except Exception:
            policy_meta = {}

        policy_version = str(policy_meta.get("version") or "unknown")
        severity_thresholds = policy_meta.get("severity_thresholds") or policy_meta.get("thresholds")

        log = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "run_id": run_id,
            "question": question,
            "created_at": _utc_now_iso(),
            "policy": {
                "name": selected_policy_name,
                "version": policy_version,
                "resolved_roles": resolved_roles or None,
                "severity_thresholds": severity_thresholds,
            },
            "queries_executed": [],
            "warnings": [],
            "errors": errors,
            "status": "error",
        }
        if policy_name == "auto":
            log["policy_selection"] = selection_log or None
        analysis_log_json.write_text(json.dumps(log, indent=2), encoding="utf-8")
        raise

    return OutputManifest(
        run_dir=run_dir,
        report_md=report_md,
        metrics_csv=metrics_csv,
        analysis_log_json=analysis_log_json,
        reproduce_sql=reproduce_sql,
        figures_dir=figures_dir,
    )
