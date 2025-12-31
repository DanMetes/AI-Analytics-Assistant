from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Optional

from ..run_orchestrator import OutputManifest, run_analysis
from ..artifacts import ArtifactWriter
from ..execute import execute_plan
from ..profile import profile_dataset_to_html, summarize_dataset_to_json
from ..plan import PlanValidationError, build_plan_from_profile, load_and_validate_plan, validate_plan_obj, write_plan
from ..synth import build_report
from ..synth.report_builder import ReportInputs
from ..synth.llm_synth import build_llm_inputs
from ..synth.llm_interpretation import generate_llm_interpretation, render_llm_interpretation_markdown
from .context import RunContext


def _write_ingest_meta(*, ctx: RunContext, project_id: str, dataset_id: str, analysis_log_path: Path) -> None:
    """Write ingest_meta.json for the run, derived from deterministic ingest artifacts.

    This must be best-effort and must not change analysis behavior.
    """

    import json

    try:
        dataset_dir = ctx.project_root / "projects" / project_id / "datasets" / dataset_id
        fp_path = dataset_dir / "fingerprint.json"
        schema_path = dataset_dir / "schema.json"

        fingerprint = json.loads(fp_path.read_text(encoding="utf-8")) if fp_path.exists() else {}
        schema = json.loads(schema_path.read_text(encoding="utf-8")) if schema_path.exists() else {}

        created_at = None
        if analysis_log_path.exists():
            al = json.loads(analysis_log_path.read_text(encoding="utf-8"))
            if isinstance(al, dict):
                created_at = al.get("created_at")

        payload: dict[str, object] = {
            "_status": "ok",
            "project_id": project_id,
            "dataset_id": dataset_id,
            "created_at": created_at,
            "fingerprint": fingerprint if isinstance(fingerprint, dict) else {},
            "schema": schema if isinstance(schema, dict) else {},
        }

        out = ctx.run_dir / "ingest_meta.json"
        # Only overwrite the placeholder or missing file.
        if out.exists():
            try:
                existing = json.loads(out.read_text(encoding="utf-8"))
            except Exception:
                existing = {}
            if isinstance(existing, dict) and existing.get("_status") not in (None, "not_implemented"):
                return
        out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except Exception:
        return


def _append_plan_error(analysis_log_path: Path, report_md_path: Path, message: str) -> None:
    """Best-effort append of a plan validation error."""
    import json

    payload: dict[str, object] = {"stage": "plan_validation", "error": message}
    # analysis_log.json: merge/append into errors list
    try:
        if analysis_log_path.exists():
            existing = json.loads(analysis_log_path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                errs = existing.get("errors")
                if not isinstance(errs, list):
                    errs = []
                errs.append(payload)
                existing["errors"] = errs
                analysis_log_path.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            else:
                analysis_log_path.write_text(json.dumps({"errors": [payload]}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        else:
            analysis_log_path.write_text(json.dumps({"errors": [payload]}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except Exception:
        pass

    # report.md: prepend a clear header
    try:
        header = f"# Run Failed\n\nPlan validation failed:\n\n- {message}\n\n"
        if report_md_path.exists():
            existing_md = report_md_path.read_text(encoding="utf-8")
            report_md_path.write_text(header + existing_md, encoding="utf-8")
        else:
            report_md_path.write_text(header, encoding="utf-8")
    except Exception:
        pass


@dataclass(frozen=True)
class RunResult:
    """Return type for pipeline runs.

    Batch R1 scope: mirror the existing CLI OutputManifest to avoid
    behavioral changes. Future batches will expand this object.
    """

    run_dir: Path
    report_md: Path
    metrics_csv: Path
    analysis_log_json: Path
    reproduce_sql: Path
    figures_dir: Optional[Path] = None

    @classmethod
    def from_manifest(cls, m: OutputManifest) -> "RunResult":
        return cls(
            run_dir=m.run_dir,
            report_md=m.report_md,
            metrics_csv=m.metrics_csv,
            analysis_log_json=m.analysis_log_json,
            reproduce_sql=m.reproduce_sql,
            figures_dir=m.figures_dir,
        )


def run_pipeline(
    *,
    project_id: str,
    dataset_id: str,
    db_path: str,
    question: str,
    policy_name: str,
    roles: dict[str, list[str]] | None,
    plots: bool = False,
    llm: bool = False,
    project_root: Path | None = None,
    dataset_hash: str = "",
) -> RunResult:
    """Pipeline entrypoint.

    Batch R1 requirement: introduce pipeline orchestration without changing
    analysis behavior or artifact outputs. This function intentionally
    delegates to the existing deterministic analysis engine via the current
    CLI orchestrator.

    The RunContext is created for future stages; it is not yet used to
    enforce artifact contracts (Batch R2).
    """

    manifest = run_analysis(
        project_id=project_id,
        dataset_id=dataset_id,
        db_path=db_path,
        question=question,
        policy_name=policy_name,
        roles=roles,
        plots=plots,
    )

    # Batch R2: create a real context using the actual run directory, then
    # ensure the README artifact contract exists (placeholders allowed).
    ctx = RunContext.create(
        project_root=project_root or Path("."),
        run_dir=manifest.run_dir,
        dataset_hash=dataset_hash,
    )

    # Provide run-local ingest metadata derived from deterministic ingest artifacts.
    _write_ingest_meta(ctx=ctx, project_id=project_id, dataset_id=dataset_id, analysis_log_path=manifest.analysis_log_json)

    # Batch F1: generate EDA HTML report. Failures must not abort the run.
    profile_dataset_to_html(
        ctx=ctx,
        project_id=project_id,
        dataset_id=dataset_id,
        analysis_log_path=manifest.analysis_log_json,
    )

    # Batch F2: generate deterministic machine-readable profile summary.
    # Failures must not abort the run.
    summarize_dataset_to_json(
        ctx=ctx,
        project_id=project_id,
        dataset_id=dataset_id,
        analysis_log_path=manifest.analysis_log_json,
    )

    # Batch G2: generate a deterministic plan from data_profile.json when the
    # plan is missing or empty, then validate the plan contract.
    aw = ArtifactWriter(ctx)
    plan_path = aw.path_analysis_plan()
    profile_path = aw.path_data_profile()

    try:
        plan = load_and_validate_plan(plan_path)
        is_empty = isinstance(plan, dict) and plan.get("steps") == []
        if is_empty and profile_path.exists():
            import json

            profile_obj = json.loads(profile_path.read_text(encoding="utf-8"))
            if isinstance(profile_obj, dict) and profile_obj.get("_status") != "not_implemented":
                generated = build_plan_from_profile(profile_obj)
                validated = validate_plan_obj(generated)
                write_plan(plan_path, validated)
                plan = validated

        # Always re-validate deterministically after potential generation.
        plan = validate_plan_obj(plan)
        write_plan(plan_path, plan)
    except PlanValidationError as e:
        _append_plan_error(manifest.analysis_log_json, manifest.report_md, str(e))
        raise RuntimeError(f"Invalid analysis plan: {e}") from e

    # Batch E1: execute primitives described in analysis_plan.json.
    # Best-effort: should not abort the run if a primitive fails.
    try:
        execute_plan(ctx=ctx, project_id=project_id, dataset_id=dataset_id, analysis_log_path=manifest.analysis_log_json)
    except Exception:
        pass

    # Ensure contract artifacts exist before synthesis.
    aw.ensure_contract()

    # Batch S1: build a deterministic report.md from existing artifacts.
    try:
        inputs = ReportInputs(
            run_dir=ctx.run_dir,
            ingest_meta=aw.path_ingest_meta(),
            data_profile=aw.path_data_profile(),
            analysis_plan=aw.path_analysis_plan(),
            metrics_csv=aw.path_metrics_csv(),
            anomalies_normalized=aw.path_anomalies_normalized(),
            eda_report=aw.path_eda_report(),
            plots_dir=aw.plots_dir(),
        )
        build_report(inputs=inputs, output_path=aw.path_report_md())
    except Exception as e:
        # Best-effort: do not abort the run if report building fails.
        try:
            aw.path_report_md().write_text(
                "# Analyst Agent Report\n\n"
                "## Report generation failed\n\n"
                f"- Error: {type(e).__name__}: {e}\n",
                encoding="utf-8",
            )
        except Exception:
            pass

    # Batch S2: optional LLM interpretation section (opt-in; default off).
    if llm:
        try:
            llm_inputs = build_llm_inputs(
                data_profile_path=aw.path_data_profile(),
                plan_path=aw.path_analysis_plan(),
                metrics_csv=aw.path_metrics_csv(),
                anomalies_path=aw.path_anomalies_normalized(),
                plots_dir=aw.plots_dir(),
            )
            llm_obj = generate_llm_interpretation(
                run_dir=ctx.run_dir,
                data_profile_path=aw.path_data_profile(),
                plan_path=aw.path_analysis_plan(),
                metrics_csv_path=aw.path_metrics_csv(),
                anomalies_path=aw.path_anomalies_normalized(),
                plots_dir=aw.plots_dir(),
                metrics_compact=llm_inputs.metrics_compact,
            )

            # Write llm_interpretation.json (structured, evidence-gated).
            llm_path = ctx.run_dir / "llm_interpretation.json"
            payload = {
                "_status": "ok",
                "generated_by": llm_obj.generated_by,
                "cache_key": llm_obj.cache_key,
                "claims": llm_obj.claims,
                "supporting_evidence": llm_obj.supporting_evidence,
                "negative_evidence": llm_obj.negative_evidence,
                "open_questions": llm_obj.open_questions,
                "recommended_next_analyses": llm_obj.recommended_next_analyses,
            }
            llm_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            # Append a readable section to report.md.
            section = "\n\n## LLM Interpretation (Optional)\n\n" + render_llm_interpretation_markdown(llm_obj)
            existing = aw.path_report_md().read_text(encoding="utf-8") if aw.path_report_md().exists() else ""
            if "## LLM Interpretation (Optional)" not in existing:
                aw.path_report_md().write_text(existing.rstrip() + section, encoding="utf-8")
        except Exception:
            pass

    return RunResult.from_manifest(manifest)
