from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import typer

from .cleanup import cleanup_expired_sessions
from .pipeline.run import run_pipeline
from .run_orchestrator import OutputManifest
from .ingest import ingest_csv_to_session
from .interpreters import get_interpreter
from .models import RetentionMode
from .policy_registry import PolicyRegistry
from .project import create_project, load_project
from .session import clear_active_session, delete_session_db, load_active_session
from .ask import answer_or_plan, latest_run_dir_for_project

app = typer.Typer(add_completion=False, help="Analyst Agent (SQLite-first CLI skeleton)")

# ---- Policy commands ----
policy_app = typer.Typer(help="Inspect available analysis policies.")
app.add_typer(policy_app, name="policy")


@policy_app.command("list")
def list_policies() -> None:
    """
    List available analysis policies.

    Each entry includes the policy name.  If the name does not include a
    version suffix (e.g. `_v1`), the version is appended in parentheses to
    clarify which version of the policy will run.
    """
    registry = PolicyRegistry()
    for name in registry.list_policies():
        # Retrieve version metadata for display
        try:
            meta = registry.describe_policy(name)
            version = str(meta.get("version", ""))
        except Exception:
            version = ""
        # Determine if the name embeds its version (convention: contains '_v')
        if "_v" in name:
            typer.echo(name)
        else:
            suffix = f" (v{version})" if version else ""
            typer.echo(f"{name}{suffix}")


@policy_app.command("describe")
def describe_policy(
    policy: str = typer.Option(..., "--policy", help="Policy name to describe")
) -> None:
    """
    Show metadata for a specific policy as JSON.

    Use the --policy flag to specify which policy to describe.  The output
    is printed in stable JSON form with sorted keys.
    """
    registry = PolicyRegistry()
    try:
        meta = registry.describe_policy(policy)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1)

    # Emit JSON with stable ordering
    typer.echo(json.dumps(meta, indent=2, sort_keys=True))




@app.command()
def init(project_name: str = typer.Argument(..., help="User-friendly project name")):
    """
    Create a new project under ./projects/<project_id>/ with project.json.
    """
    try:
        proj = create_project(project_name)
        typer.echo(f"Created project '{proj.name}' (project_id={proj.project_id})")
        typer.echo(f"Path: projects/{proj.project_id}/")
    except Exception as e:
        raise typer.Exit(code=1) from e


@app.command()
def ingest(
    project: str = typer.Option(..., "--project", help="Existing project name"),
    data: Path = typer.Option(..., "--data", exists=True, help="Path to CSV file"),
    retention: RetentionMode = typer.Option(
        RetentionMode.TTL_24H,
        "--retention",
        help="Ephemeral retention policy (only two options in v1)",
        case_sensitive=False,
    ),
):
    """
    Ingest a CSV into a per-project session SQLite DB stored in the system temp directory.

    Writes profiling artifacts under:
      ./projects/<project_id>/datasets/<dataset_id>/
    Updates:
      ./projects/<project_id>/active_dataset.json
    """
    try:
        proj = load_project(project)
        session, artifacts = ingest_csv_to_session(proj.project_id, data, retention_mode=retention)
        typer.echo(f"Ingested dataset_id={session.dataset_id}")
        typer.echo(f"Session DB: {session.db_path}")
        typer.echo(f"Retention: {session.retention_mode}")
        if session.expires_at:
            typer.echo(f"Expires at (UTC): {session.expires_at}")
        typer.echo("Artifacts:")
        typer.echo(f"  schema: {artifacts.schema_path}")
        typer.echo(f"  profile: {artifacts.profile_path}")
        typer.echo(f"  fingerprint: {artifacts.fingerprint_path}")
    except Exception as e:
        typer.echo(f"ERROR: {e}")
        raise typer.Exit(code=1)


@app.command()
def run(
    project: str = typer.Option(..., "--project", help="Existing project name"),
    question: str = typer.Option(..., "--question", help="Analysis question"),
    plots: str = typer.Option("off", "--plots", help="on|off"),
    llm: bool = typer.Option(False, "--llm", help="Enable optional LLM interpretation section (default: off)"),
    policy: str = typer.Option("generic_tabular", "--policy", help="Analysis policy name"),
    role: list[str] = typer.Option([], "--role", help="Role mapping like customer=CustomerID (repeatable)"),
):
    """
    Run a minimal analysis against the active session DB and write run artifacts.

    Always writes:
      report.md, metrics.csv, analysis_log.json, reproduce.sql
    """
    try:
        proj = load_project(project)
        session = load_active_session(proj.project_id)

        registry = PolicyRegistry()
        if policy != "auto":
            try:
                registry.describe_policy(policy)
            except KeyError as exc:
                typer.echo(str(exc), err=True)
                raise typer.Exit(code=1)

        role_map: dict[str, list[str]] = {}
        for r in role:
            if "=" not in r:
                typer.echo(f"Invalid role format (expected role=column[,col2]): {r}", err=True)
                raise typer.Exit(code=1)
            k, v = r.split("=", 1)
            key = k.strip().lower()
            candidates = [c.strip() for c in v.split(",") if c.strip()]
            if not candidates:
                typer.echo(f"No columns provided for role '{key}'.", err=True)
                raise typer.Exit(code=1)
            role_map.setdefault(key, []).extend(candidates)

        plots_on = plots.strip().lower() == "on"
        manifest = run_pipeline(
            project_id=proj.project_id,
            dataset_id=session.dataset_id,
            db_path=session.db_path,
            question=question,
            policy_name=policy,
            roles=role_map if role_map else None,
            plots=plots_on,
            llm=llm,
        )

        typer.echo("Run complete.")
        typer.echo(f"Run dir: {manifest.run_dir}")
        typer.echo(f"Report: {manifest.report_md}")
        typer.echo(f"Metrics: {manifest.metrics_csv}")
        typer.echo(f"Log: {manifest.analysis_log_json}")
        typer.echo(f"SQL: {manifest.reproduce_sql}")
        if manifest.figures_dir:
            typer.echo(f"Figures: {manifest.figures_dir}")

        # Retention enforcement: delete-after-run option
        if session.retention_mode == RetentionMode.DELETE_AFTER_RUN:
            delete_session_db(session)
            clear_active_session(proj.project_id)
            typer.echo("Session DB deleted (delete_after_run).")

    except FileNotFoundError as e:
        typer.echo(f"ERROR: {e}")
        raise typer.Exit(code=2)
    except Exception as e:
        typer.echo(f"ERROR: {e}")
        raise typer.Exit(code=1)


@app.command()
def ask(
    project: str = typer.Option(..., "--project", help="Existing project name"),
    question: str = typer.Option(..., "--question", help="A follow-up question about the most recent run"),
    llm: bool = typer.Option(True, "--llm/--no-llm", help="Use LLM for Q&A if configured (default: on)"),
):
    """Ask follow-up questions against the latest run artifacts.

    Behavior:
    - If the question can be answered using existing artifacts, prints an evidence-backed answer.
    - If not, emits a methodology plan + Python scaffold under run_dir/ask/.
    """

    try:
        proj = load_project(project)
        run_dir = latest_run_dir_for_project(proj.project_id)
        res = answer_or_plan(run_dir=run_dir, question=question, use_llm=llm)

        typer.echo(f"Run dir: {run_dir}")
        typer.echo("")
        typer.echo(res.answer_md.rstrip())
        if res.evidence_refs:
            typer.echo("\nEvidence refs:")
            for r in res.evidence_refs:
                typer.echo(f"- {r}")
        if res.mode == "plan":
            if res.plan_path:
                typer.echo(f"\nPlan written: {res.plan_path}")
            if res.code_path:
                typer.echo(f"Code written: {res.code_path}")
    except FileNotFoundError as e:
        typer.echo(f"ERROR: {e}")
        raise typer.Exit(code=2)
    except Exception as e:
        typer.echo(f"ERROR: {e}")
        raise typer.Exit(code=1)


@app.command("delete-session")
def delete_session(project: str = typer.Option(..., "--project", help="Existing project name")):
    """
    Delete the active session DB immediately and clear active_dataset.json.
    """
    try:
        proj = load_project(project)
        session = load_active_session(proj.project_id)
        delete_session_db(session)
        clear_active_session(proj.project_id)
        typer.echo("Deleted active session DB and cleared active dataset pointer.")
    except Exception as e:
        typer.echo(f"ERROR: {e}")
        raise typer.Exit(code=1)


@app.command()
def cleanup():
    """
    Cleanup expired TTL_24H session DBs and orphaned pointers (v1 scans filesystem).
    """
    try:
        n = cleanup_expired_sessions()
        typer.echo(f"Cleanup complete. Deleted {n} expired session DB(s).")
    except Exception as e:
        typer.echo(f"ERROR: {e}")
        raise typer.Exit(code=1)
