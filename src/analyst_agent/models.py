from __future__ import annotations

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


class RetentionMode(str, Enum):
    """
    Ephemeral-by-default retention modes.

    - TTL_24H: keep the session SQLite DB for up to 24 hours for iterative work
    - DELETE_AFTER_RUN: delete session DB immediately after running analysis
    """
    TTL_24H = "ttl_24h"
    DELETE_AFTER_RUN = "delete_after_run"


class Project(BaseModel):
    """
    Project metadata. A project is a stable container for datasets, runs, and artifacts.

    project_id: stable UUID
    name: user-friendly name (used to find the project)
    created_at: ISO8601 timestamp
    """
    project_id: str
    name: str
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class DatasetSession(BaseModel):
    """
    Represents the *active* dataset session for a project.

    dataset_id: UUID for the ingested dataset session
    project_id: UUID of owning project
    db_path: OS path to the ephemeral SQLite database file (in system temp directory)
    retention_mode: TTL_24H or DELETE_AFTER_RUN
    created_at: ISO8601 timestamp
    expires_at: ISO8601 timestamp for TTL_24H sessions; None for DELETE_AFTER_RUN
    row_count / column_count: basic profiling
    """
    dataset_id: str
    project_id: str
    db_path: str
    retention_mode: RetentionMode = RetentionMode.TTL_24H
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    expires_at: Optional[str] = None
    row_count: int = 0
    column_count: int = 0


class Run(BaseModel):
    """
    Represents a single analysis run against the active dataset.

    status: "success" | "failed"
    """
    run_id: str
    project_id: str
    dataset_id: str
    question: str
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    status: str = "success"


class OutputManifest(BaseModel):
    """
    Paths to run artifacts produced by `analyst run`.

    These artifacts are the *contract* that a future web UI can render
    without re-implementing analysis logic.
    """
    run_dir: str
    report_md: str
    metrics_csv: str
    analysis_log_json: str
    reproduce_sql: str
    figures_dir: Optional[str] = None


class DatasetArtifacts(BaseModel):
    """
    Paths to profiling artifacts created by ingestion.
    """
    dataset_dir: str
    schema_path: str
    profile_path: str
    fingerprint_path: str
    warnings_json: str
