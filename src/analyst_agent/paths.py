from __future__ import annotations

import tempfile
from pathlib import Path


def projects_root() -> Path:
    """
    Root directory for project metadata and artifacts.
    Kept relative for simplicity in v1.
    """
    return Path.cwd() / "projects"


def project_dir(project_id: str) -> Path:
    return projects_root() / project_id


def datasets_dir(project_id: str) -> Path:
    return project_dir(project_id) / "datasets"


def dataset_dir(project_id: str, dataset_id: str) -> Path:
    return datasets_dir(project_id) / dataset_id


def runs_dir(project_id: str) -> Path:
    return project_dir(project_id) / "runs"


def run_dir(project_id: str, run_id: str) -> Path:
    return runs_dir(project_id) / run_id


def active_dataset_path(project_id: str) -> Path:
    return project_dir(project_id) / "active_dataset.json"


def project_meta_path(project_id: str) -> Path:
    return project_dir(project_id) / "project.json"


def session_db_path(project_id: str, dataset_id: str) -> Path:
    """
    Session SQLite DB lives under system temp directory.

    This supports ephemeral-by-default behavior and avoids mixing raw-like data
    into the repo directory.
    """
    base = Path(tempfile.gettempdir()) / "analyst" / project_id
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{dataset_id}.db"
