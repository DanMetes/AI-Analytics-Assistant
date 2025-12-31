from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from .models import DatasetSession, RetentionMode
from .paths import active_dataset_path
from .utils import parse_iso, read_json, write_json


def load_active_session(project_id: str) -> DatasetSession:
    """
    Loads the project's active session metadata.

    If missing, user must run `analyst ingest ...` first.
    """
    path = active_dataset_path(project_id)
    if not path.exists():
        raise FileNotFoundError(
            "No active dataset session found for this project. Run: analyst ingest --project <name> --data <csv>"
        )
    data = read_json(path)
    return DatasetSession(**data)


def clear_active_session(project_id: str) -> None:
    path = active_dataset_path(project_id)
    if path.exists():
        path.unlink()


def delete_session_db(session: DatasetSession) -> None:
    """
    Deletes the ephemeral SQLite database file for the session.

    This is the primary privacy deletion point in v1.
    """
    db_path = Path(session.db_path)
    if db_path.exists():
        try:
            db_path.unlink()
        except Exception:
            # Best-effort delete. In v1 we don't escalate.
            pass


def is_expired(session: DatasetSession) -> bool:
    if session.retention_mode != RetentionMode.TTL_24H:
        return False
    if not session.expires_at:
        return False
    return parse_iso(session.expires_at) <= parse_iso(__import__("datetime").datetime.utcnow().isoformat())
