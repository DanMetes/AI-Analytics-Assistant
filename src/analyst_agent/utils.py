from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from .models import Project
from .paths import projects_root


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def iso_in_hours(hours: int) -> str:
    return (datetime.utcnow() + timedelta(hours=hours)).isoformat()


def parse_iso(dt: str) -> datetime:
    return datetime.fromisoformat(dt)


def new_id() -> str:
    return str(uuid.uuid4())


def safe_slug(name: str) -> str:
    """
    Simple slugging for display; project_id remains UUID.
    """
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "project"


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """
    Computes a sha256 fingerprint of the CSV file for traceability.
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def find_project_id_by_name(project_name: str) -> Optional[str]:
    """
    Finds a project by scanning ./projects/*/project.json.
    This is v1-simple: no database, no index.
    """
    root = projects_root()
    if not root.exists():
        return None

    for pdir in root.iterdir():
        if not pdir.is_dir():
            continue
        meta = pdir / "project.json"
        if not meta.exists():
            continue
        try:
            data = read_json(meta)
            if data.get("name") == project_name:
                return data.get("project_id")
        except Exception:
            continue
    return None


def ensure_projects_root() -> None:
    projects_root().mkdir(parents=True, exist_ok=True)


def make_project(project_name: str) -> Project:
    """
    Creates a new Project model. Caller is responsible for persisting it.
    """
    return Project(project_id=new_id(), name=project_name)
