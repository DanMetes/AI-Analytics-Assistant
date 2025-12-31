from __future__ import annotations

from pathlib import Path

from .models import Project
from .paths import project_dir, project_meta_path
from .utils import ensure_projects_root, find_project_id_by_name, read_json, write_json


def create_project(project_name: str) -> Project:
    """
    Create a project folder and write project.json.

    The project_name is user-facing. project_id is stable UUID.
    """
    ensure_projects_root()

    existing = find_project_id_by_name(project_name)
    if existing:
        raise ValueError(f"Project '{project_name}' already exists (project_id={existing}).")

    proj = Project(project_id=__import__("uuid").uuid4().hex, name=project_name)
    pdir = project_dir(proj.project_id)
    pdir.mkdir(parents=True, exist_ok=True)

    write_json(project_meta_path(proj.project_id), proj.model_dump())
    # Also ensure standard subfolders exist.
    (pdir / "datasets").mkdir(exist_ok=True)
    (pdir / "runs").mkdir(exist_ok=True)

    return proj


def load_project(project_name: str) -> Project:
    """
    Load project metadata by name.
    """
    pid = find_project_id_by_name(project_name)
    if not pid:
        raise FileNotFoundError(
            f"Project '{project_name}' not found. Run: analyst init {project_name}"
        )

    meta_path = project_meta_path(pid)
    data = read_json(meta_path)
    return Project(**data)
