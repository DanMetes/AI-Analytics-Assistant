from __future__ import annotations

from pathlib import Path

from .models import DatasetSession, RetentionMode
from .paths import projects_root
from .session import delete_session_db
from .utils import read_json


def cleanup_expired_sessions() -> int:
    """
    Deletes expired TTL_24H session DBs by scanning projects/*/active_dataset.json.

    v1-simple: scans filesystem, no index.
    """
    root = projects_root()
    if not root.exists():
        return 0

    deleted = 0
    for pdir in root.iterdir():
        if not pdir.is_dir():
            continue
        active = pdir / "active_dataset.json"
        if not active.exists():
            continue
        try:
            data = read_json(active)
            session = DatasetSession(**data)
        except Exception:
            continue

        if session.retention_mode == RetentionMode.TTL_24H and session.expires_at:
            # Parse expiration without importing dateutil; use fromisoformat
            from datetime import datetime

            try:
                expires = datetime.fromisoformat(session.expires_at)
                if expires <= datetime.utcnow():
                    delete_session_db(session)
                    # Clear active session pointer as well
                    try:
                        active.unlink()
                    except Exception:
                        pass
                    deleted += 1
            except Exception:
                continue

    return deleted
