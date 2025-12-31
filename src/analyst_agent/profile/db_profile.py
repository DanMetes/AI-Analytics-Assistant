from __future__ import annotations

import sqlite3
from typing import Any


def profile_database(conn: sqlite3.Connection) -> dict[str, Any]:
    """Inspect the connected SQLite database and return lightweight metadata.

    No printing or side effects; only PRAGMA metadata and row counts are read.
    """
    table_names = _list_tables(conn)
    tables: list[dict[str, object]] = []

    for table in table_names:
        columns = _get_columns(conn, table)
        row_count = _count_rows(conn, table)
        candidate_time_columns = [
            col["name"] for col in columns if _is_time_like(col["name"], col.get("type", ""))
        ]

        tables.append(
            {
                "name": table,
                "columns": columns,
                "row_count": row_count,
                "candidate_time_columns": candidate_time_columns,
            }
        )

    return {
        "table_names": table_names,
        "tables": tables,
    }


def _list_tables(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    return [row[0] for row in cur.fetchall()]


def _get_columns(conn: sqlite3.Connection, table: str) -> list[dict[str, str]]:
    quoted = _quote_ident(table)
    cur = conn.execute(f"PRAGMA table_info({quoted})")
    columns: list[dict[str, str]] = []
    for _, name, col_type, _, _, _ in cur.fetchall():
        columns.append({"name": name, "type": col_type or ""})
    return columns


def _count_rows(conn: sqlite3.Connection, table: str) -> int:
    quoted = _quote_ident(table)
    cur = conn.execute(f"SELECT COUNT(*) FROM {quoted}")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _is_time_like(name: str, col_type: str) -> bool:
    name_l = name.lower()
    type_l = (col_type or "").lower()
    for token in ("date", "time", "timestamp"):
        if token in name_l or token in type_l:
            return True
    return False


def _quote_ident(name: str) -> str:
    """Quote SQLite identifier to prevent SQL injection. Nosec: proper escaping."""
    return '"' + name.replace('"', '""') + '"'
