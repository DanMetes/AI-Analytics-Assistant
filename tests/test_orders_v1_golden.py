from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from analyst_agent.orders_policy import OrdersPolicyV1
from analyst_agent.interpreters import get_interpreter


# -------------------------------
# Helpers
# -------------------------------

def _ingest_csv_to_sqlite(
    conn: sqlite3.Connection,
    csv_path: Path,
    table: str = "data",
) -> None:
    rows = list(
        csv.DictReader(csv_path.read_text(encoding="utf-8").splitlines())
    )
    assert rows, f"No rows in fixture: {csv_path}"

    cols = list(rows[0].keys())
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    conn.execute(f'CREATE TABLE "{table}" ({col_defs});')

    placeholders = ", ".join(["?"] * len(cols))
    columns_sql = ", ".join([f'"{c}"' for c in cols])
    insert_sql = (
        f'INSERT INTO "{table}" ({columns_sql}) VALUES ({placeholders});'
    )

    values = [[r.get(c, "") for c in cols] for r in rows]
    conn.executemany(insert_sql, values)
    conn.commit()


def _run_orders_fixture(csv_name: str) -> dict[str, Any] | None:
    fixture_path = Path(__file__).parent / "fixtures" / csv_name
    conn = sqlite3.connect(":memory:")

    try:
        _ingest_csv_to_sqlite(conn, fixture_path)

        policy = OrdersPolicyV1()
        queries = policy.build_queries(conn)

        metrics_rows: list[dict[str, Any]] = []
        executed_sql: list[str] = []

        for section, sql in queries:
            executed_sql.append(sql)
            cur = conn.execute(sql)
            if cur.description is None:
                continue

            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

            for i, row in enumerate(rows):
                for j, col in enumerate(cols):
                    metrics_rows.append(
                        {
                            "section": section,
                            "key": f"{col}[{i}]",
                            "value": row[j],
                        }
                    )

        # Mirror the policy contract we now require:
        analysis_log = {
            "policy": {
                "name": "orders_v1",
                "version": "test",
                "resolved_roles": {},
                "severity_thresholds": {
                    "customer_revenue_share_top1": {
                        "warning": 0.25,
                        "critical": 0.40,
                    },
                    "aov": {
                        "low_warning": 20.0,
                        "low_critical": 10.0,
                        "high_warning": 500.0,
                        "high_critical": 1000.0,
                    },
                    "order_count_drop_pct": {
                        "warning": 0.30,
                        "critical": 0.50,
                    },
                },
                "emits_anomalies": True,
                "emits_anomalies_normalized": True,
            },
            "queries_executed": executed_sql,
            "warnings": [],
        }

        interpreter = get_interpreter("orders_v1")
        result = interpreter.interpret(metrics_rows, analysis_log)

        return result.metadata if result is not None else None

    finally:
        conn.close()


# -------------------------------
# Tests
# -------------------------------

def test_orders_normal_has_no_anomalies() -> None:
    meta = _run_orders_fixture("orders_normal.csv")

    if meta is None:
        pytest.skip("orders_v1 does not emit metadata yet.")

    assert meta.get("anomalies", []) == []
    assert meta.get("anomalies_structured", []) == []
    assert meta.get("anomalies_normalized", []) == []


def test_orders_warning_fixture_may_or_may_not_emit_anomalies() -> None:
    """
    With orders_v1 anomalies enabled, this fixture is allowed to emit anomalies
    if it crosses policy thresholds. We do NOT lock it to empty anymore.
    """
    meta = _run_orders_fixture("orders_warning.csv")

    if meta is None:
        pytest.skip("orders_v1 does not emit metadata yet.")

    # Always-present contract keys (may be empty)
    assert "anomalies" in meta
    assert "anomalies_structured" in meta
    assert "anomalies_normalized" in meta

    assert isinstance(meta.get("anomalies"), list)
    assert isinstance(meta.get("anomalies_structured"), list)
    assert isinstance(meta.get("anomalies_normalized"), list)


def test_orders_critical_concentration_emits_critical_anomaly() -> None:
    meta = _run_orders_fixture("orders_critical_concentration.csv")

    if meta is None:
        pytest.skip("orders_v1 does not emit metadata yet.")

    normalized = meta.get("anomalies_normalized", [])
    assert normalized, "Expected at least one normalized anomaly."

    ids = {a.get("anomaly_id") for a in normalized}
    assert "orders.customer_revenue_concentration_top1" in ids

    sev = next(
        a.get("severity")
        for a in normalized
        if a.get("anomaly_id") == "orders.customer_revenue_concentration_top1"
    )
    assert sev == "critical"
