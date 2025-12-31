from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any

from analyst_agent.interpreters import get_interpreter
from analyst_agent.policy_registry import PolicyRegistry


def _ingest_csv_to_sqlite(conn: sqlite3.Connection, csv_path: Path, table: str = "data") -> None:
    rows = list(csv.DictReader(csv_path.read_text(encoding="utf-8").splitlines()))
    assert rows, f"No rows in fixture: {csv_path}"

    cols = list(rows[0].keys())
    col_defs = ", ".join([f'"{c}" TEXT' for c in cols])
    conn.execute(f'CREATE TABLE "{table}" ({col_defs});')

    placeholders = ", ".join(["?"] * len(cols))
    columns_sql = ", ".join([f'"{c}"' for c in cols])
    insert_sql = f'INSERT INTO "{table}" ({columns_sql}) VALUES ({placeholders});'

    values = [[r.get(c, "") for c in cols] for r in rows]
    conn.executemany(insert_sql, values)
    conn.commit()


def _run_fixture(policy_name: str, fixture: str) -> dict[str, Any] | None:
    fixture_path = Path(__file__).parent / "fixtures" / fixture
    conn = sqlite3.connect(":memory:")
    try:
        _ingest_csv_to_sqlite(conn, fixture_path)

        reg = PolicyRegistry()
        policy_cls = reg.get_policy(policy_name)
        policy = policy_cls()  # type: ignore[call-arg]

        interpreter = get_interpreter(policy_name)

        # 🔹 Interpreter-only policies (generic_tabular)
        if not hasattr(policy, "build_queries"):
            interp = interpreter.interpret(
                metrics_rows=[],
                analysis_log={
                    "policy": {
                        "name": policy_name,
                        "version": "test",
                        "severity_thresholds": {},
                        "resolved_roles": {},
                    },
                    "warnings": [],
                },
            )
            return interp.metadata

        # 🔹 SQL-backed policies
        queries = policy.build_queries(conn)  # type: ignore[attr-defined]

        metrics_rows: list[dict[str, Any]] = []
        executed_sql: list[str] = []

        for section, sql in queries:
            executed_sql.append(sql)
            cur = conn.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            for i, row in enumerate(rows):
                for j, col in enumerate(cols):
                    metrics_rows.append(
                        {"section": section, "key": f"{col}[{i}]", "value": row[j]}
                    )

        policy_desc = reg.describe_policy(policy_name)
        analysis_log = {
            "policy": {
                "name": policy_desc.get("name", policy_name),
                "version": policy_desc.get("version", "test"),
                "resolved_roles": {},
                "severity_thresholds": policy_desc.get("severity_thresholds", {}),
            },
            "queries_executed": executed_sql,
            "warnings": [],
        }

        interp = interpreter.interpret(metrics_rows, analysis_log)
        return interp.metadata
    finally:
        conn.close()


def _assert_normalized_shape(anoms: list[dict[str, Any]], expected_policy: str) -> None:
    required_keys = {
        "id",
        "policy",
        "metric",
        "severity",
        "direction",
        "value",
        "threshold",
        "unit",
        "evidence_keys",
        "summary",
    }
    for a in anoms:
        assert set(a.keys()) == required_keys
        assert a["policy"] == expected_policy
        assert a["severity"] in {"info", "warning", "critical"}
        assert a["direction"] in {"high", "low"}
        assert isinstance(a["evidence_keys"], list)
        assert all(isinstance(e, str) for e in a["evidence_keys"])
        assert isinstance(a["threshold"], dict)
        assert set(a["threshold"].keys()) == {"warning", "critical"}
        assert isinstance(a["value"], (int, float))


def test_anomalies_normalized_present_for_all_policies() -> None:
    # sales_v1 should emit normalized anomalies when fixtures trigger them
    meta_sales = _run_fixture("sales_v1", "sales_unit_anomalies.csv")
    assert meta_sales is not None
    assert "anomalies_normalized" in meta_sales
    _assert_normalized_shape(meta_sales["anomalies_normalized"], "sales_v1")

    # orders_v1 should always provide the key with an empty list
    meta_orders = _run_fixture("orders_v1", "orders_normal.csv")
    assert meta_orders is not None
    assert meta_orders.get("anomalies_normalized") == []

    # generic_tabular should always provide the key with an empty list
    meta_generic = _run_fixture("generic_tabular", "sales_normal.csv")
    assert meta_generic is not None
    assert meta_generic.get("anomalies_normalized") == []
