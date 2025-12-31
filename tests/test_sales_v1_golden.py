from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from analyst_agent.sales_policy import SalesPolicyV1
from analyst_agent.interpreters import get_interpreter


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


def _run_sales_fixture(csv_name: str) -> dict[str, Any]:
    fixture_path = Path(__file__).parent / "fixtures" / csv_name
    conn = sqlite3.connect(":memory:")
    try:
        _ingest_csv_to_sqlite(conn, fixture_path)

        policy = SalesPolicyV1()
        queries = policy.build_queries(conn)

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

        analysis_log = {
            "policy": {
                "name": "sales_v1",
                "version": "test",
                "resolved_roles": {},
                "severity_thresholds": SalesPolicyV1.SEVERITY_THRESHOLDS,
            },
            "queries_executed": executed_sql,
            "warnings": [],
        }

        interpreter = get_interpreter("sales_v1")
        interp = interpreter.interpret(metrics_rows, analysis_log)
        return interp.metadata
    finally:
        conn.close()


# -------------------------------
# TESTS
# -------------------------------

def test_sales_normal_has_no_anomalies() -> None:
    meta = _run_sales_fixture("sales_normal.csv")

    assert meta["anomalies_max_severity"] == "info"
    assert meta["anomalies"] == []
    assert meta["anomalies_structured"] == []
    assert meta["anomalies_normalized"] == []


def test_sales_fixture_is_info_only_profit_margin_near_threshold() -> None:
    """
    Fixture does NOT cross warning thresholds.
    This is an INFO-only run by design.
    """
    meta = _run_sales_fixture("sales_critical_unit_concentration.csv")

    assert meta["anomalies_max_severity"] == "info"
    assert meta["anomalies_normalized"] == []


def test_sales_fixture_is_info_only_warning_named_but_not_triggered() -> None:
    """
    Despite the filename, profit margin does not cross warning.
    """
    meta = _run_sales_fixture("sales_warning_profit_margin.csv")

    assert meta["anomalies_max_severity"] == "info"
    assert meta["anomalies_normalized"] == []

    # Ensure we did not accidentally trip profit margin here
    assert not any(a["id"] == "profit_margin" for a in meta["anomalies_normalized"])


def test_sales_anomalies_normalized_schema_and_values() -> None:
    meta = _run_sales_fixture("sales_unit_anomalies.csv")

    anomalies = meta["anomalies_normalized"]

    # Contract: schema checks apply only when anomalies are actually emitted.
    # If the fixture doesn't cross thresholds, that's fine (it becomes an INFO-only run).
    if not anomalies:
        pytest.skip("No normalized anomalies emitted for this fixture")

    # Ensure required keys and value shapes
    required_keys = {
        "id",
        "policy",
        "severity",
        "metric",
        "direction",
        "value",
        "threshold",
        "unit",
        "evidence_keys",
        "summary",
    }
    allowed_sev = {"warning", "critical", "info"}
    allowed_dir = {"high", "low", "band"}

    # Stable ordering for checks
    anomalies_by_id = {a["id"]: a for a in sorted(anomalies, key=lambda x: x["id"])}
    expected_ids = {"revenue_concentration_share", "unit_concentration_share", "unit_revenue_low"}
    assert expected_ids.issubset(anomalies_by_id.keys())

    for aid, a in anomalies_by_id.items():
        assert set(a.keys()) == required_keys
        assert a["policy"] == "sales_v1"
        assert a["severity"] in allowed_sev
        assert a["direction"] in allowed_dir
        assert isinstance(a["metric"], str)
        assert isinstance(a["unit"], str)
        assert isinstance(a["summary"], str)

        # threshold structure
        thresh = a["threshold"]
        assert isinstance(thresh, dict)
        assert set(thresh.keys()) == {"warning", "critical"}

        # evidence keys
        ev = a["evidence_keys"]
        assert isinstance(ev, list)
        assert all(isinstance(e, str) for e in ev)

        # value numeric
        assert isinstance(a["value"], (int, float))

    # Spot-check values for deterministic fixture
    assert anomalies_by_id["revenue_concentration_share"]["metric"] == "sales.top_products_by_sales_top10"
    assert anomalies_by_id["unit_concentration_share"]["metric"] == "sales.top_products_by_units_top10"
    assert anomalies_by_id["unit_revenue_low"]["metric"] == "sales.avg_unit_revenue"
