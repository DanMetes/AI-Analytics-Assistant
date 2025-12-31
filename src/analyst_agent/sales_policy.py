from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Tuple


class SalesPolicyV1:
    """
    Domain policy for retail sales summary datasets (category/product + sales over time/geo).
    Deterministic and SQLite-native.
    """

    name = "sales_v1"
    version = "1.0.0"
    description = "Domain policy for retail sales summary datasets (category/product + sales over time/geo)."
    required_fields: list[str] = []

    # Step B: policy-owned severity thresholds (interpreter must CONSUME these; not define them)
    SEVERITY_THRESHOLDS = {
        # Higher is worse
        "revenue_concentration_share": {"warning": 0.30, "critical": 0.50},
        "unit_concentration_share": {"warning": 0.70, "critical": 0.90},
        "unit_revenue_high": {"warning": 10_000.0, "critical": 50_000.0},
        # Lower is worse
        "profit_margin": {"warning": 0.10, "critical": 0.05},
        "sales_trend_change": {"warning": -0.10, "critical": -0.25},
        "unit_revenue_low": {"warning": 1.0, "critical": 0.1},
    }

    capabilities = {
        "requires": ["product", "amount"],
        "optional": ["date", "region", "units", "profit"],
        "supports": ["top_n", "time_buckets"],
    }

    def __init__(self, roles: Optional[Dict[str, List[str]]] = None) -> None:
        self.roles = roles or {}
        self.resolved_roles: Dict[str, str] = {}

    @classmethod
    def describe_policy(cls) -> dict[str, object]:
        return {
            "name": cls.name,
            "version": cls.version,
            "required_roles": list(cls.capabilities.get("requires", [])),
            "optional_roles": list(cls.capabilities.get("optional", [])),
            "expected_metrics": [
                "sales.total_sales",
                "sales.total_profit (if profit)",
                "sales.total_units (if units)",
                "sales.avg_unit_revenue (if units)",
                "sales.top_products_by_sales_top10",
                "sales.top_products_by_units_top10 (if units)",
                "sales.sales_by_month (if date)",
                "sales.top_products_by_sales_by_month_top5 (if date)",
                "sales.sales_by_region (if region)",
            ],
            "coverage_behavior": "Requires product and amount; optional roles enrich time/region/unit/profit metrics.",
            "anomalies_emitted": [
                "Revenue concentration share (>= 30% warning; >= 50% critical)",
                "Profit margin (<= 10% warning; <= 5% critical)",
                "Sales trend change (<= -10% warning; <= -25% critical)",
                "Avg unit revenue too low (<= 1.0 warning; <= 0.1 critical)",
                "Avg unit revenue too high (>= 10,000 warning; >= 50,000 critical)",
                "Unit concentration share (>= 70% warning; >= 90% critical)",
            ],
            # Step B contract key (preferred)
            "severity_thresholds": cls.SEVERITY_THRESHOLDS,
            "emits_anomalies": True,
            "emits_anomalies_normalized": True,
        }

    def build_queries(self, conn: sqlite3.Connection) -> List[Tuple[str, str]]:
        table = self._detect_primary_table(conn)
        columns = self._get_columns(conn, table)

        resolved = self._resolve_roles(columns)
        self.resolved_roles = dict(resolved)

        required = ["product", "amount"]
        missing = [r for r in required if r not in resolved]
        if missing:
            raise ValueError(
                "SalesPolicyV1 missing required roles: "
                + ", ".join(missing)
                + f". Available columns: {', '.join(columns)}"
            )

        product_col = resolved["product"]
        amount_col = resolved["amount"]
        date_col = resolved.get("date")
        region_col = resolved.get("region")
        units_col = resolved.get("units")
        profit_col = resolved.get("profit")

        queries: List[Tuple[str, str]] = []

        # Totals
        queries.append(
            (
                "sales.total_sales",
                f"SELECT SUM({self._q(amount_col)}) AS total_sales FROM {self._q(table)};",
            )
        )

        if profit_col:
            queries.append(
                (
                    "sales.total_profit",
                    f"SELECT SUM({self._q(profit_col)}) AS total_profit FROM {self._q(table)};",
                )
            )

        if units_col:
            queries.append(
                (
                    "sales.total_units",
                    f"SELECT SUM({self._q(units_col)}) AS total_units FROM {self._q(table)};",
                )
            )
            queries.append(
                (
                    "sales.avg_unit_revenue",
                    f"""
                    SELECT
                        CASE
                            WHEN SUM({self._q(units_col)}) = 0 THEN NULL
                            ELSE SUM({self._q(amount_col)}) * 1.0 / SUM({self._q(units_col)})
                        END AS avg_unit_revenue
                    FROM {self._q(table)};
                    """.strip(),
                )
            )

        # Top products
        queries.append(
            (
                "sales.top_products_by_sales_top10",
                f"""
                SELECT
                    {self._q(product_col)} AS product,
                    SUM({self._q(amount_col)}) AS sales
                FROM {self._q(table)}
                GROUP BY {self._q(product_col)}
                ORDER BY sales DESC
                LIMIT 10;
                """.strip(),
            )
        )

        if units_col:
            queries.append(
                (
                    "sales.top_products_by_units_top10",
                    f"""
                    SELECT
                        {self._q(product_col)} AS product,
                        SUM({self._q(units_col)}) AS units
                    FROM {self._q(table)}
                    GROUP BY {self._q(product_col)}
                    ORDER BY units DESC
                    LIMIT 10;
                    """.strip(),
                )
            )

        # Time-based
        if date_col:
            month_expr = f"strftime('%Y-%m', {self._q(date_col)})"
            queries.append(
                (
                    "sales.sales_by_month",
                    f"""
                    SELECT
                        {month_expr} AS month,
                        SUM({self._q(amount_col)}) AS sales
                    FROM {self._q(table)}
                    GROUP BY month
                    ORDER BY month;
                    """.strip(),
                )
            )

            queries.append(
                (
                    "sales.top_products_by_sales_by_month_top5",
                    f"""
                    WITH agg AS (
                        SELECT
                            {month_expr} AS month,
                            {self._q(product_col)} AS product,
                            SUM({self._q(amount_col)}) AS sales
                        FROM {self._q(table)}
                        GROUP BY month, product
                    ), ranked AS (
                        SELECT
                            month, product, sales,
                            ROW_NUMBER() OVER (PARTITION BY month ORDER BY sales DESC) AS rn
                        FROM agg
                    )
                    SELECT month, product, sales
                    FROM ranked
                    WHERE rn <= 5
                    ORDER BY month, sales DESC;
                    """.strip(),
                )
            )

        # Region-based
        if region_col:
            queries.append(
                (
                    "sales.sales_by_region",
                    f"""
                    SELECT
                        {self._q(region_col)} AS region,
                        SUM({self._q(amount_col)}) AS sales
                    FROM {self._q(table)}
                    GROUP BY {self._q(region_col)}
                    ORDER BY sales DESC;
                    """.strip(),
                )
            )

        return queries

    # ----------------------------
    # Internals
    # ----------------------------

    def _detect_primary_table(self, conn: sqlite3.Connection) -> str:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        ).fetchall()
        if not rows:
            raise ValueError("No user tables found in SQLite database.")
        return rows[0][0]

    def _get_columns(self, conn: sqlite3.Connection, table: str) -> List[str]:
        rows = conn.execute(f"PRAGMA table_info({self._q(table)});").fetchall()
        cols = [r[1] for r in rows]
        if not cols:
            raise ValueError(f"Table '{table}' has no columns.")
        return cols

    def _resolve_roles(self, columns: List[str]) -> Dict[str, str]:
        cols_lower = {c.lower(): c for c in columns}

        def find_col(candidates: List[str]) -> Optional[str]:
            for cand in candidates:
                if cand.lower() in cols_lower:
                    return cols_lower[cand.lower()]
            return None

        candidates_map: Dict[str, List[str]] = {
            "product": ["sub_category", "subcategory", "category", "product", "item", "sku"],
            "amount": ["sales", "revenue", "amount", "total"],
            "date": ["order_date", "date", "created_at", "timestamp"],
            "region": ["region", "province", "state", "market"],
            "units": ["units", "quantity", "qty"],
            "profit": ["profit", "margin"],
        }

        resolved: Dict[str, str] = {}

        # explicit roles first
        for role, candidates in self.roles.items():
            col = find_col(candidates)
            if col:
                resolved[role] = col

        # fallbacks
        for role, candidates in candidates_map.items():
            if role in resolved:
                continue
            col = find_col(candidates)
            if col:
                resolved[role] = col

        return resolved

    def _q(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'
