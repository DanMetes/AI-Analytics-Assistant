from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Tuple


class OrdersPolicyV1:
    """
    Deterministic policy for e‑commerce style order datasets.

    This policy is responsible for defining *what* metrics should be computed
    for an orders table and exposing enough metadata for downstream consumers
    (interpreters, the CLI, tests) to understand its expectations.  It does
    **not** interpret any results itself—that is handled by the
    corresponding interpreter.  The goal of this class is to build a
    reproducible set of SQL queries and to provide a contract describing
    expected metrics, required and optional roles, anomaly semantics and
    severity thresholds.

    Key features:
      * Detects the primary table in the SQLite session
      * Infers or accepts user‑provided role assignments for date, order_id,
        customer, product and amount columns
      * Builds queries for totals, average order value, top N customers and
        products, as well as monthly aggregates when a date column exists
      * Describes the policy in a self contained dict including
        anomaly thresholds and emission flags (Option A)
    """

    # Policy identity
    name: str = "orders_v1"
    version: str = "1.0.0"
    description: str = "Domain policy for e‑commerce style orders datasets."

    # No specific required schema fields enforced by the policy registry
    required_fields: List[str] = []

    # Roles supported by this policy.  `requires` roles must be resolvable from
    # the table columns for the policy to operate; `optional` roles are used
    # to enrich metrics when present.
    capabilities = {
        "requires": ["customer", "product", "amount"],
        "optional": ["date", "order_id"],
        "supports": ["top_n", "time_buckets"],
    }

    # Severity thresholds for Option A anomalies.  Interpreters read
    # thresholds from analysis_log["policy"]["severity_thresholds"] but
    # describe_policy() must reflect these defaults to satisfy the
    # contract tests.  Values here mirror those used in the golden tests.
    SEVERITY_THRESHOLDS: Dict[str, Dict[str, float]] = {
        # Top customer revenue share (higher is worse)
        "customer_revenue_share_top1": {"warning": 0.25, "critical": 0.40},
        # Average order value outliers (lower and higher bounds).  These are
        # unused by the current golden tests but included for completeness.
        "aov": {
            "low_warning": 20.0,
            "low_critical": 10.0,
            "high_warning": 500.0,
            "high_critical": 1_000.0,
        },
        # Recent order volume drop (higher drop is worse)
        "order_count_drop_pct": {"warning": 0.30, "critical": 0.50},
    }

    def __init__(self, roles: Optional[Dict[str, List[str]]] = None) -> None:
        # User provided mapping of role names to candidate column names.  If
        # provided, these values guide the role resolution logic in
        # _resolve_roles().  For example:
        #   roles = {"customer": ["buyer_id", "customer_id"], "amount": ["total"]}
        self.roles = roles
        # Final resolved mapping from role -> column name.  This is
        # populated in build_queries() and exposed publicly for debugging.
        self.resolved_roles: Dict[str, str] = {}

    @classmethod
    def describe_policy(cls) -> dict[str, object]:
        """
        Return a self‑describing contract for this policy.  This method is
        consumed by the CLI and tests to understand how the policy behaves.

        The returned dict must include a fixed set of keys (see
        tests/test_policy_describe_contract.py) and reflect Option A
        configuration: anomalies are emitted and normalized anomalies are
        always present (possibly empty).
        """
        return {
            "name": cls.name,
            "version": cls.version,
            "required_roles": list(cls.capabilities.get("requires", [])),
            "optional_roles": list(cls.capabilities.get("optional", [])),
            "expected_metrics": [
                "orders.total_orders",
                "orders.total_revenue",
                "orders.avg_order_value",
                "orders.top_customers_by_revenue_top10",
                "orders.top_products_by_revenue_top10",
                "orders.revenue_by_month (if date)",
                "orders.orders_by_month (if date)",
            ],
            "coverage_behavior": (
                "Requires product, customer and amount; optional date/order_id improve coverage."
            ),
            "anomalies_emitted": [
                "Top customer revenue concentration (>= 25% warning; >= 40% critical)",
                "Average order value outlier (low/high thresholds)",
                "Recent order count drop (>= 30% warning; >= 50% critical)",
            ],
            "severity_thresholds": dict(cls.SEVERITY_THRESHOLDS),
            "emits_anomalies": True,
            "emits_anomalies_normalized": True,
        }

    def build_queries(self, conn: sqlite3.Connection) -> List[Tuple[str, str]]:
        """
        Build a list of (section, SQL) pairs representing the metrics to
        compute.  The order of queries is deterministic and each metric
        uses a stable alias to ensure reproducibility.  Role resolution
        happens once per call.
        """
        table = self._detect_primary_table(conn)
        columns = self._get_columns(conn, table)

        # Resolve roles using explicit user hints and fallbacks
        resolved = self._resolve_roles(columns)
        self.resolved_roles = dict(resolved)

        # Ensure required roles are present
        missing_required = [r for r in ("customer", "product", "amount") if r not in resolved]
        if missing_required:
            raise ValueError(
                "OrdersPolicyV1 missing required roles: "
                + ", ".join(missing_required)
                + f". Available columns: {', '.join(columns)}"
            )

        customer_col = resolved["customer"]
        product_col = resolved["product"]
        amount_col = resolved["amount"]

        order_id_col = resolved.get("order_id")
        date_col = resolved.get("date")

        # Total orders: distinct order_id if available, else row count
        total_orders_expr = (
            f"COUNT(DISTINCT {self._q(order_id_col)})" if order_id_col else "COUNT(*)"
        )

        # Total revenue: sum(amount)
        total_revenue_expr = f"COALESCE(SUM(CAST({self._q(amount_col)} AS REAL)), 0.0)"

        # AOV: total_revenue / total_orders (safe divide)
        aov_expr = (
            f"CASE WHEN {total_orders_expr} = 0 THEN 0.0 "
            f"ELSE ({total_revenue_expr} * 1.0) / ({total_orders_expr} * 1.0) END"
        )

        queries: List[Tuple[str, str]] = []

        # Core totals
        queries.append((
            "orders.total_orders",
            f"SELECT {total_orders_expr} AS value FROM {self._q(table)};",
        ))
        queries.append((
            "orders.total_revenue",
            f"SELECT {total_revenue_expr} AS value FROM {self._q(table)};",
        ))
        queries.append((
            "orders.avg_order_value",
            f"SELECT {aov_expr} AS value FROM {self._q(table)};",
        ))

        # Top customers by revenue (Top 10)
        queries.append((
            "orders.top_customers_by_revenue_top10",
            f"""
                SELECT
                    {self._q(customer_col)} AS customer,
                    COALESCE(SUM(CAST({self._q(amount_col)} AS REAL)), 0.0) AS revenue
                FROM {self._q(table)}
                GROUP BY {self._q(customer_col)}
                ORDER BY revenue DESC
                LIMIT 10;
            """.strip(),
        ))

        # Top products by revenue (Top 10)
        queries.append((
            "orders.top_products_by_revenue_top10",
            f"""
                SELECT
                    {self._q(product_col)} AS product,
                    COALESCE(SUM(CAST({self._q(amount_col)} AS REAL)), 0.0) AS revenue
                FROM {self._q(table)}
                GROUP BY {self._q(product_col)}
                ORDER BY revenue DESC
                LIMIT 10;
            """.strip(),
        ))

        # Time‑bucketed metrics (only if a date column is present)
        if date_col:
            # Month bucket: SQLite strftime('%Y‑%m', dateCol)
            month_expr = f"strftime('%Y-%m', {self._q(date_col)})"

            queries.append((
                "orders.revenue_by_month",
                f"""
                    SELECT
                        {month_expr} AS month,
                        COALESCE(SUM(CAST({self._q(amount_col)} AS REAL)), 0.0) AS revenue
                    FROM {self._q(table)}
                    GROUP BY month
                    ORDER BY month;
                """.strip(),
            ))

            queries.append((
                "orders.orders_by_month",
                f"""
                    SELECT
                        {month_expr} AS month,
                        {total_orders_expr} AS orders
                    FROM {self._q(table)}
                    GROUP BY month
                    ORDER BY month;
                """.strip(),
            ))

            # Top customers by revenue per month (Top 5 per month) using a window function
            queries.append((
                "orders.top_customers_by_revenue_by_month_top5",
                f"""
                    WITH agg AS (
                        SELECT
                            {month_expr} AS month,
                            {self._q(customer_col)} AS customer,
                            COALESCE(SUM(CAST({self._q(amount_col)} AS REAL)), 0.0) AS revenue
                        FROM {self._q(table)}
                        GROUP BY month, customer
                    ), ranked AS (
                        SELECT
                            month, customer, revenue,
                            ROW_NUMBER() OVER (PARTITION BY month ORDER BY revenue DESC) AS rn
                        FROM agg
                    )
                    SELECT month, customer, revenue
                    FROM ranked
                    WHERE rn <= 5
                    ORDER BY month, revenue DESC;
                """.strip(),
            ))

            # Top products by revenue per month (Top 5 per month) using a window function
            queries.append((
                "orders.top_products_by_revenue_by_month_top5",
                f"""
                    WITH agg AS (
                        SELECT
                            {month_expr} AS month,
                            {self._q(product_col)} AS product,
                            COALESCE(SUM(CAST({self._q(amount_col)} AS REAL)), 0.0) AS revenue
                        FROM {self._q(table)}
                        GROUP BY month, product
                    ), ranked AS (
                        SELECT
                            month, product, revenue,
                            ROW_NUMBER() OVER (PARTITION BY month ORDER BY revenue DESC) AS rn
                        FROM agg
                    )
                    SELECT month, product, revenue
                    FROM ranked
                    WHERE rn <= 5
                    ORDER BY month, revenue DESC;
                """.strip(),
            ))

        return queries

    # ----------------------------
    # Internal helpers (private)
    # ----------------------------

    def _detect_primary_table(self, conn: sqlite3.Connection) -> str:
        """
        Inspect the SQLite schema and return the first non‑sqlite system table.
        The ingest pipeline always writes a table called `data`, so this will
        typically return "data".
        """
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        ).fetchall()
        if not rows:
            raise ValueError("No user tables found in SQLite database.")
        return rows[0][0]

    def _get_columns(self, conn: sqlite3.Connection, table: str) -> List[str]:
        """Return the column names for the specified table."""
        rows = conn.execute(f"PRAGMA table_info({self._q(table)});").fetchall()
        cols = [r[1] for r in rows]  # column name at index 1
        if not cols:
            raise ValueError(f"Table '{table}' has no columns.")
        return cols

    def _resolve_roles(self, columns: List[str]) -> Dict[str, str]:
        """
        Determine the mapping from logical role names (customer, product, amount,
        date, order_id) to actual column names.  Explicit user provided
        suggestions (self.roles) are considered first; if none match, a
        synonyms list is used to find appropriate columns.
        """
        cols_lower = {c.lower(): c for c in columns}

        def find_col(candidates: List[str]) -> Optional[str]:
            for cand in candidates:
                if cand.lower() in cols_lower:
                    return cols_lower[cand.lower()]
            return None

        resolved: Dict[str, str] = {}

        # 1) apply explicit user roles first
        if self.roles:
            for role, candidates in self.roles.items():
                if not candidates:
                    continue
                col = find_col(candidates)
                if col:
                    resolved[role] = col

        # 2) fallback synonyms by role
        synonyms: Dict[str, List[str]] = {
            "date": ["order_date", "date", "created_at", "timestamp", "ts", "datetime"],
            "order_id": ["order_id", "id", "order_number", "order_no"],
            "customer": ["customer_id", "customer", "user_id", "buyer_id", "client_id"],
            "product": ["product_id", "product", "sku", "item_id", "item", "product_sku"],
            "amount": ["amount", "total", "revenue", "price", "order_total", "sales"],
        }
        for role, candidates in synonyms.items():
            if role in resolved:
                continue
            col = find_col(candidates)
            if col:
                resolved[role] = col

        return resolved

    def _q(self, identifier: Optional[str]) -> str:
        """Quote SQLite identifier to prevent SQL injection. Nosec: proper escaping."""
        if identifier is None:
            raise ValueError("Internal error: attempted to quote None identifier.")
        return '"' + identifier.replace('"', '""') + '"'