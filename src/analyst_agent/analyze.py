from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .orders_policy import OrdersPolicyV1
from .sales_policy import SalesPolicyV1
from .policy import GenericTabularPolicy, GroupBySpec, Measure
from .policy_registry import PolicyRegistry


def _get_columns(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute("PRAGMA table_info(data);")
    return [row[1] for row in cur.fetchall()]


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _build_select_list(group_exprs: list[str], measures: list[Measure]) -> list[str]:
    parts: list[str] = []
    for i, g in enumerate(group_exprs):
        parts.append(f"{g} AS g{i}")
    for m in measures:
        parts.append(f"{m.sql} AS {m.name}")
    return parts


def _execute_groupby(
    conn: sqlite3.Connection, spec: GroupBySpec, policy: GenericTabularPolicy
) -> tuple[str, list[tuple[Any, ...]]]:
    """
    Build and execute the SQL for a GroupBySpec.
    Returns (sql, rows).
    """
    group_exprs = spec.group_exprs_sql
    measures = spec.measures
    select_list = _build_select_list(group_exprs, measures)

    if spec.top_n_per_time and spec.time_bucket_expr_sql:
        # Windowed top-N per time bucket (after aggregation)
        rank_metric_sql = policy.pick_rank_metric_sql(measures)

        grouped_sql = (
            "WITH grouped AS ("
            f" SELECT {', '.join(select_list)}"
            " FROM data"
            f" GROUP BY {', '.join(group_exprs)}"
            "), ranked AS ("
            " SELECT *,"
            f" ROW_NUMBER() OVER (PARTITION BY g0 ORDER BY {rank_metric_sql} DESC) AS rn"
            " FROM grouped"
            ")"
            " SELECT * FROM ranked"
            f" WHERE rn <= {int(spec.top_n_per_time)}"
            f" ORDER BY g0, {rank_metric_sql} DESC"
            ";"
        )
        rows = conn.execute(grouped_sql).fetchall()
        return grouped_sql, rows

    # Standard grouped query
    sql = f"SELECT {', '.join(select_list)} FROM data GROUP BY {', '.join(group_exprs)} "
    if spec.order_by_sql:
        sql += f"ORDER BY {spec.order_by_sql} "
    else:
        sql += "ORDER BY 1 "
    sql += f"LIMIT {int(spec.limit)};"
    rows = conn.execute(sql).fetchall()
    return sql, rows


def _emit_metrics_rows(
    metrics_rows: list[dict[str, Any]],
    section: str,
    spec: GroupBySpec,
    measures: list[Measure],
    rows: list[tuple[Any, ...]],
) -> None:
    """
    Convert grouped rows into the existing (section,key,value) contract.

    Row format from SQL:
      g0, g1, ..., <measure1>, <measure2>, ...
    """
    metrics_rows.append({"section": section, "key": "group_by", "value": ",".join(spec.group_labels)})

    n_groups = len(spec.group_exprs_sql)
    measure_names = [m.name for m in measures]

    for r in rows[:1000]:
        groups = r[:n_groups]
        values = r[n_groups : n_groups + len(measure_names)]

        group_key_parts = []
        for i, gv in enumerate(groups):
            label = spec.group_labels[i] if i < len(spec.group_labels) else f"g{i}"
            group_key_parts.append(f"{label}={gv}")
        group_key = "|".join(group_key_parts)

        for j, mname in enumerate(measure_names):
            metrics_rows.append(
                {
                    "section": f"{section}_summary",
                    "key": f"{group_key}:{mname}",
                    "value": str(values[j]),
                }
            )


def _emit_query_results(
    metrics_rows: list[dict[str, Any]],
    section: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> None:
    """
    Generic conversion for arbitrary query results into (section,key,value) rows.
    Keys include row index to keep them deterministic and bounded.
    """
    for idx, row in enumerate(rows[:1000]):
        for col_idx, col in enumerate(columns):
            metrics_rows.append({"section": section, "key": f"row{idx}:{col}", "value": str(row[col_idx])})


def _find_first(columns: list[str], candidates: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in columns}
    for cand in candidates:
        lc = cand.lower()
        if lc in lower_map:
            return lower_map[lc]
    return None


def _fallback_candidates_for_policy(policy_cls: type) -> dict[str, list[str]]:
    if policy_cls is OrdersPolicyV1:
        return {
            "date": ["order_date", "date", "created_at", "purchased_at", "timestamp", "ts", "datetime"],
            "order_id": ["order_id", "id", "order_number", "order_no"],
            "customer": ["customer_id", "customer", "user_id", "buyer_id", "client_id"],
            "product": ["product_id", "product", "sku", "item_id", "item", "product_sku"],
            "amount": ["amount", "total", "revenue", "price", "order_total", "sales"],
        }
    if policy_cls.__name__ == "SalesPolicyV1":
        return {
            "product": ["sub_category", "subcategory", "category", "product", "item", "sku"],
            "amount": ["sales", "revenue", "amount", "total"],
            "date": ["order_date", "date", "created_at", "timestamp"],
            "region": ["region", "province", "state", "market"],
            "units": ["units", "quantity", "qty"],
            "profit": ["profit", "margin"],
        }
    return {}


def auto_select_policy(
    columns: list[str], roles: dict[str, list[str]] | None, registry: PolicyRegistry
) -> tuple[str, dict[str, Any]]:
    """
    Deterministically select a policy based on columns and optional role hints.
    Returns (selected_policy_name, selection_log).
    """
    candidates_log: list[dict[str, Any]] = []
    best_name = None
    best_score = -1

    for name in registry.list_policies():
        policy_cls = registry.get_policy(name)
        caps: dict[str, Any] = getattr(policy_cls, "capabilities", {}) or {}
        requires: list[str] = list(caps.get("requires", []) or [])
        optional: list[str] = list(caps.get("optional", []) or [])

        resolved_roles: dict[str, str] = {}
        missing_required: list[str] = []
        reasons: list[str] = []

        fallback = _fallback_candidates_for_policy(policy_cls)

        for role in requires + optional:
            candidates: list[str] = []
            if roles and role in roles:
                candidates.extend(roles[role])
            if role in fallback:
                candidates.extend(fallback[role])

            col = _find_first(columns, candidates)
            if col:
                resolved_roles[role] = col
                reasons.append(f"Resolved {role} -> {col}")
            elif role in requires:
                missing_required.append(role)

        req_resolved = len(requires) - len(missing_required)
        opt_resolved = len([r for r in optional if r in resolved_roles])
        score = req_resolved * 3 + opt_resolved
        eligible = len(missing_required) == 0

        candidates_log.append(
            {
                "name": name,
                "capabilities": caps,
                "resolved_roles": resolved_roles,
                "missing_required_roles": missing_required,
                "eligible": eligible,
                "score": score,
                "reasons": reasons,
            }
        )

        if eligible and score > best_score:
            best_score = score
            best_name = name

    selected = best_name or "generic_tabular"
    selection_log = {"candidates": candidates_log, "selected": selected}
    return selected, selection_log


def run_analysis(
    conn: sqlite3.Connection,
    question: str,
    run_dir: Path,
    *,
    policy_name: str | None = None,
    roles: dict[str, list[str]] | None = None,
    plots: bool = False,
) -> tuple[list[dict[str, Any]], list[str], list[str], dict[str, str], dict[str, Any], str]:
    """
    Policy-driven deterministic analysis engine.

    Returns:
      metrics_rows: for metrics.csv
      queries: SQL executed (for analysis_log.json + reproduce.sql)
      warnings: warnings generated by policy/engine
      resolved_roles: policy-resolved semantic mapping (optional)
    """
    metrics_rows: list[dict[str, Any]] = []
    queries: list[str] = []
    warnings: list[str] = []
    resolved_roles: dict[str, str] = {}  # ALWAYS defined, regardless of policy path
    selection_log: dict[str, Any] = {}

    cols = _get_columns(conn)

    # Always: row count
    q_count = "SELECT COUNT(*) AS n FROM data;"
    queries.append(q_count)
    n = conn.execute(q_count).fetchone()[0]
    metrics_rows.append({"section": "overall", "key": "row_count", "value": str(n)})

    selected_policy = policy_name or "generic_tabular"
    registry = PolicyRegistry()
    if selected_policy == "auto":
        selected_policy, selection_log = auto_select_policy(cols, roles, registry)
    else:
        selection_log = {"selected": selected_policy, "candidates": []}
    try:
        policy_cls = registry.get_policy(selected_policy)
    except KeyError as exc:
        raise KeyError(f"Unknown policy '{selected_policy}'") from exc

    # Instantiate policy with optional roles support for OrdersPolicyV1 / SalesPolicyV1
    if policy_cls is OrdersPolicyV1:
        policy = OrdersPolicyV1(roles=roles)
    elif policy_cls is SalesPolicyV1:
        policy = SalesPolicyV1(roles=roles)
    else:
        policy = policy_cls()  # type: ignore[call-arg]

    # Generic tabular policy path (unchanged behavior)
    if isinstance(policy, GenericTabularPolicy):
        plan = policy.build_plan(columns=cols)
        warnings.extend(plan.warnings)

        for spec in plan.groupbys:
            try:
                sql, rows = _execute_groupby(conn, spec, policy)
                queries.append(sql)
                _emit_metrics_rows(metrics_rows, spec.section, spec, spec.measures, rows)
            except Exception as e:
                warnings.append(f"Failed groupby '{spec.section}': {e}")

    # Query-based policy path
    elif hasattr(policy, "build_queries"):
        # IMPORTANT: build_queries can raise (e.g., missing required roles).
        # We want a clean failure here, not an unbound local later.
        try:
            query_specs = policy.build_queries(conn)  # type: ignore[call-arg]
        except Exception:
            # If the policy had resolved anything before failing, capture it (best-effort).
            if hasattr(policy, "resolved_roles"):
                try:
                    resolved_roles = dict(getattr(policy, "resolved_roles"))
                except Exception:
                    resolved_roles = {}
            raise

        for label, sql in query_specs:
            cur = conn.execute(sql)
            rows = cur.fetchall()
            columns = [c[0] for c in cur.description] if cur.description else []
            queries.append(sql)
            _emit_query_results(metrics_rows, label, columns, rows)

        if hasattr(policy, "resolved_roles"):
            resolved_roles = dict(getattr(policy, "resolved_roles"))

    else:
        warnings.append(f"Policy '{selected_policy}' does not provide a supported interface.")

    if plots:
        warnings.append("Plots requested, but plotting is not implemented in analyze.py (placeholder).")

    return metrics_rows, queries, warnings, resolved_roles, selection_log, selected_policy
