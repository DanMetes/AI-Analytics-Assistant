from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ---- Column role classification ---------------------------------------------


@dataclass(frozen=True)
class ColumnRoles:
    """
    Policy interpretation of the schema (column roles).
    The engine stays dumb; policy decides what to compute.
    """
    time_expr_label: Optional[str]          # e.g., "year"
    time_expr_sql: Optional[str]            # e.g., '"year"' or 'substr("order_date", 1, 4)'

    categoricals: list[str]                 # actual column names
    numerics: list[str]                     # actual column names
    booleans: list[str]                     # actual column names (assumed 0/1)
    ids: list[str]                          # actual column names


# ---- Measures and grouping plans --------------------------------------------


@dataclass(frozen=True)
class Measure:
    """
    A single measure the engine computes: SELECT <sql> AS <name>
    """
    name: str
    sql: str


@dataclass(frozen=True)
class GroupBySpec:
    """
    One grouped query the engine should execute.
    """
    section: str
    group_labels: list[str]         # human labels for grouping columns/expressions
    group_exprs_sql: list[str]      # SQL expressions used for SELECT/GROUP BY
    measures: list[Measure]

    order_by_sql: Optional[str] = None
    limit: int = 250

    # If set, engine should apply windowed top-N per time bucket (after aggregation).
    top_n_per_time: Optional[int] = None
    time_bucket_expr_sql: Optional[str] = None


@dataclass(frozen=True)
class AnalysisPlan:
    groupbys: list[GroupBySpec]
    warnings: list[str]


# ---- GenericTabularPolicy ---------------------------------------------------


class GenericTabularPolicy:
    """
    Domain-agnostic, deterministic policy.

    Behavior:
    - If a time dimension exists, produce:
        (1) time summary
        (2) time x category (top-N per time bucket)
        (3) time x region (top-N per time bucket)
        (4) optional anomaly: negative total profit by region/category if profit exists
    - If no time dimension, produce:
        categorical breakdown by the first categorical

    Metric selection is schema-driven:
    - always COUNT(*)
    - SUM for known numeric columns found
    - AVG for a small number of numerics
    - AVG for boolean 0/1 columns -> rates
    - Derived ratio (profit_margin) if profit + sales/revenue exist and allow_ratios=True
    """

    def __init__(
        self,
        *,
        top_n_per_time: int = 10,
        max_rows_per_groupby: int = 250,
        prefer_time: bool = True,
        allow_ratios: bool = True,
        apply_topn_to: str = "cat+region",  # "cat_only" or "cat+region"
    ) -> None:
        self.top_n_per_time = top_n_per_time
        self.max_rows_per_groupby = max_rows_per_groupby
        self.prefer_time = prefer_time
        self.allow_ratios = allow_ratios
        self.apply_topn_to = apply_topn_to

    capabilities = {
        "requires": [],
        "optional": [],
        "supports": ["groupby", "top_n"],
    }

    @classmethod
    def describe_policy(cls) -> dict[str, object]:
        return {
            "name": "generic_tabular",
            "version": "1.0.0",
            "required_roles": cls.capabilities.get("requires", []),
            "optional_roles": cls.capabilities.get("optional", []),
            "expected_metrics": [
                "overall.row_count",
                "grouped aggregates (categorical/time summaries)",
            ],
            "coverage_behavior": "Heuristic column-role inference; executes available groupbys only.",
            "anomalies_emitted": [],
            "severity_thresholds": {},
            "emits_anomalies": False,
            "emits_anomalies_normalized": False,
        }

    def build_plan(self, *, columns: list[str]) -> AnalysisPlan:
        roles, role_warnings = self.infer_roles(columns)
        groupbys, plan_warnings = self.plan_groupbys(roles)
        return AnalysisPlan(groupbys=groupbys, warnings=role_warnings + plan_warnings)

    # --- role inference ------------------------------------------------------

    def infer_roles(self, columns: list[str]) -> tuple[ColumnRoles, list[str]]:
        warnings: list[str] = []

        # map lowercase -> actual name (first occurrence)
        lower_map: dict[str, str] = {}
        for c in columns:
            lc = c.strip().lower()
            if lc not in lower_map:
                lower_map[lc] = c

        def get(name: str) -> Optional[str]:
            return lower_map.get(name.lower())

        # Time dimension selection
        time_label = None
        time_sql = None
        if get("year"):
            time_label = "year"
            time_sql = f'"{get("year")}"'
        elif get("order_date"):
            time_label = "year"
            time_sql = f'substr("{get("order_date")}", 1, 4)'
        elif get("date"):
            time_label = "year"
            time_sql = f'substr("{get("date")}", 1, 4)'

        # Categorical preferences (by common names)
        cat_priority = ["category", "region", "segment", "sub_category", "city", "state", "country"]
        categoricals: list[str] = []
        for n in cat_priority:
            if get(n):
                categoricals.append(get(n))  # type: ignore[arg-type]

        # Numeric candidates (name-based v1; later can be extended with PRAGMA types)
        num_priority = ["sales", "revenue", "amount", "cost", "profit", "units", "qty", "quantity", "discount"]
        numerics: list[str] = []
        for n in num_priority:
            if get(n):
                numerics.append(get(n))  # type: ignore[arg-type]

        # Boolean candidates (0/1)
        bool_priority = ["returned", "is_returned", "flag", "is_active"]
        booleans: list[str] = []
        for n in bool_priority:
            if get(n):
                booleans.append(get(n))  # type: ignore[arg-type]

        # ID-like candidates
        id_priority = ["order_id", "customer_id", "user_id", "id"]
        ids: list[str] = []
        for n in id_priority:
            if get(n):
                ids.append(get(n))  # type: ignore[arg-type]

        if not time_sql:
            warnings.append("No time-like column detected by GenericTabularPolicy (year/order_date/date).")
        if not categoricals:
            warnings.append("No known categorical columns found (name heuristics).")
        if not numerics:
            warnings.append("No known numeric columns found (name heuristics).")

        return (
            ColumnRoles(
                time_expr_label=time_label,
                time_expr_sql=time_sql,
                categoricals=categoricals,
                numerics=numerics,
                booleans=booleans,
                ids=ids,
            ),
            warnings,
        )

    # --- plan construction ---------------------------------------------------

    def plan_groupbys(self, roles: ColumnRoles) -> tuple[list[GroupBySpec], list[str]]:
        warnings: list[str] = []
        groupbys: list[GroupBySpec] = []

        measures = self._base_measures(roles)

        if self.prefer_time and roles.time_expr_sql:
            t_label = roles.time_expr_label or "time"
            t_expr = roles.time_expr_sql

            # (1) time summary
            groupbys.append(
                GroupBySpec(
                    section="time",
                    group_labels=[t_label],
                    group_exprs_sql=[t_expr],
                    measures=measures,
                    order_by_sql=t_expr,
                    limit=self.max_rows_per_groupby,
                )
            )

            # (2) time x category (if present) - top N per time bucket
            cat = self._find_by_role_name(roles, "category")
            if cat:
                topn = self.top_n_per_time
                groupbys.append(
                    GroupBySpec(
                        section="time_x_category",
                        group_labels=[t_label, "category"],
                        group_exprs_sql=[t_expr, f'"{cat}"'],
                        measures=measures,
                        order_by_sql=None,  # engine will set final order for topN query
                        limit=self.max_rows_per_groupby,
                        top_n_per_time=topn,
                        time_bucket_expr_sql=t_expr,
                    )
                )

            # (3) time x region (if present) - optional topN per time bucket
            region = self._find_by_role_name(roles, "region")
            if region:
                apply_topn = (self.apply_topn_to == "cat+region")
                groupbys.append(
                    GroupBySpec(
                        section="time_x_region",
                        group_labels=[t_label, "region"],
                        group_exprs_sql=[t_expr, f'"{region}"'],
                        measures=measures,
                        order_by_sql=None,
                        limit=self.max_rows_per_groupby,
                        top_n_per_time=self.top_n_per_time if apply_topn else None,
                        time_bucket_expr_sql=t_expr if apply_topn else None,
                    )
                )

            # (4) generic anomaly: if profit exists, flag groups with negative sum_profit
            profit_col = self._find_exact(roles.numerics, "profit")
            if profit_col:
                # Prefer region; else category; else first categorical
                group_col = region or cat or (roles.categoricals[0] if roles.categoricals else None)
                if group_col:
                    neg_measures = self._negative_total_measures(roles)
                    groupbys.append(
                        GroupBySpec(
                            section="anomaly_negative_profit",
                            group_labels=["group"],
                            group_exprs_sql=[f'"{group_col}"'],
                            measures=neg_measures,
                            order_by_sql=f'SUM("{profit_col}") ASC',
                            limit=20,
                        )
                    )
        else:
            # No time; do a single categorical breakdown if possible
            if not roles.categoricals:
                warnings.append("No time dimension and no categoricals; only row_count should be produced.")
                return groupbys, warnings

            first_cat = roles.categoricals[0]
            groupbys.append(
                GroupBySpec(
                    section="categorical",
                    group_labels=["group"],
                    group_exprs_sql=[f'"{first_cat}"'],
                    measures=measures,
                    order_by_sql=self._default_order(measures),
                    limit=self.max_rows_per_groupby,
                )
            )

        return groupbys, warnings

    # --- measure selection ---------------------------------------------------

    def _base_measures(self, roles: ColumnRoles) -> list[Measure]:
        out: list[Measure] = [Measure("n", "COUNT(*)")]

        # Sums for known numerics
        for c in roles.numerics:
            out.append(Measure(f"sum_{c.lower()}", f'SUM("{c}")'))

        # Averages for up to 3 numerics (keeps output bounded)
        for c in roles.numerics[:3]:
            out.append(Measure(f"avg_{c.lower()}", f'AVG("{c}")'))

        # Boolean rates
        for b in roles.booleans:
            out.append(Measure(f"rate_{b.lower()}", f'AVG("{b}")'))

        # Derived ratios: profit_margin if profit + sales/revenue exist
        if self.allow_ratios:
            profit = self._find_exact(roles.numerics, "profit")
            sales = self._find_exact(roles.numerics, "sales") or self._find_exact(roles.numerics, "revenue") or self._find_exact(roles.numerics, "amount")
            if profit and sales:
                out.append(
                    Measure(
                        "profit_margin",
                        f'CASE WHEN SUM("{sales}") = 0 THEN NULL ELSE (SUM("{profit}") * 1.0 / SUM("{sales}")) END',
                    )
                )

        return out

    def _negative_total_measures(self, roles: ColumnRoles) -> list[Measure]:
        out: list[Measure] = [Measure("n", "COUNT(*)")]
        profit = self._find_exact(roles.numerics, "profit")
        if profit:
            out.append(Measure("sum_profit", f'SUM("{profit}")'))

        sales = self._find_exact(roles.numerics, "sales") or self._find_exact(roles.numerics, "revenue") or self._find_exact(roles.numerics, "amount")
        if sales:
            out.append(Measure("sum_sales", f'SUM("{sales}")'))

        if self.allow_ratios and profit and sales:
            out.append(
                Measure(
                    "profit_margin",
                    f'CASE WHEN SUM("{sales}") = 0 THEN NULL ELSE (SUM("{profit}") * 1.0 / SUM("{sales}")) END',
                )
            )
        return out

    # --- ranking / ordering --------------------------------------------------

    def pick_rank_metric_sql(self, measures: list[Measure]) -> str:
        """
        Deterministic default ranking metric for top-N tables:
          sum_sales / sum_revenue / sum_amount, else sum_profit, else n.
        """
        for m in measures:
            if m.name in ("sum_sales", "sum_revenue", "sum_amount"):
                return m.sql
        for m in measures:
            if m.name == "sum_profit":
                return m.sql
        for m in measures:
            if m.name == "n":
                return m.sql
        return measures[0].sql

    def _default_order(self, measures: list[Measure]) -> str:
        return f"{self.pick_rank_metric_sql(measures)} DESC"

    # --- helpers -------------------------------------------------------------

    def _find_exact(self, cols: list[str], wanted: str) -> Optional[str]:
        for c in cols:
            if c.lower() == wanted.lower():
                return c
        return None

    def _find_by_role_name(self, roles: ColumnRoles, wanted: str) -> Optional[str]:
        """
        Find a categorical column by intended role name.
        In v1 we match exact common names if present.
        """
        for c in roles.categoricals:
            if c.lower() == wanted.lower():
                return c
        return None
