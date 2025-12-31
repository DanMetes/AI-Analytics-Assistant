from __future__ import annotations

from typing import Any, Dict, List

from .base import Finding, Interpretation, Interpreter
from .generic_tabular import _parse_sections, _to_number
from analyst_agent.anomalies import make_normalized_anomaly


def _severity_from_thresholds(value: float, thresholds: dict) -> str:
    """
    Deterministic severity mapping for "higher is worse" metrics:
      - critical if value >= critical
      - warning  if value >= warning
      - info otherwise
    """
    crit = thresholds.get("critical")
    warn = thresholds.get("warning")

    if crit is not None and value >= crit:
        return "critical"
    if warn is not None and value >= warn:
        return "warning"
    return "info"


def _severity_from_thresholds_low(value: float, thresholds: dict) -> str:
    """
    Deterministic severity mapping for "lower is worse" metrics:
      - critical if value <= critical
      - warning  if value <= warning
      - info otherwise
    """
    crit = thresholds.get("critical")
    warn = thresholds.get("warning")

    if crit is not None and value <= crit:
        return "critical"
    if warn is not None and value <= warn:
        return "warning"
    return "info"


def _severity_rank(sev: str) -> int:
    return {"info": 0, "warning": 1, "critical": 2}.get(sev, 0)


class SalesInterpreter(Interpreter):
    """
    Interpreter for sales_v1 policy outputs.

    Key goals:
      - Deterministic, evidence-backed findings (no guessing).
      - Step B: thresholds are policy-owned; interpreter consumes them from analysis_log["policy"].
      - Human-readable anomalies in report.md AND structured anomalies in interpretation.json.
    """

    EXPLAINABILITY = {
        "anomalies_emitted": [
            "Revenue concentration share (>= 30% warning; >= 50% critical)",
            "Profit margin (<= 10% warning; <= 5% critical)",
            "Sales trend change (<= -10% warning; <= -25% critical)",
            "Avg unit revenue too low (<= 1.0 warning; <= 0.1 critical)",
            "Avg unit revenue too high (>= 10,000 warning; >= 50,000 critical)",
            "Unit concentration share (>= 70% warning; >= 90% critical)",
        ],
        "expected_signals": [
            "sales.total_sales",
            "sales.total_profit (if profit)",
            "sales.total_units (if units)",
            "sales.avg_unit_revenue (if units)",
            "sales.top_products_by_sales_top10",
            "sales.top_products_by_units_top10 (if units)",
            "sales.sales_by_month (if date)",
            "sales.sales_by_region (if region)",
        ],
    }

    def interpret(self, metrics_rows: list[dict[str, Any]], analysis_log: dict) -> Interpretation:
        sections = _parse_sections(metrics_rows)
        findings: list[Finding] = []
        caveats: list[str] = []

        total_sales = _to_number(self._first_value(sections, "sales.total_sales", "total_sales"))
        total_profit = _to_number(self._first_value(sections, "sales.total_profit", "total_profit"))
        total_units = _to_number(self._first_value(sections, "sales.total_units", "total_units"))
        avg_unit_rev = _to_number(self._first_value(sections, "sales.avg_unit_revenue", "avg_unit_revenue"))

        if total_sales is not None:
            findings.append(
                Finding(
                    severity="info",
                    title="Total sales",
                    text=f"Total sales: {total_sales:.2f}",
                    evidence_keys=["sales.total_sales.total_sales"],
                )
            )
        if total_profit is not None:
            findings.append(
                Finding(
                    severity="info",
                    title="Total profit",
                    text=f"Total profit: {total_profit:.2f}",
                    evidence_keys=["sales.total_profit.total_profit"],
                )
            )
        if total_sales not in (None, 0) and total_profit is not None:
            findings.append(
                Finding(
                    severity="info",
                    title="Profit margin",
                    text=f"Profit margin: {total_profit / total_sales:.1%}",
                    evidence_keys=["sales.total_profit.total_profit", "sales.total_sales.total_sales"],
                )
            )
        if total_units is not None:
            findings.append(
                Finding(
                    severity="info",
                    title="Total units",
                    text=f"Total units: {int(total_units)}",
                    evidence_keys=["sales.total_units.total_units"],
                )
            )

        top_product_row = self._first_row(sections, "sales.top_products_by_sales_top10")
        top_units_row = self._first_row(sections, "sales.top_products_by_units_top10")

        if top_product_row:
            prod = top_product_row.get("product")
            sales = _to_number(top_product_row.get("sales"))
            if prod and sales is not None and total_sales not in (None, 0):
                findings.append(
                    Finding(
                        severity="info",
                        title="Top product concentration",
                        text=f"Top product {prod} contributes {sales / total_sales:.1%} of sales.",
                        evidence_keys=["sales.top_products_by_sales_top10.sales", "sales.total_sales.total_sales"],
                    )
                )
            elif prod:
                findings.append(
                    Finding(
                        severity="info",
                        title="Top product",
                        text=f"Top product: {prod}.",
                        evidence_keys=["sales.top_products_by_sales_top10.product"],
                    )
                )

        region_row = self._first_row(sections, "sales.sales_by_region")
        if region_row:
            region = region_row.get("region")
            sales = _to_number(region_row.get("sales"))
            if region and sales is not None and total_sales not in (None, 0):
                findings.append(
                    Finding(
                        severity="info",
                        title="Top region concentration",
                        text=f"Top region {region} contributes {sales / total_sales:.1%} of sales.",
                        evidence_keys=["sales.sales_by_region.sales", "sales.total_sales.total_sales"],
                    )
                )
            elif region:
                findings.append(
                    Finding(
                        severity="info",
                        title="Top region",
                        text=f"Top region: {region}.",
                        evidence_keys=["sales.sales_by_region.region"],
                    )
                )

        month_rows = sections.get("sales.sales_by_month") or []
        if len(month_rows) >= 2:
            first = _to_number(month_rows[0].get("sales"))
            last = _to_number(month_rows[-1].get("sales"))
            if first is not None and last is not None and first != 0:
                change = (last - first) / first
                findings.append(
                    Finding(
                        severity="info",
                        title="Sales trend",
                        text=f"Sales change from first to last month: {change:.1%}.",
                        evidence_keys=["sales.sales_by_month.sales[first]", "sales.sales_by_month.sales[last]"],
                    )
                )

        warnings = analysis_log.get("warnings") or []
        for w in warnings:
            caveats.append(str(w))

        if not findings:
            findings.append(
                Finding(severity="info", title="No specific findings", text="No specific findings.", evidence_keys=[])
            )

        metadata = self._compute_metadata(
            metrics_rows=metrics_rows,
            analysis_log=analysis_log,
            sections=sections,
            total_sales=total_sales,
            total_profit=total_profit,
            total_units=total_units,
            avg_unit_revenue=avg_unit_rev,
            month_rows=month_rows,
            top_product_row=top_product_row,
            top_units_row=top_units_row,
        )

        return Interpretation(findings=findings, caveats=caveats, metadata=metadata)

    def _first_row(self, sections: Dict[str, List[Dict[str, Any]]], name: str) -> Dict[str, Any] | None:
        rows = sections.get(name) or []
        return rows[0] if rows else None

    def _first_value(self, sections: Dict[str, List[Dict[str, Any]]], section: str, col: str) -> Any:
        row = self._first_row(sections, section)
        if not row:
            return None
        return row.get(col)

    def _compute_expected_metrics(self, analysis_log: dict) -> list[str]:
        # Make coverage role-aware so "missing" doesn't punish absent optional roles.
        resolved_roles = (analysis_log.get("policy") or {}).get("resolved_roles") or {}

        expected = ["sales.total_sales", "sales.top_products_by_sales_top10"]

        if "profit" in resolved_roles:
            expected.append("sales.total_profit")

        if "units" in resolved_roles:
            expected.extend(["sales.total_units", "sales.avg_unit_revenue", "sales.top_products_by_units_top10"])

        if "date" in resolved_roles:
            expected.append("sales.sales_by_month")

        if "region" in resolved_roles:
            expected.append("sales.sales_by_region")

        return expected

    def _require_policy_thresholds(self, analysis_log: dict) -> dict:
        policy = analysis_log.get("policy") or {}
        thresholds = policy.get("severity_thresholds") or policy.get("thresholds")
        if not isinstance(thresholds, dict) or not thresholds:
            raise ValueError(
                "Policy thresholds missing from analysis_log['policy']. "
                "Expected 'severity_thresholds' (preferred) or 'thresholds'."
            )
        return thresholds

    def _compute_metadata(
        self,
        metrics_rows: list[dict[str, Any]],
        analysis_log: dict,
        sections: Dict[str, List[Dict[str, Any]]],
        total_sales: float | None,
        total_profit: float | None,
        total_units: float | None,
        avg_unit_revenue: float | None,
        month_rows: List[Dict[str, Any]],
        top_product_row: Dict[str, Any] | None,
        top_units_row: Dict[str, Any] | None,
    ) -> dict[str, object]:
        thresholds = self._require_policy_thresholds(analysis_log)
        policy_name = (analysis_log.get("policy") or {}).get("name")

        expected_metrics = self._compute_expected_metrics(analysis_log)

        present_sections = {str(r.get("section")) for r in metrics_rows if r.get("section")}
        present = [m for m in expected_metrics if m in present_sections]
        missing = [m for m in expected_metrics if m not in present_sections]
        coverage_ratio = len(present) / len(expected_metrics) if expected_metrics else 0.0

        aggregate_conf = (
            "high"
            if len(present) >= max(3, int(0.6 * len(expected_metrics)))
            else "medium"
            if len(present) >= 2
            else "low"
        )

        trend_conf = "none"
        if "sales.sales_by_month" in present_sections:
            trend_conf = "high" if len(sections.get("sales.sales_by_month") or []) >= 2 else "medium"

        anomalies_structured: list[dict[str, object]] = []
        anomalies_normalized: list[dict[str, object]] = []
        anomalies_text: list[str] = []
        max_sev = "info"

        def _add_anomaly(
            severity: str,
            title: str,
            text: str,
            evidence_keys: list[str],
            normalized: dict[str, object] | None = None,
        ) -> None:
            nonlocal max_sev

            anomalies_structured.append(
                {"severity": severity, "title": title, "text": text, "evidence_keys": evidence_keys}
            )
            anomalies_text.append(f"[{severity.upper()}] {title}: {text}")
            if _severity_rank(severity) > _severity_rank(max_sev):
                max_sev = severity
            if normalized:
                anomalies_normalized.append(normalized)

        def _get_threshold(anomaly_id: str) -> dict:
            if anomaly_id not in thresholds:
                raise ValueError(f"Missing severity thresholds for anomaly '{anomaly_id}' in policy '{policy_name}'.")
            return thresholds[anomaly_id]

        # Only emit anomalies if coverage is reasonably complete (prevents noisy guesses).
        coverage_ok = (coverage_ratio >= 0.7) or (len(present) >= 3 and len(expected_metrics) <= 4)

        if coverage_ok:
            # Revenue concentration
            if top_product_row and total_sales not in (None, 0):
                prod = top_product_row.get("product")
                prod_sales = _to_number(top_product_row.get("sales"))
                if prod and prod_sales is not None:
                    share = prod_sales / total_sales
                    thresh = _get_threshold("revenue_concentration_share")
                    sev = _severity_from_thresholds(share, thresh)
                    if sev != "info":
                        _add_anomaly(
                            severity=sev,
                            title="Revenue concentration",
                            text=f"Top product {prod} contributes {share:.1%} of sales.",
                            evidence_keys=["sales.top_products_by_sales_top10.sales", "sales.total_sales.total_sales"],
                            normalized=make_normalized_anomaly(
                                anomaly_id="revenue_concentration_share",
                                policy=policy_name or "sales_v1",
                                metric="sales.top_products_by_sales_top10",
                                severity=sev,  # type: ignore[arg-type]
                                direction="high",
                                value=share,
                                threshold=thresh,
                                evidence_keys=[
                                    "sales.top_products_by_sales_top10.sales",
                                    "sales.total_sales.total_sales",
                                ],
                                summary=f"Top product {prod} contributes {share:.1%} of sales.",
                                unit="share",
                            ),
                        )

            # Profit margin
            if total_sales not in (None, 0) and total_profit is not None:
                margin = total_profit / total_sales
                thresh = _get_threshold("profit_margin")
                sev = _severity_from_thresholds_low(margin, thresh)
                if sev != "info":
                    _add_anomaly(
                        severity=sev,
                        title="Low profit margin",
                        text=f"Profit margin is {margin:.1%}.",
                        evidence_keys=["sales.total_profit.total_profit", "sales.total_sales.total_sales"],
                        normalized=make_normalized_anomaly(
                            anomaly_id="profit_margin",
                            policy=policy_name or "sales_v1",
                            metric="sales.total_profit",
                            severity=sev,  # type: ignore[arg-type]
                            direction="low",
                            value=margin,
                            threshold=thresh,
                            evidence_keys=[
                                "sales.total_profit.total_profit",
                                "sales.total_sales.total_sales",
                            ],
                            summary=f"Profit margin is {margin:.1%}.",
                            unit="ratio",
                        ),
                    )

            # Sales trend (first->last month)
            if len(month_rows) >= 2:
                first = _to_number(month_rows[0].get("sales"))
                last = _to_number(month_rows[-1].get("sales"))
                if first not in (None, 0) and last is not None:
                    change = (last - first) / first
                    thresh = _get_threshold("sales_trend_change")
                    sev = _severity_from_thresholds_low(change, thresh)
                    if sev != "info":
                        _add_anomaly(
                            severity=sev,
                            title="Negative sales trend",
                            text=f"Sales changed {change:.1%} from first to last month.",
                            evidence_keys=["sales.sales_by_month.sales[first]", "sales.sales_by_month.sales[last]"],
                            normalized=make_normalized_anomaly(
                                anomaly_id="sales_trend_change",
                                policy=policy_name or "sales_v1",
                                metric="sales.sales_by_month",
                                severity=sev,  # type: ignore[arg-type]
                                direction="low",
                                value=change,
                                threshold=thresh,
                                evidence_keys=[
                                    "sales.sales_by_month.sales[first]",
                                    "sales.sales_by_month.sales[last]",
                                ],
                                summary=f"Sales changed {change:.1%} from first to last month.",
                                unit="pct_change",
                            ),
                        )

            # Unit economics (avg unit revenue)
            if avg_unit_revenue is not None:
                thresh_low = _get_threshold("unit_revenue_low")
                sev_low = _severity_from_thresholds_low(avg_unit_revenue, thresh_low)
                if sev_low != "info":
                    _add_anomaly(
                        severity=sev_low,
                        title="Unit revenue too low",
                        text=f"Average unit revenue is {avg_unit_revenue:.2f}.",
                        evidence_keys=[
                            "sales.avg_unit_revenue.avg_unit_revenue",
                            "sales.total_sales.total_sales",
                            "sales.total_units.total_units",
                        ],
                        normalized=make_normalized_anomaly(
                            anomaly_id="unit_revenue_low",
                            policy=policy_name or "sales_v1",
                            metric="sales.avg_unit_revenue",
                            severity=sev_low,  # type: ignore[arg-type]
                            direction="low",
                            value=avg_unit_revenue,
                            threshold=thresh_low,
                            evidence_keys=[
                                "sales.avg_unit_revenue.avg_unit_revenue",
                                "sales.total_sales.total_sales",
                                "sales.total_units.total_units",
                            ],
                            summary=f"Average unit revenue is {avg_unit_revenue:.2f}.",
                            unit="currency",
                        ),
                    )

                thresh_high = _get_threshold("unit_revenue_high")
                sev_high = _severity_from_thresholds(avg_unit_revenue, thresh_high)
                if sev_high != "info":
                    _add_anomaly(
                        severity=sev_high,
                        title="Unit revenue too high",
                        text=f"Average unit revenue is {avg_unit_revenue:.2f}.",
                        evidence_keys=[
                            "sales.avg_unit_revenue.avg_unit_revenue",
                            "sales.total_sales.total_sales",
                            "sales.total_units.total_units",
                        ],
                        normalized=make_normalized_anomaly(
                            anomaly_id="unit_revenue_high",
                            policy=policy_name or "sales_v1",
                            metric="sales.avg_unit_revenue",
                            severity=sev_high,  # type: ignore[arg-type]
                            direction="high",
                            value=avg_unit_revenue,
                            threshold=thresh_high,
                            evidence_keys=[
                                "sales.avg_unit_revenue.avg_unit_revenue",
                                "sales.total_sales.total_sales",
                                "sales.total_units.total_units",
                            ],
                            summary=f"Average unit revenue is {avg_unit_revenue:.2f}.",
                            unit="currency",
                        ),
                    )

            # Unit concentration
            if top_units_row and total_units not in (None, 0):
                top_units_val = _to_number(top_units_row.get("units"))
                prod = top_units_row.get("product")
                if top_units_val is not None and prod:
                    share_units = top_units_val / total_units
                    thresh = _get_threshold("unit_concentration_share")
                    sev = _severity_from_thresholds(share_units, thresh)
                    if sev != "info":
                        _add_anomaly(
                            severity=sev,
                            title="Unit concentration",
                            text=f"Top product {prod} holds {share_units:.1%} of units.",
                            evidence_keys=["sales.top_products_by_units_top10.units", "sales.total_units.total_units"],
                            normalized=make_normalized_anomaly(
                                anomaly_id="unit_concentration_share",
                                policy=policy_name or "sales_v1",
                                metric="sales.top_products_by_units_top10",
                                severity=sev,  # type: ignore[arg-type]
                                direction="high",
                                value=share_units,
                                threshold=thresh,
                                evidence_keys=[
                                    "sales.top_products_by_units_top10.units",
                                    "sales.total_units.total_units",
                                ],
                                summary=f"Top product {prod} holds {share_units:.1%} of units.",
                                unit="share",
                            ),
                        )

        # Improvement 1: explicit deterministic sort for anomalies_normalized
        anomalies_normalized = sorted(
            anomalies_normalized,
            key=lambda a: (_severity_rank(str(a.get("severity", "info"))), str(a.get("id", ""))),
            reverse=True,
        )

        return {
            "coverage": {
                "expected": len(expected_metrics),
                "present": len(present),
                "missing": missing,
                "ratio": round(coverage_ratio, 2),
            },
            "confidence": {"aggregate": aggregate_conf, "trend": trend_conf},
            "anomalies": anomalies_text,
            "anomalies_structured": anomalies_structured,
            "anomalies_max_severity": max_sev,
            "anomalies_normalized": anomalies_normalized,
        }
