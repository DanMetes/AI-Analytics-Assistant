from __future__ import annotations

from typing import Any, Dict, List

from .base import Finding, Interpretation, Interpreter


def _to_number(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _parse_sections(metrics_rows: list[dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    sections: Dict[str, List[Dict[str, Any]]] = {}
    for row in metrics_rows:
        sec = row.get("section")
        key = str(row.get("key"))
        val = row.get("value")
        if sec is None:
            continue
        rows = sections.setdefault(str(sec), [])
        if key.startswith("row") and ":" in key:
            prefix, col = key.split(":", 1)
            try:
                idx = int(prefix[3:])
            except Exception:
                idx = len(rows)
            while len(rows) <= idx:
                rows.append({})
            rows[idx][col] = val
        else:
            # store non-row style as metadata row 0
            if not rows:
                rows.append({})
            rows[0][key] = val
    return sections


class GenericTabularInterpreter(Interpreter):
    EXPLAINABILITY = {
        "anomalies_emitted": [],
        "expected_signals": ["overall.row_count"],
    }

    @staticmethod
    def _extract_time_summary(metrics_rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
        """Return metric -> key -> value from time_summary section.

        Keys are typically year values encoded in the metrics key string:
        `year=2023:sum_units`.
        """
        out: dict[str, dict[str, float]] = {}
        for r in metrics_rows:
            if str(r.get("section")) != "time_summary":
                continue
            key = str(r.get("key") or "")
            if ":" not in key or "=" not in key:
                continue
            lhs, metric = key.split(":", 1)
            # lhs looks like year=2023
            dim_val = lhs.split("=", 1)[1] if "=" in lhs else lhs
            v = _to_number(r.get("value"))
            if v is None:
                continue
            out.setdefault(metric, {})[str(dim_val)] = float(v)
        return out

    @staticmethod
    def _ratio_anomalies(
        *,
        policy: str,
        metric: str,
        series: dict[str, float],
        warning_ratio: float,
        critical_ratio: float,
    ) -> tuple[list[dict[str, Any]], list[Finding]]:
        """Flag max/median ratio anomalies (robust for small n).

        Returns (anomalies_normalized, findings).
        """
        if len(series) < 3:
            return [], []
        vals = sorted(series.values())
        median = vals[len(vals) // 2]
        if median == 0:
            return [], []

        # Identify the max point.
        max_key = max(series.keys(), key=lambda k: series[k])
        max_val = float(series[max_key])
        ratio = max_val / float(median)
        if ratio < warning_ratio:
            return [], []

        severity = "critical" if ratio >= critical_ratio else "warning"
        anomaly = {
            "id": f"{policy}:{metric}:{max_key}:high_ratio",
            "policy": policy,
            "metric": metric,
            "severity": severity,
            "direction": "high",
            "value": max_val,
            "threshold": {"warning": float(warning_ratio), "critical": float(critical_ratio)},
            "unit": "",
            "evidence_keys": [f"time_summary.year={max_key}:{metric}"],
            "summary": f"{metric} for {max_key} is {ratio:.1f}× the median across periods.",
        }
        finding = Finding(
            severity=severity,
            title=f"Potential outlier in {metric}",
            text=f"{metric} is unusually high for {max_key} ({max_val:g}), about {ratio:.1f}× the median across periods."
            " Consider validating whether this reflects a data issue or a structural change.",
            evidence_keys=[f"time_summary.year={max_key}:{metric}"],
        )
        return [anomaly], [finding]

    def _get_profile_column(self, analysis_log: dict, col_name: str) -> dict[str, Any] | None:
        """Return profile stats for a column, if available in analysis_log.

        Step-6: keep this domain-agnostic. We only use distributional descriptors.
        """
        profile = analysis_log.get("data_profile")
        if not isinstance(profile, dict):
            return None
        cols = profile.get("columns")
        if not isinstance(cols, dict):
            return None
        col = cols.get(col_name)
        return col if isinstance(col, dict) else None

    @staticmethod
    def _base_col_from_metric(metric: str) -> str | None:
        """Map an aggregated metric name to a plausible base column name."""
        for prefix in ("sum_", "avg_", "min_", "max_", "rate_"):
            if metric.startswith(prefix):
                return metric[len(prefix) :]
        return None

    @staticmethod
    def _safe_float(x: Any) -> float | None:
        try:
            if x is None:
                return None
            return float(x)
        except Exception:
            return None

    def interpret(self, metrics_rows: list[dict[str, Any]], analysis_log: dict) -> Interpretation:
        sections = _parse_sections(metrics_rows)
        findings: list[Finding] = []
        caveats: list[str] = []

        overall = sections.get("overall", [{}])[0]
        row_count = overall.get("row_count")
        if row_count is not None:
            findings.append(
                Finding(severity="info", title="Row count", text=f"Row count: {row_count}", evidence_keys=["overall.row_count"])
            )

        warnings = analysis_log.get("warnings") or []
        for w in warnings:
            caveats.append(str(w))

        policy_name = str(analysis_log.get("policy", {}).get("name") or "generic_tabular")
        time_summary = self._extract_time_summary(metrics_rows)

        anomalies_normalized: list[dict[str, Any]] = []
        anomalies_human: list[str] = []

        # Conservative, stable thresholds for cross-domain data.
        # We only use computed metrics (no raw rows).
        watched_metrics = ("sum_units", "avg_units", "sum_sales", "sum_profit")
        for metric_name in watched_metrics:
            series = time_summary.get(metric_name) or {}
            anoms, fins = self._ratio_anomalies(
                policy=policy_name,
                metric=metric_name,
                series=series,
                warning_ratio=3.0,
                critical_ratio=10.0,
            )
            anomalies_normalized.extend(anoms)
            findings.extend(fins)
            for a in anoms:
                anomalies_human.append(str(a.get("summary") or ""))

        # Step-6: interpretation-quality upgrades (still deterministic, still evidence-bound).
        supporting_evidence: list[str] = []
        negative_evidence: list[str] = []

        # Group anomalies that share the same base variable and time key (symptoms vs mechanism).
        # Example: sum_units + avg_units both spike for the same year.
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for a in anomalies_normalized:
            metric = str(a.get("metric") or "")
            # attempt to recover period key from anomaly id: policy:metric:<period>:high_ratio
            aid = str(a.get("id") or "")
            parts = aid.split(":")
            period = parts[2] if len(parts) >= 4 else ""
            base = self._base_col_from_metric(metric) or metric
            grouped.setdefault((base, period), []).append(a)

        # Add distribution-aware reasoning where profile stats are available.
        for (base, period), anoms in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1])):
            if not base or not period or len(anoms) < 2:
                continue  # only consolidate when we have multiple symptoms for same mechanism

            col = self._get_profile_column(analysis_log, base)
            p95 = self._safe_float(col.get("p95")) if col else None
            p50 = self._safe_float(col.get("p50")) if col else None
            cmax = self._safe_float(col.get("max")) if col else None
            skew = self._safe_float(col.get("skew")) if col else None
            skew_flag = bool(col.get("skew_flag")) if col else False

            # Evidence: if distribution is extremely skewed and max dwarfs p95, leverage is plausible.
            leverage_ratio = None
            if cmax is not None and p95 not in (None, 0):
                leverage_ratio = cmax / p95

            if col:
                supporting_evidence.append(
                    f"{base} distribution: p50={p50:g} p95={p95:g} max={cmax:g} (skew={skew:g}, skew_flag={skew_flag})."
                    if all(v is not None for v in (p50, p95, cmax, skew))
                    else f"{base} distribution includes extreme values (max={cmax:g}) relative to upper percentiles."
                )

            # Negative evidence: if row counts for the period are not elevated, broad-based shift is less likely.
            n_series = time_summary.get("n") or {}
            n_val = n_series.get(period)
            if n_val is not None:
                # compare to median n
                n_vals = sorted([v for v in n_series.values() if isinstance(v, (int, float))])
                if n_vals:
                    n_med = n_vals[len(n_vals) // 2]
                    if n_med and float(n_val) <= 1.25 * float(n_med):
                        negative_evidence.append(
                            f"Row count for {period} is not unusually high (n={int(n_val)} vs median≈{int(n_med)}), reducing support for a broad-based volume shift."
                        )

            # Consolidated mechanism-level finding (no domain assumptions).
            confidence = "medium"
            if skew_flag and leverage_ratio is not None and leverage_ratio >= 50:
                confidence = "high"
            elif leverage_ratio is not None and leverage_ratio >= 20:
                confidence = "medium-high"

            symptom_metrics = ", ".join(sorted({str(a.get('metric') or '') for a in anoms}))
            findings.append(
                Finding(
                    severity="info",
                    title=f"Mechanism-level consolidation for {base} in {period}",
                    text=(
                        f"Multiple time-level anomalies for {base} in {period} ({symptom_metrics}) likely reflect a shared mechanism. "
                        f"Given the distribution shape{' (highly skewed)' if skew_flag else ''}, this pattern is often driven by a small number of extreme records rather than a broad shift. "
                        f"Confidence: {confidence}."
                    ),
                    evidence_keys=sorted(
                        list({k for a in anoms for k in (a.get('evidence_keys') or []) if isinstance(k, str)})
                    )
                    or [f"time_summary.year={period}:*"],
                )
            )

        if not findings:
            findings.append(
                Finding(
                    severity="info",
                    title="No findings",
                    text="No findings available under conservative, policy-bound checks.",
                    evidence_keys=[],
                )
            )

        sev_order = {"info": 0, "warning": 1, "warn": 1, "critical": 2}
        max_sev = "info"
        for a in anomalies_normalized:
            s = str(a.get("severity") or "info").lower()
            if sev_order.get(s, 0) > sev_order.get(max_sev, 0):
                max_sev = s

        metadata: dict[str, Any] = {
            "anomalies": sorted([x for x in anomalies_human if x], key=lambda x: x),
            "anomalies_structured": [],
            "anomalies_normalized": sorted(
                anomalies_normalized,
                key=lambda a: (
                    str(a.get("severity") or ""),
                    str(a.get("metric") or ""),
                    str(a.get("id") or ""),
                ),
            ),
            "anomalies_max_severity": max_sev,
        }

        # Step-6: attach evidence summaries to support better report framing downstream.
        if supporting_evidence:
            metadata["supporting_evidence"] = supporting_evidence
        if negative_evidence:
            metadata["negative_evidence"] = negative_evidence

        return Interpretation(findings=findings, caveats=caveats, metadata=metadata)
