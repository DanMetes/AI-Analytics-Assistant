"""Findings utilities for the AI Analytics Assistant."""
import streamlit as st
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Any, Optional


def extract_time_period(anomaly: dict) -> Optional[str]:
    """Extract the time period from an anomaly's evidence keys."""
    evidence_keys = anomaly.get("evidence_keys", [])
    for key in evidence_keys:
        if "year=" in key:
            parts = key.split("year=")
            if len(parts) > 1:
                return parts[1].split(":")[0]
    return None


def get_base_metric(metric: str) -> str:
    """Extract the base metric name (e.g., 'units' from 'sum_units')."""
    metric_lower = metric.lower()
    if "units" in metric_lower:
        return "units"
    elif "sales" in metric_lower:
        return "sales"
    elif "profit" in metric_lower:
        return "profit"
    elif "margin" in metric_lower:
        return "margin"
    else:
        parts = metric.split("_")
        return parts[-1] if len(parts) > 1 else metric


def determine_pattern_type(anomalies: List[dict]) -> str:
    """Determine if anomalies represent a spike or broad change."""
    has_sum = any("sum_" in a.get("metric", "") for a in anomalies)
    has_avg = any("avg_" in a.get("metric", "") for a in anomalies)
    
    if has_sum and has_avg:
        avg_ratio = None
        sum_ratio = None
        for a in anomalies:
            summary = a.get("summary", "")
            if "avg_" in a.get("metric", "") and "×" in summary:
                try:
                    avg_ratio = float(summary.split("×")[0].split()[-1])
                except (ValueError, IndexError):
                    pass
            elif "sum_" in a.get("metric", "") and "×" in summary:
                try:
                    sum_ratio = float(summary.split("×")[0].split()[-1])
                except (ValueError, IndexError):
                    pass
        
        if avg_ratio and sum_ratio:
            if avg_ratio > sum_ratio * 1.5:
                return "single_row_spike"
            elif abs(avg_ratio - sum_ratio) / max(avg_ratio, sum_ratio) < 0.2:
                return "broad_trend"
    
    if has_avg and not has_sum:
        return "average_shift"
    elif has_sum and not has_avg:
        return "volume_change"
    
    return "mixed"


def get_pattern_label(pattern_type: str) -> str:
    """Get a user-friendly label for the pattern type."""
    labels = {
        "single_row_spike": "Likely single-row spike",
        "broad_trend": "Broad trend change",
        "average_shift": "Average value shift",
        "volume_change": "Total volume change",
        "mixed": "Mixed pattern"
    }
    return labels.get(pattern_type, "Pattern detected")


def get_recommended_action(pattern_type: str, base_metric: str) -> str:
    """Get a recommended action based on pattern type."""
    actions = {
        "single_row_spike": f"Review top contributing rows for {base_metric} to identify potential data entry errors or outliers.",
        "broad_trend": f"Analyze the time series for {base_metric} to understand underlying drivers of the trend.",
        "average_shift": f"Check if the change in average {base_metric} reflects a real business shift or data quality issue.",
        "volume_change": f"Investigate the source of increased {base_metric} volume - new data sources or business expansion?",
        "mixed": f"Review both individual records and aggregate trends for {base_metric}."
    }
    return actions.get(pattern_type, f"Investigate the {base_metric} data for this period.")


def normalize_and_group_anomalies(anomalies: List[dict]) -> List[Dict[str, Any]]:
    """
    Consolidate related anomalies and add user-friendly labels.
    
    Groups anomalies by:
    - Base metric (units, sales, profit, etc.)
    - Time period (year, quarter, etc.)
    
    Returns a list of grouped anomaly objects with:
    - group_id: Unique identifier for the group
    - base_metric: The underlying metric (units, sales, etc.)
    - time_period: The time period affected
    - severity: Highest severity in the group
    - summary: One-sentence summary
    - pattern_label: User-friendly label ("Likely single-row spike", etc.)
    - underlying_cause: Description of what likely caused this
    - recommended_action: What to do next
    - anomalies: List of original anomalies in this group
    """
    if not anomalies:
        return []
    
    groups = defaultdict(list)
    
    for anomaly in anomalies:
        metric = anomaly.get("metric", "unknown")
        base_metric = get_base_metric(metric)
        time_period = extract_time_period(anomaly) or "overall"
        
        group_key = f"{base_metric}:{time_period}"
        groups[group_key].append(anomaly)
    
    result = []
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    
    for group_key, group_anomalies in groups.items():
        base_metric, time_period = group_key.split(":", 1)
        
        sorted_anomalies = sorted(
            group_anomalies,
            key=lambda x: severity_order.get(x.get("severity", "info"), 3)
        )
        top_severity = sorted_anomalies[0].get("severity", "info")
        
        pattern_type = determine_pattern_type(group_anomalies)
        pattern_label = get_pattern_label(pattern_type)
        
        metrics_involved = list(set(a.get("metric", "") for a in group_anomalies))
        metrics_str = ", ".join(metrics_involved)
        
        if len(group_anomalies) == 1:
            summary = group_anomalies[0].get("summary", "Anomaly detected")
        else:
            summary = f"Multiple related anomalies in {base_metric} for {time_period} ({len(group_anomalies)} signals: {metrics_str})"
        
        if pattern_type == "single_row_spike":
            underlying_cause = f"The high average {base_metric} relative to sum suggests a small number of extreme values driving the anomaly, possibly from data entry errors or legitimate outliers."
        elif pattern_type == "broad_trend":
            underlying_cause = f"Both sum and average {base_metric} show similar deviation ratios, indicating a systematic change affecting most records in this period."
        elif pattern_type == "average_shift":
            underlying_cause = f"The average {base_metric} has shifted without a proportional change in total volume, suggesting changes in per-record values."
        elif pattern_type == "volume_change":
            underlying_cause = f"Total {base_metric} volume has changed significantly, which could reflect changes in transaction count or data completeness."
        else:
            underlying_cause = f"The {base_metric} metrics show an unusual pattern that warrants investigation."
        
        result.append({
            "group_id": group_key,
            "base_metric": base_metric,
            "time_period": time_period,
            "severity": top_severity,
            "summary": summary,
            "pattern_label": pattern_label,
            "underlying_cause": underlying_cause,
            "recommended_action": get_recommended_action(pattern_type, base_metric),
            "anomalies": group_anomalies,
            "metrics_involved": metrics_involved
        })
    
    result.sort(key=lambda x: severity_order.get(x["severity"], 3))
    
    return result


def render_anomaly_card(group: Dict[str, Any], run_id: str, index: int):
    """Render a single grouped anomaly as a styled card."""
    severity = group["severity"]
    
    if severity == "critical":
        badge = "🔴"
        border_color = "#ff4b4b"
        bg_color = "rgba(255, 75, 75, 0.1)"
    elif severity == "warning":
        badge = "🟠"
        border_color = "#ffa500"
        bg_color = "rgba(255, 165, 0, 0.1)"
    else:
        badge = "🔵"
        border_color = "#4b9fff"
        bg_color = "rgba(75, 159, 255, 0.1)"
    
    severity_label = severity.upper()
    metric_title = group["base_metric"].replace("_", " ").title()
    period_label = f" ({group['time_period']})" if group["time_period"] != "overall" else ""
    
    st.markdown(f"""
<div style="border-left: 4px solid {border_color}; padding: 15px; margin: 15px 0; background: {bg_color}; border-radius: 4px;">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
        <strong>{badge} {severity_label}: {metric_title} Anomaly{period_label}</strong>
        <span style="font-size: 0.85em; color: #666; background: rgba(0,0,0,0.1); padding: 2px 8px; border-radius: 12px;">{group['pattern_label']}</span>
    </div>
    <p style="margin: 8px 0;"><strong>Summary:</strong> {group['summary']}</p>
    <p style="margin: 8px 0;"><strong>Likely cause:</strong> {group['underlying_cause']}</p>
    <p style="margin: 8px 0; color: #0066cc;"><strong>Recommended:</strong> {group['recommended_action']}</p>
</div>
    """, unsafe_allow_html=True)
    
    with st.expander(f"Technical details ({len(group['anomalies'])} signals)", expanded=False):
        for anomaly in group["anomalies"]:
            metric = anomaly.get("metric", "N/A")
            value = anomaly.get("value", "N/A")
            direction = anomaly.get("direction", "N/A")
            threshold = anomaly.get("threshold", {})
            
            st.markdown(f"**{metric}:** {value} ({direction})")
            if threshold:
                st.markdown(f"- Thresholds: Critical > {threshold.get('critical', 'N/A')}, Warning > {threshold.get('warning', 'N/A')}")
            
            evidence = anomaly.get("evidence_keys", [])
            if evidence:
                st.markdown(f"- Evidence: `{', '.join(evidence)}`")


def render_interpretation_bullets(interpretation: dict):
    """Render interpretation findings as bullet points."""
    if not interpretation:
        return
    
    findings = interpretation.get("findings", [])
    if findings:
        st.markdown("**Interpretation:**")
        for finding in findings:
            if isinstance(finding, dict):
                severity = finding.get("severity", "info")
                text = finding.get("text", "")
                title = finding.get("title", "")
                
                if severity == "critical":
                    icon = "🔴"
                elif severity == "warning":
                    icon = "🟠"
                else:
                    icon = "ℹ️"
                
                if title:
                    st.markdown(f"- {icon} **{title}**: {text}")
                else:
                    st.markdown(f"- {icon} {text}")
            else:
                st.markdown(f"- {finding}")
    
    negative_evidence = interpretation.get("metadata", {}).get("negative_evidence", [])
    if negative_evidence:
        st.markdown("**What's ruled out:**")
        for item in negative_evidence:
            st.markdown(f"- ✓ {item}")
    
    caveats = interpretation.get("caveats", [])
    if caveats:
        with st.expander("Analysis caveats"):
            for caveat in caveats:
                st.markdown(f"- ⚠️ {caveat}")


def generate_causal_narrative(
    group: Dict[str, Any],
    metrics_df: Optional[Any] = None,
    profile: Optional[Dict[str, Any]] = None
) -> Dict[str, str]:
    """
    Generate a one-sentence causal narrative and recommended action for a grouped anomaly.
    
    Uses metrics and profile data to provide specific context about why the anomaly occurred.
    
    Args:
        group: Grouped anomaly from normalize_and_group_anomalies()
        metrics_df: Pandas DataFrame of metrics.csv (optional)
        profile: data_profile.json dict (optional)
    
    Returns:
        Dict with 'narrative' (one-sentence explanation) and 'next_action' (one specific action)
    """
    import re
    
    base_metric = group.get("base_metric", "metric")
    time_period = group.get("time_period", "")
    metrics_involved = group.get("metrics_involved", [])
    pattern_label = group.get("pattern_label", "")
    anomalies = group.get("anomalies", [])
    
    pattern_type = "mixed"
    if "single-row spike" in pattern_label.lower() or "spike" in pattern_label.lower():
        pattern_type = "single_row_spike"
    elif "broad trend" in pattern_label.lower():
        pattern_type = "broad_trend"
    elif "average" in pattern_label.lower():
        pattern_type = "average_shift"
    elif "volume" in pattern_label.lower():
        pattern_type = "volume_change"
    
    metrics_str = ", ".join(metrics_involved) if metrics_involved else base_metric
    
    anomaly_evidence = ""
    max_ratio = 0
    max_metric = ""
    best_summary = ""
    
    for anomaly in anomalies:
        summary = anomaly.get("summary", "")
        metric = anomaly.get("metric", "")
        value = anomaly.get("value")
        
        ratio_match = re.search(r'(\d+\.?\d*)\s*[×x]', summary, re.IGNORECASE)
        if ratio_match:
            try:
                ratio = float(ratio_match.group(1))
                if ratio > max_ratio:
                    max_ratio = ratio
                    max_metric = metric
                    best_summary = summary
            except ValueError:
                pass
        
        pct_match = re.search(r'([+-]?\d+\.?\d*)\s*%', summary)
        if pct_match and max_ratio == 0:
            try:
                pct = abs(float(pct_match.group(1)))
                if pct > max_ratio:
                    max_ratio = pct / 100 + 1 if pct > 0 else 1
                    max_metric = metric
                    best_summary = summary
            except ValueError:
                pass
    
    if max_ratio > 0 and max_metric:
        anomaly_evidence = f"{max_metric} is {max_ratio:.1f}× the expected value"
    elif anomalies:
        top = anomalies[0]
        metric = top.get("metric", base_metric)
        value = top.get("value")
        direction = top.get("direction", "")
        threshold = top.get("threshold", {})
        critical_thresh = threshold.get("critical")
        warning_thresh = threshold.get("warning")
        
        if value is not None:
            if isinstance(value, (int, float)):
                if abs(value) >= 1000:
                    val_str = f"{value:,.0f}"
                elif isinstance(value, float):
                    val_str = f"{value:.2f}"
                else:
                    val_str = str(value)
                
                if critical_thresh and value > critical_thresh:
                    anomaly_evidence = f"{metric} = {val_str} (exceeds critical threshold of {critical_thresh})"
                elif warning_thresh and value > warning_thresh:
                    anomaly_evidence = f"{metric} = {val_str} (exceeds warning threshold of {warning_thresh})"
                else:
                    anomaly_evidence = f"{metric} = {val_str} ({direction})"
            else:
                anomaly_evidence = f"{metric} = {value} ({direction})"
        else:
            anomaly_evidence = f"{metric} deviation detected ({direction})"
    
    profile_context = ""
    
    if profile and "columns" in profile:
        for col_name in [base_metric] + list(metrics_involved):
            clean_col = col_name.replace("sum_", "").replace("avg_", "").replace("rate_", "")
            col_profile = profile["columns"].get(clean_col, {})
            if not col_profile:
                continue
                
            max_val = col_profile.get("max")
            p95_val = col_profile.get("p95")
            skew_val = col_profile.get("skew")
            mean_val = col_profile.get("mean")
            
            if skew_val is not None and abs(skew_val) > 5:
                direction = "right" if skew_val > 0 else "left"
                profile_context = f"the {clean_col} distribution is heavily {direction}-skewed, indicating extreme values"
                break
            elif max_val is not None and mean_val is not None and mean_val > 0 and max_val > mean_val * 50:
                ratio = int(max_val / mean_val)
                profile_context = f"some {clean_col} values are extremely high (up to {ratio}× the average)"
                break
            elif max_val is not None and p95_val is not None and p95_val > 0 and max_val > p95_val * 10:
                profile_context = f"extreme {clean_col} outliers exist (max value far exceeds typical range)"
                break
    
    evidence = profile_context or anomaly_evidence
    
    period_phrase = f" in {time_period}" if time_period and time_period != "overall" else ""
    
    if pattern_type == "single_row_spike":
        if evidence:
            narrative = f"A single outlier row is driving the {metrics_str} spike{period_phrase} — {evidence}."
        else:
            narrative = f"A small number of extreme values in {metrics_str} are driving the anomaly{period_phrase}, as the average deviation far exceeds the sum deviation."
        next_action = f"Review the top 5 records by {base_metric}{period_phrase} and check for data entry errors or legitimate outliers."
    
    elif pattern_type == "broad_trend":
        if evidence:
            narrative = f"The {metrics_str} change{period_phrase} reflects a systematic shift across most records ({evidence})."
        else:
            narrative = f"The {metrics_str} change{period_phrase} reflects a systematic shift across most records, not isolated outliers."
        next_action = f"Compare {base_metric} distributions between affected and baseline periods to identify the driver."
    
    elif pattern_type == "average_shift":
        if evidence:
            narrative = f"Per-record {metrics_str} values have shifted{period_phrase}; {evidence}."
        else:
            narrative = f"Average {metrics_str} has changed{period_phrase} without a proportional change in volume, suggesting per-record value changes."
        next_action = f"Segment the data by category or region to identify which groups show the strongest {base_metric} shift."
    
    elif pattern_type == "volume_change":
        if evidence:
            narrative = f"Total {metrics_str} volume has changed significantly{period_phrase} ({evidence})."
        else:
            narrative = f"Total {metrics_str} volume has changed significantly{period_phrase}, possibly due to changes in record count or data completeness."
        next_action = f"Check if the number of records{period_phrase} changed and whether any data sources were added or removed."
    
    else:
        if evidence:
            narrative = f"The {metrics_str} shows an unusual pattern{period_phrase} — {evidence}."
        else:
            narrative = f"The {metrics_str} metrics{period_phrase} show an unusual pattern requiring investigation."
        next_action = f"Examine both individual high-value records and aggregate trends for {base_metric}."
    
    return {
        "narrative": narrative,
        "next_action": next_action
    }
