"""Run Summary helper for generating high-level analysis summaries."""
import streamlit as st
from pathlib import Path
import json
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple


def load_json_safe(filepath: Path) -> Optional[Dict[str, Any]]:
    """Load JSON file safely, returning None if not found or invalid."""
    if filepath.exists():
        try:
            with open(filepath, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    return None


def get_top_kpis(metrics_df: pd.DataFrame, n: int = 3) -> List[Tuple[str, float, str]]:
    """
    Extract top N KPIs from metrics.csv based on absolute value.
    Returns list of (label, value, formatted_value) tuples.
    """
    kpis = []
    
    if metrics_df is None or metrics_df.empty:
        return kpis
    
    time_summary = metrics_df[metrics_df["section"] == "time_summary"].copy()
    if time_summary.empty:
        return kpis
    
    agg_metrics = {}
    for _, row in time_summary.iterrows():
        key = row["key"]
        if ":" in key:
            metric_name = key.split(":")[-1]
            if metric_name.startswith("sum_") or metric_name.startswith("avg_"):
                try:
                    val = float(row["value"])
                    if metric_name not in agg_metrics:
                        agg_metrics[metric_name] = 0
                    if metric_name.startswith("sum_"):
                        agg_metrics[metric_name] += val
                    else:
                        agg_metrics[metric_name] = val
                except (ValueError, TypeError):
                    pass
    
    priority_order = ["sum_sales", "sum_profit", "sum_units", "avg_profit_margin", "avg_units"]
    sorted_metrics = sorted(
        agg_metrics.items(),
        key=lambda x: (priority_order.index(x[0]) if x[0] in priority_order else 100, -abs(x[1]))
    )
    
    for metric_name, value in sorted_metrics[:n]:
        label = metric_name.replace("sum_", "Total ").replace("avg_", "Avg ").replace("_", " ").title()
        if "sales" in metric_name.lower() or "profit" in metric_name.lower():
            formatted = f"${value:,.0f}"
        elif "margin" in metric_name.lower() or "rate" in metric_name.lower():
            formatted = f"{value:.1%}"
        else:
            formatted = f"{value:,.0f}"
        kpis.append((label, value, formatted))
    
    return kpis


def get_date_range(data_profile: Optional[Dict], metrics_df: Optional[pd.DataFrame]) -> Optional[str]:
    """Extract date range from artifacts if available."""
    if metrics_df is not None and not metrics_df.empty:
        time_summary = metrics_df[metrics_df["section"] == "time_summary"]
        if not time_summary.empty:
            years = set()
            for key in time_summary["key"]:
                if "year=" in key:
                    try:
                        year = key.split("year=")[1].split(":")[0]
                        years.add(int(year))
                    except (IndexError, ValueError):
                        pass
            if years:
                return f"{min(years)} - {max(years)}"
    return None


def render_run_summary(run_path: Path) -> Dict[str, Any]:
    """
    Generate a comprehensive run summary.
    
    Returns a dict with:
        - description: High-level description of dataset
        - date_range: Date range if available
        - kpis: Top 3 KPIs
        - anomaly_count: Number of anomalies
        - metrics_count: Number of unique metrics
        - next_steps: Suggested tabs to visit
    """
    data_profile = load_json_safe(run_path / "data_profile.json")
    anomalies = load_json_safe(run_path / "anomalies_normalized.json")
    
    metrics_path = run_path / "metrics.csv"
    metrics_df = pd.read_csv(metrics_path) if metrics_path.exists() else None
    
    rows = data_profile.get("rows", 0) if data_profile else 0
    cols = data_profile.get("cols", 0) if data_profile else 0
    
    anomaly_count = 0
    if anomalies:
        if isinstance(anomalies, dict) and "anomalies" in anomalies:
            anomaly_count = len(anomalies.get("anomalies", []))
        elif isinstance(anomalies, list):
            anomaly_count = len(anomalies)
    
    metrics_count = 0
    if metrics_df is not None and not metrics_df.empty:
        metrics_count = metrics_df["key"].nunique()
    
    date_range = get_date_range(data_profile, metrics_df)
    kpis = get_top_kpis(metrics_df, n=3)
    
    description = f"Dataset with {rows:,} rows and {cols} columns"
    if date_range:
        description += f", spanning {date_range}"
    description += f". Analysis produced {metrics_count} metrics and detected {anomaly_count} anomalies."
    
    next_steps = []
    if anomaly_count > 0:
        next_steps.append(("Key Findings", "Review detected anomalies and their severity"))
    next_steps.append(("Metrics", "Explore KPIs and segment breakdowns"))
    if (run_path / "eda_report.html").exists():
        next_steps.append(("Profiling & EDA", "Deep-dive into data distributions"))
    
    return {
        "description": description,
        "date_range": date_range,
        "kpis": kpis,
        "anomaly_count": anomaly_count,
        "metrics_count": metrics_count,
        "next_steps": next_steps,
        "rows": rows,
        "cols": cols
    }


def display_run_summary(run_path: Path):
    """
    Render the run summary in Streamlit UI.
    This is the main entry point for displaying summaries across tabs.
    """
    summary = render_run_summary(run_path)
    
    st.markdown(f"**{summary['description']}**")
    
    if summary["kpis"]:
        st.markdown("**Top KPIs:**")
        kpi_cols = st.columns(len(summary["kpis"]))
        for i, (label, value, formatted) in enumerate(summary["kpis"]):
            with kpi_cols[i]:
                st.metric(label, formatted)
    
    if summary["next_steps"]:
        st.markdown("**Recommended next steps:**")
        for tab_name, reason in summary["next_steps"]:
            st.markdown(f"- **{tab_name}**: {reason}")
