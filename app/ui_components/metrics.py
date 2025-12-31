"""Metrics UI components."""
import streamlit as st
import pandas as pd
from typing import Dict, Any, Optional

KPI_DEFINITIONS = {
    "Total Sales": "Total revenue generated from all transactions in dollars",
    "Total Units": "Total quantity of items sold across all transactions",
    "Total Profit": "Net earnings after subtracting costs from sales revenue",
    "Avg Profit Margin": "Average ratio of profit to sales, expressed as a percentage",
    "Total Rows": "Number of records/transactions in the dataset",
    "Average Discount": "Mean discount percentage applied across transactions",
    "Total Discount": "Sum of all discount amounts applied",
}


def render_kpi_dashboard(metrics_df: pd.DataFrame, run_id: str = ""):
    """
    Render a KPI dashboard with core metrics and descriptions.
    
    Args:
        metrics_df: DataFrame containing metrics.csv data
        run_id: Run ID for unique widget keys
    """
    if metrics_df is None or metrics_df.empty:
        st.info("No metrics data available.")
        return
    
    df = metrics_df
    time_summary = df[df["section"] == "time_summary"].copy()
    overall = df[df["section"] == "overall"].copy()
    
    kpis = []
    
    if not overall.empty:
        row_count_row = overall[overall["key"] == "row_count"]
        if not row_count_row.empty:
            kpis.append({
                "label": "Total Rows",
                "value": int(float(row_count_row["value"].iloc[0])),
                "format": "count",
                "description": KPI_DEFINITIONS.get("Total Rows", "")
            })
    
    if not time_summary.empty:
        metric_mappings = [
            ("sum_sales", "Total Sales", "currency"),
            ("sum_profit", "Total Profit", "currency"),
            ("sum_units", "Total Units", "count"),
            ("sum_discount", "Total Discount", "currency"),
        ]
        
        for metric_key, label, fmt in metric_mappings:
            metric_rows = time_summary[time_summary["key"].str.contains(metric_key)]
            if not metric_rows.empty:
                total = metric_rows["value"].astype(float).sum()
                kpis.append({
                    "label": label,
                    "value": total,
                    "format": fmt,
                    "description": KPI_DEFINITIONS.get(label, "")
                })
        
        margin_rows = time_summary[time_summary["key"].str.contains("profit_margin")]
        if not margin_rows.empty:
            avg_margin = margin_rows["value"].astype(float).mean()
            kpis.append({
                "label": "Avg Profit Margin",
                "value": avg_margin,
                "format": "percent",
                "description": KPI_DEFINITIONS.get("Avg Profit Margin", "")
            })
        
        discount_rows = time_summary[time_summary["key"].str.contains("avg_discount|discount_rate")]
        if not discount_rows.empty:
            avg_discount = discount_rows["value"].astype(float).mean()
            kpis.append({
                "label": "Average Discount",
                "value": avg_discount,
                "format": "percent",
                "description": KPI_DEFINITIONS.get("Average Discount", "")
            })
    
    if not kpis:
        st.info("No KPI metrics found in the dataset.")
        return
    
    num_kpis = min(len(kpis), 5)
    cols = st.columns(num_kpis)
    
    for i, kpi in enumerate(kpis[:num_kpis]):
        with cols[i]:
            if kpi["format"] == "currency":
                value_str = f"${kpi['value']:,.0f}"
            elif kpi["format"] == "percent":
                value_str = f"{kpi['value']:.1%}"
            else:
                value_str = f"{kpi['value']:,.0f}"
            
            st.metric(kpi["label"], value_str, help=kpi["description"])
            st.caption(kpi["description"][:50] + "..." if len(kpi["description"]) > 50 else kpi["description"])


def render_metrics_glossary(metrics_df: pd.DataFrame, analysis_plan: Optional[Dict] = None):
    """
    Render a glossary of all available metrics.
    
    Args:
        metrics_df: DataFrame containing metrics.csv data
        analysis_plan: Optional analysis plan JSON for metric definitions
    """
    with st.expander("📖 Metrics Glossary", expanded=False):
        st.markdown("**Available metrics in this dataset:**")
        
        if metrics_df is None or metrics_df.empty:
            st.info("No metrics available.")
            return
        
        metric_names = set()
        for key in metrics_df["key"].unique():
            parts = key.split(":")
            metric_name = parts[-1] if len(parts) > 1 else key
            metric_names.add(metric_name)
        
        definitions = {
            "sum_sales": "Total revenue from sales transactions (currency)",
            "sum_units": "Total quantity of units sold (count)",
            "sum_profit": "Total profit after costs (currency)",
            "profit_margin": "Ratio of profit to sales (percentage)",
            "avg_units": "Average units per transaction (count)",
            "avg_sales": "Average sales per transaction (currency)",
            "avg_profit": "Average profit per transaction (currency)",
            "row_count": "Total number of records in the dataset",
            "column_count": "Number of columns/fields in the dataset",
            "n": "Number of records in a group",
            "sum_discount": "Total discount amount applied (currency)",
            "avg_discount": "Average discount per transaction (percentage)",
        }
        
        if analysis_plan and "steps" in analysis_plan:
            for step in analysis_plan.get("steps", []):
                if "metric" in step and "rationale" in step:
                    definitions[step["metric"]] = step.get("rationale", "")
        
        sorted_metrics = sorted(metric_names)
        
        for metric in sorted_metrics:
            definition = definitions.get(metric, "Metric value from analysis")
            st.markdown(f"- **{metric.replace('_', ' ').title()}**: {definition}")
