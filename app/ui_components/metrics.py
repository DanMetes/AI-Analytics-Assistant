"""Metrics UI components."""
import streamlit as st
import pandas as pd
from typing import Dict, Any, Optional, List

KPI_DEFINITIONS = {
    "Total Sales": "Total revenue generated from all transactions in dollars",
    "Total Units": "Total quantity of items sold across all transactions",
    "Total Profit": "Net earnings after subtracting costs from sales revenue",
    "Avg Profit Margin": "Average ratio of profit to sales, expressed as a percentage",
    "Total Rows": "Number of records/transactions in the dataset",
    "Average Discount": "Mean discount percentage applied across transactions",
    "Total Discount": "Sum of all discount amounts applied",
}


def format_kpi_value(value: float, format_type: str) -> str:
    """Format KPI values consistently with proper separators and symbols."""
    if format_type == "currency":
        if abs(value) >= 1_000_000:
            return f"${value / 1_000_000:,.2f}M"
        else:
            return f"${value:,.2f}"
    elif format_type == "percent":
        return f"{value:.2%}"
    else:
        return f"{value:,.0f}"


def detect_time_column(df: pd.DataFrame) -> Optional[str]:
    """Detect time column in the metrics dataframe for trend charts."""
    time_candidates = ["year", "month", "quarter", "date", "period", "time"]
    keys = df["key"].unique() if "key" in df.columns else []
    
    for key in keys:
        key_lower = key.lower()
        for candidate in time_candidates:
            if candidate in key_lower and ":" in key:
                parts = key.split(":")
                if len(parts) >= 1:
                    return parts[0]
    return None


def extract_trend_data(df: pd.DataFrame, metric_key: str) -> Optional[pd.DataFrame]:
    """Extract time-series data for trend charts.
    
    Supports key formats:
    - "<period>:<metric_name>" e.g., "2019:sum_sales"
    - "<field>:<period>:<metric_name>" e.g., "order_date:2019:sum_sales"
    
    The period is identified as the penultimate segment when 3+ parts exist,
    or the first segment for 2-part keys.
    """
    if df is None or df.empty:
        return None
    
    time_summary = df[df["section"] == "time_summary"].copy()
    if time_summary.empty:
        return None
    
    metric_rows = time_summary[time_summary["key"].str.endswith(f":{metric_key}", na=False)]
    if metric_rows.empty:
        metric_rows = time_summary[time_summary["key"].str.contains(metric_key, case=False, na=False)]
    
    if metric_rows.empty:
        return None
    
    trend_data = []
    seen_periods = set()
    
    for _, row in metric_rows.iterrows():
        key_parts = row["key"].split(":")
        
        if len(key_parts) >= 3:
            period = key_parts[-2].strip()
        elif len(key_parts) == 2:
            period = key_parts[0].strip()
        else:
            continue
        
        if period in seen_periods:
            continue
        
        try:
            value = float(row["value"])
            trend_data.append({"period": period, "value": value})
            seen_periods.add(period)
        except (ValueError, TypeError):
            continue
    
    if len(trend_data) < 2:
        return None
    
    result = pd.DataFrame(trend_data)
    
    try:
        result["sort_key"] = pd.to_numeric(result["period"], errors="coerce")
        if result["sort_key"].isna().all():
            result = result.sort_values("period")
        else:
            result = result.sort_values("sort_key")
        result = result.drop(columns=["sort_key"])
    except Exception:
        result = result.sort_values("period")
    
    return result


def render_trend_chart(df: pd.DataFrame, metric_key: str, metric_label: str):
    """Render a small trend chart for a KPI metric."""
    trend_data = extract_trend_data(df, metric_key)
    if trend_data is not None and len(trend_data) > 1:
        trend_data = trend_data.set_index("period")
        st.line_chart(trend_data, height=100)


def render_kpi_dashboard(metrics_df: pd.DataFrame, run_id: str = "", show_trends: bool = True):
    """
    Render a KPI dashboard with core metrics, descriptions, and trend charts.
    
    Args:
        metrics_df: DataFrame containing metrics.csv data
        run_id: Run ID for unique widget keys
        show_trends: Whether to show trend charts for each KPI
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
                "label": "Total Rows (count)",
                "value": int(float(row_count_row["value"].iloc[0])),
                "format": "count",
                "metric_key": None,
                "description": KPI_DEFINITIONS.get("Total Rows", "")
            })
    
    if not time_summary.empty:
        metric_mappings = [
            ("sum_sales", "Total Sales ($)", "currency"),
            ("sum_profit", "Total Profit ($)", "currency"),
            ("sum_units", "Total Units (count)", "count"),
            ("sum_discount", "Total Discount ($)", "currency"),
        ]
        
        for metric_key, label, fmt in metric_mappings:
            metric_rows = time_summary[time_summary["key"].str.contains(metric_key)]
            if not metric_rows.empty:
                total = metric_rows["value"].astype(float).sum()
                kpis.append({
                    "label": label,
                    "value": total,
                    "format": fmt,
                    "metric_key": metric_key,
                    "description": KPI_DEFINITIONS.get(label.split(" (")[0], "")
                })
        
        margin_rows = time_summary[time_summary["key"].str.contains("profit_margin")]
        if not margin_rows.empty:
            avg_margin = margin_rows["value"].astype(float).mean()
            kpis.append({
                "label": "Avg Profit Margin (%)",
                "value": avg_margin,
                "format": "percent",
                "metric_key": "profit_margin",
                "description": KPI_DEFINITIONS.get("Avg Profit Margin", "")
            })
        
        discount_rows = time_summary[time_summary["key"].str.contains("avg_discount|discount_rate")]
        if not discount_rows.empty:
            avg_discount = discount_rows["value"].astype(float).mean()
            kpis.append({
                "label": "Average Discount (%)",
                "value": avg_discount,
                "format": "percent",
                "metric_key": "discount",
                "description": KPI_DEFINITIONS.get("Average Discount", "")
            })
    
    if not kpis:
        st.info("No KPI metrics found in the dataset.")
        return
    
    num_kpis = min(len(kpis), 5)
    cols = st.columns(num_kpis)
    
    for i, kpi in enumerate(kpis[:num_kpis]):
        with cols[i]:
            value_str = format_kpi_value(kpi["value"], kpi["format"])
            st.metric(kpi["label"], value_str, help=kpi["description"])
            
            if show_trends and kpi.get("metric_key"):
                render_trend_chart(df, kpi["metric_key"], kpi["label"])
            
            if kpi["description"]:
                st.caption(kpi["description"][:60] + "..." if len(kpi["description"]) > 60 else kpi["description"])


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
