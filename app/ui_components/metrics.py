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
        
        if "=" in period:
            period = period.split("=", 1)[1]
        
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


def analyze_top_drivers(
    metrics_df: pd.DataFrame, 
    metric: str, 
    group_by: str, 
    top_n: int = 5
) -> Optional[pd.DataFrame]:
    """
    Calculate the top n contributors for a given metric grouped by a field.
    
    Args:
        metrics_df: DataFrame containing metrics.csv data
        metric: The metric to analyze (e.g., "sum_sales")
        group_by: The group-by field to analyze
        top_n: Number of top contributors to return
        
    Returns:
        DataFrame with columns: group, value, percent_of_total
    """
    if metrics_df is None or metrics_df.empty:
        return None
    
    group_summary = metrics_df[metrics_df["section"] == "group_summary"].copy()
    if group_summary.empty:
        return None
    
    metric_rows = group_summary[group_summary["key"].str.contains(metric, case=False, na=False)]
    if metric_rows.empty:
        return None
    
    results = []
    for _, row in metric_rows.iterrows():
        key = row["key"]
        key_parts = key.split(":")
        
        if len(key_parts) >= 2:
            group_value = key_parts[0]
            try:
                value = float(row["value"])
                results.append({"group": group_value, "value": value})
            except (ValueError, TypeError):
                continue
    
    if not results:
        return None
    
    df = pd.DataFrame(results)
    df = df.groupby("group", as_index=False).sum()
    
    total = df["value"].sum()
    df["percent_of_total"] = (df["value"] / total * 100) if total > 0 else 0
    
    df = df.sort_values("value", ascending=False).head(top_n)
    
    return df


def detect_outliers(metrics_df: pd.DataFrame, threshold_ratio: float = 5.0) -> List[Dict[str, Any]]:
    """
    Detect metrics where outliers may dominate results.
    
    Compares the 95th percentile vs median ratio for each numeric metric.
    If ratio exceeds threshold (default 5x), flags as potential outlier issue.
    
    Returns:
        List of dicts with metric name, median, p95, ratio, and warning message
    """
    if metrics_df is None or metrics_df.empty:
        return []
    
    warnings = []
    
    overall = metrics_df[metrics_df["section"] == "overall"].copy()
    time_summary = metrics_df[metrics_df["section"] == "time_summary"].copy()
    
    all_metrics = pd.concat([overall, time_summary])
    
    metric_names = set()
    for key in all_metrics["key"].unique():
        parts = key.split(":")
        metric_name = parts[-1] if len(parts) > 0 else key
        if metric_name and not metric_name.isdigit():
            metric_names.add(metric_name)
    
    for metric_name in metric_names:
        metric_rows = all_metrics[all_metrics["key"].str.endswith(metric_name)]
        if len(metric_rows) < 2:
            continue
        
        try:
            values = metric_rows["value"].astype(float)
            if len(values) < 3:
                continue
            
            median_val = values.median()
            p95_val = values.quantile(0.95)
            
            if median_val > 0 and p95_val > 0:
                ratio = p95_val / median_val
                if ratio >= threshold_ratio:
                    warnings.append({
                        "metric": metric_name,
                        "median": median_val,
                        "p95": p95_val,
                        "ratio": ratio,
                        "warning": f"The 95th percentile is {ratio:.1f}× the median, suggesting outliers may dominate aggregated results."
                    })
        except (ValueError, TypeError):
            continue
    
    return warnings


def render_top_drivers(metrics_df: pd.DataFrame, run_id: str = "", selected_section: str = "group_summary"):
    """Render the Top Drivers section with bar chart and table.
    
    Args:
        metrics_df: DataFrame containing metrics.csv data
        run_id: Unique run ID for widget keys
        selected_section: The section from which to extract top drivers
    """
    st.markdown("### Top Drivers")
    st.caption("Identify which groups contribute most to your key metrics.")
    
    if metrics_df is None or metrics_df.empty:
        st.info("No metrics data available for driver analysis.")
        return
    
    section_df = metrics_df[metrics_df["section"] == selected_section]
    if section_df.empty:
        summary_sections = [s for s in metrics_df["section"].unique() if "_summary" in s]
        if summary_sections:
            section_df = metrics_df[metrics_df["section"] == summary_sections[0]]
    
    if section_df.empty:
        st.info("No group-level data available. Run analysis with group-by dimensions to see top drivers.")
        return
    
    available_metrics = set()
    for key in section_df["key"].unique():
        parts = key.split(":")
        if len(parts) > 1:
            metric_name = parts[-1]
            if metric_name and not metric_name.isdigit():
                available_metrics.add(metric_name)
    
    if not available_metrics:
        st.info("No groupable metrics found in selected section.")
        return
    
    col1, col2 = st.columns(2)
    with col1:
        selected_metric = st.selectbox(
            "Select metric",
            sorted(available_metrics),
            key=f"top_driver_metric_{run_id}"
        )
    with col2:
        top_n = st.slider("Top N", min_value=3, max_value=10, value=5, key=f"top_driver_n_{run_id}")
    
    metric_rows = section_df[section_df["key"].str.contains(selected_metric, case=False, na=False)]
    
    results = []
    for _, row in metric_rows.iterrows():
        key = row["key"]
        key_parts = key.split(":")
        if len(key_parts) >= 2:
            dimension_value = key_parts[0]
            if "=" in dimension_value:
                group_value = dimension_value.split("=", 1)[1]
            else:
                group_value = dimension_value
            try:
                value = float(row["value"])
                results.append({"group": group_value, "value": value})
            except (ValueError, TypeError):
                continue
    
    if not results:
        st.info(f"No data available for {selected_metric}.")
        return
    
    drivers = pd.DataFrame(results)
    drivers = drivers.groupby("group", as_index=False).sum()
    total = drivers["value"].sum()
    drivers["percent_of_total"] = (drivers["value"] / total * 100) if total > 0 else 0
    drivers = drivers.sort_values("value", ascending=False).head(top_n)
    
    if not drivers.empty:
        import matplotlib.pyplot as plt
        
        chart_col, table_col = st.columns([1, 1])
        
        with chart_col:
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.barh(drivers["group"].iloc[::-1], drivers["value"].iloc[::-1])
            ax.set_xlabel(selected_metric.replace("_", " ").title())
            ax.set_ylabel("Group")
            ax.set_title(f"Top {top_n} by {selected_metric.replace('_', ' ').title()}")
            plt.tight_layout()
            st.pyplot(fig)
            plt.close()
        
        with table_col:
            display_df = drivers[["group", "value", "percent_of_total"]].copy()
            display_df.columns = ["Group", "Value", "% of Total"]
            display_df["Value"] = display_df["Value"].apply(lambda x: f"{x:,.2f}")
            display_df["% of Total"] = display_df["% of Total"].apply(lambda x: f"{x:.1f}%")
            st.dataframe(display_df, hide_index=True, use_container_width=True)
    else:
        st.info(f"No data available for {selected_metric}.")


def render_outlier_warnings(metrics_df: pd.DataFrame):
    """Render outlier detection warnings."""
    warnings = detect_outliers(metrics_df)
    
    if warnings:
        st.markdown("### Outlier Detection")
        st.caption("Metrics where extreme values may skew results.")
        
        for w in warnings[:3]:
            st.warning(f"""
**{w['metric'].replace('_', ' ').title()}**: {w['warning']}

- Median: {w['median']:,.2f}
- 95th Percentile: {w['p95']:,.2f}

Consider reviewing the distribution or drilling down into individual rows to understand the outliers.
            """)


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
