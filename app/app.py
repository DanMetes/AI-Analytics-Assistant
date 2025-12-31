"""AI Analytics Assistant - Artifact Viewer UI"""
import streamlit as st
import json
from pathlib import Path
import pandas as pd

st.set_page_config(
    page_title="AI Analytics Assistant",
    page_icon="📊",
    layout="wide"
)

PROJECTS_DIR = Path("projects")


def discover_projects_and_runs():
    """Scan projects/<project_id>/runs/<run_id>/ structure."""
    projects = {}
    if not PROJECTS_DIR.exists():
        return projects
    
    for project_dir in PROJECTS_DIR.iterdir():
        if project_dir.is_dir():
            project_id = project_dir.name
            runs_dir = project_dir / "runs"
            if runs_dir.exists():
                runs = [r.name for r in runs_dir.iterdir() if r.is_dir()]
                if runs:
                    projects[project_id] = runs
    return projects


def get_run_path(project_id: str, run_id: str) -> Path:
    return PROJECTS_DIR / project_id / "runs" / run_id


def load_json_artifact(run_path: Path, filename: str):
    filepath = run_path / filename
    if filepath.exists():
        with open(filepath, "r") as f:
            return json.load(f)
    return None


def load_text_artifact(run_path: Path, filename: str):
    filepath = run_path / filename
    if filepath.exists():
        with open(filepath, "r") as f:
            return f.read()
    return None


class RunContext:
    """Holds all loaded artifacts for a run."""
    def __init__(self, run_path: Path):
        self.run_path = run_path
        self.analysis_log = load_json_artifact(run_path, "analysis_log.json")
        self.data_profile = load_json_artifact(run_path, "data_profile.json")
        self.anomalies = load_json_artifact(run_path, "anomalies_normalized.json")
        self.interpretation = load_json_artifact(run_path, "interpretation.json")
        self.analysis_plan = load_json_artifact(run_path, "analysis_plan.json")
        self.ingest_meta = load_json_artifact(run_path, "ingest_meta.json")
        self.report_md = load_text_artifact(run_path, "report.md")
        
        metrics_path = run_path / "metrics.csv"
        self.metrics_df = pd.read_csv(metrics_path) if metrics_path.exists() else None


def render_run_header(ctx: RunContext):
    """Render run summary header."""
    import sys
    from pathlib import Path
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from ui_components.header import render_run_header as _render_header
    _render_header(ctx.run_path, ctx.analysis_log, ctx.data_profile, ctx.anomalies)


def render_overview(ctx: RunContext):
    """Overview tab with curated layout."""
    st.header("Overview")
    render_run_header(ctx)
    
    st.subheader("Dataset & Run")
    source_csv = "N/A"
    if ctx.analysis_log and "profile_summary" in ctx.analysis_log:
        source_csv = ctx.analysis_log["profile_summary"].get("source_csv", "N/A")
    elif ctx.data_profile:
        source_csv = ctx.data_profile.get("source_csv", "N/A")
    
    rows = ctx.data_profile.get("rows", "N/A") if ctx.data_profile else "N/A"
    cols = ctx.data_profile.get("cols", "N/A") if ctx.data_profile else "N/A"
    
    rows_display = f"{rows:,}" if isinstance(rows, (int, float)) else str(rows)
    cols_display = str(cols)
    
    st.markdown(f"""
- **Source file:** `{source_csv}`
- **Rows:** {rows_display} | **Columns:** {cols_display}
- **Run ID:** `{ctx.run_path.name}`
- **Project ID:** `{ctx.run_path.parent.parent.name}`
    """)
    
    st.subheader("Data Quality")
    if ctx.data_profile and "columns" in ctx.data_profile:
        columns = ctx.data_profile["columns"]
        
        missing_cols = [
            (name, info.get("missing_fraction", 0) * 100)
            for name, info in columns.items()
            if info.get("missing_count", 0) > 0
        ]
        if missing_cols:
            missing_cols.sort(key=lambda x: x[1], reverse=True)
            st.markdown("**Missingness:**")
            for name, pct in missing_cols[:5]:
                st.markdown(f"- `{name}`: {pct:.1f}% missing")
        else:
            st.markdown("- ✅ No missing values detected")
        
        high_card = [
            (name, info.get("cardinality", 0))
            for name, info in columns.items()
            if info.get("cardinality", 0) > 100
        ]
        if high_card:
            high_card.sort(key=lambda x: x[1], reverse=True)
            st.markdown("**High-cardinality columns:**")
            for name, card in high_card[:5]:
                st.markdown(f"- `{name}`: {card:,} unique values")
        
        skewed = [
            (name, info.get("skew", 0))
            for name, info in columns.items()
            if info.get("skew_flag", False)
        ]
        if skewed:
            skewed.sort(key=lambda x: abs(x[1]), reverse=True)
            st.markdown("**Skewed distributions:**")
            for name, skew in skewed[:5]:
                st.markdown(f"- `{name}`: skew = {skew:.2f}")
    else:
        st.info("Data profile not available.")
    
    st.subheader("What to review next")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info("📋 **Key Findings** - Review detected anomalies and insights")
    with col2:
        st.info("📊 **Metrics** - Explore KPIs and segment breakdowns")
    with col3:
        st.info("🔍 **Profiling & EDA** - Deep-dive into data distributions")
    
    with st.expander("Advanced: raw artifacts"):
        if ctx.ingest_meta:
            st.markdown("**Ingest Metadata:**")
            st.json(ctx.ingest_meta)
        if ctx.analysis_plan:
            st.markdown("**Analysis Plan:**")
            st.json(ctx.analysis_plan)
        if ctx.analysis_log:
            st.markdown("**Analysis Log:**")
            st.json(ctx.analysis_log)


def render_key_findings(ctx: RunContext):
    """Key Findings tab with severity cards."""
    st.header("Key Findings")
    render_run_header(ctx)
    
    anomaly_list = []
    if ctx.anomalies:
        if isinstance(ctx.anomalies, dict) and "anomalies" in ctx.anomalies:
            anomaly_list = ctx.anomalies.get("anomalies", [])
        elif isinstance(ctx.anomalies, list):
            anomaly_list = ctx.anomalies
    
    if anomaly_list:
        st.subheader(f"Anomalies ({len(anomaly_list)} detected)")
        
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        sorted_anomalies = sorted(
            anomaly_list, 
            key=lambda x: severity_order.get(x.get("severity", "info"), 3)
        )
        
        for anomaly in sorted_anomalies:
            severity = anomaly.get("severity", "info")
            
            if severity == "critical":
                badge = "🔴 **CRITICAL**"
                border_color = "#ff4b4b"
            elif severity == "warning":
                badge = "🟠 **WARNING**"
                border_color = "#ffa500"
            else:
                badge = "🔵 **INFO**"
                border_color = "#4b9fff"
            
            summary = anomaly.get("summary", "No summary available")
            metric = anomaly.get("metric", "N/A")
            value = anomaly.get("value", "N/A")
            
            st.markdown(f"""
<div style="border-left: 4px solid {border_color}; padding: 10px 15px; margin: 10px 0; background: rgba(0,0,0,0.05); border-radius: 4px;">
    <div>{badge}</div>
    <div style="margin-top: 8px;"><strong>{summary}</strong></div>
    <div style="margin-top: 5px; font-size: 0.9em; color: #666;">
        Metric: <code>{metric}</code> | Value: <code>{value}</code>
    </div>
</div>
            """, unsafe_allow_html=True)
            
            with st.expander("View details"):
                threshold = anomaly.get("threshold", {})
                if threshold:
                    st.markdown(f"**Thresholds:** Critical > {threshold.get('critical', 'N/A')}, Warning > {threshold.get('warning', 'N/A')}")
                st.markdown(f"**Direction:** {anomaly.get('direction', 'N/A')}")
                if "evidence_keys" in anomaly:
                    st.markdown(f"**Evidence:** `{', '.join(anomaly['evidence_keys'])}`")
                st.markdown("**Recommended:** Investigate the data for this time period to understand the spike.")
    else:
        st.success("✅ No anomalies detected in this analysis run.")
    
    if ctx.interpretation:
        st.subheader("Interpretation")
        if isinstance(ctx.interpretation, dict):
            if "summary" in ctx.interpretation:
                st.markdown(ctx.interpretation["summary"])
            if "findings" in ctx.interpretation:
                for finding in ctx.interpretation["findings"]:
                    st.markdown(f"- {finding}")
            if "recommendations" in ctx.interpretation:
                st.markdown("**Recommendations:**")
                for rec in ctx.interpretation["recommendations"]:
                    st.markdown(f"- {rec}")
        else:
            st.write(ctx.interpretation)
    
    with st.expander("Advanced: raw artifacts"):
        if ctx.anomalies:
            st.markdown("**Anomalies (raw):**")
            st.json(ctx.anomalies)
        if ctx.interpretation:
            st.markdown("**Interpretation (raw):**")
            st.json(ctx.interpretation)


def render_metrics(ctx: RunContext):
    """Metrics tab with headline KPIs and explorer."""
    st.header("Metrics")
    render_run_header(ctx)
    
    if ctx.metrics_df is None or ctx.metrics_df.empty:
        st.info("No metrics.csv found for this run.")
        return
    
    df = ctx.metrics_df
    
    st.subheader("Headline KPIs")
    
    overall_rows = df[df["section"] == "overall"]
    time_summary = df[df["section"] == "time_summary"]
    
    kpi_cols = st.columns(4)
    kpi_count = 0
    
    if not overall_rows.empty:
        row_count_row = overall_rows[overall_rows["key"] == "row_count"]
        if not row_count_row.empty:
            with kpi_cols[kpi_count % 4]:
                st.metric("Total Rows", f"{int(float(row_count_row['value'].iloc[0])):,}")
            kpi_count += 1
    
    if not time_summary.empty:
        for metric_key in ["sum_sales", "sum_profit", "sum_units"]:
            metric_rows = time_summary[time_summary["key"].str.contains(metric_key)]
            if not metric_rows.empty:
                total = metric_rows["value"].astype(float).sum()
                label = metric_key.replace("sum_", "Total ").title()
                with kpi_cols[kpi_count % 4]:
                    if "sales" in metric_key or "profit" in metric_key:
                        st.metric(label, f"${total:,.0f}")
                    else:
                        st.metric(label, f"{total:,.0f}")
                kpi_count += 1
                if kpi_count >= 4:
                    break
    
    st.subheader("Explore Drivers")
    
    sections = df["section"].unique().tolist()
    summary_sections = [s for s in sections if "summary" in s.lower() and s != "time_summary"]
    
    if summary_sections or "time_summary" in sections:
        available_sections = ["time_summary"] + summary_sections if "time_summary" in sections else summary_sections
        
        col1, col2 = st.columns(2)
        with col1:
            selected_section = st.selectbox("Group by", available_sections, format_func=lambda x: x.replace("_summary", "").replace("_", " ").title())
        
        section_df = df[df["section"] == selected_section].copy()
        
        if not section_df.empty:
            section_df["group"] = section_df["key"].apply(lambda x: x.split(":")[0] if ":" in x else x)
            section_df["metric_name"] = section_df["key"].apply(lambda x: x.split(":")[-1] if ":" in x else x)
            
            available_metrics = section_df["metric_name"].unique().tolist()
            numeric_metrics = [m for m in available_metrics if m not in ["n", "group_by"]]
            
            with col2:
                if numeric_metrics:
                    selected_metric = st.selectbox("Metric", numeric_metrics)
                else:
                    selected_metric = available_metrics[0] if available_metrics else None
            
            if selected_metric:
                metric_df = section_df[section_df["metric_name"] == selected_metric].copy()
                metric_df["value"] = pd.to_numeric(metric_df["value"], errors="coerce")
                metric_df = metric_df.dropna(subset=["value"])
                metric_df = metric_df.sort_values("value", ascending=False).head(15)
                
                if not metric_df.empty:
                    col1, col2 = st.columns([1, 1])
                    
                    with col1:
                        st.markdown("**Top 15 Groups:**")
                        display_df = metric_df[["group", "value"]].copy()
                        display_df.columns = ["Group", selected_metric.replace("_", " ").title()]
                        st.dataframe(display_df, hide_index=True, width="stretch")
                    
                    with col2:
                        st.markdown("**Distribution:**")
                        chart_df = metric_df.set_index("group")["value"]
                        st.bar_chart(chart_df)
    
    csv_data = df.to_csv(index=False)
    st.download_button(
        label="📥 Download Full Metrics CSV",
        data=csv_data,
        file_name="metrics.csv",
        mime="text/csv"
    )
    
    with st.expander("Advanced: full metrics table"):
        st.dataframe(df, width="stretch")


def render_profiling_eda(ctx: RunContext):
    """Profiling & EDA tab with highlights first."""
    st.header("Profiling & EDA")
    render_run_header(ctx)
    
    if ctx.data_profile:
        st.subheader("Data Highlights")
        
        rows = ctx.data_profile.get("rows", "N/A")
        cols = ctx.data_profile.get("cols", "N/A")
        columns = ctx.data_profile.get("columns", {})
        
        total_missing = sum(
            col_info.get("missing_count", 0) 
            for col_info in columns.values()
        ) if columns else 0
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Rows", f"{rows:,}" if isinstance(rows, int) else rows)
        with col2:
            st.metric("Columns", cols)
        with col3:
            st.metric("Missing Cells", f"{total_missing:,}")
        
        if columns:
            missing_cols = [
                (name, info.get("missing_count", 0), info.get("missing_fraction", 0) * 100)
                for name, info in columns.items()
                if info.get("missing_count", 0) > 0
            ]
            if missing_cols:
                missing_cols.sort(key=lambda x: x[1], reverse=True)
                st.markdown("**Most Missing Columns:**")
                for name, count, pct in missing_cols[:5]:
                    st.markdown(f"- `{name}`: {count:,} ({pct:.1f}%)")
            
            skewed_cols = [
                (name, info.get("skew", 0))
                for name, info in columns.items()
                if info.get("skew_flag", False)
            ]
            if skewed_cols:
                skewed_cols.sort(key=lambda x: abs(x[1]), reverse=True)
                st.markdown("**Most Skewed Columns:**")
                for name, skew in skewed_cols[:5]:
                    st.markdown(f"- `{name}`: skew = {skew:.2f}")
    
    eda_html_path = ctx.run_path / "eda_report.html"
    if eda_html_path.exists():
        st.subheader("Full Profiling Report")
        with open(eda_html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        st.components.v1.html(html_content, height=800, scrolling=True)
    else:
        st.warning("""
**Profiling report not available**

The ydata-profiling HTML report was not generated for this run.

**Why this happens:**
- ydata-profiling requires Python 3.12 or earlier
- The `ydata-profiling` package may not be installed

**How to enable:**
1. Ensure Python version is 3.11 or 3.12
2. Install with: `pip install ydata-profiling`
3. Re-run the analysis
        """)
    
    plots_dir = ctx.run_path / "plots"
    if plots_dir.exists():
        st.subheader("Generated Plots")
        plots = list(plots_dir.glob("*.png"))
        if plots:
            cols = st.columns(2)
            for i, plot_path in enumerate(sorted(plots)):
                with cols[i % 2]:
                    st.image(str(plot_path), caption=plot_path.stem.replace("_", " ").title())
        else:
            st.info("No plot images found in this run.")


def render_ask_explore(ctx: RunContext):
    """Ask & Explore tab - stub for future LLM integration."""
    st.header("Ask & Explore")
    render_run_header(ctx)
    
    st.info("🚧 This feature is coming soon - LLM-backed Q&A for your analysis artifacts.")
    
    st.subheader("Ask a Question")
    question = st.text_input("Enter your question about this analysis run:", placeholder="e.g., What caused the spike in units for 2023?")
    
    if st.button("Submit", type="primary"):
        if question:
            st.warning(f"LLM integration coming soon. Your question: **{question}**")
        else:
            st.error("Please enter a question.")
    
    st.subheader("Available Artifacts")
    if ctx.run_path.exists():
        artifacts = sorted([f.name for f in ctx.run_path.iterdir() if f.is_file()])
        for artifact in artifacts:
            st.markdown(f"- `{artifact}`")


def main():
    st.title("📊 AI Analytics Assistant")
    st.caption("Artifact-driven analytics review for your data projects")
    
    projects = discover_projects_and_runs()
    
    if not projects:
        st.warning("No projects found. Please ensure projects exist under `./projects/<project_id>/runs/<run_id>/`")
        return
    
    st.sidebar.header("Select Run")
    
    project_ids = list(projects.keys())
    selected_project = st.sidebar.selectbox(
        "Project ID",
        project_ids,
        format_func=lambda x: f"{x[:8]}..." if len(x) > 8 else x
    )
    
    if selected_project:
        run_ids = projects[selected_project]
        selected_run = st.sidebar.selectbox(
            "Run ID",
            run_ids,
            format_func=lambda x: f"{x[:8]}..." if len(x) > 8 else x
        )
        
        if selected_run:
            run_path = get_run_path(selected_project, selected_run)
            ctx = RunContext(run_path)
            
            st.sidebar.markdown("---")
            st.sidebar.markdown(f"**Project:** `{selected_project[:12]}...`")
            st.sidebar.markdown(f"**Run:** `{selected_run[:12]}...`")
            
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "Overview",
                "Key Findings", 
                "Metrics",
                "Profiling & EDA",
                "Ask & Explore"
            ])
            
            with tab1:
                render_overview(ctx)
            
            with tab2:
                render_key_findings(ctx)
            
            with tab3:
                render_metrics(ctx)
            
            with tab4:
                render_profiling_eda(ctx)
            
            with tab5:
                render_ask_explore(ctx)


if __name__ == "__main__":
    main()
