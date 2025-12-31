"""AI Analytics Assistant - Artifact Viewer UI"""
import streamlit as st
import streamlit.components.v1 as components
import json
import subprocess
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

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
    """Overview tab with curated layout using render_run_summary."""
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from ui_components.summary import render_run_summary, display_run_summary
    
    st.header("Overview")
    render_run_header(ctx)
    
    st.subheader("Run Summary")
    display_run_summary(ctx.run_path)
    
    anomaly_list = []
    if ctx.anomalies:
        if isinstance(ctx.anomalies, dict) and "anomalies" in ctx.anomalies:
            anomaly_list = ctx.anomalies.get("anomalies", [])
        elif isinstance(ctx.anomalies, list):
            anomaly_list = ctx.anomalies
    
    if anomaly_list:
        critical_count = sum(1 for a in anomaly_list if a.get("severity") == "critical")
        warning_count = sum(1 for a in anomaly_list if a.get("severity") == "warning")
        
        if critical_count > 0:
            st.warning(f"**{len(anomaly_list)} anomalies detected** ({critical_count} critical, {warning_count} warnings). See the **Key Findings** tab for details and recommended actions.")
        elif warning_count > 0:
            st.info(f"**{len(anomaly_list)} anomalies detected** ({warning_count} warnings). See the **Key Findings** tab for details.")
        else:
            st.info(f"**{len(anomaly_list)} anomalies detected**. See the **Key Findings** tab for details.")
    else:
        st.success("No anomalies detected in this run.")
    
    st.subheader("Data Quality Highlights")
    if ctx.data_profile and "columns" in ctx.data_profile:
        columns = ctx.data_profile["columns"]
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Missingness**")
            missing_cols = [
                (name, info.get("missing_count", 0), info.get("missing_fraction", 0) * 100)
                for name, info in columns.items()
                if info.get("missing_count", 0) > 0
            ]
            if missing_cols:
                missing_cols.sort(key=lambda x: x[1], reverse=True)
                for name, count, pct in missing_cols[:5]:
                    st.markdown(f"- `{name}`: {count:,} ({pct:.1f}%)")
            else:
                st.success("No missing values")
        
        with col2:
            st.markdown("**High-Cardinality**")
            high_card = [
                (name, info.get("cardinality", 0))
                for name, info in columns.items()
                if info.get("cardinality", 0) > 100
            ]
            if high_card:
                high_card.sort(key=lambda x: x[1], reverse=True)
                for name, card in high_card[:5]:
                    st.markdown(f"- `{name}`: {card:,} unique")
            else:
                st.success("No high-cardinality columns")
        
        with col3:
            st.markdown("**Skew Flags**")
            skewed = [
                (name, info.get("skew", 0))
                for name, info in columns.items()
                if info.get("skew_flag", False)
            ]
            if skewed:
                skewed.sort(key=lambda x: abs(x[1]), reverse=True)
                for name, skew in skewed[:5]:
                    st.markdown(f"- `{name}`: {skew:.2f}")
            else:
                st.success("No skewed distributions")
    else:
        st.info("Data profile not available for this run.")
    
    with st.expander("Raw artifact data"):
        if ctx.ingest_meta:
            st.markdown("**Ingest Metadata:**")
            st.json(ctx.ingest_meta)
        if ctx.analysis_plan:
            st.markdown("**Analysis Plan:**")
            st.json(ctx.analysis_plan)
        if ctx.analysis_log:
            st.markdown("**Analysis Log:**")
            st.json(ctx.analysis_log)
        if ctx.data_profile:
            st.markdown("**Data Profile:**")
            st.json(ctx.data_profile)


def render_key_findings(ctx: RunContext):
    """Key Findings tab with consolidated anomalies and interpretation."""
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from ui_components.findings import normalize_and_group_anomalies, render_anomaly_card, render_interpretation_bullets
    from llm_utils import render_llm_interpretation, render_llm_placeholder
    
    st.header("Key Findings")
    render_run_header(ctx)
    
    anomaly_list = []
    if ctx.anomalies:
        if isinstance(ctx.anomalies, dict) and "anomalies" in ctx.anomalies:
            anomaly_list = ctx.anomalies.get("anomalies", [])
        elif isinstance(ctx.anomalies, list):
            anomaly_list = ctx.anomalies
    
    if anomaly_list:
        grouped_anomalies = normalize_and_group_anomalies(anomaly_list)
        
        critical_count = sum(1 for g in grouped_anomalies if g["severity"] == "critical")
        warning_count = sum(1 for g in grouped_anomalies if g["severity"] == "warning")
        
        st.markdown(f"**{len(grouped_anomalies)} issue groups** detected ({critical_count} critical, {warning_count} warnings)")
        
        run_id = ctx.run_path.name
        for i, group in enumerate(grouped_anomalies):
            render_anomaly_card(group, run_id, i)
    else:
        st.success("No anomalies detected in this analysis run.")
    
    st.subheader("Interpretation Summary")
    
    if ctx.interpretation:
        render_interpretation_bullets(ctx.interpretation)
    else:
        if anomaly_list:
            st.info("Detailed interpretation not available. Review the anomaly cards above for findings.")
        else:
            st.info("No interpretation available for this run.")
    
    if not render_llm_interpretation(ctx.run_path):
        render_llm_placeholder()
    
    with st.expander("Raw anomaly data"):
        if ctx.anomalies:
            st.json(ctx.anomalies)
        if ctx.interpretation:
            st.markdown("**Interpretation JSON:**")
            st.json(ctx.interpretation)


def render_metrics(ctx: RunContext):
    """Metrics tab with headline KPIs and Trends/Drivers panel."""
    st.header("Metrics")
    render_run_header(ctx)
    
    if ctx.metrics_df is None or ctx.metrics_df.empty:
        st.info("No metrics.csv found for this run.")
        return
    
    df = ctx.metrics_df
    
    st.subheader("Headline KPIs")
    
    time_summary = df[df["section"] == "time_summary"].copy()
    overall = df[df["section"] == "overall"].copy()
    
    kpi_data = {}
    
    if not overall.empty:
        row_count_row = overall[overall["key"] == "row_count"]
        if not row_count_row.empty:
            kpi_data["Total Rows"] = (int(float(row_count_row["value"].iloc[0])), "count")
    
    if not time_summary.empty:
        for metric_key in ["sum_sales", "sum_profit", "sum_units"]:
            metric_rows = time_summary[time_summary["key"].str.contains(metric_key)]
            if not metric_rows.empty:
                total = metric_rows["value"].astype(float).sum()
                label = metric_key.replace("sum_", "Total ").replace("_", " ").title()
                metric_type = "currency" if metric_key in ["sum_sales", "sum_profit"] else "count"
                kpi_data[label] = (total, metric_type)
        
        margin_rows = time_summary[time_summary["key"].str.contains("profit_margin")]
        if not margin_rows.empty:
            avg_margin = margin_rows["value"].astype(float).mean()
            kpi_data["Avg Profit Margin"] = (avg_margin, "percent")
    
    if kpi_data:
        kpi_cols = st.columns(min(len(kpi_data), 5))
        for i, (label, (value, metric_type)) in enumerate(list(kpi_data.items())[:5]):
            with kpi_cols[i]:
                if metric_type == "currency":
                    st.metric(label, f"${value:,.0f}")
                elif metric_type == "percent":
                    st.metric(label, f"{value:.1%}")
                else:
                    st.metric(label, f"{value:,.0f}")
    
    st.subheader("Trends and Drivers")
    
    col1, col2 = st.columns(2)
    
    sections = df["section"].unique().tolist()
    summary_sections = [s for s in sections if "summary" in s.lower()]
    
    available_sections = summary_sections if summary_sections else sections[:5]
    
    with col1:
        selected_section = st.selectbox(
            "Group by", 
            available_sections, 
            format_func=lambda x: x.replace("_summary", "").replace("_", " ").title()
        )
    
    section_df = df[df["section"] == selected_section].copy()
    
    if not section_df.empty:
        section_df["group"] = section_df["key"].apply(lambda x: x.split(":")[0] if ":" in x else x)
        section_df["metric_name"] = section_df["key"].apply(lambda x: x.split(":")[-1] if ":" in x else x)
        
        available_metrics = section_df["metric_name"].unique().tolist()
        numeric_metrics = [m for m in available_metrics if m not in ["n", "group_by"]]
        
        with col2:
            if numeric_metrics:
                default_metric = "sum_sales" if "sum_sales" in numeric_metrics else numeric_metrics[0]
                selected_metric = st.selectbox("Metric", numeric_metrics, index=numeric_metrics.index(default_metric) if default_metric in numeric_metrics else 0)
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
                    fig, ax = plt.subplots(figsize=(8, 6))
                    bars = ax.barh(metric_df["group"].iloc[::-1], metric_df["value"].iloc[::-1])
                    ax.set_xlabel(selected_metric.replace("_", " ").title())
                    ax.set_ylabel("Group")
                    plt.tight_layout()
                    st.pyplot(fig)
                    plt.close()
    
    with st.expander("Full metrics data"):
        st.dataframe(df, width="stretch")
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="Download Full Metrics CSV",
            data=csv_data,
            file_name="metrics.csv",
            mime="text/csv"
        )


def render_profiling_eda(ctx: RunContext):
    """Profiling & EDA tab with merged highlights."""
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from ui_components.plots import render_plots
    
    st.header("Profiling & EDA")
    render_run_header(ctx)
    
    st.subheader("EDA Highlights")
    
    if ctx.data_profile:
        rows = ctx.data_profile.get("rows", "N/A")
        cols = ctx.data_profile.get("cols", "N/A")
        columns = ctx.data_profile.get("columns", {})
        
        total_missing = sum(
            col_info.get("missing_count", 0) 
            for col_info in columns.values()
        ) if columns else 0
        
        total_cells = rows * cols if isinstance(rows, int) and isinstance(cols, int) else 0
        missing_rate = (total_missing / total_cells * 100) if total_cells > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Rows", f"{rows:,}" if isinstance(rows, int) else rows)
        with col2:
            st.metric("Columns", cols)
        with col3:
            st.metric("Missing Cells", f"{total_missing:,}")
        with col4:
            st.metric("Missing Rate", f"{missing_rate:.2f}%")
        
        if columns:
            numeric_cols = [name for name, info in columns.items() if info.get("dtype") in ["int", "float"]]
            string_cols = [name for name, info in columns.items() if info.get("dtype") == "string"]
            
            st.markdown(f"**Column Types:** {len(numeric_cols)} numeric, {len(string_cols)} categorical")
            
            skewed_cols = [
                (name, info.get("skew", 0))
                for name, info in columns.items()
                if info.get("skew_flag", False)
            ]
            if skewed_cols:
                skewed_cols.sort(key=lambda x: abs(x[1]), reverse=True)
                st.markdown("**Highly Skewed Columns:**")
                for name, skew in skewed_cols[:3]:
                    direction = "right" if skew > 0 else "left"
                    st.markdown(f"- `{name}`: {skew:.2f} ({direction}-skewed)")
    else:
        st.info("Data profile not available for this run.")
    
    st.subheader("Full Profiling Report")
    eda_html_path = ctx.run_path / "eda_report.html"
    if eda_html_path.exists():
        with open(eda_html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        components.html(html_content, height=800, scrolling=True)
    else:
        st.warning("""
**Profiling report not available**

The ydata-profiling HTML report was not generated for this run.

**How to enable profiling:**
1. Ensure Python version is 3.11 or 3.12 (required by ydata-profiling)
2. Install with: `pip install -e '.[profiling]'`
3. Re-run the analysis

Profiling provides detailed statistical summaries, correlations, and data quality checks.
        """)
    
    st.subheader("Generated Plots")
    render_plots(ctx.run_path)


def render_ask_explore(ctx: RunContext):
    """Ask & Explore tab with CLI integration."""
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from llm_utils import load_llm_interpretation, render_llm_summary
    from ask_engine import run_ask_query, AskResult
    
    st.header("Ask & Explore")
    render_run_header(ctx)
    
    st.info("""
**LLM Q&A features coming soon**

Advanced natural language Q&A powered by LLM will be available when running with `--llm` flag.
Currently, questions are answered using existing analysis artifacts.
    """)
    
    st.subheader("Ask a Question")
    st.markdown("Query the analysis artifacts using natural language.")
    
    question = st.text_input(
        "Enter your question:",
        placeholder="e.g., What caused the spike in units for 2023?",
        key=f"ask_question_{ctx.run_path.name}"
    )
    
    project_id = ctx.run_path.parent.parent.name
    
    col1, col2 = st.columns([1, 4])
    with col1:
        submit = st.button("Submit", type="primary", key=f"ask_submit_{ctx.run_path.name}")
    
    if submit and question:
        with st.spinner("Processing your question..."):
            result = run_ask_query(project_id, question, use_llm=False, timeout=60)
        
        if result.success:
            if result.answerable:
                st.success("Answer found from existing artifacts:")
                st.markdown(result.answer)
                
                if result.evidence_keys:
                    st.markdown("**Supporting Evidence:**")
                    for key in result.evidence_keys:
                        st.markdown(f"- `{key}`")
            else:
                st.warning("This question cannot be answered from existing artifacts. A methodology plan has been generated.")
                
                if result.plan_steps:
                    st.markdown("**Methodology Plan:**")
                    for i, step in enumerate(result.plan_steps, 1):
                        st.markdown(f"{i}. {step}")
                elif result.plan:
                    st.markdown("**Methodology Plan:**")
                    st.markdown(result.plan)
                
                if result.code:
                    st.markdown("**Generated Python Code:**")
                    st.code(result.code, language="python")
                    
                    st.download_button(
                        label="Download Code",
                        data=result.code,
                        file_name="generated_query.py",
                        mime="text/x-python",
                        key=f"download_code_{ctx.run_path.name}"
                    )
        else:
            if "not installed" in (result.error or "").lower() or "not in path" in (result.error or "").lower():
                st.error("""
**CLI not available**

The analyst-agent CLI is not installed or configured.
Please ensure the package is installed with: `pip install -e .`
                """)
            elif "no active dataset" in (result.error or "").lower():
                st.error("""
**No dataset configured**

This project doesn't have an active dataset. Please configure a dataset before asking questions.
                """)
            elif "no runs found" in (result.error or "").lower() or "no analysis runs" in (result.error or "").lower():
                st.error("""
**No analysis runs found**

Please run an analysis first using: `analyst-agent run --project <project_id>`
                """)
            elif "project" in (result.error or "").lower() and "not found" in (result.error or "").lower():
                st.error(f"Project not found: `{project_id}`")
            elif "timeout" in (result.error or "").lower():
                st.warning("""
**Request timed out**

The question is taking longer than expected to process. Try a simpler question or wait and try again.
                """)
            else:
                st.error(f"Error processing question: {result.error}")
            
            if result.raw_output:
                with st.expander("Technical details"):
                    st.code(result.raw_output, language="text")
    elif submit:
        st.error("Please enter a question.")
    
    st.subheader("Available Artifacts")
    if ctx.run_path.exists():
        artifacts = sorted([f.name for f in ctx.run_path.iterdir() if f.is_file()])
        col1, col2 = st.columns(2)
        mid = len(artifacts) // 2
        with col1:
            for artifact in artifacts[:mid]:
                st.markdown(f"- `{artifact}`")
        with col2:
            for artifact in artifacts[mid:]:
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
