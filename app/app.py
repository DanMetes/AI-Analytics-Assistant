"""AI Analytics Assistant - Artifact Viewer UI"""
import streamlit as st
import streamlit.components.v1 as components
import json
import subprocess
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

import os

st.set_page_config(
    page_title="AI Analytics Assistant",
    page_icon="📊",
    layout="wide"
)

APP_USER = os.environ.get("app_user", "admin")
APP_PASS = os.environ.get("app_pass", "password")

def check_login():
    """Simple username/password authentication."""
    if "authenticated" not in st.session_state or not st.session_state["authenticated"]:
        st.title("AI Analytics Assistant")
        st.markdown("Please log in to continue.")
        
        username = st.text_input("Username", "", key="login_username")
        password = st.text_input("Password", "", type="password", key="login_password")
        
        if st.button("Login", type="primary"):
            if username == APP_USER and password == APP_PASS:
                st.session_state["authenticated"] = True
                st.success("Logged in!")
                st.rerun()
            else:
                st.error("Invalid credentials")
        st.stop()

check_login()

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
    """Key Findings tab with causal narratives and consolidated anomalies."""
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from ui_components.findings import normalize_and_group_anomalies, render_anomaly_card, render_interpretation_bullets, generate_causal_narrative
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
        
        st.subheader("Causal Analysis")
        
        for group in grouped_anomalies:
            narrative_data = generate_causal_narrative(group, ctx.metrics_df, ctx.data_profile)
            
            severity = group["severity"]
            if severity == "critical":
                icon = "🔴"
            elif severity == "warning":
                icon = "🟠"
            else:
                icon = "🔵"
            
            metric_title = group["base_metric"].replace("_", " ").title()
            period_label = f" ({group['time_period']})" if group["time_period"] != "overall" else ""
            
            st.markdown(f"**{icon} {metric_title}{period_label}**")
            st.markdown(f"_{narrative_data['narrative']}_")
            st.markdown(f"**Next step:** {narrative_data['next_action']}")
            st.markdown("---")
        
        run_id = ctx.run_path.name
        with st.expander("Detailed Anomaly Cards", expanded=False):
            for i, group in enumerate(grouped_anomalies):
                render_anomaly_card(group, run_id, i)
    else:
        st.success("No anomalies detected in this analysis run.")
    
    with st.expander("Technical details", expanded=False):
        if ctx.interpretation:
            render_interpretation_bullets(ctx.interpretation)
        else:
            if anomaly_list:
                st.info("Detailed interpretation not available.")
            else:
                st.info("No interpretation available for this run.")
        
        if ctx.anomalies:
            st.markdown("**Raw Anomaly Data:**")
            st.json(ctx.anomalies)
        if ctx.interpretation:
            st.markdown("**Interpretation JSON:**")
            st.json(ctx.interpretation)
    
    if not render_llm_interpretation(ctx.run_path):
        render_llm_placeholder()


def render_metrics(ctx: RunContext):
    """Metrics tab with headline KPIs and Trends/Drivers panel."""
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from ui_components.metrics import render_kpi_dashboard, render_metrics_glossary
    
    st.header("Metrics")
    render_run_header(ctx)
    
    if ctx.metrics_df is None or ctx.metrics_df.empty:
        st.info("No metrics.csv found for this run.")
        return
    
    df = ctx.metrics_df
    
    st.subheader("Headline KPIs")
    st.caption("Key performance indicators summarizing your data. Hover over metrics for definitions.")
    
    render_kpi_dashboard(df, ctx.run_path.name)
    
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
    
    render_metrics_glossary(df, ctx.analysis_plan)


def render_profiling_eda(ctx: RunContext):
    """Profiling & EDA tab with summarized highlights."""
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from ui_components.plots import render_plots
    from ui_components.profile_utils import summarize_profile
    from style_utils import COLORS
    
    st.header("Profiling & EDA")
    render_run_header(ctx)
    
    with st.expander("What is a profiling report?", expanded=False):
        st.markdown("""
A profiling report provides statistical summaries of your dataset including:
- **Distributions**: How values are spread across columns
- **Correlations**: Relationships between numeric variables  
- **Missing Values**: Completeness of your data
- **Data Quality**: Type consistency, outliers, and anomalies

Use these insights to understand your data before analysis and identify potential issues.
        """)
    
    if ctx.data_profile:
        columns_data = ctx.data_profile.get("columns", {})
        numeric_cols = sum(1 for c in columns_data.values() if c.get("dtype") in ("int", "float"))
        string_cols = sum(1 for c in columns_data.values() if c.get("dtype") == "string")
        other_cols = len(columns_data) - numeric_cols - string_cols
        
        row_count = ctx.data_profile.get("row_count", 0) or 0
        col_count = ctx.data_profile.get("column_count", len(columns_data)) or len(columns_data)
        
        total_missing = 0
        for col_info in columns_data.values():
            missing_frac = col_info.get("missing_fraction", 0) or 0
            total_missing += missing_frac * row_count if row_count else 0
        
        total_cells = row_count * col_count
        completeness = ((total_cells - total_missing) / total_cells * 100) if total_cells > 0 else 100
        
        st.subheader("Data Quality Summary")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Completeness", f"{completeness:.1f}%", help="Percentage of non-missing cells")
        with col2:
            st.metric("Numeric Columns", numeric_cols, help="Columns with integer or float data")
        with col3:
            st.metric("Text Columns", string_cols, help="Columns with string/categorical data")
        with col4:
            st.metric("Other Types", other_cols, help="Date, boolean, or other column types")
    
    st.subheader("EDA Highlights")
    st.caption("Key data quality insights. Row/column counts shown in the header above.")
    
    from ui_components.profile_utils import STAT_TOOLTIPS
    
    if ctx.data_profile:
        summary = summarize_profile(ctx.data_profile)
        
        if summary.has_data:
            has_highlights = (summary.top_missing or summary.top_skewed or 
                            summary.top_positive_correlations or summary.top_negative_correlations or 
                            summary.top_unique_counts)
            
            if has_highlights:
                left_col, right_col = st.columns(2)
                
                with left_col:
                    if summary.top_missing:
                        st.markdown("**Missing Data**")
                        for col, pct in summary.top_missing:
                            severity_color = COLORS["critical"] if pct > 20 else COLORS["warning"] if pct > 5 else COLORS["info"]
                            st.markdown(f"<span style='color:{severity_color}'>●</span> `{col}`: {pct:.1f}% missing", unsafe_allow_html=True)
                    
                    if summary.top_skewed:
                        st.markdown("**Skewed Distributions** ℹ️", help=STAT_TOOLTIPS["skew"])
                        st.caption("_Skew > 1 or < -1 indicates significant asymmetry_")
                        for col, skew in summary.top_skewed:
                            direction = "→" if skew > 0 else "←"
                            st.markdown(f"- `{col}`: {direction} skew = {skew:.2f}")
                    
                    if summary.top_unique_counts:
                        st.markdown("**Unique Value Counts** ℹ️", help=STAT_TOOLTIPS["unique_values"])
                        for col, count in summary.top_unique_counts:
                            st.markdown(f"- `{col}`: {count:,} unique values")
                
                with right_col:
                    if summary.top_positive_correlations:
                        st.markdown("**Positive Correlations** ℹ️", help=STAT_TOOLTIPS["correlation"])
                        for col_a, col_b, r in summary.top_positive_correlations:
                            strength = "strong" if r > 0.7 else "moderate" if r > 0.4 else "weak"
                            st.markdown(f"- `{col_a}` ↔ `{col_b}`: r = +{r:.3f} ({strength})")
                    
                    if summary.top_negative_correlations:
                        st.markdown("**Negative Correlations** ℹ️", help=STAT_TOOLTIPS["correlation"])
                        for col_a, col_b, r in summary.top_negative_correlations:
                            strength = "strong" if abs(r) > 0.7 else "moderate" if abs(r) > 0.4 else "weak"
                            st.markdown(f"- `{col_a}` ↔ `{col_b}`: r = {r:.3f} ({strength})")
                    
                    if summary.high_cardinality:
                        st.markdown("**High-Cardinality Fields**")
                        st.caption("_Many unique values may need grouping or encoding_")
                        for col, card in summary.high_cardinality:
                            st.markdown(f"- `{col}`: {card:,} unique values")
            else:
                st.success("No notable data quality issues detected in this dataset.")
        else:
            st.info("Profile data not available for highlights.")
    else:
        st.warning("""
**Data profile not available**

The `data_profile.json` was not generated for this run.

**How to enable profiling:**
1. Ensure Python version is 3.11 or 3.12 (required by ydata-profiling)
2. Install with: `pip install -e '.[profiling]'`
3. Re-run the analysis
        """)
    
    from llm_utils import render_llm_profile, render_llm_profile_placeholder
    
    if not render_llm_profile(ctx.run_path):
        render_llm_profile_placeholder()
    
    st.subheader("Full Profiling Report")
    eda_html_path = ctx.run_path / "eda_report.html"
    if eda_html_path.exists():
        with st.expander("Full profiling report (ydata-profiling)", expanded=False):
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
    """Ask & Explore tab with CLI integration and LLM Q&A."""
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from llm_utils import load_llm_interpretation, render_llm_summary
    from ask_engine import run_ask, is_llm_available, run_llm_ask, build_llm_context
    
    st.header("Ask & Explore")
    render_run_header(ctx)
    
    st.markdown("""
**Ask questions about your data** and get answers from computed metrics and analysis artifacts.
Choose between deterministic answers (fully reproducible) or AI-powered explanations (natural language).
    """)
    
    st.subheader("Ask a Question")
    
    with st.expander("Understanding Q&A Modes", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
**Deterministic Q&A**
- Uses the analyst-agent CLI to query existing artifacts
- Answers based on computed metrics and detected anomalies
- Fully reproducible — same question always gives same answer
- May generate code scaffolds for complex questions
            """)
        with col2:
            st.markdown("""
**AI-Powered Q&A**
- Uses a language model to interpret your artifacts
- Provides natural language explanations and insights
- Results may vary slightly between queries
- Requires an OpenAI API key (see Settings)
            """)
    
    llm_available = is_llm_available()
    
    use_ai = st.checkbox(
        "Use AI (LLM)",
        value=False,
        help="Enable AI-powered answers using a language model. Falls back to deterministic if unavailable.",
        key=f"use_ai_{ctx.run_path.name}"
    )
    
    if use_ai and not llm_available:
        st.warning("AI mode requested but no API key configured. Falling back to deterministic Q&A.")
        use_ai = False
    
    question = st.text_input(
        "Enter your question:",
        placeholder="e.g., What caused the spike in units for 2023?",
        key=f"ask_question_{ctx.run_path.name}"
    )
    
    project_id = ctx.run_path.parent.parent.name
    run_id = ctx.run_path.name
    
    col1, col2 = st.columns([1, 4])
    with col1:
        submit = st.button("Submit", type="primary", key=f"ask_submit_{ctx.run_path.name}")
    
    if submit and question:
        ai_answered = False
        
        if use_ai:
            with st.spinner("AI is thinking..."):
                context = build_llm_context(ctx.run_path)
                llm_answer, references = run_llm_ask(question, context)
            
            if llm_answer and not llm_answer.startswith("Error:"):
                st.success("**AI-Powered Answer:**")
                st.markdown(llm_answer)
                
                if references:
                    st.markdown("**Referenced Artifacts:**")
                    for ref in references:
                        st.markdown(f"- `{ref}`")
                
                st.caption("_This answer was generated by AI and should be verified against the source data._")
                ai_answered = True
            else:
                st.warning("AI could not generate an answer. Trying deterministic Q&A...")
        
        if not ai_answered:
            with st.spinner("Processing your question..."):
                answer, plan, code, evidence_keys = run_ask(project_id, run_id, question, use_llm=False, timeout=60)
            
            if plan and "error" in plan:
                error = plan.get("error", "")
                if "not installed" in error.lower() or "not in path" in error.lower():
                    st.error("""
**CLI not available**

The analyst-agent CLI is not installed or configured.
Please ensure the package is installed with: `pip install -e .`
                    """)
                elif "no active dataset" in error.lower():
                    st.error("""
**No dataset configured**

This project doesn't have an active dataset. Please configure a dataset before asking questions.
                    """)
                elif "no runs found" in error.lower() or "no analysis runs" in error.lower():
                    st.error("""
**No analysis runs found**

Please run an analysis first using: `analyst-agent run --project <project_id>`
                    """)
                elif "project" in error.lower() and "not found" in error.lower():
                    st.error(f"Project not found: `{project_id}`")
                elif "timeout" in error.lower():
                    st.warning("""
**Request timed out**

The question is taking longer than expected to process. Try a simpler question or wait and try again.
                    """)
                else:
                    st.error(f"Error processing question: {error}")
            elif answer is not None:
                st.success("Answer found from existing artifacts:")
                st.markdown(answer)
                
                if evidence_keys:
                    st.markdown("**Supporting Evidence:**")
                    for key in evidence_keys:
                        st.markdown(f"- `{key}`")
            elif plan is not None:
                st.warning("This question cannot be answered from existing artifacts. A methodology plan has been generated.")
                
                steps = plan.get("steps", [])
                methodology = plan.get("methodology", "")
                
                if steps:
                    st.markdown("**Methodology Plan:**")
                    for i, step in enumerate(steps, 1):
                        st.markdown(f"{i}. {step}")
                elif methodology:
                    st.markdown("**Methodology Plan:**")
                    st.markdown(methodology)
                
                if code:
                    st.markdown("**Generated Python Code:**")
                    st.code(code, language="python")
                    
                    st.download_button(
                        label="Download Code",
                        data=code,
                        file_name="generated_query.py",
                        mime="text/x-python",
                        key=f"download_code_{ctx.run_path.name}"
                    )
            else:
                st.info("No response generated. Please try rephrasing your question.")
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


def render_home(ctx: RunContext):
    """Home tab with welcome content and getting started guide."""
    import sys
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from home_content import (WELCOME_TITLE, MISSION_STATEMENT, WELCOME_INTRO, 
                              TAB_DESCRIPTIONS, HOW_TO_READ, ENABLE_AI_HELP, 
                              GET_STARTED, ABOUT_ENGINE)
    from style_utils import COLORS
    
    st.header(f"Welcome to {WELCOME_TITLE}")
    
    st.markdown(MISSION_STATEMENT)
    st.markdown(WELCOME_INTRO)
    
    project_id = ctx.run_path.parent.parent.name
    run_id = ctx.run_path.name
    
    st.success(f"**Currently viewing:** Project `{project_id[:12]}...` / Run `{run_id[:12]}...`")
    
    st.markdown(TAB_DESCRIPTIONS)
    st.markdown(HOW_TO_READ)
    
    with st.expander("How to Enable AI & Profiling", expanded=False):
        st.markdown(ENABLE_AI_HELP)
    
    st.markdown(GET_STARTED)
    st.markdown(ABOUT_ENGINE)


def render_summary_report(ctx: RunContext):
    """Summary Report tab with downloadable one-page narrative."""
    import sys
    from datetime import datetime
    app_dir = Path(__file__).parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from style_utils import format_currency, format_number, format_percent
    from ui_components.findings import normalize_and_group_anomalies, generate_causal_narrative
    from ui_components.profile_utils import summarize_profile
    from llm_utils import load_llm_interpretation, load_profile_llm_summary
    
    st.header("Summary Report")
    render_run_header(ctx)
    
    st.markdown("Generate a downloadable summary combining key insights from your analysis.")
    
    project_id = ctx.run_path.parent.parent.name
    run_id = ctx.run_path.name
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    report_lines = []
    report_lines.append(f"# AI Analytics Summary Report")
    report_lines.append(f"")
    report_lines.append(f"**Project:** `{project_id}`")
    report_lines.append(f"**Run:** `{run_id}`")
    report_lines.append(f"**Generated:** {timestamp}")
    report_lines.append(f"")
    report_lines.append(f"---")
    report_lines.append(f"")
    
    report_lines.append(f"## Key Metrics")
    report_lines.append(f"")
    
    if ctx.metrics_df is not None and not ctx.metrics_df.empty:
        df = ctx.metrics_df
        overall = df[df["section"] == "overall"]
        time_summary = df[df["section"] == "time_summary"]
        
        if not overall.empty:
            row_count_row = overall[overall["key"] == "row_count"]
            if not row_count_row.empty:
                report_lines.append(f"- **Total Rows:** {int(float(row_count_row['value'].iloc[0])):,}")
        
        if not time_summary.empty:
            for metric_key in ["sum_sales", "sum_profit", "sum_units"]:
                metric_rows = time_summary[time_summary["key"].str.contains(metric_key)]
                if not metric_rows.empty:
                    total = metric_rows["value"].astype(float).sum()
                    label = metric_key.replace("sum_", "Total ").replace("_", " ").title()
                    if "sales" in metric_key or "profit" in metric_key:
                        report_lines.append(f"- **{label}:** ${total:,.0f}")
                    else:
                        report_lines.append(f"- **{label}:** {total:,.0f}")
    else:
        report_lines.append("_No metrics available._")
    
    report_lines.append(f"")
    report_lines.append(f"## Key Findings")
    report_lines.append(f"")
    
    anomaly_list = []
    if ctx.anomalies:
        if isinstance(ctx.anomalies, dict) and "anomalies" in ctx.anomalies:
            anomaly_list = ctx.anomalies.get("anomalies", [])
        elif isinstance(ctx.anomalies, list):
            anomaly_list = ctx.anomalies
    
    if anomaly_list:
        grouped = normalize_and_group_anomalies(anomaly_list)
        for group in grouped[:5]:
            narrative_data = generate_causal_narrative(group, ctx.metrics_df, ctx.data_profile)
            severity = group["severity"]
            icon = {"critical": "🔴", "warning": "🟠", "info": "🔵"}.get(severity, "⚪")
            metric_title = group["base_metric"].replace("_", " ").title()
            report_lines.append(f"- {icon} **{metric_title}:** {narrative_data['narrative']}")
            report_lines.append(f"  - _Next step:_ {narrative_data['next_action']}")
    else:
        report_lines.append("_No anomalies detected._")
    
    llm_interp = load_llm_interpretation(ctx.run_path)
    if llm_interp and "summary" in llm_interp:
        report_lines.append(f"")
        report_lines.append(f"### AI-Enhanced Interpretation")
        report_lines.append(f"")
        report_lines.append(llm_interp["summary"])
    
    report_lines.append(f"")
    report_lines.append(f"## Data Quality Highlights")
    report_lines.append(f"")
    
    if ctx.data_profile:
        summary = summarize_profile(ctx.data_profile)
        if summary.has_data:
            if summary.top_missing:
                report_lines.append("**Most Missing Columns:**")
                for col, pct in summary.top_missing[:3]:
                    report_lines.append(f"- `{col}`: {pct:.2f}% missing")
            if summary.top_skewed:
                report_lines.append("**Most Skewed Columns:**")
                for col, skew in summary.top_skewed[:3]:
                    report_lines.append(f"- `{col}`: skew = {skew:.2f}")
        else:
            report_lines.append("_Profile highlights not available._")
    else:
        report_lines.append("_No data profile available._")
    
    llm_profile = load_profile_llm_summary(ctx.run_path)
    if llm_profile and "summary" in llm_profile:
        report_lines.append(f"")
        report_lines.append(f"### AI Profile Summary")
        report_lines.append(f"")
        report_lines.append(llm_profile["summary"])
    
    report_lines.append(f"")
    report_lines.append(f"## Recommended Next Steps")
    report_lines.append(f"")
    
    if anomaly_list:
        report_lines.append("1. Investigate critical anomalies flagged in Key Findings")
        report_lines.append("2. Review data quality issues in the Profiling & EDA tab")
        report_lines.append("3. Use Ask & Explore to query specific metrics or trends")
    else:
        report_lines.append("1. Explore metrics trends in the Metrics tab")
        report_lines.append("2. Review data distributions in Profiling & EDA")
        report_lines.append("3. Ask questions about your data in Ask & Explore")
    
    report_lines.append(f"")
    report_lines.append(f"---")
    report_lines.append(f"")
    report_lines.append(f"_This report is based on a data snapshot and should be validated against source data._")
    report_lines.append(f"_Generated by AI Analytics Assistant._")
    
    report_content = "\n".join(report_lines)
    
    st.subheader("Report Preview")
    with st.expander("View Full Report", expanded=True):
        st.markdown(report_content)
    
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="Download as Markdown",
            data=report_content,
            file_name=f"analytics_report_{run_id[:8]}.md",
            mime="text/markdown",
            key=f"download_md_{ctx.run_path.name}"
        )
    with col2:
        st.info("PDF export coming soon")


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
            
            tab0, tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
                "🏠 Home",
                "Overview",
                "Key Findings", 
                "Metrics",
                "Profiling & EDA",
                "Ask & Explore",
                "Summary Report"
            ])
            
            with tab0:
                render_home(ctx)
            
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
            
            with tab6:
                render_summary_report(ctx)


if __name__ == "__main__":
    main()
