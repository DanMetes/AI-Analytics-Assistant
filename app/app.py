import streamlit as st
import json
import os
from pathlib import Path
import pandas as pd

st.set_page_config(
    page_title="AI Analytics Artifact Viewer",
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


def render_overview(run_path: Path):
    st.header("Overview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        ingest_meta = load_json_artifact(run_path, "ingest_meta.json")
        if ingest_meta:
            st.subheader("Ingest Metadata")
            st.json(ingest_meta)
        else:
            st.info("No ingest metadata found.")
    
    with col2:
        analysis_plan = load_json_artifact(run_path, "analysis_plan.json")
        if analysis_plan:
            st.subheader("Analysis Plan")
            st.json(analysis_plan)
        else:
            st.info("No analysis plan found.")
    
    report_md = load_text_artifact(run_path, "report.md")
    if report_md:
        st.subheader("Report")
        st.markdown(report_md)


def render_key_findings(run_path: Path):
    st.header("Key Findings")
    
    interpretation = load_json_artifact(run_path, "interpretation.json")
    if interpretation:
        st.subheader("Interpretation")
        if isinstance(interpretation, dict):
            if "summary" in interpretation:
                st.markdown(interpretation["summary"])
            if "findings" in interpretation:
                for i, finding in enumerate(interpretation["findings"], 1):
                    with st.expander(f"Finding {i}"):
                        st.write(finding)
            st.json(interpretation)
        else:
            st.json(interpretation)
    else:
        st.info("No interpretation data found.")
    
    anomalies = load_json_artifact(run_path, "anomalies_normalized.json")
    if anomalies:
        st.subheader("Anomalies Detected")
        if isinstance(anomalies, list):
            for anomaly in anomalies:
                severity = anomaly.get("severity", "info")
                color = {"critical": "🔴", "warning": "🟠", "info": "🔵"}.get(severity, "⚪")
                with st.expander(f"{color} {anomaly.get('title', 'Anomaly')}"):
                    st.write(f"**Type:** {anomaly.get('type', 'N/A')}")
                    st.write(f"**Severity:** {severity}")
                    st.write(f"**Description:** {anomaly.get('description', 'N/A')}")
                    if "details" in anomaly:
                        st.json(anomaly["details"])
        else:
            st.json(anomalies)
    else:
        st.info("No anomalies data found.")


def render_metrics(run_path: Path):
    st.header("Metrics")
    
    metrics_path = run_path / "metrics.csv"
    if metrics_path.exists():
        df = pd.read_csv(metrics_path)
        st.dataframe(df, width="stretch")
        
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="Download Metrics CSV",
            data=csv_data,
            file_name="metrics.csv",
            mime="text/csv"
        )
    else:
        st.info("No metrics.csv found.")
    
    data_profile = load_json_artifact(run_path, "data_profile.json")
    if data_profile:
        st.subheader("Data Profile Summary")
        st.json(data_profile)


def render_profiling_eda(run_path: Path):
    st.header("Profiling / EDA")
    
    eda_html_path = run_path / "eda_report.html"
    if eda_html_path.exists():
        st.subheader("ydata-profiling Report")
        with open(eda_html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        st.components.v1.html(html_content, height=800, scrolling=True)
    else:
        st.info("No EDA report (eda_report.html) found.")
    
    plots_dir = run_path / "plots"
    if plots_dir.exists():
        st.subheader("Generated Plots")
        plots = list(plots_dir.glob("*.png"))
        if plots:
            cols = st.columns(2)
            for i, plot_path in enumerate(plots):
                with cols[i % 2]:
                    st.image(str(plot_path), caption=plot_path.stem)
        else:
            st.info("No plot images found.")


def render_ask_explore(run_path: Path):
    st.header("Ask & Explore")
    
    st.info("🚧 This feature is a stub for future LLM-backed Q&A functionality.")
    
    st.subheader("Ask a Question")
    question = st.text_input("Enter your question about this analysis run:")
    
    if st.button("Submit"):
        if question:
            st.warning("LLM integration coming soon. Your question was: " + question)
        else:
            st.error("Please enter a question.")
    
    st.subheader("Available Artifacts")
    if run_path.exists():
        artifacts = [f.name for f in run_path.iterdir() if f.is_file()]
        st.write(artifacts)
    
    analysis_log = load_json_artifact(run_path, "analysis_log.json")
    if analysis_log:
        with st.expander("Analysis Log"):
            st.json(analysis_log)


def main():
    st.title("📊 AI Analytics Artifact Viewer")
    
    projects = discover_projects_and_runs()
    
    if not projects:
        st.warning("No projects found. Please ensure projects exist under ./projects/<project_id>/runs/<run_id>/")
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
            
            st.sidebar.markdown("---")
            st.sidebar.markdown(f"**Project:** `{selected_project[:12]}...`")
            st.sidebar.markdown(f"**Run:** `{selected_run[:12]}...`")
            
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "Overview",
                "Key Findings", 
                "Metrics",
                "Profiling/EDA",
                "Ask & Explore"
            ])
            
            with tab1:
                render_overview(run_path)
            
            with tab2:
                render_key_findings(run_path)
            
            with tab3:
                render_metrics(run_path)
            
            with tab4:
                render_profiling_eda(run_path)
            
            with tab5:
                render_ask_explore(run_path)


if __name__ == "__main__":
    main()
