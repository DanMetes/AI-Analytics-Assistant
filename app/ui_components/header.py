"""Shared Run Summary Header component."""
import streamlit as st
from pathlib import Path
from datetime import datetime


def render_run_header(run_path: Path, analysis_log: dict = None, data_profile: dict = None, anomalies: dict = None):
    """
    Render a consistent run summary header across all tabs.
    
    Args:
        run_path: Path to the run directory
        analysis_log: Loaded analysis_log.json data
        data_profile: Loaded data_profile.json data
        anomalies: Loaded anomalies_normalized.json data
    """
    project_id = run_path.parent.parent.name
    run_id = run_path.name
    
    run_timestamp = "N/A"
    if analysis_log and "created_at" in analysis_log:
        try:
            ts = analysis_log["created_at"]
            if isinstance(ts, str):
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                run_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except:
            run_timestamp = analysis_log.get("created_at", "N/A")
    
    rows = "N/A"
    cols = "N/A"
    missing_cells = "N/A"
    if data_profile:
        rows = data_profile.get("rows", "N/A")
        cols = data_profile.get("cols", "N/A")
        columns = data_profile.get("columns", {})
        if columns and rows != "N/A":
            total_missing = sum(
                col_info.get("missing_count", 0) 
                for col_info in columns.values()
            )
            missing_cells = f"{total_missing:,}"
    
    anomaly_count = 0
    if anomalies:
        if isinstance(anomalies, dict) and "anomalies" in anomalies:
            anomaly_count = len(anomalies.get("anomalies", []))
        elif isinstance(anomalies, list):
            anomaly_count = len(anomalies)
    
    eda_available = (run_path / "eda_report.html").exists()
    
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Rows", f"{rows:,}" if isinstance(rows, int) else rows)
        st.caption(f"**Project:** `{project_id[:12]}...`")
    
    with col2:
        st.metric("Columns", cols)
        st.caption(f"**Run:** `{run_id[:12]}...`")
    
    with col3:
        st.metric("Missing Cells", missing_cells)
        st.caption(f"**Timestamp:** {run_timestamp[:16]}...")
    
    with col4:
        severity_color = "🔴" if anomaly_count > 0 else "✅"
        st.metric("Anomalies", f"{severity_color} {anomaly_count}")
        profiling_status = "✅ Available" if eda_available else "❌ Not available"
        st.caption(f"**Profiling:** {profiling_status}")
    
    st.markdown("---")
