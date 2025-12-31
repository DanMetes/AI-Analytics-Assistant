# AI Analytics Artifact Viewer

## Overview
A Streamlit-based artifact viewer for the AI Analytics platform. This application allows browsing analysis artifacts from projects and runs.

## Project Structure
- `app/` - Streamlit UI application
- `src/analyst_agent/` - Core analyst agent library
- `projects/` - Project artifacts storage (`projects/<project_id>/runs/<run_id>/`)
- `tests/` - Test suite
- `docs/` - Documentation
- `tools/` - Utility scripts
- `test_data/` - Sample test data

## Key Features
- Artifact browser with project/run selection
- Findings viewer with severity indicators
- Metrics table with CSV download
- Embedded ydata-profiling HTML reports
- Ask & Explore stub (future LLM-backed Q&A)

## Running the App
```bash
streamlit run app/app.py --server.port 5000
```

## Artifact Storage Structure
```
projects/
  <project_id>/
    runs/
      <run_id>/
        analysis_log.json
        analysis_plan.json
        anomalies_normalized.json
        data_profile.json
        eda_report.html
        ingest_meta.json
        interpretation.json
        metrics.csv
        report.md
        reproduce.sql
        plots/
          *.png
```

## Dependencies
See `pyproject.toml` for full dependency list. Key dependencies:
- streamlit
- pandas
- ydata-profiling (optional, for EDA reports)
- typer, pydantic, matplotlib
