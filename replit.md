# AI Analytics Assistant

## Overview
A product-grade Streamlit-based artifact viewer for the AI Analytics platform. This application provides a user-friendly interface for browsing analysis artifacts from projects and runs.

## Project Structure
- `app/` - Streamlit UI application
  - `app.py` - Main application entry point
  - `ui_components/` - Reusable UI components
    - `header.py` - Shared run summary header
- `src/analyst_agent/` - Core analyst agent library (DO NOT MODIFY)
- `projects/` - Project artifacts storage (`projects/<project_id>/runs/<run_id>/`)
- `tests/` - Test suite
- `docs/` - Documentation
- `tools/` - Utility scripts
- `test_data/` - Sample test data

## Key Features
- **Run Summary Header**: Displays project ID, run ID, timestamp, row/column counts, missing cells, anomaly count, and profiling availability
- **Overview Tab**: Curated dataset info, data quality summary (missingness, high-cardinality, skew flags), navigation hints
- **Key Findings Tab**: Severity-sorted anomaly cards (Critical/Warning/Info), interpretation bullets, drill-down details
- **Metrics Tab**: Headline KPIs, Explore Drivers section with group-by/metric dropdowns, top 15 groups table + bar chart, CSV download
- **Profiling & EDA Tab**: Data highlights first, embedded ydata-profiling HTML report, generated plots
- **Ask & Explore Tab**: Stub for future LLM-backed Q&A

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
