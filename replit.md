# AI Analytics Assistant

## Overview
A product-grade Streamlit-based artifact viewer for the AI Analytics platform. This application provides a user-friendly interface for browsing analysis artifacts from projects and runs.

## Project Structure
- `app/` - Streamlit UI application
  - `app.py` - Main application entry point
  - `llm_utils.py` - LLM integration utilities (placeholder for future LLM features)
  - `ui_components/` - Reusable UI components
    - `header.py` - Shared run summary header
    - `summary.py` - Run summary helper with top KPIs and next steps
- `src/analyst_agent/` - Core analyst agent library (DO NOT MODIFY)
- `projects/` - Project artifacts storage (`projects/<project_id>/runs/<run_id>/`)
- `tests/` - Test suite
- `docs/` - Documentation
- `tools/` - Utility scripts
- `test_data/` - Sample test data

## Key Features
- **Run Summary Header**: Displays project ID, run ID, timestamp, row/column counts, missing cells, anomaly count, and profiling availability
- **Overview Tab**: Curated run summary with top KPIs, Data Quality Highlights (missingness, high-cardinality, skew flags), recommended next steps
- **Key Findings Tab**: Anomalies grouped by mechanism (units, sales, profit), severity-sorted cards (Critical/Warning/Info), unified interpretation summary
- **Metrics Tab**: Headline KPIs, Trends and Drivers panel with matplotlib bar charts, group-by/metric dropdowns, CSV download
- **Profiling & EDA Tab**: EDA Highlights section, embedded ydata-profiling HTML report, generated plots, clear callout for missing reports
- **Ask & Explore Tab**: CLI subprocess integration for artifact queries, LLM placeholder for future Q&A features

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
        llm_interpretation.json (optional - for LLM features)
        metrics.csv
        report.md
        reproduce.sql
        plots/
          *.png
```

## UI Components
- `ui_components/summary.py`: Provides `render_run_summary(run_path)` for generating high-level run summaries including top KPIs and recommended next steps
- `ui_components/header.py`: Provides `render_run_header()` for consistent run metrics display across tabs
- `ui_components/plots.py`: Provides `render_plots(run_path)` for plot rendering with deduplication, category grouping, and selection UI (Select Plot / Grid View modes)
- `ui_components/findings.py`: Provides `normalize_and_group_anomalies()` for consolidating related anomalies with user-friendly labels, `render_anomaly_card()` for styled anomaly cards with severity/summary/cause/action, and `render_interpretation_bullets()` for interpretation display
- `llm_utils.py`: Provides `load_llm_interpretation()`, `render_llm_interpretation()` for displaying LLM claims/evidence/questions, and `render_llm_placeholder()` for when LLM output is unavailable
- `ask_engine.py`: Provides `run_ask_query()` CLI wrapper for analyst-agent ask command, returning answers with evidence or methodology plans with generated code

## Dependencies
See `pyproject.toml` for full dependency list. Key dependencies:
- streamlit
- pandas
- matplotlib
- ydata-profiling (optional, for EDA reports)
- typer, pydantic
