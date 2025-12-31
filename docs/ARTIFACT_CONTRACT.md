# Artifact Contract

Artifacts are the canonical interface between computation, interpretation, UI, and monetization.

## Required Artifacts
- metrics.csv
- anomalies_normalized.json
- interpretation.json
- analysis_log.json
- analysis_plan.json

## Optional Artifacts
- data_profile.json (profiling)
- eda_report.html (profiling)
- llm_interpretation.json (LLM enabled)
- ask/* (Q&A outputs)

Artifacts are backward-compatible and append-only by design.
