"""
LLM PLACEHOLDER MODULE (DO NOT IMPLEMENT IN v1)

This module exists to document the intended trust boundary:

- A future LLM integration must NOT see raw data rows.
- It should consume only computed artifacts produced by the analysis engine, e.g.:
  - schema.json
  - profile.json
  - metrics.csv
  - plot filenames/captions
  - analysis_log.json (queries, warnings)

Rationale:
- Prevents the model from hallucinating about unseen records.
- Keeps privacy risk lower by avoiding row-level exposure.
- Makes outputs reproducible and auditable.
"""

# Intentionally empty in v1.
