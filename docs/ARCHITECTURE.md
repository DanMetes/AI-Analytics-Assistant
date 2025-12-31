# Architecture

## Execution Model
Analyst Agent operates as a job-based analytic system. Each dataset analysis runs as a bounded job in an application-owned runtime.

## Layers
1. Deterministic Analysis Layer (metrics, anomalies, baseline interpretation)
2. Profiling Layer (ydata-profiling, CPU/memory intensive)
3. Intelligence Layer (LLM-based, evidence-gated, metered)
4. Interaction Layer (Q&A, methodology planning, code generation)

CLI usage is considered developer-mode. The web app runtime is authoritative.
