# Architecture Notes (Baseline Lock)

This file establishes a **baseline checkpoint** before the pipeline-centric refactor begins (see `README_ANALYST_AGENT.md`).

## Execution Core (deterministic, contract-locked)

The **execution core** is the part of the system that must remain:
- **deterministic** (no heuristics / no probabilistic behavior), and
- **contract-driven** (tests enforce artifact shapes and required fields).

In this repository, the execution core currently consists of:
- **Policies**: domain policies (e.g., sales/orders) that define metrics, thresholds, and anomaly semantics.
- **Interpreters**: convert policy outputs into contract-compliant metadata, including `anomalies_normalized`.
- **Analysis runner / engine**: orchestrates policy execution over input data and produces structured results.
- **Core models / contracts**: shared types and required output fields enforced by tests.

Rule of thumb: if changing a file could change computed metrics/anomalies or their schema, it is part of the execution core.

## Pipeline / Orchestration Layer (pluggable, refactor target)

The **pipeline/orchestration layer** is responsible for:
- run context (run IDs, run directories, dataset identity),
- stage sequencing (ingest → profile → plan → execute → synthesize), and
- artifact writing/organization.

In the baseline, orchestration is primarily via the CLI and existing run functions. The pipeline-centric redesign will introduce an explicit pipeline module and artifact writer, while keeping the execution core’s **behavior and contracts unchanged**.

Rule of thumb: if changing a file only changes *where/when* steps run or *where* artifacts are written (not what is computed), it belongs to orchestration.

## Checkpoint Statement

**Pipeline refactor begins after `v0.6.0-pre-pipeline`.**

This checkpoint is intended to be a stable reference point for:
- test expectations,
- existing CLI behavior,
- policy/interpreter contracts (notably `anomalies_normalized`).

### Tagging note

If you are working in a Git clone that supports tags, tag the checkpoint commit as:
- `v0.6.0-pre-pipeline`

If tags are not available in the current environment, treat the introduction of this file (`ARCHITECTURE_NOTES.md`) on top of the provided baseline as the logical checkpoint representing `v0.6.0-pre-pipeline`.
