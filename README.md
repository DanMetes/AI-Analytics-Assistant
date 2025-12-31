# Analyst Agent

A **policy-driven, deterministic analytics engine** for tabular data, built on SQLite and strict contracts.

This project intentionally avoids probabilistic logic, heuristics, or LLM-driven interpretation in its core execution path. All outputs are reproducible, auditable, and test-locked.

---

## 1. Core Design Principles (LOCKED)

These principles are **non-negotiable** and must be preserved in all future changes.

### 1.1 Determinism First
- All metrics, thresholds, and anomalies are:
  - Deterministic
  - Policy-defined
  - Fully testable
- No inference, guessing, or adaptive logic.

### 1.2 SQLite as Execution Substrate
- SQLite is the canonical execution engine.
- Policies generate SQL.
- Interpreters consume query results only.
- Artifacts can always be replayed.

### 1.3 Policy-Driven Semantics
- Policies define:
  - Required / optional roles
  - Expected metrics
  - Severity thresholds
  - Whether anomalies are emitted
- Interpreters **never invent semantics**.

### 1.4 Strict Contracts
- All policies must always emit:
  - `anomalies_normalized` (empty list if none)
- Shape and types are enforced by tests.
- Breaking contracts is considered a regression.

---

## 2. Architecture Overview

### 2.1 High-Level Flow

1. CSV ingested into SQLite session DB  
2. Policy selected  
3. Policy:
   - Declares expected metrics
   - Declares anomaly behavior  
4. Interpreter:
   - Executes SQL
   - Computes metrics deterministically
   - Emits anomalies only if policy allows  
5. Artifacts written:
   - `metrics.csv`
   - `analysis_log.json`
   - `report.md`
   - `reproduce.sql`
   - `analysis_plan.json` (Batch G2; deterministic plan; user-editable)
   - `plots/*.png` (Batch E1; deterministic plots for plan steps)
   - `eda_report.html` (Batch F1; generated via ydata-profiling when available; errors are captured in the HTML and the run continues)
   - `data_profile.json` (Batch F2; deterministic, machine-readable dataset summary; includes per-column stats, time candidates, and high-correlation flags)

### 2.2 Artifact Notes

- `report.md` is built deterministically from run artifacts (Batch S1). It includes:
  1) Executive Summary (from `anomalies_normalized.json`)
  2) Dataset Overview (from `ingest_meta.json` + `data_profile.json`)
  3) Analyses Executed (from `analysis_plan.json`)
  4) Key Metrics (from `metrics.csv`)
  5) Anomalies (normalized anomalies summary)
  6) Limitations & Caveats (profiling-derived; e.g., missingness/sampling/time candidates)
  It also links to `eda_report.html` and enumerates plots in `plots/`.
- Optional: when `run` is invoked with `--llm` (Batch S2; default off), the report appends a clearly labeled section
  "LLM Interpretation (Optional)" generated only from non-row-level artifacts (`data_profile.json`, `analysis_plan.json`,
  compact `metrics.csv` summary, `anomalies_normalized.json`, and deterministic plot captions).
- `reproduce.sql` is an exact replay of the SQL used to compute metrics.

---

## 3. Policies Implemented (CURRENT STATE)

### 3.1 `sales_v1` ✅ (Stable)

**Purpose**
- Revenue, units, margin analysis.

**Behavior**
- Emits anomalies.
- Severity thresholds defined in policy.
- Emits:
  - `anomalies` (human text)
  - `anomalies_structured`
  - `anomalies_normalized`

**Status**
- Golden tests passing.
- Contract-locked.

---

### 3.2 `orders_v1` ✅ (Stable)

**Purpose**
- Order-level revenue and concentration analysis.

**Metrics**
- Total revenue
- Order count
- Revenue trend
- Top customer revenue share
- (Optional, policy-controlled) AOV and recent order-count drop anomalies

**Anomaly Logic**
- Policy-driven thresholds (no interpreter-owned thresholds).
- Coverage guards:
  - No anomalies emitted for insufficient sample size.
- **Normalized anomalies are created via `make_normalized_anomaly(...)` and sorted deterministically**.

**Behavior Expectations (Golden)**
- `orders_normal.csv` → no anomalies
- `orders_warning.csv` → may or may not emit (depends on thresholds + coverage)
- `orders_critical_concentration.csv` → must emit critical anomaly

---

### 3.3 `generic_tabular` ✅ (Baseline)

**Purpose**
- Schema-driven metric aggregation only.

**Behavior**
- No anomalies by design.
- Always emits empty `anomalies_normalized`.

---

## 4. Normalized Anomaly Contract (CRITICAL)

Every policy **must** include the following key in interpretation metadata:

```json
"anomalies_normalized": []
```

or a list of normalized anomaly objects.

This is enforced by:
- `tests/test_anomalies_normalized_contract.py`
- Golden tests per policy

**Never remove or conditionalize this key.**

### 4.1 Normalized anomaly shape

Normalized anomalies must be produced via `make_normalized_anomaly(...)` and include at least:

- `id`
- `policy`
- `metric`
- `severity`
- `direction`
- `value`
- `threshold`
- `unit`
- `evidence_keys`
- `summary`

Ordering must be deterministic (severity desc, metric asc, id asc).

---

## 5. CLI Commands

### 5.1 Core commands (stable)

**Installation**

Before running the CLI you should install the package in editable mode:

```bash
pip install -e .
```

This installs only the core dependencies and will work on Python 3.11 and newer, including Python 3.14.  The EDA profiling step (Batch F1) uses the optional `ydata-profiling` library; to enable full HTML profiling reports on supported Python versions (< 3.14) install the optional profiling extra:

```bash
pip install -e ".[profiling]"
```

If the profiling extra is not installed or not available on your Python version, the run still completes and generates a deterministic `eda_report.html` using the built-in fallback EDA generator (a lightweight report with missingness, distributions, categorical counts, and correlations). The report will note that full ydata-profiling output was unavailable.

**Preferred invocation (after installing as above):**

```bash
analyst-agent run path/to.csv
```

**Module invocation (also supported):**

```bash
python -m analyst_agent run path/to.csv
```

Note: executing the CLI module file directly (e.g. `python src/analyst_agent/cli.py ...`)
is not supported because it breaks package-relative imports.

- `init` — create project
- `ingest` — load CSV into session DB
- `run` — execute policy
- `cleanup` — remove expired sessions

### 5.2 Policy inspection commands (Batch E)

- `analyst-agent policy list`  
  Lists available policies. If a policy name does not embed a version suffix, the CLI appends `(v<version>)` for clarity.

- `analyst-agent policy describe --policy <name>`  
  Prints policy metadata as JSON to stdout using stable ordering (`sort_keys=True`, consistent indent).

### 5.3 LLM-enhanced analysis (Optional)

Run the analysis with the `--llm` flag to generate AI-powered insights:

```bash
analyst-agent run --project <project_id> --llm
```

This produces additional artifact files:

| Artifact | Description |
|----------|-------------|
| `llm_interpretation.json` | Anomaly/metric interpretations with claims, confidence scores, evidence references, and recommended next analyses |
| `profile_llm_summary.json` | Data profile synthesis with natural language summary, key observations, data quality assessment, and column insights |

**Important:** The UI does NOT invoke LLMs at runtime. It only consumes these pre-generated files when present. If the files are missing, the UI displays a placeholder explaining how to generate them.

---

## 6. Executive Summary in `report.md` (Batch C)

`report.md` includes an **Executive Summary** section near the top, derived strictly from `anomalies_normalized`:

- If `anomalies_normalized` is empty:
  - `No anomalies detected under policy thresholds.`
- Otherwise:
  - Deterministic bullet list ordered by severity, then metric, then id.
  - Each bullet includes: severity, short summary, and metric/value.

No LLMs, inference, or heuristics are used.

---

## 7. Policy Versioning Scaffold (Batch D)

The registry supports multiple versions of a policy family (e.g., `orders_v1`, `orders_v2`) without breaking existing policy names or CLI usage.

### 7.1 Conventions

- Versioned policies should be registered as distinct names: `orders_v1`, `orders_v2`, etc.
- The registry derives a **base name** for introspection (e.g., `orders` from `orders_v1`).

### 7.2 How to add a v2 policy safely

1. Create a new policy class (e.g., `OrdersPolicyV2`) in a new module (recommended) or alongside v1.
2. Set class attributes:
   - `name` (e.g., `"orders_v2"`)
   - `version` (e.g., `"2.0.0"`)
   - `description`
   - `required_fields`
   - `SEVERITY_THRESHOLDS` (policy-owned)
3. Register it in `PolicyRegistry._register_builtin()` **without removing** the v1 entry.
4. Add golden/contract tests (or extend existing golden fixtures) to lock behavior.
5. Run the full test suite (see below).

---

## 8. Repo Hygiene (Batch A)

Generated/runtime artifacts must not be committed. `.gitignore` includes patterns for:

- `projects/**/runs/`
- `projects/**/datasets/`
- `projects/**/active_dataset.json`
- `projects/**/project.json`
- `*.egg-info/`
- `**/__pycache__/`
- `.pytest_cache/`

Note: if you have already generated `.pytest_cache/` locally, it may still exist on disk; it simply should not be tracked by git.

---

## 9. Test Suite (LOCKED)

### 9.1 Running tests

This repo uses a `src/` layout. In environments that do not automatically add `src` to `PYTHONPATH`, run tests as:

```bash
PYTHONPATH=src python -m pytest -q
```

Alternatively, install the package in editable mode and run pytest normally:

```bash
pip install -e .
pytest -q
```

### 9.2 Contract tests
- `tests/test_policy_describe_contract.py`
- `tests/test_anomalies_normalized_contract.py`

### 9.3 Golden tests
- `tests/test_sales_v1_golden.py`
- `tests/test_orders_v1_golden.py`

Golden tests define **expected behavior**, not suggestions. If behavior changes, update tests intentionally with a clear rationale.

---

## 10. What Has NOT Been Implemented (INTENTIONALLY)

Out of scope so far:

- Cross-policy anomaly ranking (beyond per-report Executive Summary)
- Alerting / notifications
- UI / visualization layer
- ML baselines or time-series modeling
- LLM-driven interpretation

---

## 11. Development Workflow

- **Agent Mode**
  - For bulk mechanical multi-file changes
  - Must run tests before returning output
- **Chat Mode**
  - For architecture decisions and planning

Do not mix responsibilities.

---

**End of README**


## analysis_plan.json schema (G1)

`analysis_plan.json` is validated before downstream pipeline stages. Empty plans are allowed.

Top-level:
- `steps`: list of step objects

Each step requires:
- `id` (string)
- `type` in: `distribution`, `trend`, `concentration`, `quality`, `segmentation`
- `rationale` (string, may be empty)
- required fields by `type`:
  - `distribution`: `metric`
  - `trend`: `metric`, `time_axis`
  - `concentration`: `metric`, `entity`
  - `quality`: `metric`
  - `segmentation`: `metric`, `by`

If validation fails, the run is marked failed with a clear error recorded in `analysis_log.json` and `report.md`.
