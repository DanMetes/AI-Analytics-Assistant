## Deterministic interpretation contract

Inputs to interpreters:
- `metrics_rows`: list of dicts with keys `section`, `key`, `value` (the existing engine format).
- `analysis_log`: dict as written to `analysis_log.json` (includes policy, warnings, etc.).

Outputs:
- `Interpretation(findings, caveats)` where:
  - `findings` is a list of `Finding(severity, title, text, evidence_keys)`.
  - `caveats` is a list of strings.

Rules / prohibitions:
- No randomness, no I/O, no network, no LLM usage, no reading raw data.
- Deterministic: given the same inputs, outputs must be identical.
- Conservative: only claim what is directly supported by `metrics_rows` / `analysis_log`.
- When possible, include evidence keys (section/key labels) in `evidence_keys`.

Expectations:
- Interpreters stay minimal and deterministic; they are not responsible for generating prose beyond short, evidence-backed statements.
- If insufficient evidence exists, return empty/default findings rather than guessing.
