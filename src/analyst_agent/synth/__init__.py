"""Deterministic synthesis layer.

Batch S1 introduces a report builder that produces report.md strictly from
already-computed artifacts (metrics/anomalies/profile/plan/plots).
"""

from .report_builder import build_report
from .llm_synth import append_llm_interpretation, build_llm_inputs

__all__ = ["build_report", "append_llm_interpretation", "build_llm_inputs"]
