"""Profile stage.

Batch F1 introduces automated EDA report generation. This package is
intentionally small and defensive: failures must not break the run.
"""

from .profiler import profile_dataset_to_html
from .summarize import summarize_dataset_to_json

__all__ = ["profile_dataset_to_html", "summarize_dataset_to_json"]
