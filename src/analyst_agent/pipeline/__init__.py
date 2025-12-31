"""Pipeline orchestration layer.

Introduced in Batch R1 to wrap the existing deterministic analysis engine
without changing runtime behavior. Contract enforcement and artifact
standardization are implemented in later batches.
"""

from .context import RunContext
from .run import RunResult, run_pipeline

__all__ = ["RunContext", "RunResult", "run_pipeline"]
