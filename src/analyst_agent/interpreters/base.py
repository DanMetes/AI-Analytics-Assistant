from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Finding:
    severity: str  # e.g., "info", "warn"
    title: str
    text: str
    evidence_keys: List[str]


@dataclass(frozen=True)
class Interpretation:
    findings: List[Finding]
    caveats: List[str]
    metadata: dict[str, object] | None = None


class Interpreter:
    """Deterministic interpreter for policy outputs."""

    def interpret(  # pragma: no cover - interface
        self, metrics_rows: list[dict[str, str]], analysis_log: dict
    ) -> Interpretation:
        raise NotImplementedError
