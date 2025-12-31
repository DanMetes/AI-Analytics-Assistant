from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RunContext:
    """Minimal run context for the pipeline-centric redesign.

    Batch R1 scope: provide a stable container for run identifiers and
    standard path helpers. Contract enforcement is deferred to Batch R2.
    """

    project_root: Path
    run_dir: Path
    dataset_hash: str
    run_id: str

    @classmethod
    def create(
        cls,
        *,
        project_root: Path,
        run_dir: Path,
        dataset_hash: str,
        run_id: str | None = None,
    ) -> "RunContext":
        return cls(
            project_root=project_root,
            run_dir=run_dir,
            dataset_hash=dataset_hash,
            run_id=run_id or str(uuid.uuid4()),
        )

    # Thin stubs for standard artifact paths (full enforcement in Batch R2)
    def path(self, filename: str) -> Path:
        return self.run_dir / filename

    def eda_report_path(self) -> Path:
        return self.path("eda_report.html")

    def plots_dir(self) -> Path:
        # README contract expects a "plots" directory.
        return self.run_dir / "plots"

    # Back-compat helper for the current implementation (pre-refactor).
    def figures_dir(self) -> Path:
        return self.run_dir / "figures"
