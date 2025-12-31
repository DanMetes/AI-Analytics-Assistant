"""Package entry point.

Preferred invocation is via the installed console script:

    analyst-agent ...

For convenience we also support:

    python -m analyst_agent ...

Executing the CLI module file directly (e.g. `python src/analyst_agent/cli.py`)
is not supported because it breaks package-relative imports.
"""

from __future__ import annotations

from .cli import app


def main() -> None:
    """Entry point used by `python -m analyst_agent`."""

    app()


if __name__ == "__main__":
    main()
