from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from .models import DatasetArtifacts, DatasetSession, RetentionMode
from .paths import active_dataset_path, dataset_dir, session_db_path
from .utils import iso_in_hours, new_id, read_json, sha256_file, write_json


def _maybe_split_compound_columns(
    df: pd.DataFrame,
    delimiters: tuple[str, ...] = (";", "|"),
    min_fraction: float = 0.8,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Detect and split compound columns such as:
      "Username; Identifier; First name; Last name"

    This runs BEFORE SQLite ingestion.

    Rules:
    - Column name contains a delimiter
    - At least `min_fraction` of non-null values contain that delimiter
    - Split produces a consistent number of fields

    Returns:
      (possibly modified df, list of warnings describing transformations)
    """
    warnings: list[str] = []
    df = df.copy()

    for col in list(df.columns):
        for delim in delimiters:
            if delim not in col:
                continue

            series = df[col].dropna().astype(str)
            if series.empty:
                continue

            fraction_with_delim = (series.str.contains(delim)).mean()
            if fraction_with_delim < min_fraction:
                continue

            split_counts = series.str.split(delim).map(len)
            if split_counts.nunique() != 1:
                continue

            parts = [c.strip() for c in col.split(delim)]
            if len(parts) != split_counts.iloc[0]:
                continue

            new_cols = series.str.split(delim, expand=True)
            new_cols.columns = parts

            df = df.drop(columns=[col])
            df = pd.concat([df, new_cols], axis=1)

            warnings.append(
                f"Split compound column '{col}' into {parts} using delimiter '{delim}'."
            )

            break  # only split once per column

    return df, warnings


def _escape_sqlite_identifier(name: str) -> str:
    """
    Safely escape a SQLite identifier (column/table name) to prevent SQL injection.
    Doubles any embedded double-quotes and wraps in double quotes.
    """
    return '"' + name.replace('"', '""') + '"'


def _infer_sqlite_type(dtype: Any) -> str:
    """
    Maps pandas dtypes to SQLite column affinity types.
    SQLite is flexible; we keep it simple for v1.
    """
    s = str(dtype).lower()
    if "int" in s:
        return "INTEGER"
    if "float" in s:
        return "REAL"
    if "bool" in s:
        return "INTEGER"
    # Dates are stored as TEXT in v1 for portability (ISO strings).
    return "TEXT"


def _write_schema_json(out_path: Path, df: pd.DataFrame) -> None:
    schema = {
        "columns": [
            {"name": c, "pandas_dtype": str(df.dtypes[c]), "sqlite_type": _infer_sqlite_type(df.dtypes[c])}
            for c in df.columns
        ]
    }
    write_json(out_path, schema)


def _write_profile_json(out_path: Path, df: pd.DataFrame, max_cols: int = 60) -> Dict[str, Any]:
    """
    Simple bounded profiling. We cap per-column computations to avoid slowdowns on huge wide datasets.
    """
    nrows, ncols = df.shape
    cols = list(df.columns)

    # Missingness (bounded)
    missingness = {}
    for c in cols[:max_cols]:
        missingness[c] = int(df[c].isna().sum())

    profile = {
        "row_count": int(nrows),
        "column_count": int(ncols),
        "missingness": missingness,
        "missingness_capped_at_columns": max_cols if ncols > max_cols else None,
    }
    write_json(out_path, profile)
    return profile


def _create_sqlite_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    # Pragmas for better performance in analytics-style reads; safe defaults.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _ingest_dataframe_to_sqlite(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """
    Writes df into SQLite as table 'data'. Replaces if exists.
    """
    # Convert booleans to int for predictable SQLite behavior.
    for c in df.columns:
        if str(df.dtypes[c]).lower().startswith("bool"):
            df[c] = df[c].astype("Int64")

    df.to_sql("data", conn, if_exists="replace", index=False)


def _create_basic_indexes(conn: sqlite3.Connection, df: pd.DataFrame) -> List[str]:
    """
    Create a few pragmatic indexes for repeated GROUP BY / filtering.
    We keep this minimal; indexes can be expanded later.
    """
    created = []
    cols = [c.lower() for c in df.columns]
    # Common time columns
    for candidate in ["year", "yr"]:
        if candidate in cols:
            col = df.columns[cols.index(candidate)]
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_data_{candidate} ON data("{col}");')
            created.append(col)
            break

    # Light indexing on the first 2 TEXT columns (heuristic)
    text_cols = [c for c in df.columns if _infer_sqlite_type(df.dtypes[c]) == "TEXT"]
    for col in text_cols[:2]:
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_data_{col} ON data("{col}");')
        created.append(col)

    conn.commit()
    return created


def ingest_csv_to_session(
    project_id: str,
    csv_path: Path,
    retention_mode: RetentionMode = RetentionMode.TTL_24H,
) -> Tuple[DatasetSession, DatasetArtifacts]:
    """
    Ingest a CSV into a per-project session SQLite DB and write profiling artifacts.

    Privacy/trust boundary:
    - The SQLite DB is stored in system temp (ephemeral).
    - This function writes only schema/profile/fingerprint to ./projects/... for reproducibility.
    - Raw CSV is not copied into the project directory.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    dataset_id = new_id()
    db_path = session_db_path(project_id, dataset_id)

    # Load data (pandas). For large files, this can be optimized later (chunks).
    df = pd.read_csv(csv_path)
    df, split_warnings = _maybe_split_compound_columns(df)

    conn = _create_sqlite_db(db_path)
    try:
        _ingest_dataframe_to_sqlite(conn, df)
        created_indexes = created_indexes = []
    finally:
        conn.close()

    # Profiling artifacts stored under project datasets folder
    ddir = dataset_dir(project_id, dataset_id)
    ddir.mkdir(parents=True, exist_ok=True)

    schema_path = ddir / "schema.json"
    profile_path = ddir / "profile.json"
    fingerprint_path = ddir / "fingerprint.json"
    warnings_path = ddir / "warnings.json"
    dataset_meta_path = ddir / "dataset.json"

    _write_schema_json(schema_path, df)
    profile = _write_profile_json(profile_path, df)

    fp = sha256_file(csv_path)
    write_json(fingerprint_path, {"sha256": fp, "source_path": str(csv_path)})

    # Basic warnings for v1
    warnings: List[str] = split_warnings.copy()
    if profile["column_count"] > 200:
        warnings.append("Dataset is very wide; profiling is capped and some analyses may be slow.")
    if profile["row_count"] > 2_000_000:
        warnings.append("Dataset is large; consider chunked ingestion and more selective profiling.")
    write_json(warnings_path, {"warnings": warnings, "indexes_created_on": created_indexes})

    # Retention handling: only two modes in v1
    expires_at = iso_in_hours(24) if retention_mode == RetentionMode.TTL_24H else None

    session = DatasetSession(
        dataset_id=dataset_id,
        project_id=project_id,
        db_path=str(db_path),
        retention_mode=retention_mode,
        expires_at=expires_at,
        row_count=int(profile["row_count"]),
        column_count=int(profile["column_count"]),
    )

    # Persist session metadata as the "active dataset" for the project
    write_json(active_dataset_path(project_id), session.model_dump())

    # Persist dataset metadata (useful for future persistence model)
    write_json(dataset_meta_path, session.model_dump())

    artifacts = DatasetArtifacts(
        dataset_dir=str(ddir),
        schema_path=str(schema_path),
        profile_path=str(profile_path),
        fingerprint_path=str(fingerprint_path),
        warnings_json=str(warnings_path),
    )

    return session, artifacts
