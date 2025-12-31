"""Profile utilities for EDA highlights extraction."""
from typing import Any, Optional
from dataclasses import dataclass, field


@dataclass
class ProfileSummary:
    """Summarized profile highlights for EDA display."""
    top_missing: list[tuple[str, float]] = field(default_factory=list)
    top_skewed: list[tuple[str, float]] = field(default_factory=list)
    top_correlations: list[tuple[str, str, float]] = field(default_factory=list)
    high_cardinality: list[tuple[str, int]] = field(default_factory=list)
    has_data: bool = False


def summarize_profile(profile_json: Optional[dict[str, Any]]) -> ProfileSummary:
    """
    Extract EDA highlights from data_profile.json.
    
    Returns:
        ProfileSummary with:
        - top_missing: Top 3 columns by missing percentage [(col, pct), ...]
        - top_skewed: Top 3 most skewed numeric columns [(col, skew), ...]
        - top_correlations: Top 3 highest absolute correlations [(col_a, col_b, r), ...]
        - high_cardinality: Top 3 high-cardinality categorical fields [(col, cardinality), ...]
    """
    if not profile_json:
        return ProfileSummary(has_data=False)
    
    columns = profile_json.get("columns", {})
    if not columns:
        return ProfileSummary(has_data=False)
    
    missing_cols = []
    skewed_cols = []
    categorical_cardinality = []
    
    for col_name, col_info in columns.items():
        missing_frac = col_info.get("missing_fraction", 0) or 0
        if missing_frac > 0:
            missing_cols.append((col_name, missing_frac * 100))
        
        dtype = col_info.get("dtype", "")
        if dtype in ("int", "float"):
            skew = col_info.get("skew")
            if skew is not None:
                skewed_cols.append((col_name, skew))
        
        if dtype == "string":
            cardinality = col_info.get("cardinality", 0)
            if cardinality and cardinality > 10:
                categorical_cardinality.append((col_name, cardinality))
    
    missing_cols.sort(key=lambda x: x[1], reverse=True)
    top_missing = missing_cols[:3]
    
    skewed_cols.sort(key=lambda x: abs(x[1]), reverse=True)
    top_skewed = skewed_cols[:3]
    
    categorical_cardinality.sort(key=lambda x: x[1], reverse=True)
    high_cardinality = categorical_cardinality[:3]
    
    top_correlations = []
    correlations = profile_json.get("correlations", [])
    if correlations:
        sorted_corrs = sorted(correlations, key=lambda x: abs(x.get("r", 0)), reverse=True)
        for corr in sorted_corrs[:3]:
            a = corr.get("a", "")
            b = corr.get("b", "")
            r = corr.get("r", 0)
            if a and b:
                top_correlations.append((a, b, r))
    
    return ProfileSummary(
        top_missing=top_missing,
        top_skewed=top_skewed,
        top_correlations=top_correlations,
        high_cardinality=high_cardinality,
        has_data=True
    )


def load_profile_llm_summary(run_path) -> Optional[dict[str, Any]]:
    """
    Load profile_llm_summary.json if it exists.
    
    Returns:
        Dict with LLM synthesis data or None if not available.
    """
    import json
    from pathlib import Path
    
    llm_path = Path(run_path) / "profile_llm_summary.json"
    if not llm_path.exists():
        return None
    
    try:
        with open(llm_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None
