"""Profile utilities for EDA highlights extraction."""
from typing import Any, Optional
from dataclasses import dataclass, field


@dataclass
class DistributionStats:
    """Distribution statistics for a numeric column.
    
    Uses p05 (5th percentile), p50 (median), p95 (95th percentile) from profile schema.
    """
    column: str
    min_val: float
    p05: float
    median: float
    p95: float
    max_val: float
    std_dev: float
    mean: float = 0.0


@dataclass 
class ProfileSummary:
    """Summarized profile highlights for EDA display."""
    top_missing: list[tuple[str, float]] = field(default_factory=list)
    top_skewed: list[tuple[str, float]] = field(default_factory=list)
    top_correlations: list[tuple[str, str, float]] = field(default_factory=list)
    top_positive_correlations: list[tuple[str, str, float]] = field(default_factory=list)
    top_negative_correlations: list[tuple[str, str, float]] = field(default_factory=list)
    high_cardinality: list[tuple[str, int]] = field(default_factory=list)
    top_unique_counts: list[tuple[str, int]] = field(default_factory=list)
    distribution_stats: list[DistributionStats] = field(default_factory=list)
    top_categories: dict[str, list[tuple[str, int]]] = field(default_factory=dict)
    has_data: bool = False


STAT_TOOLTIPS = {
    "skew": "Skewness measures asymmetry in data distribution. Values > 1 or < -1 indicate significant skew. Positive = right tail longer, negative = left tail longer.",
    "correlation": "Correlation coefficient (r) measures linear relationship between two variables. Range: -1 to +1. |r| > 0.7 = strong, |r| > 0.4 = moderate.",
    "unique_values": "Number of distinct values in a categorical column. High counts may need grouping or encoding for analysis.",
    "distribution": "Shows how values spread across a column. Compare median vs mean to detect skew; wide Q25-Q75 range indicates high variability.",
    "top_categories": "Most frequent values in categorical columns. High concentration in few categories may indicate data imbalance or natural groupings.",
    "q95": "95th percentile: 95% of values fall below this. Large gap between Q95 and max often indicates outliers.",
    "std_dev": "Standard deviation: measures spread around the mean. Larger values = more variability in the data.",
}


def get_top_categories(profile_json: Optional[dict[str, Any]], n: int = 3) -> dict[str, list[tuple[str, int]]]:
    """
    Extract top n categories and their counts for each categorical field.
    
    Note: The standard profile schema only includes cardinality for string columns,
    not individual value counts. If value_counts or top_values are available,
    they will be used. Otherwise, returns empty dict (the EDA tab handles this gracefully).
    
    Args:
        profile_json: The data_profile.json content
        n: Number of top categories to return (default 3)
        
    Returns:
        Dict mapping column name -> list of (category, count) tuples
        Returns empty if value counts not available in profile schema.
    """
    if not profile_json:
        return {}
    
    columns = profile_json.get("columns", {})
    result = {}
    
    for col_name, col_info in columns.items():
        dtype = col_info.get("dtype", "")
        if dtype != "string":
            continue
        
        value_counts = col_info.get("value_counts", {})
        if value_counts:
            sorted_counts = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)
            result[col_name] = sorted_counts[:n]
            continue
        
        top_values = col_info.get("top_values", [])
        if top_values:
            result[col_name] = [(v.get("value", ""), v.get("count", 0)) for v in top_values[:n]]
    
    return result


def get_distribution_stats(profile_json: Optional[dict[str, Any]]) -> list[DistributionStats]:
    """
    Compute distribution stats for each numeric column.
    
    Schema fields used:
    - min, max: range values
    - p05, p50 (median), p95: percentiles
    - std: standard deviation
    - mean: average value
    
    Returns stats: min, p05, median (p50), p95, max, std_dev, mean
    """
    if not profile_json:
        return []
    
    columns = profile_json.get("columns", {})
    result = []
    
    for col_name, col_info in columns.items():
        dtype = col_info.get("dtype", "")
        if dtype not in ("int", "float"):
            continue
        
        min_val = col_info.get("min")
        max_val = col_info.get("max")
        
        if min_val is None or max_val is None:
            continue
        
        try:
            p05 = float(col_info.get("p05", min_val))
        except (ValueError, TypeError):
            p05 = float(min_val)
        
        try:
            median = float(col_info.get("p50")) if col_info.get("p50") is not None else None
        except (ValueError, TypeError):
            median = None
        
        if median is None:
            continue
        
        try:
            p95 = float(col_info.get("p95", max_val))
        except (ValueError, TypeError):
            p95 = float(max_val)
        
        try:
            std_dev = float(col_info.get("std", 0))
        except (ValueError, TypeError):
            std_dev = 0.0
        
        try:
            mean_val = float(col_info.get("mean", 0))
        except (ValueError, TypeError):
            mean_val = 0.0
        
        result.append(DistributionStats(
            column=col_name,
            min_val=float(min_val),
            p05=p05,
            median=median,
            p95=p95,
            max_val=float(max_val),
            std_dev=std_dev,
            mean=mean_val
        ))
    
    return result


def summarize_profile(profile_json: Optional[dict[str, Any]]) -> ProfileSummary:
    """
    Extract EDA highlights from data_profile.json.
    
    Returns:
        ProfileSummary with:
        - top_missing: Top 3 columns by missing percentage [(col, pct), ...]
        - top_skewed: Top 3 most skewed numeric columns [(col, skew), ...]
        - top_positive_correlations: Top 3 positive correlations [(col_a, col_b, r), ...]
        - top_negative_correlations: Top 3 negative correlations [(col_a, col_b, r), ...]
        - top_correlations: Top 3 strongest correlations by absolute value (for backward compat)
        - high_cardinality: Top 3 high-cardinality categorical fields [(col, cardinality), ...]
        - top_unique_counts: Top 3 unique value counts for categoricals [(col, count), ...]
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
            if cardinality and cardinality > 0:
                categorical_cardinality.append((col_name, cardinality))
    
    missing_cols.sort(key=lambda x: x[1], reverse=True)
    top_missing = missing_cols[:3]
    
    skewed_cols.sort(key=lambda x: abs(x[1]), reverse=True)
    top_skewed = skewed_cols[:3]
    
    categorical_cardinality.sort(key=lambda x: x[1], reverse=True)
    high_cardinality = [(c, n) for c, n in categorical_cardinality if n > 10][:3]
    top_unique_counts = categorical_cardinality[:3]
    
    top_correlations = []
    top_positive_correlations = []
    top_negative_correlations = []
    
    correlations = profile_json.get("correlations", [])
    if correlations:
        positive_corrs = [c for c in correlations if c.get("r", 0) > 0]
        negative_corrs = [c for c in correlations if c.get("r", 0) < 0]
        
        positive_corrs.sort(key=lambda x: x.get("r", 0), reverse=True)
        for corr in positive_corrs[:3]:
            a, b, r = corr.get("a", ""), corr.get("b", ""), corr.get("r", 0)
            if a and b and abs(r) > 0.1:
                top_positive_correlations.append((a, b, r))
        
        negative_corrs.sort(key=lambda x: x.get("r", 0))
        for corr in negative_corrs[:3]:
            a, b, r = corr.get("a", ""), corr.get("b", ""), corr.get("r", 0)
            if a and b and abs(r) > 0.1:
                top_negative_correlations.append((a, b, r))
        
        sorted_corrs = sorted(correlations, key=lambda x: abs(x.get("r", 0)), reverse=True)
        for corr in sorted_corrs[:3]:
            a, b, r = corr.get("a", ""), corr.get("b", ""), corr.get("r", 0)
            if a and b:
                top_correlations.append((a, b, r))
    
    distribution_stats = get_distribution_stats(profile_json)
    top_categories = get_top_categories(profile_json)
    
    return ProfileSummary(
        top_missing=top_missing,
        top_skewed=top_skewed,
        top_correlations=top_correlations,
        top_positive_correlations=top_positive_correlations,
        top_negative_correlations=top_negative_correlations,
        high_cardinality=high_cardinality,
        top_unique_counts=top_unique_counts,
        distribution_stats=distribution_stats,
        top_categories=top_categories,
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
