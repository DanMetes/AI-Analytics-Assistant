from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping


_ID_LIKE_RE = re.compile(r"(^id$|_id$|id$|^id_|identifier)", re.IGNORECASE)


@dataclass(frozen=True)
class PlannedStep:
    """Internal representation for deterministic plan construction."""

    id: str
    type: str
    rationale: str
    fields: dict[str, Any]

    def to_obj(self) -> dict[str, Any]:
        obj = {"id": self.id, "type": self.type, "rationale": self.rationale}
        obj.update(self.fields)
        return obj


def build_plan_from_profile(profile: Mapping[str, Any]) -> dict[str, Any]:
    """Create a deterministic analysis plan from data_profile.json.

    Batch G2 rules are intentionally simple and fully deterministic.
    This function does not inspect raw data; it relies only on the
    machine-readable profile summary.
    """

    columns = profile.get("columns")
    if not isinstance(columns, Mapping):
        return {"steps": []}

    rows = int(profile.get("rows") or 0)

    numeric_cols = _rank_numeric_columns(columns)
    low_card_cats = _rank_low_cardinality_categoricals(columns)
    high_card_cats = _rank_high_cardinality_categoricals(columns, rows=rows)
    time_axis = _pick_time_axis(profile, columns)

    # Key metric: prefer the highest-variance numeric column.
    key_metric = numeric_cols[0] if numeric_cols else None

    steps: list[PlannedStep] = []

    # Always add quality checks.
    steps.append(
        PlannedStep(
            id="quality_checks",
            type="quality",
            rationale="Baseline data quality checks.",
            fields={"metric": "__dataset__"},
        )
    )

    # Trend analyses (time axis + top 1–3 numeric columns by variance), excluding ID-like.
    if time_axis and numeric_cols:
        for i, metric in enumerate(numeric_cols[:3], start=1):
            steps.append(
                PlannedStep(
                    id=f"trend_{i}_{metric}",
                    type="trend",
                    rationale="Detect time-based movement in key numeric signals.",
                    fields={"metric": metric, "time_axis": time_axis},
                )
            )

    # Concentration analysis (requires a high-cardinality categorical to act as entity).
    if high_card_cats and key_metric:
        entity = high_card_cats[0]
        steps.append(
            PlannedStep(
                id=f"concentration_{entity}_{key_metric}",
                type="concentration",
                rationale="Check whether outcomes are concentrated among a small set of entities.",
                fields={"entity": entity, "metric": key_metric},
            )
        )

    # Distribution analyses for top 1–3 numeric columns.
    for i, metric in enumerate(numeric_cols[:3], start=1):
        steps.append(
            PlannedStep(
                id=f"distribution_{i}_{metric}",
                type="distribution",
                rationale="Understand the distribution and scale of numeric fields.",
                fields={"metric": metric},
            )
        )

    # Segmentation analyses: low-cardinality categorical columns (<= 25 distinct), cap at 3.
    if key_metric and low_card_cats:
        for i, by in enumerate(low_card_cats[:3], start=1):
            steps.append(
                PlannedStep(
                    id=f"segmentation_{i}_{by}_{key_metric}",
                    type="segmentation",
                    rationale="Compare key metric across common categorical segments.",
                    fields={"metric": key_metric, "by": by},
                )
            )

    # Stable ordering: sort by id.
    steps_out = [s.to_obj() for s in sorted(steps, key=lambda s: s.id)]
    return {"steps": steps_out}


def _rank_numeric_columns(columns: Mapping[str, Any]) -> list[str]:
    ranked: list[tuple[float, str]] = []
    for name, info_any in columns.items():
        if not isinstance(info_any, Mapping):
            continue
        dtype = str(info_any.get("dtype") or "")
        if dtype not in {"int", "float"}:
            continue
        if _is_id_like(name, info_any, rows=None):
            continue
        std = info_any.get("std")
        try:
            std_f = float(std) if std is not None else 0.0
        except Exception:
            std_f = 0.0
        ranked.append((std_f, str(name)))
    # Descending by std (proxy for variance), then name.
    ranked.sort(key=lambda t: (-t[0], t[1]))
    return [n for _, n in ranked]


def _rank_low_cardinality_categoricals(columns: Mapping[str, Any]) -> list[str]:
    ranked: list[tuple[float, int, str]] = []
    for name, info_any in columns.items():
        if not isinstance(info_any, Mapping):
            continue
        dtype = str(info_any.get("dtype") or "")
        if dtype not in {"string", "category", "bool"}:
            continue
        card = info_any.get("cardinality")
        try:
            card_i = int(card) if card is not None else 0
        except Exception:
            card_i = 0
        if card_i <= 0 or card_i > 25:
            continue
        miss = float(info_any.get("missing_fraction") or 0.0)
        # Prefer lower missingness and higher cardinality.
        ranked.append((miss, -card_i, str(name)))
    ranked.sort(key=lambda t: (t[0], t[1], t[2]))
    return [n for _, __, n in ranked]


def _rank_high_cardinality_categoricals(columns: Mapping[str, Any], *, rows: int) -> list[str]:
    ranked: list[tuple[int, float, str]] = []
    for name, info_any in columns.items():
        if not isinstance(info_any, Mapping):
            continue
        dtype = str(info_any.get("dtype") or "")
        if dtype not in {"string", "category"}:
            continue
        card = info_any.get("cardinality")
        try:
            card_i = int(card) if card is not None else 0
        except Exception:
            card_i = 0
        if card_i <= 25:
            continue
        if _is_id_like(name, info_any, rows=rows):
            continue
        miss = float(info_any.get("missing_fraction") or 0.0)
        if miss >= 0.5:
            continue
        ranked.append((card_i, miss, str(name)))
    ranked.sort(key=lambda t: (-t[0], t[1], t[2]))
    return [n for _, __, n in ranked]


def _pick_time_axis(profile: Mapping[str, Any], columns: Mapping[str, Any]) -> str | None:
    tc = profile.get("time_candidates")
    if not isinstance(tc, list) or not tc:
        return None

    best: tuple[float, str] | None = None
    for name_any in tc:
        name = str(name_any)
        info_any = columns.get(name)
        if not isinstance(info_any, Mapping):
            miss = 0.0
            dtype = ""
        else:
            miss = float(info_any.get("missing_fraction") or 0.0)
            dtype = str(info_any.get("dtype") or "")

        parse_score = 0.8
        if dtype == "datetime":
            parse_score = 1.0
        elif re.search(r"(date|time|dt|timestamp)", name, re.IGNORECASE):
            parse_score = 0.9
        score = parse_score - miss
        candidate = (score, name)
        if best is None or candidate > best:
            best = candidate

    return best[1] if best else None


def _is_id_like(name: str, info: Mapping[str, Any], rows: int | None) -> bool:
    if _ID_LIKE_RE.search(name) is None:
        # also treat near-unique columns as ID-like even if name doesn't contain id
        pass
    else:
        return True

    # Heuristic: very high cardinality relative to rows suggests an identifier.
    if rows and rows > 0:
        try:
            card = int(info.get("cardinality") or 0)
        except Exception:
            card = 0
        if card > 0 and (card / float(rows)) >= 0.9:
            return True
    return False
