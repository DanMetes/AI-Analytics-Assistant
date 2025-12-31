from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


def canon_category(cat: str) -> str:
    """
    Normalize category labels for internal lookups.

    We intentionally allow messy casing in the OUTPUT dataset (e.g., 'technology'),
    but for internal pricing/margin logic we must map to canonical keys.
    """
    c = str(cat).strip().lower()
    mapping = {
        "furniture": "Furniture",
        "office supplies": "Office Supplies",
        "technology": "Technology",
    }
    return mapping.get(c, cat)


def main(out_path: str = "test_data/synth_orders.csv", n: int = 5000, seed: int = 42) -> None:
    random.seed(seed)

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    start = date(2022, 1, 1)
    end = date(2024, 12, 31)
    days = (end - start).days

    regions = ["West", "Prairies", "Ontario", "Quebec", "Atlantic", "North"]
    cities_by_region = {
        "West": ["Vancouver", "Victoria", "Kelowna"],
        "Prairies": ["Calgary", "Edmonton", "Winnipeg", "Regina"],
        "Ontario": ["Toronto", "Ottawa", "London"],
        "Quebec": ["Montreal", "Quebec City", "Sherbrooke"],
        "Atlantic": ["Halifax", "St. John's", "Moncton"],
        "North": ["Whitehorse", "Yellowknife", "Iqaluit"],
    }

    segments = ["Consumer", "Corporate", "Home Office"]
    categories = ["Furniture", "Office Supplies", "Technology"]
    subcats = {
        "Furniture": ["Chairs", "Tables", "Bookcases"],
        "Office Supplies": ["Binders", "Paper", "Storage", "Supplies"],
        "Technology": ["Phones", "Accessories", "Machines"],
    }

    rows = []

    for i in range(n):
        d = start + timedelta(days=random.randint(0, days))
        region = random.choice(regions)
        city = random.choice(cities_by_region[region])
        segment = random.choices(segments, weights=[0.6, 0.25, 0.15], k=1)[0]
        category = random.choices(categories, weights=[0.25, 0.45, 0.30], k=1)[0]
        sub_category = random.choice(subcats[category])

        # Introduce a small casing inconsistency edge case (for later normalization testing)
        if random.random() < 0.03:
            category = category.lower()

        # Canonical category for internal lookups (fixes KeyError like "Office supplies")
        cat_key = canon_category(category)

        units = max(1, int(random.gauss(3, 2)))

        base_price = {
            "Furniture": random.uniform(80, 500),
            "Office Supplies": random.uniform(5, 60),
            "Technology": random.uniform(50, 1200),
        }[cat_key]

        # Unit price varies by segment a bit
        unit_price = base_price * (1.0 + (0.05 if segment == "Corporate" else 0.0))

        # Discount: mostly 0–20%, sometimes missing, rarely high
        if random.random() < 0.02:
            discount = None
        else:
            discount = max(0.0, min(0.6, random.betavariate(2, 10)))  # right-skewed, mostly small

        gross_sales = units * unit_price
        sales = gross_sales * (1 - discount) if discount is not None else gross_sales

        # Profit model: category-based margin + noise, with occasional loss-making orders
        margin = {
            "Furniture": random.uniform(0.05, 0.25),
            "Office Supplies": random.uniform(0.10, 0.35),
            "Technology": random.uniform(0.08, 0.30),
        }[cat_key]

        profit = sales * margin + random.gauss(0, sales * 0.03)

        # Occasional negative profit (returns, fulfillment issues)
        if random.random() < 0.07:
            profit *= -random.uniform(0.2, 1.2)

        # Occasional outlier big orders
        if random.random() < 0.01:
            sales *= random.uniform(5, 20)
            profit *= random.uniform(3, 12)

        # Missing profit edge case
        if random.random() < 0.01:
            profit = None

        returned = 1 if random.random() < 0.06 else 0

        rows.append(
            {
                "order_id": f"ORD-{i+1:06d}",
                "order_date": d.isoformat(),
                "year": d.year,
                "month": d.month,
                "region": region,
                "city": city,
                "segment": segment,
                "category": category,  # keep the messy casing edge case in output
                "sub_category": sub_category,
                "units": units,
                "unit_price": round(unit_price, 2),
                "discount": None if discount is None else round(discount, 3),
                "sales": round(sales, 2),
                "profit": None if profit is None else round(profit, 2),
                "returned": returned,
            }
        )

    df = pd.DataFrame(rows)

    # Add deliberate cleanliness issue: whitespace in some city values
    mask = df.index.to_series().sample(frac=0.02, random_state=seed).index
    df.loc[mask, "city"] = df.loc[mask, "city"].astype(str) + " "

    df.to_csv(out, index=False)
    print(f"Wrote {len(df):,} rows to {out.resolve()}")


if __name__ == "__main__":
    main()
