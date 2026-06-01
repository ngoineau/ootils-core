"""
forecast_program_poc.py — does buy-program segmentation beat a blended calendar
forecast? (Pyramide axis C, using order_type).

On PPS end-demand (interco excluded), per local gen_group, holds out the last 12
months and compares:
  - A (calendar) : seasonal-naive on the blended group monthly total.
  - B (segmented): seasonal-naive per order_type bucket (SPRING/SUMMER/EARLY/
                   FWD/BASE), recombined.

Pure numpy/pandas, read-only.
"""
from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd
import psycopg

H = "product_local_gen"

SQL = """
SELECT grp.code AS grp, dh.order_type AS ot,
       to_char(dh.booked_date, 'YYYY-MM') AS ym, SUM(dh.ordered_quantity) AS u
FROM demand_history dh
JOIN item_hierarchy ih ON ih.item_id = dh.item_id AND ih.hierarchy_id = %(h)s
JOIN hierarchy_node prod ON prod.hierarchy_id = %(h)s AND prod.code = ih.leaf_code
JOIN hierarchy_node grp  ON grp.hierarchy_id = %(h)s  AND grp.code = prod.parent_code
WHERE dh.stream = 'regular' AND dh.fulfillment <> 'inter_entity'
  AND dh.org_id = 'PPS' AND dh.booked_date IS NOT NULL
GROUP BY 1, 2, 3
"""


def bucket(ot: str | None) -> str:
    o = (ot or "").upper()
    if "SPRING BUY" in o:
        return "SPRING"
    if "SUMMER BUY" in o:
        return "SUMMER"
    if "EARLY BUY" in o:
        return "EARLY"
    if "FWD BUY" in o or "FORWARD BUY" in o:
        return "FWD"
    return "BASE"


def seasonal_naive(vals: np.ndarray, horizon: int) -> np.ndarray:
    vals = vals.astype(float)
    if len(vals) < 24:
        last12 = vals[-12:] if len(vals) >= 12 else np.pad(vals, (12 - len(vals), 0))
        return np.array([last12[i % 12] for i in range(horizon)])
    recent, prior = vals[-12:].sum(), vals[-24:-12].sum()
    trend = float(np.clip((recent / prior) if prior > 0 else 1.0, 0.5, 2.0))
    last12 = vals[-12:]
    return np.array([last12[i % 12] * trend for i in range(horizon)])


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--top", type=int, default=8)
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL", file=sys.stderr)
        return 2

    with psycopg.connect(args.dsn) as conn:
        conn.execute("SET default_transaction_read_only = on")
        conn.execute("SET statement_timeout = '180s'")
        rows = conn.execute(SQL, {"h": H}).fetchall()

    df = pd.DataFrame(rows, columns=["grp", "ot", "ym", "u"])
    df["u"] = df["u"].astype(float)
    df["bucket"] = df["ot"].map(bucket)
    df["period"] = pd.PeriodIndex(df["ym"], freq="M")
    idx = pd.period_range(df["period"].min(), df["period"].max(), freq="M")

    top = df.groupby("grp")["u"].sum().sort_values(ascending=False).head(args.top).index
    print(f"PPS end-demand, {idx.min()}→{idx.max()}  ({len(idx)} months)\n")
    print(f"{'group':9s} {'calendar(A)':>12s} {'segmented(B)':>13s} {'winner':>11s}")
    print("-" * 50)
    agg_a = agg_b = agg_act = 0.0
    for g in top:
        sub = df[df["grp"] == g]
        total = sub.groupby("period")["u"].sum().reindex(idx, fill_value=0.0)
        if len(total) < 36:
            continue
        test = total.iloc[-12:].values
        # A: calendar on blended total
        a = seasonal_naive(total.iloc[:-12].values, 12)
        # B: per-bucket seasonal naive, recombined
        b = np.zeros(12)
        for bk, sb in sub.groupby("bucket"):
            s = sb.groupby("period")["u"].sum().reindex(idx, fill_value=0.0)
            b += seasonal_naive(s.iloc[:-12].values, 12)
        denom = np.abs(test).sum()
        wa = np.abs(test - a).sum() / denom if denom else float("nan")
        wb = np.abs(test - b).sum() / denom if denom else float("nan")
        print(f"{g:9s} {wa:>11.1%} {wb:>12.1%} {('segmented' if wb < wa else 'calendar'):>11s}")
        agg_a += np.abs(test - a).sum()
        agg_b += np.abs(test - b).sum()
        agg_act += denom
    print("-" * 50)
    print(f"{'OVERALL':9s} {agg_a/agg_act:>11.1%} {agg_b/agg_act:>12.1%} "
          f"{('segmented' if agg_b < agg_a else 'calendar'):>11s}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
