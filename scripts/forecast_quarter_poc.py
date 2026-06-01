"""
forecast_quarter_poc.py — the quarterly dynamic (Pyramide).

The buy programs land at quarter boundaries (Spring=Mar=Q1-end, Summer=Jun=Q2-end,
Early=Sep/Oct=Q3/Q4). This looks at PPS end-demand at the QUARTER level:
  1. quarterly seasonal shape (share by Q1..Q4),
  2. month-within-quarter concentration (the quarter-end buy effect),
  3. quarterly forecast accuracy (hold out last 4 quarters) — vs the ~37% monthly.

Read-only.
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
SELECT grp.code AS grp, to_char(dh.booked_date, 'YYYY-MM') AS ym,
       SUM(dh.ordered_quantity) AS u
FROM demand_history dh
JOIN item_hierarchy ih ON ih.item_id = dh.item_id AND ih.hierarchy_id = %(h)s
JOIN hierarchy_node prod ON prod.hierarchy_id = %(h)s AND prod.code = ih.leaf_code
JOIN hierarchy_node grp  ON grp.hierarchy_id = %(h)s  AND grp.code = prod.parent_code
WHERE dh.stream = 'regular' AND dh.fulfillment <> 'inter_entity'
  AND dh.org_id = 'PPS' AND dh.booked_date IS NOT NULL
GROUP BY 1, 2
"""


def snaive(vals, h, season):
    vals = np.asarray(vals, float)
    if len(vals) < 2 * season:
        last = vals[-season:] if len(vals) >= season else np.pad(vals, (season - len(vals), 0))
        return np.array([last[i % season] for i in range(h)])
    recent, prior = vals[-season:].sum(), vals[-2 * season:-season].sum()
    trend = float(np.clip((recent / prior) if prior > 0 else 1.0, 0.5, 2.0))
    last = vals[-season:]
    return np.array([last[i % season] * trend for i in range(h)])


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--top", type=int, default=10)
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL", file=sys.stderr)
        return 2
    with psycopg.connect(args.dsn) as conn:
        conn.execute("SET default_transaction_read_only = on")
        conn.execute("SET statement_timeout = '180s'")
        rows = conn.execute(SQL, {"h": H}).fetchall()
    df = pd.DataFrame(rows, columns=["grp", "ym", "u"])
    df["u"] = df["u"].astype(float)
    df["per"] = pd.PeriodIndex(df["ym"], freq="M")
    df["q"] = df["per"].dt.quarter
    df["miq"] = ((df["per"].dt.month - 1) % 3) + 1  # month-in-quarter 1..3

    tot = df["u"].sum()
    print("=== Forme saisonnière par TRIMESTRE (part du volume total) ===")
    for q, u in df.groupby("q")["u"].sum().items():
        print(f"   Q{q}: {u/tot:6.1%}  {'#' * int(u/tot*120)}")
    print("\n=== Concentration MOIS-DANS-LE-TRIMESTRE (effet fin-de-trimestre / buy) ===")
    for m, u in df.groupby("miq")["u"].sum().items():
        print(f"   mois {m} du trimestre: {u/tot:6.1%}  {'#' * int(u/tot*120)}")

    # quarterly backtest per top group (hold out last 4 quarters)
    df["qper"] = df["per"].dt.asfreq("Q")
    qidx = pd.period_range(df["qper"].min(), df["qper"].max(), freq="Q")
    top = df.groupby("grp")["u"].sum().sort_values(ascending=False).head(args.top).index
    print(f"\n=== Précision au TRIMESTRE (hold-out 4 derniers Q) — {qidx.min()}→{qidx.max()} ===")
    print(f"{'group':9s} {'quarterly WMAPE':>16s}")
    print("-" * 28)
    num = den = 0.0
    for g in top:
        s = df[df["grp"] == g].groupby("qper")["u"].sum().reindex(qidx, fill_value=0.0)
        if len(s) < 12:
            continue
        train, test = s.iloc[:-4].values, s.iloc[-4:].values
        pred = snaive(train, 4, 4)
        d = np.abs(test).sum()
        w = np.abs(test - pred).sum() / d if d else float("nan")
        print(f"{g:9s} {w:>15.1%}")
        num += np.abs(test - pred).sum()
        den += d
    print("-" * 28)
    print(f"{'OVERALL':9s} {num/den:>15.1%}   (rappel mensuel ~37-39%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
