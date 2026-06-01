"""
forecast_eval_by_group.py — quarterly forecast precision by Gen_Group.

PPS end-demand (interco excluded). For each local gen_group: last-4-quarter volume
(importance) + quarterly WMAPE (hold out the last 4 complete quarters, seasonal-naive
season=4 — the planning-level accuracy). Sorted by volume, with a volume-weighted
summary so you can judge how much of the business forecasts well. Read-only.
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
SELECT grp.code AS grp, grp.description AS desc, to_char(dh.booked_date,'YYYY-MM') AS ym,
       SUM(dh.ordered_quantity) AS u
FROM demand_history dh
JOIN item_hierarchy ih ON ih.item_id = dh.item_id AND ih.hierarchy_id = %(h)s
JOIN hierarchy_node prod ON prod.hierarchy_id = %(h)s AND prod.code = ih.leaf_code
JOIN hierarchy_node grp  ON grp.hierarchy_id = %(h)s  AND grp.code = prod.parent_code
WHERE dh.stream = 'regular' AND dh.fulfillment <> 'inter_entity'
  AND dh.org_id = 'PPS' AND dh.booked_date IS NOT NULL
GROUP BY 1, 2, 3
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


def band(w):
    return "GOOD " if w < 0.25 else ("MED  " if w < 0.40 else "POOR ")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--show", type=int, default=30, help="rows to print (rest summarized)")
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL", file=sys.stderr)
        return 2
    with psycopg.connect(args.dsn) as conn:
        conn.execute("SET default_transaction_read_only = on")
        conn.execute("SET statement_timeout = '180s'")
        rows = conn.execute(SQL, {"h": H}).fetchall()
    df = pd.DataFrame(rows, columns=["grp", "desc", "ym", "u"])
    df["u"] = df["u"].astype(float)
    df["per"] = pd.PeriodIndex(df["ym"], freq="M")
    qcount = df.groupby(df["per"].dt.asfreq("Q"))["per"].nunique()
    last_q = max(q for q in qcount.index if qcount[q] >= 3)
    cut = pd.period_range(df["per"].min(), last_q.asfreq("M", "end"), freq="M")

    res = []
    for (g, desc), sub in df.groupby(["grp", "desc"]):
        s = sub.groupby("per")["u"].sum().reindex(cut, fill_value=0.0)
        if len(s) < 24:
            continue
        sq = s.groupby(s.index.asfreq("Q")).sum()
        if len(sq) < 8:
            continue
        train, test = sq.iloc[:-4].values, sq.iloc[-4:].values
        d = np.abs(test).sum()
        if d <= 0:
            continue
        w = np.abs(test - snaive(train, 4, 4)).sum() / d
        res.append((g, desc, float(test.sum()), w))   # vol = last 4Q units

    res.sort(key=lambda x: -x[2])
    tot_vol = sum(r[2] for r in res)
    print(f"PPS end-demand — quarterly WMAPE by Gen_Group  ({len(res)} groups, "
          f"eval last 4 complete quarters)\n")
    print(f"{'group':9s} {'description':26s} {'vol(4Q)':>11s} {'WMAPE':>7s} {'%vol':>6s}  band")
    print("-" * 70)
    for g, desc, vol, w in res[:args.show]:
        print(f"{g:9s} {str(desc)[:26]:26s} {vol:>11,.0f} {w:>6.0%} {vol/tot_vol:>5.1%}  {band(w)}")
    if len(res) > args.show:
        rest = res[args.show:]
        print(f"... +{len(rest)} smaller groups ({sum(r[2] for r in rest)/tot_vol:.0%} of volume)")

    # volume-weighted summary + bands
    wavg = sum(r[3] * r[2] for r in res) / tot_vol
    print("\n=== Synthèse (pondérée par le volume) ===")
    print(f"   WMAPE moyen pondéré volume : {wavg:.1%}")
    for lab, lo, hi in [("GOOD  (<25%)", 0, 0.25), ("MED   (25-40%)", 0.25, 0.40), ("POOR  (>40%)", 0.40, 9)]:
        v = sum(r[2] for r in res if lo <= r[3] < hi)
        n = sum(1 for r in res if lo <= r[3] < hi)
        print(f"   {lab:16s}: {v/tot_vol:>5.1%} du volume  ({n} groupes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
