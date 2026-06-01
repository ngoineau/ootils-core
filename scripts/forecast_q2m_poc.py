"""
forecast_q2m_poc.py — the business method: forecast at QUARTER, disaggregate to
MONTH on historical share (temporal middle-out).

Compares, at MONTHLY accuracy (held-out last 4 complete quarters = 12 months):
  - DIRECT MONTHLY : seasonal-naive on the monthly series (the ~37% baseline).
  - Q->MONTH       : seasonal-naive on the QUARTER series, then split each quarter
                     to its 3 months via the historical month-in-quarter share.

Hypothesis: Q->month inherits the cleaner quarterly signal and beats direct
monthly — the temporal analogue of spatial middle-out. Read-only.
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
SELECT grp.code AS grp, to_char(dh.booked_date, 'YYYY-MM') AS ym, SUM(dh.ordered_quantity) AS u
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
    full = pd.period_range(df["per"].min(), df["per"].max(), freq="M")

    # Truncate to the last COMPLETE calendar quarter (3 months present in data).
    qcount = df.groupby([df["per"].dt.asfreq("Q")])["per"].nunique()
    complete = [q for q in qcount.index if qcount[q] >= 3]
    last_q = max(complete)
    cut = full[full <= last_q.asfreq("M", "end")]
    print(f"PPS end-demand — eval on last 4 complete quarters, history ends {last_q}\n")
    print(f"{'group':9s} {'direct-month':>13s} {'Q->month':>10s} {'winner':>10s}")
    print("-" * 46)

    num_d = num_q = den = 0.0
    for g in df.groupby("grp")["u"].sum().sort_values(ascending=False).head(args.top).index:
        s = df[df["grp"] == g].groupby("per")["u"].sum().reindex(cut, fill_value=0.0)
        if len(s) < 24:
            continue
        train_m, test_m = s.iloc[:-12], s.iloc[-12:]

        # DIRECT MONTHLY
        d_pred = snaive(train_m.values, 12, 12)

        # Q -> MONTH
        sq = s.groupby(s.index.asfreq("Q")).sum()
        q_fc = snaive(sq.iloc[:-4].values, 4, 4)              # 4 forecast quarters
        # month-in-quarter share from training months
        tr = train_m
        share = {}
        for (cq, pos), v in tr.groupby([tr.index.quarter, ((tr.index.month - 1) % 3) + 1]).sum().items():
            share[(cq, pos)] = v
        qtot = tr.groupby(tr.index.quarter).sum()
        q_pred = np.zeros(12)
        for i, per in enumerate(test_m.index):
            cq = per.quarter
            pos = ((per.month - 1) % 3) + 1
            sh = share.get((cq, pos), 0.0) / qtot.get(cq, np.nan) if qtot.get(cq, 0) else 1 / 3
            q_pred[i] = q_fc[i // 3] * sh

        act = test_m.values
        d = np.abs(act).sum()
        wd = np.abs(act - d_pred).sum() / d
        wq = np.abs(act - q_pred).sum() / d
        print(f"{g:9s} {wd:>12.1%} {wq:>9.1%} {('Q->month' if wq < wd else 'direct'):>10s}")
        num_d += np.abs(act - d_pred).sum()
        num_q += np.abs(act - q_pred).sum()
        den += d
    print("-" * 46)
    print(f"{'OVERALL':9s} {num_d/den:>12.1%} {num_q/den:>9.1%} "
          f"{('Q->month' if num_q < num_d else 'direct'):>10s}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
