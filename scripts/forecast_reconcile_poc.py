"""
forecast_reconcile_poc.py — does middle-out beat bottom-up? (Pyramide axis A).

Tests the core thesis of the Pyramide forecasting design on real demand_history:
forecasting at the clean AGGREGATE level (gen_group) and disaggregating to SKUs
via historical share beats forecasting each noisy SKU series directly.

For each top gen_group, holds out the last 12 months and compares SKU-level
accuracy of:
  - BOTTOM-UP : seasonal-naive per SKU, summed.
  - MIDDLE-OUT: seasonal-naive on the GROUP total, split to SKUs by their
                training-period share.

Pure numpy/pandas, read-only.

Usage:
    DATABASE_URL=... python scripts/forecast_reconcile_poc.py --top 5
"""
from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd
import psycopg

H = "product_local_gen"

TOP_GROUPS_SQL = """
SELECT grp.code, grp.description, SUM(dh.ordered_quantity) AS u
FROM demand_history dh
JOIN item_hierarchy ih ON ih.item_id = dh.item_id AND ih.hierarchy_id = %(h)s
JOIN hierarchy_node prod ON prod.hierarchy_id = %(h)s AND prod.code = ih.leaf_code
JOIN hierarchy_node grp  ON grp.hierarchy_id = %(h)s  AND grp.code = prod.parent_code
WHERE dh.stream = 'regular' AND dh.booked_date IS NOT NULL
GROUP BY 1, 2 ORDER BY u DESC LIMIT %(n)s
"""

SKU_SERIES_SQL = """
SELECT dh.item_id::text AS sku, to_char(dh.booked_date, 'YYYY-MM') AS ym,
       SUM(dh.ordered_quantity) AS u
FROM demand_history dh
JOIN item_hierarchy ih ON ih.item_id = dh.item_id AND ih.hierarchy_id = %(h)s
JOIN hierarchy_node prod ON prod.hierarchy_id = %(h)s AND prod.code = ih.leaf_code
JOIN hierarchy_node grp  ON grp.hierarchy_id = %(h)s  AND grp.code = prod.parent_code
WHERE dh.stream = 'regular' AND dh.booked_date IS NOT NULL AND grp.code = %(g)s
GROUP BY 1, 2
"""


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
    p.add_argument("--top", type=int, default=5)
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL", file=sys.stderr)
        return 2

    with psycopg.connect(args.dsn) as conn:
        conn.execute("SET default_transaction_read_only = on")
        conn.execute("SET statement_timeout = '180s'")
        groups = conn.execute(TOP_GROUPS_SQL, {"h": H, "n": args.top}).fetchall()

        print(f"{'group':9s} {'description':24s} {'#SKU':>5s} "
              f"{'bottom-up':>9s} {'mid-flat':>9s} {'mid-seasonal':>11s} {'best':>13s}")
        print("-" * 86)
        agg_bu = agg_mo = agg_ms = agg_act = 0.0
        for code, desc, _ in groups:
            rows = conn.execute(SKU_SERIES_SQL, {"h": H, "g": code}).fetchall()
            df = pd.DataFrame(rows, columns=["sku", "ym", "u"])
            df["u"] = df["u"].astype(float)
            df["period"] = pd.PeriodIndex(df["ym"], freq="M")
            idx = pd.period_range(df["period"].min(), df["period"].max(), freq="M")
            if len(idx) < 30:
                continue
            mat = (df.pivot_table(index="sku", columns="period", values="u", aggfunc="sum")
                     .reindex(columns=idx, fill_value=0.0).fillna(0.0))
            train = mat.iloc[:, :-12]
            test = mat.iloc[:, -12:].values  # (n_sku, 12)

            # BOTTOM-UP: seasonal naive per SKU
            bu = np.vstack([seasonal_naive(train.iloc[i].values, 12) for i in range(len(train))])

            # MIDDLE-OUT (flat share): forecast the GROUP total, split by total share
            grp_train = train.values.sum(axis=0)
            grp_fc = seasonal_naive(grp_train, 12)            # (12,)
            sku_tot = train.values.sum(axis=1)
            share = sku_tot / sku_tot.sum() if sku_tot.sum() > 0 else np.zeros_like(sku_tot)
            mo = share[:, None] * grp_fc[None, :]             # (n_sku, 12)

            # MIDDLE-OUT (seasonal share): each SKU's share varies by calendar month
            # share_seasonal[sku, c] = SKU's fraction of the group in calendar month c.
            cal = np.array([per.month for per in train.columns])      # 1..12 per train col
            fc_months = [per.month for per in idx[-12:]]              # calendar months of the 12 fc periods
            mo_s = np.zeros_like(mo)
            for j, c in enumerate(fc_months):
                cols = cal == c
                grp_c = train.values[:, cols].sum()
                if grp_c > 0:
                    share_c = train.values[:, cols].sum(axis=1) / grp_c
                else:
                    share_c = share
                mo_s[:, j] = share_c * grp_fc[j]

            denom = np.abs(test).sum()
            wmape_bu = np.abs(test - bu).sum() / denom if denom else float("nan")
            wmape_mo = np.abs(test - mo).sum() / denom if denom else float("nan")
            wmape_ms = np.abs(test - mo_s).sum() / denom if denom else float("nan")
            best = min([("bottom-up", wmape_bu), ("mid-flat", wmape_mo), ("mid-seasonal", wmape_ms)], key=lambda x: x[1])[0]
            print(f"{code:9s} {str(desc)[:24]:24s} {len(mat):>5d} "
                  f"{wmape_bu:>9.1%} {wmape_mo:>9.1%} {wmape_ms:>11.1%} {best:>13s}")
            agg_bu += np.abs(test - bu).sum()
            agg_mo += np.abs(test - mo).sum()
            agg_ms += np.abs(test - mo_s).sum()
            agg_act += denom

        print("-" * 86)
        best_overall = min([("bottom-up", agg_bu), ("mid-flat", agg_mo), ("mid-seasonal", agg_ms)], key=lambda x: x[1])[0]
        print(f"{'OVERALL (SKU-level WMAPE)':30s} "
              f"{agg_bu/agg_act:>9.1%} {agg_mo/agg_act:>9.1%} {agg_ms/agg_act:>11.1%} {best_overall:>13s}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
