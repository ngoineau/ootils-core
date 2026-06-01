"""
forecast_poc.py — first forecasting slice on the real demand foundation.

Demonstrates Pyramide axis A (seasonality at an aggregate level) on the loaded
demand_history: builds the monthly BOOKING series per local gen_group, fits a
seasonal-naive + trend baseline, and backtests it (hold out the last 12 months).

Pure numpy/pandas — no statsforecast/Chronos (not installed). This is the
axis-A seasonal baseline the design builds hierarchical reconciliation +
foundation models on top of. Read-only.

Usage:
    DATABASE_URL=... python scripts/forecast_poc.py --top 6 --horizon 12
"""
from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd
import psycopg

H_LOCAL = "product_local_gen"

SERIES_SQL = """
SELECT grp.code AS grp_code, grp.description AS grp_desc,
       to_char(dh.booked_date, 'YYYY-MM') AS ym,
       SUM(dh.ordered_quantity) AS units
FROM demand_history dh
JOIN item_hierarchy ih ON ih.item_id = dh.item_id AND ih.hierarchy_id = %(h)s
JOIN hierarchy_node prod ON prod.hierarchy_id = %(h)s AND prod.code = ih.leaf_code
JOIN hierarchy_node grp  ON grp.hierarchy_id = %(h)s  AND grp.code = prod.parent_code
WHERE dh.stream = 'regular' AND dh.booked_date IS NOT NULL
GROUP BY 1, 2, 3
"""


def seasonal_naive_trend(series: pd.Series, horizon: int) -> np.ndarray:
    """Forecast = same month last year, scaled by the YoY trend of the last
    12 months vs the prior 12. The simplest baseline that respects seasonality."""
    vals = series.values.astype(float)
    if len(vals) < 24:
        # not enough for YoY trend → flat seasonal naive
        last12 = vals[-12:]
        return np.array([last12[i % 12] for i in range(horizon)])
    recent = vals[-12:].sum()
    prior = vals[-24:-12].sum()
    trend = (recent / prior) if prior > 0 else 1.0
    trend = float(np.clip(trend, 0.5, 2.0))  # guard against wild ratios
    last12 = vals[-12:]
    return np.array([last12[i % 12] * trend for i in range(horizon)])


def wmape(actual: np.ndarray, pred: np.ndarray) -> float:
    denom = np.abs(actual).sum()
    return float(np.abs(actual - pred).sum() / denom) if denom else float("nan")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--top", type=int, default=6, help="top N gen_groups by volume")
    p.add_argument("--horizon", type=int, default=12)
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL", file=sys.stderr)
        return 2

    with psycopg.connect(args.dsn) as conn:
        conn.execute("SET default_transaction_read_only = on")
        conn.execute("SET statement_timeout = '120s'")
        rows = conn.execute(SERIES_SQL, {"h": H_LOCAL}).fetchall()

    df = pd.DataFrame(rows, columns=["grp_code", "grp_desc", "ym", "units"])
    df["units"] = df["units"].astype(float)
    df["period"] = pd.PeriodIndex(df["ym"], freq="M")

    totals = df.groupby(["grp_code", "grp_desc"])["units"].sum().sort_values(ascending=False)
    top_groups = totals.head(args.top).index.tolist()

    full_idx = pd.period_range(df["period"].min(), df["period"].max(), freq="M")
    print(f"history: {full_idx.min()} → {full_idx.max()} ({len(full_idx)} months)\n")

    for code, desc in top_groups:
        s = (df[df["grp_code"] == code]
             .set_index("period")["units"]
             .reindex(full_idx, fill_value=0.0))
        # Backtest: hold out last 12 months
        if len(s) >= 36:
            train, test = s.iloc[:-12], s.iloc[-12:]
            bt = seasonal_naive_trend(train, 12)
            err = wmape(test.values.astype(float), bt)
            bt_str = f"backtest WMAPE={err:6.1%}"
        else:
            bt_str = "backtest n/a (short history)"

        fc = seasonal_naive_trend(s, args.horizon)
        fut_idx = pd.period_range(full_idx.max() + 1, periods=args.horizon, freq="M")
        hist_12 = s.iloc[-12:].sum()
        fc_12 = fc[:12].sum()

        print(f"=== {code}  {str(desc)[:28]:28s} | {bt_str} ===")
        print(f"    last 12m booked = {hist_12:>12,.0f}   forecast next 12m = {fc_12:>12,.0f}"
              f"   ({(fc_12/hist_12-1)*100:+.0f}%)")
        # show the forecast monthly (seasonality visible)
        line = "    fc: " + "  ".join(
            f"{ix.strftime('%y-%m')}:{v:,.0f}" for ix, v in zip(fut_idx[:6], fc[:6])
        )
        print(line + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
