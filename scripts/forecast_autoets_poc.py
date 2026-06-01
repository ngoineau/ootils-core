"""
forecast_autoets_poc.py — does a real seasonal model (AutoETS) beat the
seasonal-naive baseline at the aggregate level? (Pyramide axis A/B, heavier path).

On PPS end-demand (interco excluded), per local gen_group monthly total, holds
out the last 12 months and compares:
  - seasonal-naive (the cheap baseline used so far)
  - AutoETS (statsforecast, season_length=12) — the real seasonal model the
    whole hierarchy inherits via middle-out.

Read-only.
"""
from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd
import psycopg
from statsforecast.models import AutoETS

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


def seasonal_naive(vals: np.ndarray, h: int) -> np.ndarray:
    vals = vals.astype(float)
    if len(vals) < 24:
        last12 = vals[-12:] if len(vals) >= 12 else np.pad(vals, (12 - len(vals), 0))
        return np.array([last12[i % 12] for i in range(h)])
    recent, prior = vals[-12:].sum(), vals[-24:-12].sum()
    trend = float(np.clip((recent / prior) if prior > 0 else 1.0, 0.5, 2.0))
    last12 = vals[-12:]
    return np.array([last12[i % 12] * trend for i in range(h)])


def autoets(train: np.ndarray, h: int) -> np.ndarray:
    try:
        res = AutoETS(season_length=12).forecast(y=train.astype(np.float64), h=h)
        pred = np.asarray(res["mean"], dtype=float)
        return np.clip(pred, 0, None)
    except Exception:
        return seasonal_naive(train, h)  # fall back if the fit fails


def autoselect(train: np.ndarray, h: int) -> tuple[np.ndarray, str]:
    """Pick naive vs AutoETS per series using a validation holdout (no oracle):
    score both on the last 12 months of `train`, apply the winner to forecast h."""
    if len(train) >= 36:
        tr_v, va = train[:-12], train[-12:]
        dv = np.abs(va).sum() or 1.0
        en = np.abs(va - seasonal_naive(tr_v, 12)).sum() / dv
        ee = np.abs(va - autoets(tr_v, 12)).sum() / dv
        if ee < en:
            return autoets(train, h), "AutoETS"
    return seasonal_naive(train, h), "naive"


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
    df["period"] = pd.PeriodIndex(df["ym"], freq="M")
    idx = pd.period_range(df["period"].min(), df["period"].max(), freq="M")
    top = df.groupby("grp")["u"].sum().sort_values(ascending=False).head(args.top).index

    print(f"PPS end-demand, {idx.min()}→{idx.max()}\n")
    print(f"{'group':9s} {'naive':>8s} {'AutoETS':>8s} {'auto-sel':>9s} {'(picked)':>10s}")
    print("-" * 50)
    an = ae = asel = act = 0.0
    for g in top:
        s = df[df["grp"] == g].groupby("period")["u"].sum().reindex(idx, fill_value=0.0)
        if len(s) < 36:
            continue
        train, test = s.iloc[:-12].values, s.iloc[-12:].values
        pn, pe = seasonal_naive(train, 12), autoets(train, 12)
        ps, picked = autoselect(train, 12)
        d = np.abs(test).sum()
        wn, we, ws = (np.abs(test - x).sum() / d for x in (pn, pe, ps))
        print(f"{g:9s} {wn:>7.1%} {we:>7.1%} {ws:>8.1%} {picked:>10s}")
        an += np.abs(test - pn).sum()
        ae += np.abs(test - pe).sum()
        asel += np.abs(test - ps).sum()
        act += d
    print("-" * 50)
    best = min([("naive", an), ("AutoETS", ae), ("auto-sel", asel)], key=lambda x: x[1])[0]
    print(f"{'OVERALL':9s} {an/act:>7.1%} {ae/act:>7.1%} {asel/act:>8.1%}   best={best}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
