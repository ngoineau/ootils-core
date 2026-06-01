"""
compute_asp.py — (re)compute the rolling-12-month ASP into item_asp.

ASP = T12M SUM(value_ext) / SUM(ordered_quantity) over ASP-eligible demand
(demand_history.counts_for_asp = TRUE), per (item, org). Full recompute,
idempotent (DELETE + INSERT in one transaction). Run on the monthly cycle.

Usage:
    DATABASE_URL=... python scripts/compute_asp.py
"""
from __future__ import annotations

import argparse
import os
import sys

import psycopg

RECOMPUTE_SQL = """
INSERT INTO item_asp (item_id, org_id, asp, units_12m, value_12m, window_start, window_end, computed_at)
SELECT dh.item_id,
       dh.org_id,
       ROUND(SUM(dh.value_ext) / NULLIF(SUM(dh.ordered_quantity), 0), 6) AS asp,
       SUM(dh.ordered_quantity) AS units_12m,
       SUM(dh.value_ext)        AS value_12m,
       %(start)s::date          AS window_start,
       %(end)s::date            AS window_end,
       now()
FROM demand_history dh
WHERE dh.counts_for_asp = TRUE
  AND dh.item_id IS NOT NULL
  AND dh.booked_date >= %(start)s
  AND dh.booked_date <= %(end)s
GROUP BY dh.item_id, dh.org_id
HAVING SUM(dh.ordered_quantity) > 0
"""


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--months", type=int, default=12, help="trailing window length")
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL", file=sys.stderr)
        return 2

    with psycopg.connect(args.dsn) as conn:
        conn.execute("SET statement_timeout = '180s'")
        end = conn.execute("SELECT max(booked_date) FROM demand_history").fetchone()[0]
        if end is None:
            print("demand_history is empty — nothing to compute")
            return 1
        start = end.replace(year=end.year - 1) if args.months == 12 else end
        # full recompute in one transaction
        conn.execute("DELETE FROM item_asp")
        cur = conn.execute(RECOMPUTE_SQL, {"start": start, "end": end})
        n = cur.rowcount
        conn.commit()
        # quick summary
        row = conn.execute(
            "SELECT count(*), "
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY asp), "
            "min(asp), max(asp) FROM item_asp WHERE org_id = 'PPS' AND asp > 0"
        ).fetchone()
    print(f"[asp] window {start} → {end} | {n:,} (item,org) ASP rows written")
    print(f"[asp] PPS: {row[0]:,} priced items | median ASP=${float(row[1]):,.2f} "
          f"| range ${float(row[2]):,.2f}–${float(row[3]):,.0f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
