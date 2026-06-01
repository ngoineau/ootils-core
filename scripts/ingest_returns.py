"""
ingest_returns.py — load the returns series into returns_history.

Returns = negative-quantity sales lines (the sign rule: never netted into demand).
Streams SALES_MARINE, keeps qty < 0, stores POSITIVE magnitudes, resolves item_id.
Idempotent (DELETE + reload). Read-only on the file.

Usage:
    DATABASE_URL=... python scripts/ingest_returns.py
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import psycopg

C_BOOKED, C_ITEM, C_AMOUNT, C_LINE_TYPE = 1, 3, 5, 12
C_QTY, C_ORG, C_COUNTRY, C_CHANNEL, C_STATE, C_WAREHOUSE = 19, 20, 23, 27, 28, 35

COLS = ("item_id", "item_code", "org_id", "return_date", "return_quantity",
        "return_value", "line_type", "warehouse_id", "ship_state", "channel")


def _g(r, i):
    return r[i] if i < len(r) else ""


def _d(s):
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


def _f(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sales", default="Raw Data/SALES_MARINE.full.tsv")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL", file=sys.stderr)
        return 2

    with psycopg.connect(args.dsn) as conn:
        conn.execute("SET statement_timeout = '600s'")
        ext2id = {
            row[1]: row[0]
            for row in conn.execute(
                "SELECT item_id, external_id FROM items WHERE external_id IS NOT NULL"
            ).fetchall()
        }
        deleted = conn.execute("DELETE FROM returns_history").rowcount
        print(f"[returns] cleared {deleted} existing rows")
        n = unresolved = 0
        with conn.cursor() as cur, cur.copy(
            f"COPY returns_history ({', '.join(COLS)}) FROM STDIN"
        ) as cp, open(args.sales, encoding="utf-8-sig") as f:
            next(f)  # header
            for line in f:
                r = line.rstrip("\n").split("\t")
                qty = _f(_g(r, C_QTY))
                if qty is None or qty >= 0:        # returns only (negative qty)
                    continue
                code = _g(r, C_ITEM)
                iid = ext2id.get(code)
                if iid is None:
                    unresolved += 1
                amt = _f(_g(r, C_AMOUNT)) or 0.0
                cp.write_row((
                    iid, code, _g(r, C_ORG) or None, _d(_g(r, C_BOOKED)),
                    abs(qty), abs(amt), _g(r, C_LINE_TYPE) or None,
                    _g(r, C_WAREHOUSE) or None, _g(r, C_STATE) or None,
                    _g(r, C_CHANNEL) or None,
                ))
                n += 1
        conn.commit()
    print(f"[returns] inserted {n:,} return rows (unresolved item_id={unresolved:,})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
