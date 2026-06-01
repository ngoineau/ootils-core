"""
ingest_demand_history.py — the demand-facts import path (Pyramide D5/D6).

Streams the raw ERP sales export (SALES_MARINE.full.tsv) and turns each line into
a demand-history fact, applying the LOCKED business rules:
  - LINE_TYPE classification (data/demand/line_type_classification.tsv): only
    `in_demand=yes` classes count; the rest (invoice-only, no-bill, non-warranty
    service, internal, returns) are dropped from demand.
  - SIGN RULE: demand = POSITIVE ORDERED_QUANTITY only. Negatives are returns and
    are NEVER netted into demand.
  - Two streams: REGULAR vs WARRANTY (warranty forecast separately; value-excluded).
  - Value/ASP: sum LINE_AMOUNT_EXT only when `in_asp=yes`.
  - Booking series keys on BOOKED_DATE (forecast-on-booking rule).
  - Per-site dimensions: SHIP_STATE (demand geo) + WAREHOUSE_ID (fulfilling DC).

Modes:
  --preview (default): aggregate in-memory and print the demand series, NO DB write.
  --load:   resolve item_code -> items.item_id and COPY qualifying rows into
            demand_history (bounded by --since for safety). Idempotent: deletes the
            loaded window first, so re-runs replace it cleanly.

Usage:
    python scripts/ingest_demand_history.py --load --since 2025-06-01
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

# SALES_MARINE column indices (0-based).
C_SHIP_DATE, C_BOOKED, C_ITEM, C_AMOUNT = 0, 1, 3, 5
C_LINE_ID, C_LINE_TYPE, C_ORDER_TYPE, C_ORDER_NUM, C_QTY = 9, 12, 17, 14, 19
C_ORG, C_COUNTRY, C_CHANNEL, C_STATE = 20, 23, 27, 28
C_WAREHOUSE, C_FULFILLED = 35, 51

DH_COLS = (
    "item_id", "item_code", "stream", "booked_date", "shipment_date",
    "ordered_quantity", "fulfilled_quantity", "value_ext", "counts_for_asp",
    "ship_state", "ship_country", "warehouse_id", "channel", "fulfillment",
    "order_number", "line_id", "org_id", "order_type",
)


def load_classification(path: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    with open(path, encoding="utf-8") as f:
        header = None
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if header is None:
                header = parts
                continue
            row = dict(zip(header, parts))
            out[row["line_type"]] = {
                "in_demand": row["in_demand"] == "yes",
                "in_asp": row["in_asp"] == "yes",
                "stream": row["forecast_stream"],
                "fulfillment": row["fulfillment"],
            }
    return out


def _g(r: list[str], i: int) -> str:
    """Safe field getter (handles trailing-empty truncation)."""
    return r[i] if i < len(r) else ""


def _d(s: str):
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


def _f(s: str):
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def iter_demand_rows(sales: Path, cls: dict, since: str | None):
    """Yield qualifying demand facts as dicts. Applies all locked rules."""
    with open(sales, encoding="utf-8-sig") as f:
        next(f)  # header
        for line in f:
            r = line.rstrip("\n").split("\t")
            lt = _g(r, C_LINE_TYPE)
            rule = cls.get(lt)
            if rule is None or not rule["in_demand"]:
                continue
            qty = _f(_g(r, C_QTY))
            if qty is None or qty <= 0:        # sign rule: positives only
                continue
            booked = _g(r, C_BOOKED)[:10]
            if since and booked < since:
                continue
            in_asp = rule["in_asp"]
            amt = _f(_g(r, C_AMOUNT)) or 0.0
            yield {
                "item_code": _g(r, C_ITEM),
                "stream": rule["stream"],
                "booked_date": booked,
                "shipment_date": _g(r, C_SHIP_DATE)[:10],
                "ordered_quantity": qty,
                "fulfilled_quantity": _f(_g(r, C_FULFILLED)),
                "value_ext": amt if in_asp else 0.0,
                "counts_for_asp": in_asp,
                "ship_state": _g(r, C_STATE) or None,
                "ship_country": _g(r, C_COUNTRY) or None,
                "warehouse_id": _g(r, C_WAREHOUSE) or None,
                "channel": _g(r, C_CHANNEL) or None,
                "fulfillment": rule["fulfillment"],
                "order_number": _g(r, C_ORDER_NUM) or None,
                "line_id": _g(r, C_LINE_ID) or None,
                "org_id": _g(r, C_ORG) or None,
                "order_type": _g(r, C_ORDER_TYPE) or None,
            }


def do_load(dsn: str, sales: Path, cls: dict, since: str) -> None:
    import psycopg

    with psycopg.connect(dsn) as conn:
        conn.execute("SET statement_timeout = '600s'")
        ext2id = {
            row[1]: row[0]
            for row in conn.execute(
                "SELECT item_id, external_id FROM items WHERE external_id IS NOT NULL"
            ).fetchall()
        }
        # idempotent: clear the window we're about to (re)load
        deleted = conn.execute(
            "DELETE FROM demand_history WHERE booked_date >= %s", (since,)
        ).rowcount
        print(f"[load] cleared {deleted} existing rows in window >= {since}")

        n = unresolved = 0
        with conn.cursor() as cur:
            with cur.copy(
                f"COPY demand_history ({', '.join(DH_COLS)}) FROM STDIN"
            ) as cp:
                for d in iter_demand_rows(sales, cls, since):
                    iid = ext2id.get(d["item_code"])
                    if iid is None:
                        unresolved += 1
                    cp.write_row((
                        iid, d["item_code"], d["stream"],
                        _d(d["booked_date"]), _d(d["shipment_date"]),
                        d["ordered_quantity"], d["fulfilled_quantity"],
                        d["value_ext"], d["counts_for_asp"],
                        d["ship_state"], d["ship_country"], d["warehouse_id"],
                        d["channel"], d["fulfillment"], d["order_number"], d["line_id"],
                        d["org_id"], d["order_type"],
                    ))
                    n += 1
        conn.commit()
        print(f"[load] inserted {n:,} demand_history rows "
              f"(unresolved item_id={unresolved:,}) since {since}")


def do_preview(sales: Path, cls: dict, since: str | None) -> None:
    by_stream_u: dict[str, float] = defaultdict(float)
    by_month: dict[str, float] = defaultdict(float)
    n = 0
    for d in iter_demand_rows(sales, cls, since):
        by_stream_u[d["stream"]] += d["ordered_quantity"]
        by_month[d["booked_date"][:7]] += d["ordered_quantity"]
        n += 1
    print(f"[preview] {n:,} demand rows" + (f" since {since}" if since else ""))
    for s in sorted(by_stream_u, key=lambda x: -by_stream_u[x]):
        print(f"   {s:10s} units={by_stream_u[s]:>14,.0f}")
    for m, u in sorted(by_month.items())[-14:]:
        print(f"   {m}  {u:>12,.0f}")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sales", default="Raw Data/SALES_MARINE.full.tsv")
    p.add_argument("--classification", default="data/demand/line_type_classification.tsv")
    p.add_argument("--load", action="store_true", help="write to demand_history (else preview)")
    p.add_argument("--since", default="2025-06-01", help="only booked_date >= this (YYYY-MM-DD)")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    args = p.parse_args(argv)

    cls = load_classification(Path(args.classification))
    print(f"[classification] {len(cls)} LINE_TYPE rules loaded")

    if args.load:
        if not args.dsn:
            print("ERROR: set DATABASE_URL for --load", file=sys.stderr)
            return 2
        do_load(args.dsn, Path(args.sales), cls, args.since)
    else:
        do_preview(Path(args.sales), cls, args.since)
    return 0


if __name__ == "__main__":
    sys.exit(main())
