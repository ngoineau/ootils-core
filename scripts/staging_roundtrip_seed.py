"""
scripts/staging_roundtrip_seed.py — round-trip the seed dataset through staging.

Reads the master entities (items, locations, suppliers) from a fully-seeded
ootils database (typically `ootils_seed_test` after running
`seed_realistic_dataset.py --profile M`), exports each to TSV / CSV / JSON
in memory, runs them back through the staging parser + loader, and asserts
the resulting ingest_rows match the source row count.

This is the "real volume" sister of tests/integration/test_staging_roundtrip.py
which only handles a synthetic 6-row dataset. Use this to validate the
pipeline against the 5K SKUs from the seed generator before pushing the
upload endpoint to production.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@127.0.0.1:15432/ootils_seed_test \\
        python scripts/staging_roundtrip_seed.py

The script DOES write to staging.* and public.ingest_* in the target DB
(it's a real load test). No data in items/locations/suppliers is modified.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time

import psycopg
from psycopg.rows import dict_row

from ootils_core.staging.loader import load_to_staging
from ootils_core.staging.parser import parse


def _export_items_to_tsv(conn) -> bytes:
    rows = conn.execute(
        "SELECT external_id, name, item_type, uom, status FROM items "
        "WHERE status != 'obsolete' ORDER BY external_id"
    ).fetchall()
    buf = io.StringIO()
    w = csv.writer(buf, delimiter="\t")
    w.writerow(["external_id", "name", "item_type", "uom", "status"])
    for r in rows:
        w.writerow([r["external_id"], r["name"], r["item_type"], r["uom"], r["status"]])
    return buf.getvalue().encode("utf-8"), len(rows)


def _export_locations_to_csv(conn) -> bytes:
    rows = conn.execute(
        "SELECT external_id, name, location_type, country, timezone "
        "FROM locations ORDER BY name"
    ).fetchall()
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=",", quotechar='"')
    w.writerow(["external_id", "name", "location_type", "country", "timezone"])
    for r in rows:
        w.writerow([r["external_id"], r["name"], r["location_type"], r["country"], r["timezone"]])
    return buf.getvalue().encode("utf-8"), len(rows)


def _export_suppliers_to_json(conn) -> bytes:
    rows = conn.execute(
        "SELECT external_id, name, country, lead_time_days, reliability_score, status "
        "FROM suppliers ORDER BY external_id"
    ).fetchall()
    payload = [
        {
            "external_id": r["external_id"],
            "name": r["name"],
            "country": r["country"],
            "lead_time_days": r["lead_time_days"],
            "reliability_score": float(r["reliability_score"]) if r["reliability_score"] is not None else None,
            "status": r["status"],
        }
        for r in rows
    ]
    return json.dumps(payload, ensure_ascii=False).encode("utf-8"), len(rows)


def _roundtrip(
    conn,
    entity: str,
    source: str,
    blob: bytes,
    filename: str,
    fmt_hint: str | None,
    expected_rows: int,
) -> dict:
    t0 = time.perf_counter()
    pr = parse(blob, filename=filename, format_hint=fmt_hint)
    t_parse = time.perf_counter() - t0

    if pr.row_count != expected_rows:
        raise AssertionError(
            f"{entity}: parsed {pr.row_count} rows but expected {expected_rows}"
        )

    t0 = time.perf_counter()
    result = load_to_staging(
        conn,
        parse_result=pr,
        entity_type=entity,
        source_system=source,
        raw_bytes_size=len(blob),
        filename=filename,
        submitted_by="staging_roundtrip_seed.py",
        notes=f"Round-trip validation on {entity}",
    )
    t_load = time.perf_counter() - t0
    conn.commit()

    if result.rows_inserted != expected_rows:
        raise AssertionError(
            f"{entity}: loaded {result.rows_inserted} rows but expected {expected_rows}"
        )

    return {
        "entity": entity,
        "format": pr.format,
        "encoding": pr.encoding,
        "delimiter": pr.delimiter,
        "rows": result.rows_inserted,
        "file_size_kb": round(len(blob) / 1024, 1),
        "parse_seconds": round(t_parse, 3),
        "load_seconds": round(t_load, 3),
        "batch_id": str(result.batch_id),
        "upload_id": str(result.upload_id),
        "sha256_prefix": result.sha256[:12],
    }


def main() -> int:
    parser_arg = argparse.ArgumentParser(description=__doc__)
    parser_arg.add_argument(
        "--dsn", default=os.environ.get("DATABASE_URL"),
        help="DSN of the seeded ootils DB (default: $DATABASE_URL)",
    )
    args = parser_arg.parse_args()

    if not args.dsn:
        print("FATAL: pass --dsn or set DATABASE_URL", file=sys.stderr)
        return 2

    print(f"[setup] connecting to {args.dsn.split('@')[-1] if '@' in args.dsn else args.dsn}")
    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        items_blob, n_items = _export_items_to_tsv(conn)
        locs_blob, n_locs = _export_locations_to_csv(conn)
        sups_blob, n_sups = _export_suppliers_to_json(conn)

        print(f"[export] items     {n_items:>6,} -> TSV  ({len(items_blob)/1024:.1f} KB)")
        print(f"[export] locations {n_locs:>6,} -> CSV  ({len(locs_blob)/1024:.1f} KB)")
        print(f"[export] suppliers {n_sups:>6,} -> JSON ({len(sups_blob)/1024:.1f} KB)")
        print()

        results = [
            _roundtrip(conn, "items",     "ROUNDTRIP-TSV",  items_blob, "items.tsv",     "tsv",  n_items),
            _roundtrip(conn, "locations", "ROUNDTRIP-CSV",  locs_blob,  "locations.csv", "csv",  n_locs),
            _roundtrip(conn, "suppliers", "ROUNDTRIP-JSON", sups_blob,  "suppliers.json", "json", n_sups),
        ]

    print("=" * 70)
    print("ROUND-TRIP RESULTS")
    print("=" * 70)
    for r in results:
        print(f"  {r['entity']:10s} {r['format']:5s} "
              f"rows={r['rows']:>6,}  "
              f"size={r['file_size_kb']:>7,.1f} KB  "
              f"parse={r['parse_seconds']:>5,.2f}s  "
              f"load={r['load_seconds']:>5,.2f}s  "
              f"batch={r['batch_id'][:8]}..")
    total_rows = sum(r["rows"] for r in results)
    total_time = sum(r["parse_seconds"] + r["load_seconds"] for r in results)
    print()
    print(f"TOTAL: {total_rows:,} rows round-tripped in {total_time:.2f}s")
    print("PARITY: OK (all rows_inserted match expected counts)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
