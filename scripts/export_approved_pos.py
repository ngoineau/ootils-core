"""
export_approved_pos.py — turn APPROVED recommendations into purchase-order drafts.

DEPRECATED (ADR-042 decision 4, PR-5, 2026-07-18) — REFERENCE ONLY, DO NOT
REUSE. This script is NOT idempotent: it re-reads every ``status = 'APPROVED'``
recommendation on EVERY invocation with no ``exported_at``-style marker, so
running it twice re-exports the same PO drafts a second time — exactly the
failure mode ADR-042 decision 4 was written to close. The idempotent
replacement is ``ootils_core.engine.reporting.outbound_export``
(``load_pending_export_rows`` / ``render_outbound_export`` / ``execute_export``
— ``WHERE status IN ('APPROVED','APPLIED') AND exported_at IS NULL``, stamped
after every write, one ``export_executed`` event per run), wired into
``scripts/run_daily_ingest.py``'s EXPORT phase behind
``OOTILS_OUTBOUND_EXPORT_ENABLED``. This file is kept only as a historical
reference for the PO-draft consolidation-by-supplier idea (multiple item
lines per PO, order-by-date math) — do not point any new caller at it.

The L4 boundary (strategy doc §13): this tool GENERATES the PO drafts a planner
sends to purchasing / pushes to the ERP. It never pushes to the ERP itself.

Consolidates APPROVED recommendations into one PO draft per supplier (multiple
item lines), computes the order-by date (need_by − lead_time), totals per PO and
per currency. Prints a readable worklist and writes a TSV.

Usage:
    DATABASE_URL=... python scripts/export_approved_pos.py [--out /path/draft.tsv]
"""
from __future__ import annotations

import argparse
import datetime as _dt
import logging
import os
import sys
from collections import defaultdict

import psycopg

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("export_pos")

SQL = """
SELECT supplier_external_id, item_external_id, recommended_qty, estimated_cost,
       currency, lead_time_days, shortage_date, action, recommendation_id,
       evidence->>'unit_cost' AS unit_cost
FROM recommendations
WHERE status = 'APPROVED'
ORDER BY supplier_external_id, margin_days ASC
"""


def _guard(dsn, allow_dev):
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Export APPROVED recommendations as PO drafts.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--out", default="/tmp/po_drafts.tsv")
    p.add_argument("--today", default=None, help="reference date YYYY-MM-DD (default: server CURRENT_DATE)")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    logger.warning(
        "DEPRECATED: export_approved_pos.py is NOT idempotent (no exported_at "
        "marker) — re-running it re-exports the same POs. Use "
        "ootils_core.engine.reporting.outbound_export (wired into "
        "scripts/run_daily_ingest.py's EXPORT phase) instead. See ADR-042 "
        "decision 4."
    )
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    _guard(args.dsn, args.allow_dev)

    with psycopg.connect(args.dsn) as conn:
        today = args.today or conn.execute("SELECT CURRENT_DATE").fetchone()[0].isoformat()
        rows = conn.execute(SQL).fetchall()

    if not rows:
        logger.info("No APPROVED recommendations to export.")
        return 0

    today_d = _dt.date.fromisoformat(today)
    by_sup = defaultdict(list)
    for r in rows:
        by_sup[r[0]].append(r)

    # Write TSV + print worklist
    tsv_lines = ["po_number\tsupplier_external_id\tline\titem_external_id\tquantity\tuom\t"
                 "unit_cost\tcurrency\tline_total\tneed_by_date\torder_by_date\tsource_recommendation_id"]
    grand_total = defaultdict(float)
    n_pos = 0
    datestr = today_d.strftime("%Y%m%d")

    logger.info("=" * 96)
    logger.info("PURCHASE ORDER DRAFTS — generated %s from APPROVED recommendations", today)
    logger.info("=" * 96)

    for si, (sup, items) in enumerate(sorted(by_sup.items()), 1):
        n_pos += 1
        po_number = f"PO-DRAFT-{datestr}-{si:03d}"
        po_total = defaultdict(float)
        logger.info("")
        logger.info("%s   supplier %s   (%d line%s)", po_number, sup, len(items), "s" if len(items) > 1 else "")
        logger.info("  %-4s %-16s %9s %12s %14s %-12s %-12s", "ln", "item", "qty", "unit_cost", "line_total", "need_by", "order_by")
        for ln, (s_ext, item, qty, cost, ccy, lt, need_by, action, rid, unit_cost) in enumerate(items, 1):
            ccy = ccy or "EUR"
            qty = float(qty)
            line_total = float(cost) if cost is not None else 0.0
            uc = float(unit_cost) if unit_cost not in (None, "") else None
            uc_s = f"{uc:,.2f}" if uc is not None else "—"
            order_by = (need_by - _dt.timedelta(days=int(lt))) if lt is not None else need_by
            po_total[ccy] += line_total
            grand_total[ccy] += line_total
            logger.info("  %-4d %-16s %9.0f %12s %14s %-12s %-12s",
                        ln, item, qty, uc_s, f"{line_total:,.0f}", str(need_by), str(order_by))
            tsv_lines.append(
                f"{po_number}\t{sup}\t{ln}\t{item}\t{qty:.0f}\tEA\t"
                f"{uc if uc is not None else ''}\t{ccy}\t{line_total:.2f}\t"
                f"{need_by}\t{order_by}\t{rid}"
            )
        tot_s = ", ".join(f"{ccy} {amt:,.0f}" for ccy, amt in po_total.items())
        logger.info("  PO total: %s", tot_s)

    logger.info("")
    logger.info("=" * 96)
    logger.info("SUMMARY: %d PO drafts, %d lines", n_pos, len(rows))
    for ccy, amt in sorted(grand_total.items(), key=lambda x: -x[1]):
        logger.info("  Grand total %-5s %s", ccy, f"{amt:,.2f}")
    logger.info("=" * 96)

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(tsv_lines) + "\n")
    logger.info("PO drafts written to %s (%d lines)", args.out, len(tsv_lines) - 1)
    logger.info("These are DRAFTS — a planner reviews and pushes to the ERP. The agent never does.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
