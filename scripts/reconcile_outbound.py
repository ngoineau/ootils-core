"""
reconcile_outbound.py — heuristic outbound-reconciliation CLI (ADR-042
decision 4, PR-5b; migration 086).

Runs ``engine.reconciliation.matcher.run_reconciliation`` for one ``run_date``:
loads every exported-but-not-yet-fulfilled recommendation, loads every inbound
ERP purchase order (baseline, active ``PurchaseOrderSupply`` nodes carrying an
ERP PO number), heuristically pairs them on business attributes (item, qty ±
tolerance, date ± window — see the matcher module docstring for the two KNOWN
GAPS: the PO node carries no supplier, non-TRANSFER recos carry no location),
and — for an UNAMBIGUOUS pair only — stamps
``recommendations.fulfilled_at`` / ``fulfilled_erp_id``. It is an OBSERVATION:
it never changes ``recommendations.status`` or the state machine.

DRY-RUN BY DEFAULT: without ``--apply`` this calls ``run_reconciliation(...,
dry_run=True)`` — it loads and matches but writes NOTHING (no stamp, no
``reconciliation_runs`` row, no ``reconciliation_completed`` event) and prints
the counts AND the individual pairs so an operator can eyeball the heuristic
before letting it stamp. ``--apply`` is additionally gated by the
``OOTILS_RECONCILIATION_ENABLED`` kill switch — default ON (the reconciliation
is an observation, not a destructive write, so the safe default differs from
``purge_maintenance.py``'s OFF; same default-ON posture as ``outcomes.py``).
``--apply`` is refused only when the switch is EXPLICITLY disabled.

Thin over the engine: all logic lives in
``engine/reconciliation/matcher.py``. The CLI only parses args, opens the
connection, calls the engine, and reports.

Usage:
    DATABASE_URL=postgresql://... python scripts/reconcile_outbound.py \\
        [--date 2026-07-18] [--apply] [--allow-dev]

Exit codes: 0 success (dry-run or apply, including "nothing to reconcile"); 1
--apply refused because ``OOTILS_RECONCILIATION_ENABLED`` is explicitly
disabled; 2 missing DATABASE_URL or a bad --date.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone

import psycopg
from psycopg.rows import dict_row

import mrp_core as core

from ootils_core.engine.reconciliation.matcher import (
    ReconciliationRunResult,
    run_reconciliation,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("reconcile_outbound")


def _reconciliation_enabled() -> bool:
    """Kill switch, default ON (an observation, not a destructive write — the
    safe default is opt-out, mirroring ``api/routers/outcomes.py``, unlike
    ``purge_maintenance.py``'s opt-in OFF). Same truthy-set as every other kill
    switch in the repo so the entry points can never disagree."""
    return os.environ.get("OOTILS_RECONCILIATION_ENABLED", "1").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _report_result(result: ReconciliationRunResult, *, applied: bool) -> None:
    logger.info(
        "RECONCILIATION %s run_date=%s candidates=%d matched=%d ambiguous=%d unmatched=%d",
        "APPLIED" if applied else "PREVIEW",
        result.run_date, result.candidates, result.matched, result.ambiguous,
        result.unmatched,
    )
    match = result.match
    for reco_id, po_external_id in match.matched:
        logger.info("  MATCH   recommendation_id=%s <- po=%s", reco_id, po_external_id)
    for reco_id in match.ambiguous_reco_ids:
        logger.info("  AMBIG   recommendation_id=%s (>=2 plausible POs)", reco_id)
    for po_external_id in match.ambiguous_po_ids:
        logger.info("  AMBIG   po=%s (>=2 plausible recommendations)", po_external_id)
    for reco_id in match.unmatched_reco_ids:
        logger.info("  UNMATCH recommendation_id=%s (no plausible PO)", reco_id)
    if applied and result.run_id is not None:
        logger.info("  run_id=%s event_id=%s", result.run_id, result.event_id)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Heuristic outbound reconciliation (ADR-042 PR-5b) — pairs "
        "inbound ERP POs with exported recommendations and stamps fulfilled_at "
        "for unambiguous matches. Observation only. Dry-run by default."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--date", default=None, help="run_date as YYYY-MM-DD (default: today UTC)")
    p.add_argument("--apply", action="store_true",
                   help="actually stamp + persist the run (default: dry-run / preview only)")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)

    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2

    if args.date is None:
        run_date = datetime.now(timezone.utc).date()
    else:
        try:
            run_date = date.fromisoformat(args.date)
        except ValueError:
            logger.error("--date %r is not a valid YYYY-MM-DD date", args.date)
            return 2

    if args.apply and not _reconciliation_enabled():
        logger.error(
            "REFUSED: --apply requires OOTILS_RECONCILIATION_ENABLED not to be "
            "disabled (got %r)",
            os.environ.get("OOTILS_RECONCILIATION_ENABLED"),
        )
        return 1

    db = core.guard_db(args.dsn, args.allow_dev)
    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(
        "Outbound Reconciliation (ADR-042 PR-5b) running on DB=%s run_date=%s mode=%s",
        db, run_date, mode,
    )

    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        result = run_reconciliation(conn, run_date, dry_run=not args.apply)
        if args.apply:
            conn.commit()
        _report_result(result, applied=args.apply)

    logger.info("=" * 92)
    logger.info("OUTBOUND RECONCILIATION — COMPLETED (mode=%s, run_date=%s)", mode, run_date)
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
