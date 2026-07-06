"""
evaluate_outcomes.py — recommendation-outcome evaluation CLI (chantier #393
A3-PR2, ADR-030).

The proof machine's scoring pass: for every governed recommendation, classify
what ACTUALLY happened at its coordinate against what it predicted, and value
the shortage $ that was avoided. Run it (cron) after the daily snapshot capture
(scripts/snapshot_inventory.py) so the observation the classifier reads is
today's stock/shortage picture.

Thin over the engine: all logic lives in engine/outcome/evaluator.py
(``evaluate_outcome`` PURE + ``evaluate_and_persist`` orchestration). The CLI
only parses args, opens the connection, and reports.

Deterministic, never an LLM (North Star). Read-only on recommendations/
shortages/inventory_snapshots (ADR-021 — we READ shortages, never write it);
writes ONLY recommendation_outcomes. Idempotent: a re-run for the same
scenario/observation-day OVERWRITES each verdict (ON CONFLICT on the UNIQUE
key), never duplicates.

V1 baseline-only: an outcome is the REAL observed result, always baseline. The
--scenario flag exists for parity but the evaluation target is baseline.

Usage:
    DATABASE_URL=postgresql://... python scripts/evaluate_outcomes.py \
        [--scenario <uuid>] [--as-of YYYY-MM-DD] [--allow-dev]
"""
from __future__ import annotations

import argparse
import datetime as _dt
import logging
import os
import sys
import time

import psycopg

import mrp_core as core

from ootils_core.engine.outcome import evaluate_and_persist

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("evaluate_outcomes")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Recommendation-outcome evaluation (#393) — chain each "
        "governed reco to its observed result and value the $ avoided."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--scenario", default=core.BASELINE,
                   help="scenario_id to evaluate (default: baseline)")
    p.add_argument("--as-of", default=None,
                   help="observation day YYYY-MM-DD (default: DB CURRENT_DATE)")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2

    as_of: _dt.date | None = None
    if args.as_of is not None:
        try:
            as_of = _dt.date.fromisoformat(args.as_of)
        except ValueError:
            logger.error("invalid --as-of %r — expected YYYY-MM-DD", args.as_of)
            return 2

    db = core.guard_db(args.dsn, args.allow_dev)
    scenario = args.scenario
    logger.info(
        "Outcome Evaluation (#393) running on DB=%s scenario=%s as_of=%s",
        db, scenario, as_of if as_of is not None else "CURRENT_DATE",
    )
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        summary = evaluate_and_persist(conn, scenario, as_of)
        conn.commit()

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 92)
    logger.info("OUTCOME EVALUATION — COMPLETED in %.2fs", elapsed)
    logger.info("  Scenario                     : %s", scenario)
    logger.info("  As-of date                   : %s", summary["evaluated_as_of"])
    logger.info("  Recommendations evaluated    : %d", summary["evaluated"])
    logger.info("  Outcomes upserted            : %d", summary["upserted"])
    logger.info("  With avoided $ credited      : %d", summary["with_avoided_usd"])
    for st in sorted(summary["by_status"]):
        logger.info("    %-16s : %d", st, summary["by_status"][st])
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
