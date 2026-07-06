"""
snapshot_inventory.py — inventory snapshot capture CLI (chantier #393 A3-PR1).

Cron-friendly capturer: scans on-hand per (item, location) for a scenario and
upserts one ``inventory_snapshots`` row per coordinate for the capture day
(source='cli'). This is the proof-machine historisation entry point — run it
daily (cron) to build the stock history a later pass reads to compare "what we
projected" against "what actually happened".

Thin over the engine: all logic lives in engine/snapshot/capture.py
(``capture_snapshot`` SELECT-only + ``persist_snapshot`` idempotent upsert). The
CLI only parses args, opens the connection, and reports.

Idempotent: a re-run for the same scenario/day OVERWRITES each coordinate's row
(ON CONFLICT on the UNIQUE key) — running twice a day is safe, never
duplicates. Per-site, never pooled (the DRP lesson): the snapshot is a
site-level fact.

V1 baseline-only (migration 067): defaults to the baseline scenario. The
--scenario flag exists for schema/forkability parity but the V1 capture target
is baseline.

Usage:
    DATABASE_URL=postgresql://... python scripts/snapshot_inventory.py \
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

from ootils_core.engine.snapshot import capture_snapshot, persist_snapshot

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("snapshot_inventory")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Inventory snapshot capture (#393) — per-(item, location) "
        "on-hand history for the proof machine."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--scenario", default=core.BASELINE,
                   help="scenario_id to capture (default: baseline)")
    p.add_argument("--as-of", default=None,
                   help="capture day YYYY-MM-DD (default: DB CURRENT_DATE)")
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
        "Inventory Snapshot (#393) running on DB=%s scenario=%s as_of=%s",
        db, scenario, as_of if as_of is not None else "CURRENT_DATE",
    )
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        rows = capture_snapshot(conn, scenario, as_of, source="cli")
        written = persist_snapshot(conn, rows, source="cli")
        conn.commit()

    resolved_as_of = rows[0].as_of_date if rows else as_of
    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 92)
    logger.info("INVENTORY SNAPSHOT — COMPLETED in %.2fs", elapsed)
    logger.info("  Scenario                     : %s", scenario)
    logger.info("  As-of date                   : %s", resolved_as_of)
    logger.info("  Coordinates captured (upsert): %d", written)
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
