"""
agent_transfer_watcher.py — DRP Transfer Watcher (#395 PR2b), thin over drp_core.

The 6th watcher of the fleet. Turns the deterministic DRP fair-share transfer
signals (drp_core.transfer_signals: inter-site moves of finished stock that
cover a projected per-site deficit from a linked source's excess) into GOVERNED
L1 DRAFT recommendations — the same `recommendations` queue a planner reviews,
the North Star channel. NEVER `shortages` (ADR-021: that table is
ShortageDetector's alone — the DRP is read-only against it and emits governed
DRAFTs instead).

No counter-factual fork (like the reschedule watcher #346, ADR-026, and UNLIKE
the shortage/material watchers #340): a transfer signal is a deterministic fact
— "this site is projected short and this linked source holds excess" — so the
signal IS its own evidence. There is nothing to simulate; forking would add
noise, not proof.

Scenario-scoped: the watcher runs on a given scenario_id (default baseline) and
loads the distribution plan through that scenario
(load_drp_data(scenario=...)). A fork with a safety-stock overlay (#347) shifts
per-site deficits/excess, which shifts the emitted transfers — automatically,
with no scenario branch here. The recommendations it writes carry that
scenario_id and their own deterministic ids, so baseline and a fork never
collide.

Idempotence / stability (the same contract as #346): the recommendation_id is
DETERMINISTIC over (scenario, item, source_location, dest_location, ship_date)
— engine/recommendation/transfer.transfer_recommendation_id. Rows are UPSERTED
with ON CONFLICT (recommendation_id) DO NOTHING, so re-running on an UNCHANGED
plan re-derives the same ids and inserts ZERO new rows. Prior TRANSFER DRAFTs of
this agent/scenario whose signal no longer fires are marked EXPIRED.

North Star: deterministic core, DRAFT only (never applies; decision level from
agent_governance.decision_level('TRANSFER') = L1, a new-order draft — never
hardcoded), auditable (agent_runs), explainable (evidence = the fair-share
detail), idempotent (deterministic id + ON CONFLICT).

Usage:
    DATABASE_URL=postgresql://... python scripts/agent_transfer_watcher.py \
        [--scenario <uuid>] [--horizon-days 180]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import psycopg

import mrp_core as core
from agent_governance import decision_level, governed_run

from ootils_core.engine.recommendation.transfer import emit_transfer_recommendations

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("transfer_watcher")
AGENT_NAME = "transfer_watcher"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="DRP Transfer Watcher (#395) — governed DRAFT transfer recommendations."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--scenario", default=core.BASELINE,
                   help="scenario_id to run on (default: baseline)")
    p.add_argument("--horizon-days", type=int, default=180)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    scenario = args.scenario
    # Resolve the decision level ONCE from the shared fleet mapping (never
    # hardcode 'L1' — TRANSFER is a new-order draft, L1).
    level = decision_level("TRANSFER")
    logger.info(
        "DRP Transfer Watcher (#395) running on DB=%s scenario=%s level=%s",
        db, scenario, level,
    )
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        with governed_run(conn, AGENT_NAME, scenario, t0=t0) as run:
            metrics = emit_transfer_recommendations(
                conn,
                scenario,
                args.horizon_days,
                agent_name=AGENT_NAME,
                agent_run_id=run.run_id,
                decision_level=level,
            )
            run.set_metrics(metrics)

    m = metrics
    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 92)
    logger.info("TRANSFER WATCHER — run %s COMPLETED in %.2fs", str(run.run_id)[:8], elapsed)
    logger.info("  Signals from drp_core               : %d", m["signals"])
    logger.info("  Recommendations affirmed (DRAFT)    : %d  (new: %d, idempotent no-op: %d)",
                m["recommendations_affirmed"], m["recommendations_inserted"],
                m["recommendations_idempotent_noop"])
    logger.info("  Prior DRAFTs expired (signal gone)  : %d", m["expired_stale_drafts"])
    if m["unresolved_coords"]:
        logger.warning("  Unresolved coordinates (skipped)    : %d", m["unresolved_coords"])
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
