"""
daily_recompute.py — incremental (or full) propagation recompute for a scenario.

Coalesces every unprocessed event for a scenario into ONE calc run and
propagates the affected WHOLE SERIES in a single deterministic pass
(``PropagationEngine.process_pending``, chantier C3-PR2): ONE advisory lock,
ONE calc_run, ONE ANALYZE, ONE calc_run_finished — never N. This is the
consumer the daily timer runs after the governed ingest has landed the day's
feeds; ``scripts/run_daily_ingest.py`` deliberately does NOT recompute (see its
"NO AUTOMATIC RECOMPUTE" note) — that is this script's whole job.

    INCREMENTAL (default) — dirty only the (item_id, location_id) series touched
      by a pending event's trigger node. The cheap daily cadence.
    --full — dirty every active PI of the scenario (reconciliation): use it when
      the incremental backlog may be incomplete (e.g. after a bulk load that
      bypassed event emission, or to re-anchor after a code/engine change).

This script REPLACES the unversioned ``~/daily_recompute.py`` artefact that
used to live only on the VM. The VM timer will call THIS versioned script after
deployment; wiring that timer is a separate assembly step, NOT part of this
code.

NO --apply / NO dry-run: a recompute is a deterministic CALCULATION, not a
destructive write. It re-derives ProjectedInventory + shortage state from events
ALREADY committed to the graph (ADR-003) — it introduces no new baseline facts
and is fully reproducible, so there is nothing to preview or gate (unlike the
ingest / purge / outbound-export scripts, whose ``--apply`` guards a real
mutation of source-of-truth data). It obeys the engine selected by
``OOTILS_ENGINE`` (default ``sql``) via the same factory the API uses.

Engine note: ``OOTILS_ENGINE=rust-svc`` is OUT OF SCOPE — that engine overrides
``process_event`` wholesale but not ``_propagate``, so ``process_pending`` would
fall through to the base pure-Python compute (its in-RAM graph untouched). See
``PropagationEngine.process_pending``'s docstring. The default ``sql`` and the
in-process ``rust`` flavours work unchanged.

Usage:
    DATABASE_URL=postgresql://... python scripts/daily_recompute.py \\
        [--scenario <uuid>] [--full] [--allow-dev]

Exit codes: 0 the recompute ran to completion (including a 0-node no-op when
nothing was pending); 1 the scenario is already locked by another calc run, or
the recompute failed; 2 missing DATABASE_URL or a bad --scenario UUID.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

import mrp_core as core

from ootils_core.constants import BASELINE_SCENARIO_ID

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("daily_recompute")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Incremental propagation recompute (C3-PR2): coalesce a "
        "scenario's pending events into ONE calc run and propagate the affected "
        "whole series. --full recomputes every series (reconciliation)."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument(
        "--scenario", default=None,
        help="scenario UUID to recompute (default: baseline)",
    )
    p.add_argument(
        "--full", action="store_true",
        help="full reconciliation recompute (every active PI of the scenario), "
        "not just the series touched by pending events",
    )
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)

    if not args.dsn:
        logger.error("DATABASE_URL not set and --dsn not provided")
        return 2

    if args.scenario is None:
        scenario_id = BASELINE_SCENARIO_ID
    else:
        try:
            scenario_id = UUID(args.scenario)
        except ValueError:
            logger.error("--scenario %r is not a valid UUID", args.scenario)
            return 2

    db = core.guard_db(args.dsn, args.allow_dev)
    mode = "FULL" if args.full else "INCREMENTAL"
    logger.info(
        "Daily Recompute (C3-PR2) DB=%s scenario=%s mode=%s engine=%s",
        db, scenario_id, mode, os.environ.get("OOTILS_ENGINE", "sql"),
    )

    # Lazy import (mirrors api/routers/calc.py's use of the same factory): the
    # canonical engine factory respects OOTILS_ENGINE, so this script runs the
    # exact same backend the API would. Deferred past arg validation so `--help`
    # and a bad --dsn/--scenario never pay the import cost.
    from ootils_core.api.routers.events import _build_propagation_engine

    t0 = time.perf_counter()
    try:
        with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
            engine = _build_propagation_engine(conn)
            calc_run = engine.process_pending(scenario_id, conn, full=args.full)
            if calc_run is None:
                logger.error(
                    "REFUSED: scenario %s is already locked by another calc run",
                    scenario_id,
                )
                return 1
            conn.commit()
            dt = time.perf_counter() - t0
            # Honest counters straight off the CalcRun. dirty_node_count is the
            # unambiguous "PIs processed" measure (see the CalcRun model note);
            # nodes_recalculated is engine-dependent (SQL: every dirty PI; Python:
            # only PIs whose values changed).
            logger.info(
                "RECOMPUTE DONE scenario=%s mode=%s status=%s calc_run_id=%s "
                "events_consumed=%d pi_nodes_dirtied=%d nodes_recalculated=%d "
                "nodes_unchanged=%d duration_s=%.3f",
                scenario_id, mode, calc_run.status, calc_run.calc_run_id,
                len(calc_run.triggered_by_event_ids or []),
                calc_run.dirty_node_count or 0,
                calc_run.nodes_recalculated or 0,
                calc_run.nodes_unchanged or 0,
                dt,
            )
            return 0
    except Exception:  # noqa: BLE001 — any failure is a logged non-zero exit
        logger.exception(
            "recompute FAILED for scenario %s (mode=%s)", scenario_id, mode,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
