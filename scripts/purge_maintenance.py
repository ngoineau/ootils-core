"""
purge_maintenance.py — scenario-fork purge + shortage-retention CLI (PURGE-1,
migration 076).

Runs BOTH maintenance sweeps engine/maintenance/purge.py implements:
  * fork purge — deletes an archived, non-baseline scenario's child data
    (never the scenarios row itself) once its archived_at is older than
    --ttl-days.
  * shortage retention — deletes long-``resolved`` shortages rows older than
    --retention-days, per scenario, never the scenario's own latest
    ``completed`` calc_run and never a ``status='active'`` row.

DRY-RUN BY DEFAULT: without --apply this only calls the plan_* (SELECT-only)
functions and reports what WOULD be deleted, table by table — no DB write,
no commit. --apply is additionally gated by the OOTILS_PURGE_ENABLED kill
switch (must be exactly '1'; any other value, including unset, refuses
--apply before a connection is even opened).

Thin over the engine: all logic lives in engine/maintenance/purge.py. The
CLI only parses args, opens the connection, and reports.

Usage:
    DATABASE_URL=postgresql://... python scripts/purge_maintenance.py \
        [--ttl-days 7] [--retention-days 30] [--apply] [--allow-dev] \
        [--executed-by cli:purge_maintenance]

Exit codes: 0 success (dry-run or apply, including "nothing to do"); 1 apply
refused by the kill switch or a purge guard violation; 2 missing
DATABASE_URL or bad CLI args.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import psycopg
from psycopg.rows import dict_row

import mrp_core as core

from ootils_core.engine.maintenance import (
    PurgeGuardError,
    apply_fork_purge,
    apply_shortage_retention,
    plan_fork_purge,
    plan_shortage_retention,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("purge_maintenance")

_DEFAULT_EXECUTED_BY = "cli:purge_maintenance"


def _purge_enabled() -> bool:
    """Kill switch, default OFF (a purge is destructive-adjacent — the safer
    default is opt-in, unlike the read-mostly kill switches elsewhere in the
    repo). Same truthy-set as the API router (routers/maintenance.py) so the
    two entry points can never disagree on whether the switch is on."""
    return os.environ.get("OOTILS_PURGE_ENABLED", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _report_fork_plan(plan) -> None:
    logger.info(
        "FORK PURGE — %d candidate(s), %d row(s) total",
        len(plan.candidates), plan.rows_total,
    )
    for candidate in plan.candidates:
        logger.info(
            "  scenario_id=%s name=%r archived_at=%s (%d rows)",
            candidate.scenario_id, candidate.name, candidate.archived_at,
            candidate.rows_total,
        )
        for table, count in candidate.per_table_counts.items():
            if count > 0:
                logger.info("    %-28s : %d", table, count)


def _report_retention_plan(plan) -> None:
    logger.info(
        "SHORTAGE RETENTION — %d scenario(s), %d row(s) total",
        len(plan.candidates), plan.rows_total,
    )
    for candidate in plan.candidates:
        logger.info(
            "  scenario_id=%s : %d resolved shortage(s) eligible",
            candidate.scenario_id, candidate.rows_to_delete,
        )


def _report_apply_results(label: str, results) -> None:
    applied = [r for r in results if not r.skipped]
    skipped = [r for r in results if r.skipped]
    logger.info(
        "%s — applied=%d skipped=%d rows_deleted_total=%d",
        label, len(applied), len(skipped),
        sum(r.rows_deleted_total for r in results),
    )
    for r in applied:
        logger.info(
            "  scenario_id=%s run_id=%s rows_deleted=%d",
            r.scenario_id, r.run_id, r.rows_deleted_total,
        )
    for r in skipped:
        logger.info("  scenario_id=%s SKIPPED (already up to date)", r.scenario_id)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Maintenance purge (PURGE-1) — TTL-driven scenario-fork "
        "purge + long-resolved shortage retention. Dry-run by default."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--ttl-days", type=int, default=7,
                   help="archived-scenario TTL in days before fork purge (default: 7)")
    p.add_argument("--retention-days", type=int, default=30,
                   help="resolved-shortage retention in days (default: 30)")
    p.add_argument("--apply", action="store_true",
                   help="actually delete (default: dry-run / preview only)")
    p.add_argument("--executed-by", default=_DEFAULT_EXECUTED_BY,
                   help="audit attribution stamped on maintenance_purge_runs")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)

    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    if args.ttl_days < 0 or args.retention_days < 0:
        logger.error("--ttl-days / --retention-days must be >= 0")
        return 2

    if args.apply and not _purge_enabled():
        logger.error(
            "REFUSED: --apply requires OOTILS_PURGE_ENABLED=1 (got %r)",
            os.environ.get("OOTILS_PURGE_ENABLED"),
        )
        return 1

    db = core.guard_db(args.dsn, args.allow_dev)
    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(
        "Maintenance Purge (PURGE-1) running on DB=%s ttl_days=%d retention_days=%d mode=%s",
        db, args.ttl_days, args.retention_days, mode,
    )
    t0 = time.perf_counter()

    try:
        with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
            fork_plan = plan_fork_purge(conn, args.ttl_days)
            retention_plan = plan_shortage_retention(conn, args.retention_days)
            _report_fork_plan(fork_plan)
            _report_retention_plan(retention_plan)

            if args.apply:
                fork_results = apply_fork_purge(conn, fork_plan, args.executed_by)
                retention_results = apply_shortage_retention(
                    conn, retention_plan, args.executed_by
                )
                conn.commit()
                _report_apply_results("FORK PURGE APPLIED", fork_results)
                _report_apply_results("SHORTAGE RETENTION APPLIED", retention_results)
    except PurgeGuardError as exc:
        logger.error("purge guard refused: %s", exc)
        return 1

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 92)
    logger.info("MAINTENANCE PURGE — COMPLETED in %.2fs (mode=%s)", elapsed, mode)
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
