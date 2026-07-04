"""
bench_reconciliation.py — reconciliation bench CLI (Pyramide axis A, design §8).

Thin CLI over ``ootils_core.pyramide.hierarchy.bench.run_reconciliation_bench``:
for each summing block of the domain's default hierarchy, replays a forecast
as of ``cutoff = bucket_start(today) - holdout buckets`` (train strictly
before the cutoff, never CURRENT_DATE-bounded — the anti-leak split lives in
the module) and scores every requested reconciliation method plus the uniform
'base' comparator against the demand that actually booked in the holdout
window. ``--grain`` picks the evaluation bucket (day / ISO week / calendar
month — window flags then count buckets, eval buckets always complete);
``--today`` fixes the as-of date for deterministic replays.

**Read-only**: the bench only SELECTs (windowed demand_history reads +
hierarchy registry); reconciliation and scoring are in-memory Python. Like
scripts/bench_mrp.py, this harness forces a read-only transaction as a
belt-and-braces guard, so it is safe against a loaded/pilote DB.

Output: one table row per (block, level, method) with WAPE / MASE / bias /
n_series / n_obs, then the per-block verdict (method with the minimal
leaf-level WAPE; '(cannot rank)' when no leaf WAPE is defined), then any
skip/fallback warnings.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_pilote_test \
        python scripts/bench_reconciliation.py --domain product \
            --methods middleout,mintrace_wls_shrink --holdout-days 28
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import psycopg
from psycopg.rows import dict_row

from ootils_core.pyramide.hierarchy.bench import (
    GRAIN_DAY,
    GRAIN_MONTH,
    GRAIN_WEEK,
    BenchReport,
    run_reconciliation_bench,
)


def _fmt(value, width: int = 9, digits: int = 4) -> str:
    """Fixed-width numeric cell; '-' for the accuracy module's None sentinel."""
    if value is None:
        return "-".rjust(width)
    return f"{value:.{digits}f}".rjust(width)


def print_report(report: BenchReport) -> None:
    unit = report.grain  # horizon/holdout/lookback count grain buckets
    print(
        f"=== Reconciliation bench -- domain={report.domain} "
        f"grain={report.grain} cutoff={report.cutoff.isoformat()} "
        f"horizon={report.horizon}{unit[0]} "
        f"holdout={report.holdout_days}{unit[0]} "
        f"lookback={report.lookback_days}{unit[0]} ===\n"
    )
    if not report.rows:
        print("(no rows -- every block was skipped, see warnings)")
    else:
        header = (
            f"{'block':<20}{'level':<14}{'method':<22}"
            f"{'WAPE':>9}{'MASE':>9}{'bias':>9}{'series':>8}{'n_obs':>8}"
        )
        print(header)
        print("-" * len(header))
        for block, level, method, wape, mase, bias, n_series, n_obs in (
            report.to_rows()
        ):
            print(
                f"{block:<20}{level:<14}{method:<22}"
                f"{_fmt(wape)}{_fmt(mase)}{_fmt(bias)}"
                f"{n_series:>8d}{n_obs:>8d}"
            )

    print("\n--- verdicts (minimal leaf-level WAPE per block) ---")
    verdicts = report.verdicts()
    if not verdicts:
        print("(no block benched)")
    for block in sorted(verdicts):
        winner = verdicts[block] or "(cannot rank: no defined leaf WAPE)"
        print(f"  {block:<20} -> {winner}")

    if report.warnings:
        print(f"\n--- warnings ({len(report.warnings)}) ---")
        for warning in report.warnings:
            print(f"  ! {warning}")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--domain", required=True,
                   help="hierarchy domain (uses the domain's default hierarchy)")
    p.add_argument("--block-level", default=None,
                   help="level defining the blocks (default: hierarchy root)")
    p.add_argument("--recon-level", default=None,
                   help="reconciliation level (default: the block level)")
    p.add_argument("--grain", default=GRAIN_DAY,
                   choices=[GRAIN_DAY, GRAIN_WEEK, GRAIN_MONTH],
                   help="evaluation bucket (default: day). horizon / "
                        "holdout-days / lookback-days count BUCKETS of "
                        "this grain; eval buckets are always complete "
                        "(today snaps to its bucket start: ISO Monday / "
                        "1st of month — the partial bucket is excluded)")
    p.add_argument("--today", type=date.fromisoformat, default=None,
                   help="ISO date the bench runs 'as of' (default: wall "
                        "clock) — for deterministic replays")
    p.add_argument("--lookback-days", type=int, default=365,
                   help="training window, in grain buckets (default 365)")
    p.add_argument("--horizon", type=int, default=28,
                   help="forecast/eval length, in grain buckets (default 28)")
    p.add_argument("--holdout-days", type=int, default=28,
                   help="held-out span before today's bucket, in grain "
                        "buckets (default 28)")
    p.add_argument("--methods", default="middleout",
                   help="comma-separated reconciliation methods "
                        "('base' is always benched implicitly)")
    p.add_argument("--blocks", default=None,
                   help="comma-separated block codes (default: every block)")
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL or pass --dsn", file=sys.stderr)
        return 2

    methods = [m.strip() for m in args.methods.split(",") if m.strip()]
    blocks = None
    if args.blocks:
        blocks = [b.strip() for b in args.blocks.split(",") if b.strip()]

    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        # Belt-and-braces: the bench only reads, but make it impossible to
        # write — protects a loaded/pilote DB from any accidental mutation.
        # COMMIT right after the SETs: in psycopg3 (non-autocommit) the first
        # execute() opens the very transaction the SETs run in, and
        # default_transaction_read_only only applies to SUBSEQUENT
        # transactions — without this commit the guard would be decorative
        # for the whole bench run (review PR4).
        conn.execute("SET statement_timeout = '120s'")
        conn.execute("SET default_transaction_read_only = on")
        conn.commit()
        report = run_reconciliation_bench(
            conn,
            domain=args.domain,
            block_level=args.block_level,
            recon_level=args.recon_level,
            lookback_days=args.lookback_days,
            horizon=args.horizon,
            holdout_days=args.holdout_days,
            methods=methods,
            block_codes=blocks,
            grain=args.grain,
            today=args.today,
        )

    print_report(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
