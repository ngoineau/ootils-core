"""
prove_segmentation_fva.py — DEM-2 PR1 proof harness: does forecasting each
buy program (SPRING/SUMMER/EARLY/FWD BUY, ``demand_history.order_type``,
migration 048) separately and summing beat the single blended forecast?

Thin CLI over ``ootils_core.pyramide.segmentation``: for the ``--top`` pilot
(item, location) series with the most distinct buy programs, builds the
dense per-program calendar (:func:`get_historical_demand_by_program`), runs
the AVANT (1-partition)/APRÈS (N-partition) rolling-origin backtest pair
(:func:`run_segmented_fva_proof`) and prints ΔFVA per series plus a volume-
weighted aggregate. The proof machine (ADR-030) judges — this script only
wires data in and prints the verdict, never invents one (None-honest: a
series with too little history prints ``-``, never a fabricated delta).

**Read-only**: only ``demand_history``/``items``/``locations``/
``location_aliases`` SELECTs; the backtest itself is in-memory Python (same
belt-and-braces read-only transaction guard as ``scripts/bench_mrp.py`` /
``scripts/bench_reconciliation.py``).

The forecast model injected into the backtest is
``PyramideForecastEngine.forecast(method=AUTO_SELECT)`` — the SAME stat
engine the production Pyramide backtest path uses
(``pyramide/engines.py:_backtest_report``) for both the AVANT (blended) and
APRÈS (per-program) orchestrations: the proof isolates the effect of
segmentation, not a change of forecasting method.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_pilote_test \\
        python scripts/prove_segmentation_fva.py --top 8 --granularity monthly
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from decimal import Decimal
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from ootils_core.db.types import DictRowConnection
from ootils_core.pyramide.accuracy import ForecastFn
from ootils_core.pyramide.engines import PyramideForecastEngine
from ootils_core.pyramide.models import METHOD_AUTO_SELECT
from ootils_core.pyramide.repository import _DEMAND_HISTORY_BUSINESS_PREDICATES
from ootils_core.pyramide.segmentation import (
    SegmentationProofResult,
    SegmentationProofRow,
    aggregate_delta_fva_wape,
    get_historical_demand_by_program,
    run_segmented_fva_proof,
    verify_partition_exhaustive,
)


# Series eligible for the proof: at least this many DISTINCT order_type
# CLASSES observed, where NULL/blank counts as its own class (the UNKNOWN
# bucket) -- a SPRING+NULL series IS segmentable (SPRING/UNKNOWN), so NULL
# must not be invisible to the gate. This is a SELECTION heuristic, never a
# correctness invariant: two order_types folding into the same bucket
# (e.g. STANDARD + VISTA -> BASE) pass the gate but yield a no-op delta of
# exactly 0 -- harmless, just not informative.
_DEFAULT_MIN_ORDER_TYPES = 2

# ---------------------------------------------------------------------------
# Eligible-series discovery. Alias-aware (ADR-031) but in the REVERSE
# direction of segmentation.py's _warehouse_codes_subquery (that helper maps
# ONE known location_id -> its accepted warehouse codes; here we need the
# other direction, RAW warehouse_id code -> its owning location, across every
# location in one scan). site_codes is the general form of that mapping --
# built from the SAME two tables (locations.external_id UNION
# location_aliases.alias) -- NOT a hand-written
# ``warehouse_id = locations.external_id`` equality: a site with aliases
# still resolves correctly here.
# ---------------------------------------------------------------------------
_ELIGIBLE_SERIES_SQL = f"""
    WITH site_codes AS (
        SELECT external_id AS code, location_id FROM locations
        UNION ALL
        SELECT alias AS code, location_id FROM location_aliases
    )
    SELECT dh.item_id AS item_id, sc.location_id AS location_id,
           i.external_id AS item_code, l.external_id AS location_code,
           COUNT(DISTINCT COALESCE(NULLIF(TRIM(dh.order_type), ''), '<NULL>')) AS n_order_types,
           COALESCE(SUM(dh.ordered_quantity), 0) AS total_qty
    FROM demand_history dh
    JOIN site_codes sc ON sc.code = dh.warehouse_id
    JOIN items i ON i.item_id = dh.item_id
    JOIN locations l ON l.location_id = sc.location_id
    WHERE {_DEMAND_HISTORY_BUSINESS_PREDICATES}
    GROUP BY dh.item_id, sc.location_id, i.external_id, l.external_id
    HAVING COUNT(DISTINCT COALESCE(NULLIF(TRIM(dh.order_type), ''), '<NULL>')) >= %(min_order_types)s
    ORDER BY total_qty DESC
    LIMIT %(top)s
"""


def _eligible_series(
    conn: DictRowConnection,
    *,
    lookback_days: int,
    min_order_types: int,
    top: int,
) -> list[dict[str, Any]]:
    return conn.execute(
        _ELIGIBLE_SERIES_SQL,
        {
            "lookback_days": lookback_days,
            "min_order_types": min_order_types,
            "top": top,
        },
    ).fetchall()


def _stat_forecast_fn(granularity: str, horizon_start: date) -> ForecastFn:
    """Wire the SAME stat engine the production backtest path uses
    (``PyramideForecastEngine`` AUTO_SELECT candidate search) as the
    ``ForecastFn`` injected into ``run_segmented_fva_proof`` -- reused for
    BOTH the mixed and the per-program forecasts, never re-implemented."""
    engine = PyramideForecastEngine()

    def _fn(train: Sequence[Decimal], periods: int) -> Sequence[Decimal]:
        if not train:
            raise ValueError(
                "forecast_fn requires a non-empty training slice "
                "(rolling-origin min_train must be >= 1)"
            )
        computation = engine.forecast(
            history=train,
            periods=periods,
            method=METHOD_AUTO_SELECT,
            method_params={},
            model_strategy="stat",
            granularity=granularity,
            horizon_start=horizon_start,
            random_seed=0,
        )
        return list(computation.values)

    return _fn


def _fmt(value: Decimal | None, width: int = 9, digits: int = 4) -> str:
    if value is None:
        return "-".rjust(width)
    return f"{value:.{digits}f}".rjust(width)


def _print_row(item_code: str, location_code: str, result: SegmentationProofResult) -> None:
    wape_mixed = result.mixed_report.wape if result.mixed_report is not None else None
    wape_segmented = result.segmented_report.wape if result.segmented_report is not None else None
    print(
        f"{item_code:<14}{location_code:<10}{result.n_buckets:>8d}"
        f"{','.join(p[:2] for p in result.programs):<18}"
        f"{_fmt(wape_mixed)}{_fmt(wape_segmented)}"
        f"{_fmt(result.delta_fva_wape)}{_fmt(result.delta_fva_mase)}"
        f"{result.basis_count:>8d}"
    )


def run(
    conn: DictRowConnection,
    *,
    granularity: str,
    lookback_days: int,
    tail_origins: int,
    horizon: int,
    min_order_types: int,
    top: int,
) -> list[SegmentationProofRow]:
    eligible = _eligible_series(
        conn, lookback_days=lookback_days, min_order_types=min_order_types, top=top
    )
    if not eligible:
        print("(no eligible series -- none has >= "
              f"{min_order_types} distinct order_type values in the "
              f"{lookback_days}-day lookback)")
        return []

    header = (
        f"{'item':<14}{'location':<10}{'buckets':>8}{'programs':<18}"
        f"{'WAPE(mix)':>9}{'WAPE(seg)':>9}{'dFVA_wape':>9}{'dFVA_mase':>9}{'basis':>8}"
    )
    print(header)
    print("-" * len(header))

    rows: list[SegmentationProofRow] = []
    for record in eligible:
        calendar = get_historical_demand_by_program(
            conn, record["item_id"], record["location_id"], lookback_days, granularity
        )
        if not verify_partition_exhaustive(calendar):
            # Structural bug in the taxonomy/calendar builder, not a data
            # condition -- surfaced loudly, series skipped rather than
            # silently trusted.
            print(
                f"  ! {record['item_code']}/{record['location_code']}: "
                "partition-exhaustive invariant failed -- skipped",
                file=sys.stderr,
            )
            continue

        n_buckets = len(calendar.bucket_starts)
        min_train = max(1, n_buckets - tail_origins)
        horizon_start = calendar.bucket_starts[0] if calendar.bucket_starts else date.today()
        forecast_fn = _stat_forecast_fn(granularity, horizon_start)

        try:
            result = run_segmented_fva_proof(
                calendar, forecast_fn, min_train=min_train, horizon=horizon
            )
        except ValueError as exc:
            print(
                f"  ! {record['item_code']}/{record['location_code']}: "
                f"backtest failed ({exc}) -- skipped",
                file=sys.stderr,
            )
            continue

        volume = sum(calendar.total, Decimal("0"))
        rows.append(
            SegmentationProofRow(
                item_id=record["item_id"],
                location_id=record["location_id"],
                result=result,
                volume=volume,
            )
        )
        _print_row(record["item_code"], record["location_code"], result)

    print("-" * len(header))
    weighted_mean, n_contributing = aggregate_delta_fva_wape(rows)
    if weighted_mean is None:
        print(
            f"AGGREGATE dFVA_wape: n/a (0/{len(rows)} series had a "
            "computable delta -- insufficient history/cutoffs)"
        )
    else:
        print(
            f"AGGREGATE dFVA_wape (volume-weighted): {weighted_mean:.4f} "
            f"over {n_contributing}/{len(rows)} series"
        )
    return rows


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--top", type=int, default=8, help="eligible series to prove (default 8)")
    parser.add_argument(
        "--granularity", default="monthly", choices=["weekly", "monthly"],
        help="dense bucket size (default monthly; daily is out of scope, see #433)",
    )
    parser.add_argument("--lookback-days", type=int, default=1095, help="history window (default 1095 = ~3y)")
    parser.add_argument(
        "--tail-origins", type=int, default=52,
        help="max rolling-origin cutoffs evaluated, counted from the series end "
             "(default 52 -- same tail convention as pyramide/engines.py _backtest_report)",
    )
    parser.add_argument("--horizon", type=int, default=1, help="forecast horizon per cutoff, in buckets (default 1)")
    parser.add_argument(
        "--min-order-types", type=int, default=_DEFAULT_MIN_ORDER_TYPES,
        help="minimum distinct order_type values for a series to be eligible (default 2)",
    )
    args = parser.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL or pass --dsn", file=sys.stderr)
        return 2

    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        # Belt-and-braces: this harness only reads (demand_history/items/
        # locations/location_aliases); the backtest itself is in-memory
        # Python. Guards against any accidental mutation on a loaded/pilote
        # DB, like scripts/bench_mrp.py / scripts/bench_reconciliation.py.
        conn.execute("SET statement_timeout = '180s'")
        conn.execute("SET default_transaction_read_only = on")
        conn.commit()
        run(
            conn,
            granularity=args.granularity,
            lookback_days=args.lookback_days,
            tail_origins=args.tail_origins,
            horizon=args.horizon,
            min_order_types=args.min_order_types,
            top=args.top,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
