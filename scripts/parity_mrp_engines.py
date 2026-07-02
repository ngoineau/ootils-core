"""
parity_mrp_engines.py — measure the drift between the TWO MRP implementations.

Ootils carries two parallel MRP stacks that re-implement the same APICS math:
  A. scripts/mrp_core.py             — read-only, in-memory cascade (CLIs + watcher agents)
  B. src/ootils_core/engine/mrp/     — the APICS engine that backs the API (writes graph)

There is NO parity test between them — unlike the propagation engine. This harness
runs both on the SAME data + horizon and diffs per-item planned-order quantities,
to turn "they might have drifted" into a measured number.

Read-only: engine B writes to mrp_runs/mrp_bucket_records/nodes/edges, so it runs
inside a transaction that is ROLLED BACK — nothing persists. Engine A is naturally
read-only.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_pilote_test \
        python scripts/parity_mrp_engines.py --horizon-days 360

CI mode (#332 / ADR-020 step 1):
    python scripts/parity_mrp_engines.py --horizon-days 360 --check
exits 1 when the drift leaves the tolerated band (see check_thresholds for
how the default band was chosen). The band is an anti-regression guard, NOT
a convergence target — the residual drift is owned by ADR-020 steps 3-4.
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
from uuid import UUID

import logging

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mrp_core as core  # noqa: E402

from ootils_core.engine.mrp.mrp_apics_engine import MrpApicsEngine, MrpRunConfig  # noqa: E402

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def run_core(dsn: str, horizon_days: int) -> dict[str, float]:
    """Engine A — return {item_id_str: total_planned_qty}."""
    with psycopg.connect(dsn) as conn:
        conn.execute("SET default_transaction_read_only = on")
        conn.execute("SET statement_timeout = '180s'")
        d = core.load_planning_data(conn, horizon_days)
    gross = core.consume_demand(d)
    r = core.run_timephased(d, gross)
    out: dict[str, float] = defaultdict(float)
    for item, qty, rel, need, kind, pd in r["planned"]:
        out[str(item)] += float(qty)
    return dict(out)


def _top_demand_items(conn, n: int) -> list[UUID]:
    """The n items carrying the most forecast demand — a representative sample
    of finished goods so engine B (which queries per-item) stays tractable."""
    rows = conn.execute(
        "SELECT item_id, SUM(quantity) AS q FROM nodes "
        "WHERE scenario_id = %s AND active AND node_type = 'ForecastDemand' "
        "AND quantity IS NOT NULL GROUP BY item_id ORDER BY q DESC LIMIT %s",
        (BASELINE, n),
    ).fetchall()
    return [UUID(str(r["item_id"])) for r in rows]


def run_engine(dsn: str, horizon_days: int, sample: int | None) -> tuple[dict[str, float], object]:
    """Engine B — run the APICS engine, read its bucket records, then ROLLBACK.

    Returns ({item_id_str: total_planned_order_receipts}, MrpRunResult).
    """
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("SET statement_timeout = '600s'")
        engine = MrpApicsEngine(conn)
        item_ids = _top_demand_items(conn, sample) if sample else None
        cfg = MrpRunConfig(
            scenario_id=BASELINE,
            location_id=None,
            item_ids=item_ids,
            horizon_days=horizon_days,
            bucket_grain="week",
            recalculate_llc=False,
        )
        result = engine.run(cfg)
        rows = conn.execute(
            "SELECT item_id, SUM(planned_order_receipts) AS qty "
            "FROM mrp_bucket_records WHERE run_id = %s GROUP BY item_id",
            (result.run_id,),
        ).fetchall()
        conn.rollback()  # discard everything the engine wrote
    out: dict[str, float] = {}
    for r in rows:
        if r["qty"] is not None and float(r["qty"]) != 0.0:
            out[str(r["item_id"])] = float(r["qty"])
    return out, result


def diff(a: dict[str, float], b: dict[str, float], sampled: bool) -> dict:
    """Print the parity report and return the headline metrics for --check."""
    ka, kb = set(a), set(b)
    only_a, only_b, common = ka - kb, kb - ka, ka & kb

    print("=== Coverage ===")
    print(f"  items planned by A (mrp_core) : {len(ka):>7}{'  (full graph)' if sampled else ''}")
    print(f"  items planned by B (engine)   : {len(kb):>7}{'  (sampled scope + explosion)' if sampled else ''}")
    if not sampled:
        print(f"  only in A                     : {len(only_a):>7}")
    print(f"  planned by B but NOT by A     : {len(only_b):>7}  <- B says order, A says none")
    print(f"  common (both plan an order)   : {len(common):>7}")
    total_a = sum(a[k] for k in common)
    total_b = sum(b[k] for k in common)
    print(f"  total qty A (common items)    : {total_a:>15,.1f}")
    print(f"  total qty B (common items)    : {total_b:>15,.1f}")
    print()

    # Per-item relative drift on the common set
    rels = []
    exact = within1 = within5 = big = 0
    worst: list[tuple[str, float, float, float]] = []
    for k in common:
        va, vb = a[k], b[k]
        denom = max(abs(va), abs(vb), 1e-9)
        rel = abs(va - vb) / denom
        rels.append(rel)
        if rel < 1e-9:
            exact += 1
        elif rel <= 0.01:
            within1 += 1
        elif rel <= 0.05:
            within5 += 1
        else:
            big += 1
            worst.append((k, va, vb, rel))

    print("=== Per-item drift (common items) ===")
    if rels:
        print(f"  exact (<1e-9)        : {exact:>7}  ({100*exact/len(common):.1f}%)")
        print(f"  within 1%            : {within1:>7}")
        print(f"  within 5%            : {within5:>7}")
        print(f"  >5% drift            : {big:>7}  ({100*big/len(common):.1f}%)")
        print(f"  median rel drift     : {statistics.median(rels):.4f}")
        print(f"  p90 rel drift        : {statistics.quantiles(rels, n=10)[-1]:.4f}" if len(rels) >= 10 else "")
        print(f"  max rel drift        : {max(rels):.4f}")
    worst.sort(key=lambda x: -x[3])
    if worst:
        print("\n  Worst divergences (item, A_qty, B_qty, rel):")
        for k, va, vb, rel in worst[:10]:
            print(f"    {k}  A={va:>12,.1f}  B={vb:>12,.1f}  rel={rel:.2f}")

    return {
        "common": len(common),
        "only_b": len(only_b),
        "total_a_common": total_a,
        "total_b_common": total_b,
        "median_rel": statistics.median(rels) if rels else None,
        "max_rel": max(rels) if rels else None,
    }


def check_thresholds(m: dict, max_median: float, max_ratio: float) -> list[str]:
    """CI drift guard (#332 / ADR-020 step 1). Returns failure messages ([] = pass).

    The band is an ANTI-REGRESSION guard, not a convergence target. Where the
    defaults come from (ADR-020 "Validation du fix de netting", pilot DB,
    500 items, horizon 360 d):

    - median per-item drift: measured 4.1% AFTER the netting fix. Default
      cap 5% sits just above that residual, which is owned by planning-unit
      (pooled vs per-location) and lot-rule nuances slated for ADR-020
      steps 3-4. Tighten only after those land — do not chase it sooner.
    - total-qty ratio B/A on common items: the netting bug made B over-plan
      ~48x (worst x261); post-fix ratio is ~1.02x. A symmetric 1.5x band
      catches any return of serial over-counting — or a new under-planning
      bug — while it is still orders of magnitude below pilot impact.
    - zero common items means parity is unmeasurable (bad seed / empty DB),
      which must fail loudly rather than vacuously pass.
    """
    if m["common"] == 0:
        return ["no item planned by BOTH engines — parity unmeasurable (empty or missing seed?)"]
    failures: list[str] = []
    if m["median_rel"] is None:
        # diff() guarantees median_rel when common > 0; a None here means the
        # metrics contract was broken upstream — fail loudly, never skip.
        failures.append("median_rel is None despite common > 0 — metrics contract broken")
    elif m["median_rel"] > max_median:
        failures.append(
            f"median per-item drift {m['median_rel']:.4f} > cap {max_median:.4f}"
        )
    if m["total_a_common"] <= 0 or m["total_b_common"] <= 0:
        failures.append(
            f"degenerate totals on common items: A={m['total_a_common']:.1f} "
            f"B={m['total_b_common']:.1f} — ratio guard unusable"
        )
    else:
        ratio = m["total_b_common"] / m["total_a_common"]
        if not (1.0 / max_ratio <= ratio <= max_ratio):
            failures.append(
                f"total qty ratio B/A {ratio:.3f} outside "
                f"[{1.0 / max_ratio:.3f}, {max_ratio:.3f}]"
            )
    return failures


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=360)
    p.add_argument("--sample-items", type=int, default=500,
                   help="Cap engine B to the N highest-demand items + their BOM "
                        "explosion (B queries per-item; full 36K is minutes). 0 = full.")
    p.add_argument("--check", action="store_true",
                   help="CI mode: exit 1 when drift leaves the tolerated band "
                        "(see check_thresholds for how the defaults were set).")
    p.add_argument("--max-median-drift", type=float, default=0.05,
                   help="--check cap on median per-item relative drift "
                        "(default 0.05, just above the 4.1%% pilot residual).")
    p.add_argument("--max-total-ratio", type=float, default=1.5,
                   help="--check symmetric band on total qty B/A over common "
                        "items: fail outside [1/R, R] (default 1.5; the netting "
                        "bug was ~48x, post-fix ~1.02x).")
    args = p.parse_args()
    if not args.dsn:
        print("ERROR: set DATABASE_URL or pass --dsn", file=sys.stderr)
        return 2

    # The APICS engine logs one warning per item when location_id is NULL —
    # silence the flood so the parity report is readable (the calc proceeds).
    logging.disable(logging.WARNING)
    name = args.dsn.rstrip("/").split("/")[-1].split("?")[0]
    print(f"=== MRP cross-engine parity -- DB={name} horizon={args.horizon_days}d ===\n")

    t0 = time.perf_counter()
    a = run_core(args.dsn, args.horizon_days)
    print(f"[A] mrp_core      : {len(a)} items planned in {time.perf_counter()-t0:.1f}s")

    sample = args.sample_items or None
    t1 = time.perf_counter()
    b, result = run_engine(args.dsn, args.horizon_days, sample)
    scope = f"top-{sample} demand items + explosion" if sample else "full graph"
    print(f"[B] APICS engine  : {len(b)} items planned in {time.perf_counter()-t1:.1f}s "
          f"({scope}; engine items_processed={result.items_processed}, "
          f"records={result.total_records})\n")

    metrics = diff(a, b, sampled=bool(sample))
    if args.check:
        failures = check_thresholds(metrics, args.max_median_drift, args.max_total_ratio)
        if failures:
            print("\n=== PARITY CHECK: FAIL ===", file=sys.stderr)
            for f in failures:
                print(f"  - {f}", file=sys.stderr)
            return 1
        print("\n=== PARITY CHECK: PASS (within anti-regression band) ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
