"""
calibration.py — propagate + measure shortages + adjust OH iteratively.

After the projection graph is seeded, this module:

1. Marks all PI nodes dirty + starts a calc_run
2. Runs the SQL propagation engine (the fast one — Tier 3)
3. Measures the shortage rate over the just-computed PIs
4. If outside the target band, scales all OnHandSupply quantities and reruns
5. Stops when the band is hit or after `max_iterations`

The OH scaling is a simple proportional controller — coarse but fast and
deterministic. A future v2 could do per-(item, loc) calibration based on
which series are short, but global scaling is enough to land in a 5-9% band.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.engine.orchestration.calc_run import CalcRunManager
from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine
from ootils_core.seed.transactional.nodes import BASELINE_SCENARIO_ID


@dataclass
class CalibrationIteration:
    iteration: int
    oh_scale_applied: float
    pi_total: int
    pi_with_shortage: int
    shortage_pct: float
    propagation_seconds: float


@dataclass
class CalibrationResult:
    iterations: list[CalibrationIteration]
    converged: bool
    final_shortage_pct: float
    total_seconds: float


def _measure_shortage_pct(conn: DictRowConnection) -> tuple[int, int]:
    """Return (pi_with_shortage, pi_total) for the baseline scenario."""
    row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE has_shortage = TRUE) AS short_n,
            COUNT(*)                                    AS total_n
        FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND scenario_id = %s
          AND active = TRUE
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchone()
    return int(row["short_n"]), int(row["total_n"])


def _mark_all_pi_dirty_and_start_run(conn: DictRowConnection) -> tuple[UUID, set[UUID]]:
    """Create a fresh calc_run, persist all active PIs as dirty under it."""
    # Complete any prior running calc_run on this scenario so the advisory
    # lock can be re-acquired.
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE scenario_id = %s AND status = 'running'",
        (BASELINE_SCENARIO_ID,),
    )
    conn.execute("DELETE FROM dirty_nodes WHERE scenario_id = %s", (BASELINE_SCENARIO_ID,))

    pi_rows = conn.execute(
        """
        SELECT node_id FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND scenario_id = %s AND active = TRUE
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchall()
    pi_ids = {UUID(str(r["node_id"])) for r in pi_rows}

    calc_mgr = CalcRunManager()
    calc_run = calc_mgr.start_calc_run(
        scenario_id=BASELINE_SCENARIO_ID, event_ids=[], db=conn,
    )
    assert calc_run is not None, "could not acquire advisory lock"
    dirty = DirtyFlagManager()
    dirty.mark_dirty(pi_ids, BASELINE_SCENARIO_ID, calc_run.calc_run_id, conn)
    dirty.flush_to_postgres(calc_run.calc_run_id, BASELINE_SCENARIO_ID, conn)
    conn.commit()
    return calc_run.calc_run_id, pi_ids


def _scale_on_hand(conn: DictRowConnection, factor: float) -> int:
    """Multiply every OnHandSupply.quantity by `factor`. Returns rowcount."""
    cur = conn.execute(
        """
        UPDATE nodes SET quantity = quantity * %s::numeric, updated_at = now()
        WHERE node_type = 'OnHandSupply'
          AND scenario_id = %s
          AND active = TRUE
        """,
        (Decimal(str(factor)), BASELINE_SCENARIO_ID),
    )
    conn.commit()
    return cur.rowcount or 0


def _build_sql_engine(conn: DictRowConnection) -> SqlPropagationEngine:
    from ootils_core.engine.kernel.graph.store import GraphStore
    from ootils_core.engine.kernel.graph.traversal import GraphTraversal
    from ootils_core.engine.kernel.calc.projection import ProjectionKernel
    from ootils_core.engine.kernel.shortage.detector import ShortageDetector

    store = GraphStore(conn)
    return SqlPropagationEngine(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        shortage_detector=ShortageDetector(),
    )


def _run_propagation(conn: DictRowConnection) -> float:
    """One full propagation pass on all dirty PIs. Returns wall_seconds."""
    from ootils_core.models import CalcRun

    calc_run_id, dirty = _mark_all_pi_dirty_and_start_run(conn)
    engine = _build_sql_engine(conn)
    row = conn.execute(
        "SELECT * FROM calc_runs WHERE calc_run_id = %s",
        (calc_run_id,),
    ).fetchone()
    calc_run = CalcRun(
        calc_run_id=UUID(str(row["calc_run_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        triggered_by_event_ids=[],
        is_full_recompute=bool(row.get("is_full_recompute", False)),
        dirty_node_count=row.get("dirty_node_count"),
        nodes_recalculated=int(row.get("nodes_recalculated", 0)),
        nodes_unchanged=int(row.get("nodes_unchanged", 0)),
        status=row.get("status", "running"),
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        error_message=row.get("error_message"),
    )
    started = time.perf_counter()
    engine._propagate(calc_run, dirty, conn)
    if engine._shortage_detector is not None:
        engine._shortage_detector.resolve_stale(
            scenario_id=BASELINE_SCENARIO_ID,
            calc_run_id=calc_run_id,
            db=conn,
        )
    # Close the calc_run cleanly so the next iteration can start a new one
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE calc_run_id = %s",
        (calc_run_id,),
    )
    conn.commit()
    return time.perf_counter() - started


def _bootstrap_oh_from_demand(
    conn: DictRowConnection,
    horizon_days: int,
    target_cover_days: int = 30,
) -> int:
    """One-shot rebalancing of OH based on the demand actually wired into the graph.

    Reads total outflows per (item, location) over the horizon (after one
    propagation), divides by horizon_days to get the daily average, and
    sets OH = daily_avg * target_cover_days.

    Returns the number of OH rows updated. Idempotent — calling it twice
    just overwrites with the same target.
    """
    cur = conn.execute(
        """
        WITH demand_per_pair AS (
            SELECT
                pi.item_id,
                pi.location_id,
                COALESCE(SUM(pi.outflows), 0)::numeric / NULLIF(%s, 0) AS daily_demand
            FROM nodes pi
            WHERE pi.node_type = 'ProjectedInventory'
              AND pi.scenario_id = %s
              AND pi.active = TRUE
              AND pi.outflows IS NOT NULL
            GROUP BY pi.item_id, pi.location_id
        )
        UPDATE nodes oh
        SET quantity = GREATEST(
                COALESCE(dpp.daily_demand, 0) * %s::numeric,
                oh.quantity
            ),
            updated_at = now()
        FROM demand_per_pair dpp
        WHERE oh.node_type = 'OnHandSupply'
          AND oh.scenario_id = %s
          AND oh.active = TRUE
          AND oh.item_id = dpp.item_id
          AND oh.location_id = dpp.location_id
        """,
        (horizon_days, BASELINE_SCENARIO_ID, target_cover_days, BASELINE_SCENARIO_ID),
    )
    conn.commit()
    return cur.rowcount or 0


def _next_oh_scale(pct: float, target_pct: float, tolerance: float) -> tuple[float, bool]:
    """Pick the OH multiplier for the next iteration.

    Symmetric log-step controller: scale up or down by similar magnitudes
    so we don't overshoot massively then crawl back. Cap aggressive cases
    so a single iteration can't push OH by more than ~2.5x in either
    direction — keeps the trajectory monotonic-ish near the band.

    Special case: when pct is exactly 0, gap-based reasoning breaks (the
    signal is "more than enough OH"). Use a fixed aggressive downscale.
    """
    if abs(pct - target_pct) <= tolerance:
        return 1.0, True
    if pct > target_pct:
        gap = pct - target_pct
        if gap >= 0.50:
            return 2.5, False
        if gap >= 0.20:
            return 1.8, False
        if gap >= 0.10:
            return 1.4, False
        if gap >= 0.05:
            return 1.15, False
        return 1.05, False
    # pct < target -> reduce OH
    if pct < 0.005:
        # Stuck at zero — system has way too much OH. Big symmetric step down.
        return 0.4, False
    gap = target_pct - pct
    if gap >= 0.10:
        return 0.5, False
    if gap >= 0.05:
        return 0.7, False
    if gap >= 0.02:
        return 0.85, False
    return 0.95, False


def calibrate(
    conn: DictRowConnection,
    target_pct: float = 0.07,
    tolerance: float = 0.02,
    max_iterations: int = 10,
    bootstrap_cover_days: int = 30,
    horizon_days: int = 90,
) -> CalibrationResult:
    """Iteratively scale OH until shortage_pct is within [target-tol, target+tol].

    Sequence:
      1. Run one propagation pass (with the safety-stock-sized initial OH).
      2. Bootstrap OH from the observed demand (daily_avg * cover_days). This
         puts us in the right order of magnitude before the iterative phase.
      3. Iterate: propagate, measure, scale, repeat until within band or
         max_iterations is exhausted.
    """
    started = time.perf_counter()
    iterations: list[CalibrationIteration] = []
    converged = False

    # --- bootstrap pass: one propagation just to populate outflows ---
    bootstrap_seconds = _run_propagation(conn)
    _bootstrap_oh_from_demand(conn, horizon_days, bootstrap_cover_days)
    short_n, total_n = _measure_shortage_pct(conn)
    iterations.append(CalibrationIteration(
        iteration=0,  # iteration 0 = bootstrap probe + OH resize
        oh_scale_applied=float("nan"),  # we don't apply a scalar — we resize per-pair
        pi_total=total_n,
        pi_with_shortage=short_n,
        shortage_pct=round((short_n / total_n) if total_n else 0.0, 4),
        propagation_seconds=round(bootstrap_seconds, 2),
    ))

    # --- iterative scaling phase ---
    for i in range(max_iterations):
        propagation_seconds = _run_propagation(conn)
        short_n, total_n = _measure_shortage_pct(conn)
        pct = (short_n / total_n) if total_n else 0.0

        scale, conv = _next_oh_scale(pct, target_pct, tolerance)
        iterations.append(CalibrationIteration(
            iteration=i + 1,
            oh_scale_applied=scale,
            pi_total=total_n,
            pi_with_shortage=short_n,
            shortage_pct=round(pct, 4),
            propagation_seconds=round(propagation_seconds, 2),
        ))
        if conv:
            converged = True
            break
        _scale_on_hand(conn, scale)

    return CalibrationResult(
        iterations=iterations,
        converged=converged,
        final_shortage_pct=iterations[-1].shortage_pct if iterations else 0.0,
        total_seconds=round(time.perf_counter() - started, 2),
    )
