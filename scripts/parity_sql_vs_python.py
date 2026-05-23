"""
scripts/parity_sql_vs_python.py — Tier 3 parity harness.

Seeds a rich scenario (OH supplies + PO supplies anchored on various dates),
runs the Python propagator AND the SQL window spike on the same data, and
diffs the resulting node state row-by-row.

This is the foundation for the SQL engine rollout: if it ever reports a
mismatch, we don't ship.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@127.0.0.1:15432/ootils_dev \\
        OOTILS_API_TOKEN=bench \\
        python scripts/parity_sql_vs_python.py --items 20 --buckets 30

The script DROPs and recreates `ootils_test_bench`. Same WARNING as the
sibling bench script.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_propagation import (  # type: ignore[import-not-found]
    _admin_recreate_db,
    _apply_migrations,
    _mark_all_pi_dirty,
    BASELINE_SCENARIO_ID,
)
from ootils_core.engine.orchestration.propagator_sql import (  # type: ignore[import-not-found]
    SqlPropagationEngine,
)


def _seed_rich(
    conn: psycopg.Connection,
    items: int,
    buckets: int,
    supplies_per_item: int,
    demands_per_item: int,
    seed: int,
) -> dict:
    """Seed bench scenario plus PO supplies anchored on random dates within the horizon.

    For each item we emit `supplies_per_item` PO nodes with random quantities
    and `time_ref` distributed uniformly across the horizon. Each PO is
    connected via 'replenishes' to the PI bucket whose time_span contains
    its time_ref (mirrors what the ingest pipeline produces).
    """
    rng = random.Random(seed)
    started = time.perf_counter()
    location_id = uuid4()
    today = date.today()
    horizon_start = today
    horizon_end = today + timedelta(days=buckets)

    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, 'PARITY-LOC')",
        (location_id,),
    )

    # 1. Items
    item_ids: list[UUID] = [uuid4() for _ in range(items)]
    item_names: list[str] = [f"PARITY-ITEM-{i:05d}" for i in range(items)]
    conn.execute(
        "INSERT INTO items (item_id, name) SELECT * FROM UNNEST(%s::uuid[], %s::text[])",
        (item_ids, item_names),
    )

    # 2. projection_series
    series_ids: list[UUID] = [uuid4() for _ in range(items)]
    conn.execute(
        """
        INSERT INTO projection_series (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
        SELECT * FROM UNNEST(
            %s::uuid[], %s::uuid[],
            ARRAY_FILL(%s::uuid, ARRAY[%s]),
            ARRAY_FILL(%s::uuid, ARRAY[%s]),
            ARRAY_FILL(%s::date, ARRAY[%s]),
            ARRAY_FILL(%s::date, ARRAY[%s])
        )
        """,
        (series_ids, item_ids,
         location_id, items, BASELINE_SCENARIO_ID, items,
         horizon_start, items, horizon_end, items),
    )

    # 3. OnHandSupply — varied quantities to exercise the OH path
    oh_ids: list[UUID] = [uuid4() for _ in range(items)]
    oh_qtys: list[Decimal] = [Decimal(rng.choice([0, 25, 50, 100, 200])) for _ in range(items)]
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             quantity, qty_uom, time_grain, time_ref, is_dirty, active)
        SELECT
            oh.id, 'OnHandSupply', %s, oh.item_id, %s,
            oh.qty, 'EA', 'exact_date', %s, FALSE, TRUE
        FROM UNNEST(%s::uuid[], %s::uuid[], %s::numeric[]) AS oh(id, item_id, qty)
        """,
        (BASELINE_SCENARIO_ID, location_id, horizon_start, oh_ids, item_ids, oh_qtys),
    )

    # 4. PI nodes (items × buckets)
    pi_node_count = items * buckets
    pi_ids: list[UUID] = [uuid4() for _ in range(pi_node_count)]
    pi_item_id: list[UUID] = []
    pi_series_id: list[UUID] = []
    pi_bs: list[date] = []
    pi_be: list[date] = []
    pi_seq: list[int] = []
    for i in range(items):
        for b in range(buckets):
            pi_item_id.append(item_ids[i])
            pi_series_id.append(series_ids[i])
            pi_bs.append(horizon_start + timedelta(days=b))
            pi_be.append(horizon_start + timedelta(days=b + 1))
            pi_seq.append(b)

    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             time_grain, time_span_start, time_span_end,
             projection_series_id, bucket_sequence, is_dirty, active)
        SELECT
            pi.id, 'ProjectedInventory', %s, pi.item_id, %s,
            'day', pi.bs, pi.be, pi.series_id, pi.seq, TRUE, TRUE
        FROM UNNEST(%s::uuid[], %s::uuid[], %s::date[], %s::date[], %s::uuid[], %s::int[])
             AS pi(id, item_id, bs, be, series_id, seq)
        """,
        (BASELINE_SCENARIO_ID, location_id,
         pi_ids, pi_item_id, pi_bs, pi_be, pi_series_id, pi_seq),
    )

    # 5. PO supplies — random dates within the horizon, varied quantities
    po_ids: list[UUID] = []
    po_item_ids: list[UUID] = []
    po_qtys: list[Decimal] = []
    po_dates: list[date] = []
    for i in range(items):
        for _ in range(supplies_per_item):
            offset = rng.randint(0, buckets - 1)
            po_ids.append(uuid4())
            po_item_ids.append(item_ids[i])
            po_qtys.append(Decimal(rng.choice([10, 25, 50, 100])))
            po_dates.append(horizon_start + timedelta(days=offset))

    if po_ids:
        conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_ref, is_dirty, active)
            SELECT
                p.id, 'PurchaseOrderSupply', %s, p.item_id, %s,
                p.qty, 'EA', 'exact_date', p.dt, FALSE, TRUE
            FROM UNNEST(%s::uuid[], %s::uuid[], %s::numeric[], %s::date[]) AS p(id, item_id, qty, dt)
            """,
            (BASELINE_SCENARIO_ID, location_id, po_ids, po_item_ids, po_qtys, po_dates),
        )

    # 5a. item_planning_params — safety stock for ~half the items.
    # Mix of zero and non-zero safety_stock_qty exercises both branches of
    # ShortageDetector (stockout vs. below_safety_stock).
    ipp_item_ids: list[UUID] = []
    ipp_ss_qtys: list[Decimal] = []
    for i, item_id in enumerate(item_ids):
        if i % 2 == 0:  # half get safety stock
            ipp_item_ids.append(item_id)
            ipp_ss_qtys.append(Decimal(rng.choice([0, 20, 50, 100])))
    if ipp_item_ids:
        conn.execute(
            """
            INSERT INTO item_planning_params
                (item_id, location_id, safety_stock_qty, effective_from, effective_to)
            SELECT
                p.item_id, %s, p.ss, %s, '9999-12-31'::DATE
            FROM UNNEST(%s::uuid[], %s::numeric[]) AS p(item_id, ss)
            """,
            (location_id, horizon_start, ipp_item_ids, ipp_ss_qtys),
        )

    # 5b. Demand nodes — mix of point-in-time (time_ref only) and multi-day
    # spans (time_span_start/end). Spans are connected to EVERY PI bucket they
    # overlap; the kernel prorates daily_rate * overlap_days per bucket.
    dem_ids: list[UUID] = []
    dem_item_ids: list[UUID] = []
    dem_qtys: list[Decimal] = []
    dem_time_refs: list[date | None] = []
    dem_span_starts: list[date | None] = []
    dem_span_ends: list[date | None] = []
    dem_types: list[str] = []
    # Tracks the PI buckets each demand consumes (for edge creation below).
    dem_bucket_indices: list[list[int]] = []
    for i in range(items):
        for k in range(demands_per_item):
            dem_ids.append(uuid4())
            dem_item_ids.append(item_ids[i])
            dem_qtys.append(Decimal(rng.choice([5, 10, 20, 40, 80])))
            dem_types.append("ForecastDemand" if k % 2 == 0 else "CustomerOrderDemand")
            # 1/3 of demands are multi-day spans; 2/3 are point-in-time.
            if rng.random() < 0.33 and buckets > 5:
                span_len = rng.randint(2, 5)
                offset = rng.randint(0, buckets - span_len)
                ts = horizon_start + timedelta(days=offset)
                te = horizon_start + timedelta(days=offset + span_len)
                dem_time_refs.append(None)
                dem_span_starts.append(ts)
                dem_span_ends.append(te)
                # Buckets the demand overlaps (daily grain, contiguous)
                dem_bucket_indices.append(list(range(offset, offset + span_len)))
            else:
                offset = rng.randint(0, buckets - 1)
                dt = horizon_start + timedelta(days=offset)
                dem_time_refs.append(dt)
                dem_span_starts.append(None)
                dem_span_ends.append(None)
                dem_bucket_indices.append([offset])

    if dem_ids:
        conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_ref,
                 time_span_start, time_span_end, is_dirty, active)
            SELECT
                d.id, d.tp, %s, d.item_id, %s,
                d.qty, 'EA', 'exact_date', d.tr, d.ts, d.te, FALSE, TRUE
            FROM UNNEST(%s::uuid[], %s::text[], %s::uuid[], %s::numeric[],
                        %s::date[], %s::date[], %s::date[])
                 AS d(id, tp, item_id, qty, tr, ts, te)
            """,
            (BASELINE_SCENARIO_ID, location_id,
             dem_ids, dem_types, dem_item_ids, dem_qtys,
             dem_time_refs, dem_span_starts, dem_span_ends),
        )

    # 6. Edges
    edge_ids: list[UUID] = []
    edge_types: list[str] = []
    edge_from: list[UUID] = []
    edge_to: list[UUID] = []
    # OH → bucket 0 (replenishes); feeds_forward chain
    for i in range(items):
        base = i * buckets
        edge_ids.append(uuid4())
        edge_types.append("replenishes")
        edge_from.append(oh_ids[i])
        edge_to.append(pi_ids[base])
        for b in range(1, buckets):
            edge_ids.append(uuid4())
            edge_types.append("feeds_forward")
            edge_from.append(pi_ids[base + b - 1])
            edge_to.append(pi_ids[base + b])

    # PO → PI bucket whose window contains po time_ref (replenishes)
    for po_id, po_item_id, po_date in zip(po_ids, po_item_ids, po_dates):
        item_idx = item_ids.index(po_item_id)
        bucket_idx = (po_date - horizon_start).days
        if 0 <= bucket_idx < buckets:
            edge_ids.append(uuid4())
            edge_types.append("replenishes")
            edge_from.append(po_id)
            edge_to.append(pi_ids[item_idx * buckets + bucket_idx])

    # Demand -> PI bucket(s). For point-in-time, one edge to the bucket
    # whose window contains time_ref. For multi-day spans, one edge per
    # overlapping daily bucket.
    item_id_to_idx = {iid: idx for idx, iid in enumerate(item_ids)}
    for dem_id, dem_item_id, bucket_indices in zip(dem_ids, dem_item_ids, dem_bucket_indices):
        item_idx = item_id_to_idx[dem_item_id]
        for b in bucket_indices:
            if 0 <= b < buckets:
                edge_ids.append(uuid4())
                edge_types.append("consumes")
                edge_from.append(dem_id)
                edge_to.append(pi_ids[item_idx * buckets + b])

    conn.execute(
        """
        INSERT INTO edges
            (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
        SELECT
            e.id, e.type, e.frm, e.dest, %s, TRUE
        FROM UNNEST(%s::uuid[], %s::text[], %s::uuid[], %s::uuid[]) AS e(id, type, frm, dest)
        """,
        (BASELINE_SCENARIO_ID, edge_ids, edge_types, edge_from, edge_to),
    )

    conn.commit()
    return {
        "items": items,
        "buckets": buckets,
        "pi_nodes": pi_node_count,
        "oh_nodes": items,
        "po_nodes": len(po_ids),
        "demand_nodes": len(dem_ids),
        "edges": len(edge_ids),
        "seed_seconds": round(time.perf_counter() - started, 2),
    }


def _run_python_propagation(conn: psycopg.Connection, calc_run_id: UUID, dirty: set[UUID]) -> float:
    """Run the production Python propagator over the dirty set. Returns wall_seconds."""
    from ootils_core.api.routers.events import _build_propagation_engine
    from ootils_core.models import CalcRun

    engine = _build_propagation_engine(conn)
    row = conn.execute(
        "SELECT * FROM calc_runs WHERE calc_run_id = %s",
        (calc_run_id,),
    ).fetchone()
    calc_run = CalcRun(
        calc_run_id=UUID(str(row["calc_run_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        triggered_by_event_ids=[UUID(str(e)) for e in (row.get("triggered_by_event_ids") or [])],
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
    # Mirror _finish_run's resolve_stale call (production behaviour).
    if engine._shortage_detector is not None:
        try:
            engine._shortage_detector.resolve_stale(
                scenario_id=BASELINE_SCENARIO_ID,
                calc_run_id=calc_run_id,
                db=conn,
            )
        except Exception:
            pass
    conn.commit()
    return time.perf_counter() - started


def _snapshot_pi_state(conn: psycopg.Connection) -> dict[UUID, dict]:
    """Return {node_id: {opening, inflows, outflows, closing, has_shortage, shortage_qty}} for every PI node."""
    rows = conn.execute(
        """
        SELECT node_id, opening_stock, inflows, outflows, closing_stock,
               has_shortage, shortage_qty
        FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND scenario_id = %s
          AND active = TRUE
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchall()
    return {UUID(str(r["node_id"])): r for r in rows}


def _reset_pi_state(conn: psycopg.Connection) -> None:
    """Clear computed fields on PI nodes and re-mark dirty for the next engine."""
    conn.execute(
        """
        UPDATE nodes
        SET opening_stock = NULL,
            inflows       = NULL,
            outflows      = NULL,
            closing_stock = NULL,
            has_shortage  = FALSE,
            shortage_qty  = 0,
            is_dirty      = TRUE,
            last_calc_run_id = NULL
        WHERE node_type = 'ProjectedInventory'
          AND scenario_id = %s
          AND active = TRUE
        """,
        (BASELINE_SCENARIO_ID,),
    )
    # Also wipe any dirty_nodes residual + complete the prior calc_run so a fresh
    # one can be started. Wipe shortages too — each engine writes them from scratch.
    conn.execute("DELETE FROM dirty_nodes WHERE scenario_id = %s", (BASELINE_SCENARIO_ID,))
    conn.execute("DELETE FROM shortages WHERE scenario_id = %s", (BASELINE_SCENARIO_ID,))
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE scenario_id = %s AND status = 'running'",
        (BASELINE_SCENARIO_ID,),
    )
    conn.commit()


def _snapshot_shortages(conn: psycopg.Connection) -> dict[UUID, dict]:
    """Return {pi_node_id: shortage_row} for every active shortage in scenario.
    Keyed by pi_node_id since (pi_node_id, calc_run_id) is unique per run."""
    rows = conn.execute(
        """
        SELECT pi_node_id, item_id, location_id, shortage_date,
               shortage_qty, severity_score, severity_class, status
        FROM shortages
        WHERE scenario_id = %s AND status = 'active'
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchall()
    return {UUID(str(r["pi_node_id"])): r for r in rows}


def _diff_shortages(py: dict[UUID, dict], sql: dict[UUID, dict]) -> dict:
    """Diff shortages by pi_node_id, comparing qty/score/class/date."""
    keys_py = set(py)
    keys_sql = set(sql)
    only_py = keys_py - keys_sql
    only_sql = keys_sql - keys_py
    common = keys_py & keys_sql

    TOL = Decimal("1e-12")
    mismatches: list[tuple[UUID, str, object, object]] = []
    fields_num = ("shortage_qty", "severity_score")
    fields_other = ("severity_class", "shortage_date")
    for nid in common:
        for f in fields_num:
            a_n = Decimal(str(py[nid][f]))
            b_n = Decimal(str(sql[nid][f]))
            if abs(a_n - b_n) > TOL:
                mismatches.append((nid, f, py[nid][f], sql[nid][f]))
        for f in fields_other:
            if py[nid][f] != sql[nid][f]:
                mismatches.append((nid, f, py[nid][f], sql[nid][f]))
        if len(mismatches) >= 10:
            break

    return {
        "shortages_python": len(py),
        "shortages_sql": len(sql),
        "missing_from_sql": len(only_py),
        "extra_in_sql": len(only_sql),
        "field_mismatches": len(mismatches),
        "sample_mismatches": mismatches,
    }


def _save_pi_nodes_snapshot(conn: psycopg.Connection) -> None:
    """Stash current PI fields into a temp table so we can restore between engines."""
    conn.execute("DROP TABLE IF EXISTS _pi_snapshot")
    conn.execute(
        """
        CREATE TEMP TABLE _pi_snapshot AS
        SELECT node_id, opening_stock, inflows, outflows, closing_stock,
               has_shortage, shortage_qty, is_dirty, last_calc_run_id
        FROM nodes
        WHERE node_type = 'ProjectedInventory' AND scenario_id = %s AND active = TRUE
        """,
        (BASELINE_SCENARIO_ID,),
    )
    conn.execute("DROP TABLE IF EXISTS _shortages_snapshot")
    conn.execute(
        "CREATE TEMP TABLE _shortages_snapshot AS SELECT * FROM shortages WHERE scenario_id = %s",
        (BASELINE_SCENARIO_ID,),
    )
    conn.commit()


def _restore_pi_nodes_snapshot(conn: psycopg.Connection) -> None:
    """Restore PI fields from the temp table built by _save_pi_nodes_snapshot."""
    conn.execute(
        """
        UPDATE nodes n
        SET opening_stock    = s.opening_stock,
            inflows          = s.inflows,
            outflows         = s.outflows,
            closing_stock    = s.closing_stock,
            has_shortage     = s.has_shortage,
            shortage_qty     = s.shortage_qty,
            is_dirty         = s.is_dirty,
            last_calc_run_id = s.last_calc_run_id
        FROM _pi_snapshot s
        WHERE n.node_id = s.node_id
        """,
    )
    conn.execute("DELETE FROM shortages WHERE scenario_id = %s", (BASELINE_SCENARIO_ID,))
    conn.execute(
        "INSERT INTO shortages SELECT * FROM _shortages_snapshot",
    )
    conn.commit()


def _mark_subset_dirty(
    conn: psycopg.Connection,
    pi_node_ids: list[UUID],
) -> UUID:
    """Reset the listed PI nodes' computed fields and mark them dirty for a new calc_run.

    Returns the new calc_run_id. Mirrors what the propagator would do given a
    cascade-expanded dirty set — without any actual change to upstream data.
    Both engines should produce identical results to a no-op recompute of
    those buckets (re-using the persisted closing_stock of bucket N-1 as seed).
    """
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from ootils_core.engine.orchestration.calc_run import CalcRunManager

    # Reset the subset's computed fields so neither engine reads stale state.
    conn.execute(
        """
        UPDATE nodes
        SET opening_stock    = NULL,
            inflows          = NULL,
            outflows         = NULL,
            closing_stock    = NULL,
            has_shortage     = FALSE,
            shortage_qty     = 0,
            is_dirty         = TRUE,
            last_calc_run_id = NULL
        WHERE node_id = ANY(%s) AND scenario_id = %s
        """,
        (pi_node_ids, BASELINE_SCENARIO_ID),
    )
    # Clean up any leftovers from prior runs that may interfere with the new one
    conn.execute("DELETE FROM dirty_nodes WHERE scenario_id = %s", (BASELINE_SCENARIO_ID,))
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE scenario_id = %s AND status = 'running'",
        (BASELINE_SCENARIO_ID,),
    )
    conn.commit()

    calc_mgr = CalcRunManager()
    calc_run = calc_mgr.start_calc_run(
        scenario_id=BASELINE_SCENARIO_ID, event_ids=[], db=conn
    )
    assert calc_run is not None
    dirty = DirtyFlagManager()
    dirty.mark_dirty(set(pi_node_ids), BASELINE_SCENARIO_ID, calc_run.calc_run_id, conn)
    dirty.flush_to_postgres(calc_run.calc_run_id, BASELINE_SCENARIO_ID, conn)
    conn.commit()
    return calc_run.calc_run_id


def _pick_incremental_subset(conn: psycopg.Connection, buckets: int) -> list[UUID]:
    """Pick a contiguous slice of buckets [10..buckets-1] of one series.

    Mimics what `expand_dirty_subgraph` produces when a change lands on the
    middle of a series: a contiguous downstream suffix is marked dirty.
    """
    if buckets < 12:
        # Not enough buckets to test incremental — return empty (caller skips)
        return []
    rows = conn.execute(
        """
        SELECT DISTINCT projection_series_id FROM nodes
        WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE
        ORDER BY projection_series_id LIMIT 1
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchall()
    if not rows:
        return []
    series_id = rows[0]["projection_series_id"]
    rows = conn.execute(
        """
        SELECT node_id FROM nodes
        WHERE projection_series_id = %s
          AND bucket_sequence >= 10
          AND scenario_id = %s
          AND active = TRUE
        ORDER BY bucket_sequence
        """,
        (series_id, BASELINE_SCENARIO_ID),
    ).fetchall()
    return [UUID(str(r["node_id"])) for r in rows]


def _build_sql_engine(conn: psycopg.Connection) -> SqlPropagationEngine:
    """Build a SqlPropagationEngine using the same wiring as _build_propagation_engine."""
    from ootils_core.engine.kernel.graph.store import GraphStore
    from ootils_core.engine.kernel.graph.traversal import GraphTraversal
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from ootils_core.engine.orchestration.calc_run import CalcRunManager
    from ootils_core.engine.kernel.calc.projection import ProjectionKernel
    from ootils_core.engine.kernel.explanation.builder import ExplanationBuilder
    from ootils_core.engine.kernel.shortage.detector import ShortageDetector

    store = GraphStore(conn)
    return SqlPropagationEngine(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        explanation_builder=ExplanationBuilder(),
        shortage_detector=ShortageDetector(),
    )


def _run_sql_propagation(conn: psycopg.Connection, calc_run_id: UUID) -> float:
    """Run SqlPropagationEngine._propagate + shortage_detector.resolve_stale.

    Mirrors what the Python harness does: skips _finish_run (which would also
    do scenario-level bookkeeping the harness doesn't need), but calls
    resolve_stale explicitly to match the production end-of-run contract.
    """
    from ootils_core.models import CalcRun

    sql_engine = _build_sql_engine(conn)
    row = conn.execute(
        "SELECT * FROM calc_runs WHERE calc_run_id = %s",
        (calc_run_id,),
    ).fetchone()
    calc_run = CalcRun(
        calc_run_id=UUID(str(row["calc_run_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        triggered_by_event_ids=[UUID(str(e)) for e in (row.get("triggered_by_event_ids") or [])],
        is_full_recompute=bool(row.get("is_full_recompute", False)),
        dirty_node_count=row.get("dirty_node_count"),
        nodes_recalculated=int(row.get("nodes_recalculated", 0)),
        nodes_unchanged=int(row.get("nodes_unchanged", 0)),
        status=row.get("status", "running"),
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        error_message=row.get("error_message"),
    )
    # Load the current dirty set from the persisted dirty_nodes table so the
    # engine sees the same set the production caller would pass in.
    dirty_rows = conn.execute(
        "SELECT node_id FROM dirty_nodes WHERE calc_run_id = %s AND scenario_id = %s",
        (calc_run_id, BASELINE_SCENARIO_ID),
    ).fetchall()
    dirty = {UUID(str(r["node_id"])) for r in dirty_rows}

    started = time.perf_counter()
    sql_engine._propagate(calc_run, dirty, conn)
    if sql_engine._shortage_detector is not None:
        sql_engine._shortage_detector.resolve_stale(
            scenario_id=BASELINE_SCENARIO_ID,
            calc_run_id=calc_run_id,
            db=conn,
        )
    conn.commit()
    return time.perf_counter() - started


def _diff_snapshots(py: dict[UUID, dict], sql: dict[UUID, dict]) -> dict:
    """Compare two PI snapshots field-by-field. Return summary + sample mismatches."""
    keys_py = set(py)
    keys_sql = set(sql)
    only_py = keys_py - keys_sql
    only_sql = keys_sql - keys_py
    common = keys_py & keys_sql

    # Tolerance accounts for the ~24-26th digit rounding difference between
    # Python's Decimal default context (28 sig digits, ROUND_HALF_EVEN) and
    # Postgres NUMERIC division/multiplication. 1e-12 is parts-per-trillion,
    # ~12 orders of magnitude below any business-meaningful inventory value.
    TOL = Decimal("1e-12")
    mismatches: list[tuple[UUID, str, object, object]] = []
    numeric_fields = ("opening_stock", "inflows", "outflows", "closing_stock", "shortage_qty")
    bool_fields = ("has_shortage",)
    for nid in common:
        for f in numeric_fields:
            a = py[nid][f]
            b = sql[nid][f]
            if a is None and b is None:
                continue
            if a is None or b is None:
                mismatches.append((nid, f, a, b))
                continue
            a_n = Decimal(str(a))
            b_n = Decimal(str(b))
            if abs(a_n - b_n) > TOL:
                mismatches.append((nid, f, a, b))
        for f in bool_fields:
            if py[nid][f] != sql[nid][f]:
                mismatches.append((nid, f, py[nid][f], sql[nid][f]))
        if len(mismatches) >= 10:
            break

    return {
        "nodes_python": len(py),
        "nodes_sql": len(sql),
        "missing_from_sql": len(only_py),
        "extra_in_sql": len(only_sql),
        "field_mismatches": len(mismatches),
        "sample_mismatches": mismatches,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--items", type=int, default=20)
    parser.add_argument("--buckets", type=int, default=30)
    parser.add_argument("--supplies-per-item", type=int, default=3,
                        help="Number of PO supplies per item (default: 3)")
    parser.add_argument("--demands-per-item", type=int, default=4,
                        help="Number of demand events per item (default: 4)")
    parser.add_argument("--seed", type=int, default=42,
                        help="RNG seed for reproducible scenarios")
    parser.add_argument("--dbname", default="ootils_test_bench")
    args = parser.parse_args()

    base_dsn = os.environ.get("DATABASE_URL")
    if not base_dsn:
        print("FATAL: set DATABASE_URL")
        return 2
    target_dsn = base_dsn.rsplit("/", 1)[0] + f"/{args.dbname}"
    os.environ["DATABASE_URL"] = target_dsn

    _admin_recreate_db(base_dsn, args.dbname)
    _apply_migrations(target_dsn)

    with psycopg.connect(target_dsn, row_factory=dict_row) as conn:
        seed_stats = _seed_rich(
            conn,
            items=args.items,
            buckets=args.buckets,
            supplies_per_item=args.supplies_per_item,
            demands_per_item=args.demands_per_item,
            seed=args.seed,
        )
        print(f"[seed] {seed_stats}")

        # ---- Phase 1: Python ----
        calc_run_py, dirty = _mark_all_pi_dirty(conn)
        py_wall = _run_python_propagation(conn, calc_run_py, dirty)
        snap_py = _snapshot_pi_state(conn)
        shortages_py = _snapshot_shortages(conn)
        print(f"[python] {len(snap_py)} PI nodes, {len(shortages_py)} shortages in {py_wall:.2f}s")

        # ---- Phase 2: reset + SQL ----
        _reset_pi_state(conn)
        calc_run_sql, _ = _mark_all_pi_dirty(conn)
        sql_wall = _run_sql_propagation(conn, calc_run_sql)
        snap_sql = _snapshot_pi_state(conn)
        shortages_sql = _snapshot_shortages(conn)
        print(f"[sql]    {len(snap_sql)} PI nodes, {len(shortages_sql)} shortages in {sql_wall:.2f}s")

        # ---- Phase 3: diff ----
        diff = _diff_snapshots(snap_py, snap_sql)
        sh_diff = _diff_shortages(shortages_py, shortages_sql)

        # ---- Phase 4: incremental parity (subset re-dirty) ----
        inc_pi_diff = None
        inc_sh_diff = None
        inc_py_wall = 0.0
        inc_sql_wall = 0.0
        subset = _pick_incremental_subset(conn, args.buckets)
        if subset:
            print(f"[incremental] testing on {len(subset)} buckets of one series")
            # Snapshot the current (= post-SQL-full) state so we can restore
            # between the two incremental runs.
            _save_pi_nodes_snapshot(conn)

            # Python incremental
            calc_run_inc_py = _mark_subset_dirty(conn, subset)
            inc_py_wall = _run_python_propagation(conn, calc_run_inc_py, set(subset))
            snap_py_inc = _snapshot_pi_state(conn)
            shortages_py_inc = _snapshot_shortages(conn)

            # Restore + SQL incremental
            _restore_pi_nodes_snapshot(conn)
            calc_run_inc_sql = _mark_subset_dirty(conn, subset)
            inc_sql_wall = _run_sql_propagation(conn, calc_run_inc_sql)
            snap_sql_inc = _snapshot_pi_state(conn)
            shortages_sql_inc = _snapshot_shortages(conn)

            inc_pi_diff = _diff_snapshots(snap_py_inc, snap_sql_inc)
            inc_sh_diff = _diff_shortages(shortages_py_inc, shortages_sql_inc)
            print(f"[python-inc] {inc_py_wall:.2f}s | [sql-inc] {inc_sql_wall:.2f}s")

    print()
    print("=" * 60)
    print("PARITY REPORT — PI nodes")
    print("=" * 60)
    print(f"  nodes_python              {diff['nodes_python']}")
    print(f"  nodes_sql                 {diff['nodes_sql']}")
    print(f"  missing_from_sql          {diff['missing_from_sql']}")
    print(f"  extra_in_sql              {diff['extra_in_sql']}")
    print(f"  field_mismatches          {diff['field_mismatches']}")
    if diff["sample_mismatches"]:
        print("  Sample mismatches:")
        for nid, field, py_val, sql_val in diff["sample_mismatches"]:
            print(f"    {nid}  {field:14s}  py={py_val!r:>12}  sql={sql_val!r:>12}")
    print()
    print("=" * 60)
    print("PARITY REPORT — shortages")
    print("=" * 60)
    print(f"  shortages_python          {sh_diff['shortages_python']}")
    print(f"  shortages_sql             {sh_diff['shortages_sql']}")
    print(f"  missing_from_sql          {sh_diff['missing_from_sql']}")
    print(f"  extra_in_sql              {sh_diff['extra_in_sql']}")
    print(f"  field_mismatches          {sh_diff['field_mismatches']}")
    if sh_diff["sample_mismatches"]:
        print("  Sample mismatches:")
        for nid, field, py_val, sql_val in sh_diff["sample_mismatches"]:
            print(f"    {nid}  {field:14s}  py={py_val!r:>20}  sql={sql_val!r:>20}")
    print()
    inc_ok = True
    if inc_pi_diff is not None and inc_sh_diff is not None:
        print()
        print("=" * 60)
        print("PARITY REPORT — incremental PI nodes")
        print("=" * 60)
        print(f"  nodes_python              {inc_pi_diff['nodes_python']}")
        print(f"  nodes_sql                 {inc_pi_diff['nodes_sql']}")
        print(f"  missing_from_sql          {inc_pi_diff['missing_from_sql']}")
        print(f"  extra_in_sql              {inc_pi_diff['extra_in_sql']}")
        print(f"  field_mismatches          {inc_pi_diff['field_mismatches']}")
        if inc_pi_diff["sample_mismatches"]:
            print("  Sample mismatches:")
            for nid, field, py_val, sql_val in inc_pi_diff["sample_mismatches"]:
                print(f"    {nid}  {field:14s}  py={py_val!r:>12}  sql={sql_val!r:>12}")
        print()
        print("=" * 60)
        print("PARITY REPORT — incremental shortages")
        print("=" * 60)
        print(f"  shortages_python          {inc_sh_diff['shortages_python']}")
        print(f"  shortages_sql             {inc_sh_diff['shortages_sql']}")
        print(f"  missing_from_sql          {inc_sh_diff['missing_from_sql']}")
        print(f"  extra_in_sql              {inc_sh_diff['extra_in_sql']}")
        print(f"  field_mismatches          {inc_sh_diff['field_mismatches']}")
        if inc_sh_diff["sample_mismatches"]:
            print("  Sample mismatches:")
            for nid, field, py_val, sql_val in inc_sh_diff["sample_mismatches"]:
                print(f"    {nid}  {field:14s}  py={py_val!r:>20}  sql={sql_val!r:>20}")
        inc_ok = (
            inc_pi_diff["missing_from_sql"] == 0
            and inc_pi_diff["extra_in_sql"] == 0
            and inc_pi_diff["field_mismatches"] == 0
            and inc_sh_diff["missing_from_sql"] == 0
            and inc_sh_diff["extra_in_sql"] == 0
            and inc_sh_diff["field_mismatches"] == 0
        )
    print()
    full_ok = (
        diff["missing_from_sql"] == 0
        and diff["extra_in_sql"] == 0
        and diff["field_mismatches"] == 0
        and sh_diff["missing_from_sql"] == 0
        and sh_diff["extra_in_sql"] == 0
        and sh_diff["field_mismatches"] == 0
    )
    ok = full_ok and inc_ok
    print(f"PARITY (full):        {'OK' if full_ok else 'FAILED'}")
    print(f"PARITY (incremental): {'OK' if inc_ok else 'FAILED'}")
    print(f"SPEEDUP (full):       SQL is {py_wall / max(sql_wall, 1e-9):.2f}x faster ({py_wall:.2f}s -> {sql_wall:.2f}s)")
    if inc_py_wall > 0:
        print(f"SPEEDUP (incremental):SQL is {inc_py_wall / max(inc_sql_wall, 1e-9):.2f}x faster ({inc_py_wall:.2f}s -> {inc_sql_wall:.2f}s)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
