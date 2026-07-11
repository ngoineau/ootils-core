"""
tests/integration/test_propagation_plan_stability_integration.py — plan-stability
regression guard for the 2026-07 re-bench finding (PERF2).

The measured failure mode: DirtyFlagManager.flush_to_postgres bulk-INSERTs the
dirty set and PROPAGATE_SQL reads it straight back IN THE SAME TRANSACTION.
With stale dirty_nodes statistics the planner estimated rows=1 on the freshly
inserted batch, picked a Nested Loop Left Join, and re-executed the
inflows/outflows GroupAggregate PER ROW (EXPLAIN ANALYZE: loops=2000 x ~1 ms
-> 43 nodes/s, ~200x). The fix is the `ANALYZE dirty_nodes` at the end of
flush_to_postgres (the causal site, common to every consumer of the table).

These tests assert PLAN properties, never durations:
  (a) after a non-empty flush, pg_class.reltuples for dirty_nodes is ~N —
      the observable proof that flush ran ANALYZE over the new rows
      (ANALYZE samples the flushing transaction's own uncommitted inserts);
  (b) EXPLAIN (FORMAT JSON) of PROPAGATE_SQL with the real bound parameters
      estimates the dirty_nodes scan / dirty_pi CTE at order-of-N rows
      (wide x10 tolerance), never the pathological rows=1;
  (c) no Nested Loop node in that plan has, on its INNER side, an Aggregate
      fed by the dirty_pi CTE without a materializing barrier in between —
      i.e. the healthy shape where inflows_agg/outflows_agg are computed ONCE
      (hashed / materialized), never re-executed per outer row.

Seeding mirrors test_param_overlay_propagation_integration.py (direct SQL
seed, committed, so the planner and ANALYZE see the graph), scaled to
N_SERIES x BUCKETS_PER_SERIES ProjectedInventory buckets with a handful of
replenishes/consumes edges so the inflows/outflows CTEs join real rows.
nodes/edges/projection_series are ANALYZEd explicitly at seed time: the only
stale-stats variable under test is dirty_nodes itself (in production the big
tables are autoanalyzed long before a run; dirty_nodes is bulk-loaded right
before PROPAGATE_SQL fires — that is exactly the race the fix closes).

The flush + EXPLAIN in each test run inside the test's own (rolled back)
transaction — the same visibility contract as the production propagator.
No mocks — CLAUDE.md.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.engine.orchestration.propagator_sql import PROPAGATE_SQL

from .conftest import requires_db

pytestmark = requires_db

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

# ~2k dirty PI nodes — same order of magnitude as the VM re-bench (2000).
N_SERIES = 40
BUCKETS_PER_SERIES = 50
N_DIRTY = N_SERIES * BUCKETS_PER_SERIES

# "Order of N, wide x10 tolerance": an estimate below N/10 means the planner
# did not see the flushed batch (the rows=1 pathology estimates 1).
MIN_EXPECTED_EST = N_DIRTY / 10

HORIZON_START = date(2026, 1, 1)

# Plan nodes whose rescan does NOT re-execute their subtree (they cache /
# materialize their input): an Aggregate sitting below one of these on a
# nested loop's inner side is computed once, which is the healthy shape.
_RESCAN_BARRIERS = {"Materialize", "Memoize", "Sort", "Hash"}


# ---------------------------------------------------------------------------
# Seed: N_SERIES projection series x BUCKETS_PER_SERIES day-grain PI buckets,
# plus per series 1 OnHandSupply (replenishes bucket 0), 1 PurchaseOrderSupply
# (replenishes bucket 10) and 1 ForecastDemand (consumes bucket 20) so the
# inflows_agg / outflows_agg CTEs have real edges to join.
# ---------------------------------------------------------------------------


def _seed_graph(conn) -> list[UUID]:
    location_id = conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s) RETURNING location_id",
        (uuid4(), f"plan-stability-loc-{uuid4()}"),
    ).fetchone()["location_id"]

    item_rows = [(uuid4(), f"plan-stability-item-{i}-{uuid4()}") for i in range(N_SERIES)]
    series_rows = [
        (uuid4(), item_id, location_id, BASELINE,
         HORIZON_START, HORIZON_START + timedelta(days=BUCKETS_PER_SERIES))
        for item_id, _name in item_rows
    ]

    pi_ids: list[UUID] = []
    pi_rows = []
    supply_demand_rows = []
    edge_rows = []
    for (series_id, item_id, _loc, _scen, _hs, _he) in series_rows:
        bucket_ids = []
        for k in range(BUCKETS_PER_SERIES):
            node_id = uuid4()
            bucket_ids.append(node_id)
            pi_rows.append((
                node_id, BASELINE, item_id, location_id,
                HORIZON_START + timedelta(days=k),
                HORIZON_START + timedelta(days=k + 1),
                series_id, k,
            ))
        pi_ids.extend(bucket_ids)

        oh_id, po_id, fd_id = uuid4(), uuid4(), uuid4()
        supply_demand_rows.extend([
            (oh_id, "OnHandSupply", BASELINE, item_id, location_id,
             100, HORIZON_START),
            (po_id, "PurchaseOrderSupply", BASELINE, item_id, location_id,
             50, HORIZON_START + timedelta(days=10)),
            (fd_id, "ForecastDemand", BASELINE, item_id, location_id,
             5, HORIZON_START + timedelta(days=20)),
        ])
        edge_rows.extend([
            (uuid4(), "replenishes", oh_id, bucket_ids[0], BASELINE),
            (uuid4(), "replenishes", po_id, bucket_ids[10], BASELINE),
            (uuid4(), "consumes", fd_id, bucket_ids[20], BASELINE),
        ])

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO items (item_id, name) VALUES (%s, %s)",
            item_rows,
        )
        cur.executemany(
            """
            INSERT INTO projection_series
                (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            series_rows,
        )
        cur.executemany(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                time_grain, time_span_start, time_span_end,
                projection_series_id, bucket_sequence,
                opening_stock, inflows, outflows, closing_stock,
                has_shortage, shortage_qty, active
            ) VALUES (
                %s, 'ProjectedInventory', %s, %s, %s,
                'day', %s, %s,
                %s, %s,
                0, 0, 0, 0,
                FALSE, 0, TRUE
            )
            """,
            pi_rows,
        )
        cur.executemany(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                quantity, time_grain, time_ref, active
            ) VALUES (%s, %s, %s, %s, %s, %s, 'exact_date', %s, TRUE)
            """,
            supply_demand_rows,
        )
        cur.executemany(
            """
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
            VALUES (%s, %s, %s, %s, %s, TRUE)
            """,
            edge_rows,
        )

    return pi_ids


@pytest.fixture(scope="module")
def plan_graph(migrated_db) -> list[UUID]:
    """Committed seeded graph, with fresh stats on everything EXCEPT
    dirty_nodes — isolating the variable the fix targets."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        pi_ids = _seed_graph(c)
        c.commit()
        c.execute("ANALYZE nodes")
        c.execute("ANALYZE edges")
        c.execute("ANALYZE projection_series")
        c.commit()
    return pi_ids


# ---------------------------------------------------------------------------
# Drivers
# ---------------------------------------------------------------------------


def _flush_dirty(conn, node_ids: list[UUID]) -> UUID:
    """Mark the whole PI set dirty and flush it through the REAL
    DirtyFlagManager.flush_to_postgres (INSERT + ANALYZE), inside the test's
    open transaction — the production visibility contract (PROPAGATE_SQL runs
    before the propagator's commit). Returns the calc_run_id."""
    calc_run_id = conn.execute(
        """
        INSERT INTO calc_runs (calc_run_id, scenario_id, status, started_at)
        VALUES (%s, %s, 'running', now())
        RETURNING calc_run_id
        """,
        (uuid4(), BASELINE),
    ).fetchone()["calc_run_id"]

    mgr = DirtyFlagManager()
    mgr.mark_dirty(set(node_ids), BASELINE, calc_run_id, conn)
    mgr.flush_to_postgres(calc_run_id, BASELINE, conn)
    return calc_run_id


def _dirty_nodes_reltuples(conn) -> float:
    return conn.execute(
        "SELECT reltuples FROM pg_class WHERE relname = 'dirty_nodes' AND relkind = 'r'"
    ).fetchone()["reltuples"]


def _explain_propagate(conn, calc_run_id: UUID) -> dict:
    """EXPLAIN (FORMAT JSON) of the real PROPAGATE_SQL with real bound params.
    Plain EXPLAIN (no ANALYZE option): plans the UPDATE without executing it."""
    row = conn.execute(
        "EXPLAIN (FORMAT JSON) " + PROPAGATE_SQL,
        {"scenario_id": BASELINE, "calc_run_id": calc_run_id},
    ).fetchone()
    doc = row["QUERY PLAN"]
    if isinstance(doc, str):
        doc = json.loads(doc)
    return doc[0]["Plan"]


# ---------------------------------------------------------------------------
# Plan-tree helpers
# ---------------------------------------------------------------------------


def _iter_plan_nodes(plan: dict):
    yield plan
    for child in plan.get("Plans") or []:
        yield from _iter_plan_nodes(child)


def _contains_dirty_pi_scan(plan: dict) -> bool:
    return any(
        n.get("Node Type") == "CTE Scan" and n.get("CTE Name") == "dirty_pi"
        for n in _iter_plan_nodes(plan)
    )


def _rescanned_aggregate_over_dirty_pi(plan: dict) -> bool:
    """True if this subtree — as the INNER side of a nested loop — would
    re-execute an Aggregate fed by the dirty_pi CTE on every outer row.

    Descent stops at materializing nodes (their subtree runs once, then gets
    rescanned from the cache — the healthy shape) and skips InitPlans (run
    once per statement). In EXPLAIN JSON every aggregate strategy
    (GroupAggregate/HashAggregate/plain) is Node Type "Aggregate"."""
    if plan.get("Node Type") in _RESCAN_BARRIERS:
        return False
    if plan.get("Node Type") == "Aggregate" and _contains_dirty_pi_scan(plan):
        return True
    return any(
        _rescanned_aggregate_over_dirty_pi(child)
        for child in plan.get("Plans") or []
        if child.get("Parent Relationship") != "InitPlan"
    )


def _find_per_row_aggregate_rescans(plan: dict) -> list[dict]:
    """The pathological shape: a Nested Loop whose Inner subtree re-executes
    an Aggregate over the dirty_pi CTE per outer row."""
    offenders = []
    for node in _iter_plan_nodes(plan):
        if node.get("Node Type") != "Nested Loop":
            continue
        for child in node.get("Plans") or []:
            if child.get("Parent Relationship") != "Inner":
                continue
            if _rescanned_aggregate_over_dirty_pi(child):
                offenders.append(node)
    return offenders


def _dump(plan: dict, limit: int = 6000) -> str:
    text = json.dumps(plan, indent=1)
    return text if len(text) <= limit else text[:limit] + "\n... [truncated]"


# ===========================================================================
# (a) flush_to_postgres ANALYZEs the freshly inserted batch
# ===========================================================================


def test_flush_analyzes_dirty_nodes_stats_cover_the_batch(conn, plan_graph):
    """After a non-empty flush, pg_class.reltuples for dirty_nodes is ~N.
    A never-analyzed table reads -1 (or 0 when analyzed empty) — only an
    ANALYZE run AFTER the INSERT, inside the flushing transaction, can see
    the N uncommitted rows and land in this window."""
    before = _dirty_nodes_reltuples(conn)

    _flush_dirty(conn, plan_graph)

    after = _dirty_nodes_reltuples(conn)
    assert N_DIRTY * 0.5 <= after <= N_DIRTY * 2, (
        f"dirty_nodes reltuples={after} not ~{N_DIRTY} after flush "
        f"(before={before}) — flush_to_postgres did not ANALYZE the batch"
    )


# ===========================================================================
# (b) PROPAGATE_SQL planner estimates see the dirty set's cardinality
# ===========================================================================


def test_propagate_sql_estimates_dirty_set_at_order_of_n(conn, plan_graph):
    """The dirty_nodes scan and the dirty_pi CTE must be estimated at
    order-of-N rows (x10 tolerance). The regression estimated rows=1, which
    is what licensed the per-row nested-loop plan."""
    calc_run_id = _flush_dirty(conn, plan_graph)
    plan = _explain_propagate(conn, calc_run_id)
    nodes = list(_iter_plan_nodes(plan))

    dn_scans = [n for n in nodes if n.get("Relation Name") == "dirty_nodes"]
    assert dn_scans, f"no dirty_nodes scan found in plan:\n{_dump(plan)}"
    dn_est = max(n.get("Plan Rows", 0) for n in dn_scans)
    assert dn_est > 1, f"dirty_nodes scan estimated at {dn_est} row(s):\n{_dump(plan)}"
    assert dn_est >= MIN_EXPECTED_EST, (
        f"dirty_nodes scan estimated at {dn_est} rows, expected >= "
        f"{MIN_EXPECTED_EST} (~{N_DIRTY} flushed) — stale stats regression:\n"
        f"{_dump(plan)}"
    )

    # dirty_pi is referenced by 4 downstream CTEs -> always a materialized
    # CTE subplan in the plan tree ("Subplan Name": "CTE dirty_pi").
    cte_subplans = [n for n in nodes if n.get("Subplan Name") == "CTE dirty_pi"]
    assert cte_subplans, f"no 'CTE dirty_pi' subplan found in plan:\n{_dump(plan)}"
    cte_est = cte_subplans[0].get("Plan Rows", 0)
    assert cte_est > 1, f"dirty_pi CTE estimated at {cte_est} row(s):\n{_dump(plan)}"
    assert cte_est >= MIN_EXPECTED_EST, (
        f"dirty_pi CTE estimated at {cte_est} rows, expected >= "
        f"{MIN_EXPECTED_EST} (~{N_DIRTY} dirty PIs):\n{_dump(plan)}"
    )


# ===========================================================================
# (c) no per-row re-execution of the inflows/outflows aggregates
# ===========================================================================


def test_propagate_sql_never_rescans_dirty_pi_aggregates_per_row(conn, plan_graph):
    """The healthy plan computes inflows_agg/outflows_agg ONCE (hashed or
    materialized) and joins them back. The pathological plan put the
    GroupAggregate on the inner side of a Nested Loop with no materializing
    barrier — re-executed per outer row (loops=2000 on the VM re-bench)."""
    calc_run_id = _flush_dirty(conn, plan_graph)
    plan = _explain_propagate(conn, calc_run_id)

    offenders = _find_per_row_aggregate_rescans(plan)
    assert not offenders, (
        f"{len(offenders)} Nested Loop node(s) re-execute an Aggregate over "
        f"the dirty_pi CTE per outer row (the 43 nodes/s pathology). "
        f"First offender:\n{_dump(offenders[0])}"
    )
