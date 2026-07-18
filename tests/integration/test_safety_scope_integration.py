"""
tests/integration/test_safety_scope_integration.py — DB-backed tests of the
`OOTILS_SAFETY_SCOPE` shortage-DETECTION policy (ADR-021 amendment, DESC-1
PR-C, pilot arbitration 2026-07-18 — `engine/kernel/shortage/policy.py`)
against a real PostgreSQL — no mocks (CLAUDE.md).

Business context: per-site `safety_stock_qty` values are dispatch/execution
artefacts; safety is judged at the NATIONAL level (risk-pooling cushion,
Truth B / `engine/mrp/loader.py` — untouched here). Under the arbitrated
default 'national', per-site detection only fires on a PHYSICAL stockout
(`closing_stock < -EPS`) — the `below_safety_stock` branch never does.
'per_site' preserves the historical behaviour byte-for-byte.

Covers, on BOTH engines (SqlPropagationEngine — SHORTAGES_SQL's
`%(safety_scope_national)s` CASE built by `shortage_params()` — and the
Python PropagationEngine — `resolve_safety_scope()` once per calc_run,
threaded to `detect_with_params(safety_scope=...)`):

  1. 'per_site' (explicit env pin): on-hand ABOVE zero but UNDER the per-site
     safety stock + weak demand → exactly one `below_safety_stock` row,
     business values pinned exactly (qty / $ severity / date).
  2. 'national' (the DEFAULT — env explicitly UNSET): the IDENTICAL seed
     yields ZERO `shortages` rows, in ANY status — while the PROJECTION
     stays computed and visible on the PI node (detection gating only,
     same doctrine as `is_stocking`, migration 081).
  3. A genuine physical stockout (demand > on-hand) is detected in BOTH
     scopes; the test runs BOTH engines on identical twin seeds and compares
     their rows business-column by business-column — the SQL/Python parity
     check on the safety_scope axis.

The Rust in-process engine needs no case of its own: both its shortage-
detection call sites run this same SHORTAGES_SQL string with the same
`shortage_params()` dict (see propagator_rust.py's module docstring).

ISOLATION (the committed-seed lesson, cf. test_is_stocking_integration):
every committed seed is neutralized by a finalizer registered BEFORE the
first commit — DEACTIVATION only (edges/nodes active=FALSE, shortages
resolved, items obsolete), never a DELETE. The module-scoped migrated_db
teardown drops the schema afterwards as the backstop.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
import pytest

from ootils_core.engine.kernel.calc.projection import ProjectionKernel
from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.engine.kernel.graph.traversal import GraphTraversal
from ootils_core.engine.kernel.shortage.detector import ShortageDetector
from ootils_core.engine.orchestration.calc_run import CalcRunManager
from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine

from .conftest import requires_db

pytestmark = requires_db

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

_ENV_VAR = "OOTILS_SAFETY_SCOPE"

# One OnHandSupply, one CustomerOrderDemand, one 1-day bucket-0 PI, one CURRENT
# item_planning_params row. The item is deliberately unpriced (no supplier_items,
# no standard_cost) and the bucket is 1 day, so severity_score == shortage_qty
# exactly (unit-cost proxy 1) — single-valued assertions both engines must hit
# bit-identically.
ON_HAND = Decimal("5")
SS_PER_SITE = Decimal("10")
# Weak demand: closing = 5 - 2 = 3 → ABOVE zero, UNDER the per-site safety.
WEAK_DEMAND = Decimal("2")
BELOW_SS_QTY = SS_PER_SITE - (ON_HAND - WEAK_DEMAND)  # 10 - 3 = 7
# Stockout demand: closing = 5 - 20 = -15 → genuine physical stockout.
STOCKOUT_DEMAND = Decimal("20")
STOCKOUT_QTY = STOCKOUT_DEMAND - ON_HAND  # 15

ENGINES = [
    pytest.param(SqlPropagationEngine, id="sql"),
    pytest.param(PropagationEngine, id="python"),
]


# ---------------------------------------------------------------------------
# Seed: one stocking location, on-hand + one CO consuming a 1-day bucket-0 PI
# ---------------------------------------------------------------------------


def _seed_case(conn, *, demand_qty: Decimal) -> dict:
    """A default (stocking) location carrying ON_HAND units of an unpriced item
    (OnHandSupply → bucket-0 PI via 'replenishes'), one CustomerOrderDemand of
    `demand_qty` due today ('consumes'), and a CURRENT item_planning_params row
    with safety_stock_qty=SS_PER_SITE. Projection: closing = ON_HAND - demand."""
    today = date.today()
    location_id, item_id, series_id = uuid4(), uuid4(), uuid4()
    pi_id, onhand_id, demand_id = uuid4(), uuid4(), uuid4()

    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"SAFETY-SCOPE-LOC-{uuid4()}"),
    )
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"SAFETY-SCOPE-ITEM-{uuid4()}"),
    )
    conn.execute(
        """
        INSERT INTO item_planning_params
            (item_id, location_id, effective_from, effective_to, safety_stock_qty)
        VALUES (%s, %s, %s, NULL, %s)
        """,
        (item_id, location_id, today, SS_PER_SITE),
    )
    conn.execute(
        """
        INSERT INTO projection_series
            (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (series_id, item_id, location_id, BASELINE, today, today + timedelta(days=1)),
    )
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             time_grain, time_span_start, time_span_end,
             projection_series_id, bucket_sequence, is_dirty, active)
        VALUES (%s, 'ProjectedInventory', %s, %s, %s,
                'day', %s, %s, %s, 0, TRUE, TRUE)
        """,
        (pi_id, BASELINE, item_id, location_id,
         today, today + timedelta(days=1), series_id),
    )
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             quantity, qty_uom, time_grain, time_ref, is_dirty, active)
        VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'EA',
                'exact_date', %s, FALSE, TRUE)
        """,
        (onhand_id, BASELINE, item_id, location_id, ON_HAND, today),
    )
    conn.execute(
        """
        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
        VALUES (%s, 'replenishes', %s, %s, %s, TRUE)
        """,
        (uuid4(), onhand_id, pi_id, BASELINE),
    )
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             quantity, qty_uom, time_grain, time_ref, is_dirty, active)
        VALUES (%s, 'CustomerOrderDemand', %s, %s, %s, %s, 'EA',
                'exact_date', %s, FALSE, TRUE)
        """,
        (demand_id, BASELINE, item_id, location_id, demand_qty, today),
    )
    conn.execute(
        """
        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
        VALUES (%s, 'consumes', %s, %s, %s, TRUE)
        """,
        (uuid4(), demand_id, pi_id, BASELINE),
    )
    return {
        "location_id": location_id,
        "item_id": item_id,
        "pi_id": pi_id,
        "today": today,
    }


def _register_neutralizer(request, dsn: str, seed: dict) -> None:
    """Register BEFORE the first commit (isolation lesson). Deactivation only,
    scoped strictly to this test's own uuids — never a DELETE. Runs on its own
    autocommit connection so it cannot depend on the test's `conn` state. The
    item_planning_params row is closed (effective_to) rather than deleted —
    SCD2 discipline, and an obsolete random-uuid item can no longer match."""
    location_id, item_id = seed["location_id"], seed["item_id"]

    def _sweep():
        try:
            with psycopg.connect(dsn, autocommit=True) as c:
                c.execute(
                    """
                    UPDATE edges SET active = FALSE
                    WHERE scenario_id = %s AND to_node_id IN (
                        SELECT node_id FROM nodes WHERE location_id = %s)
                    """,
                    (BASELINE, location_id),
                )
                c.execute(
                    "UPDATE shortages SET status = 'resolved' "
                    "WHERE location_id = %s AND status = 'active'",
                    (location_id,),
                )
                c.execute(
                    "UPDATE nodes SET active = FALSE, is_dirty = FALSE "
                    "WHERE location_id = %s",
                    (location_id,),
                )
                c.execute(
                    "UPDATE items SET status = 'obsolete' WHERE item_id = %s",
                    (item_id,),
                )
                c.execute(
                    "UPDATE item_planning_params SET effective_to = CURRENT_DATE + 1 "
                    "WHERE item_id = %s AND effective_to IS NULL",
                    (item_id,),
                )
        except Exception:
            pass  # best-effort — migrated_db teardown is the backstop

    request.addfinalizer(_sweep)


# ---------------------------------------------------------------------------
# Engine drivers + readers (same shape as test_is_stocking_integration)
# ---------------------------------------------------------------------------


def _build_engine(cls, conn):
    store = GraphStore(conn)
    return cls(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        shortage_detector=ShortageDetector(),
    )


def _propagate_seed(engine, conn, seed: dict) -> UUID:
    """Start a calc_run on baseline, mark the seed's PI dirty, run the
    engine's _propagate, resolve stale shortages, close the run. Returns the
    calc_run_id. The env-resolved safety_scope is read INSIDE _propagate
    (Python: resolve_safety_scope(); SQL: shortage_params()) — exactly the
    production resolution point under test."""
    pi_id = seed["pi_id"]
    calc_mgr = CalcRunManager()
    calc_run = calc_mgr.start_calc_run(scenario_id=BASELINE, event_ids=[], db=conn)
    assert calc_run is not None, "could not acquire advisory lock for baseline"
    dirty = DirtyFlagManager()
    dirty.mark_dirty({pi_id}, BASELINE, calc_run.calc_run_id, conn)
    dirty.flush_to_postgres(calc_run.calc_run_id, BASELINE, conn)

    engine._propagate(calc_run, {pi_id}, conn)
    engine._shortage_detector.resolve_stale(
        scenario_id=BASELINE, calc_run_id=calc_run.calc_run_id, db=conn
    )
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE calc_run_id = %s",
        (calc_run.calc_run_id,),
    )
    conn.execute("SELECT pg_advisory_unlock(hashtext(%s)::bigint)", (str(BASELINE),))
    conn.commit()
    return calc_run.calc_run_id


def _pi_row(conn, pi_id: UUID) -> dict:
    row = conn.execute(
        """
        SELECT opening_stock, inflows, outflows, closing_stock,
               has_shortage, shortage_qty
        FROM nodes WHERE node_id = %s
        """,
        (pi_id,),
    ).fetchone()
    assert row is not None
    return row


def _shortage_rows(conn, location_id: UUID) -> list[dict]:
    """EVERY shortages row for this location, in ANY status — the national
    invariant is 'not one row was ever written', not 'no active row'."""
    return conn.execute(
        """
        SELECT status, severity_class, shortage_qty, severity_score, shortage_date
        FROM shortages WHERE location_id = %s
        """,
        (location_id,),
    ).fetchall()


# ===========================================================================
# 1. 'per_site' (explicit): below-safety detected — the historical behaviour
# ===========================================================================


@pytest.mark.parametrize("engine_cls", ENGINES)
def test_per_site_scope_detects_below_safety_stock(
    conn, migrated_db, request, engine_cls, monkeypatch
):
    """closing = 3 (above zero, under SS_PER_SITE=10) under an EXPLICIT
    per_site pin → exactly one below_safety_stock row, qty = SS - closing = 7,
    severity == qty (1-day bucket, unpriced item)."""
    monkeypatch.setenv(_ENV_VAR, "per_site")
    seed = _seed_case(conn, demand_qty=WEAK_DEMAND)
    _register_neutralizer(request, migrated_db, seed)
    conn.commit()

    engine = _build_engine(engine_cls, conn)
    _propagate_seed(engine, conn, seed)

    pi = _pi_row(conn, seed["pi_id"])
    assert Decimal(str(pi["closing_stock"])) == ON_HAND - WEAK_DEMAND  # 3
    assert pi["has_shortage"] is False, "positive closing is never a projection stockout"

    rows = _shortage_rows(conn, seed["location_id"])
    assert len(rows) == 1, (
        f"{engine_cls.__name__}: expected exactly one below_safety_stock row, got {rows!r}"
    )
    row = rows[0]
    assert row["status"] == "active"
    assert row["severity_class"] == "below_safety_stock"
    assert Decimal(str(row["shortage_qty"])) == BELOW_SS_QTY
    # 1-day bucket x unpriced item (unit-cost proxy 1) → severity == qty.
    assert Decimal(str(row["severity_score"])) == BELOW_SS_QTY
    assert row["shortage_date"] == seed["today"]


# ===========================================================================
# 2. 'national' (the DEFAULT — env unset): the IDENTICAL seed yields ZERO rows
# ===========================================================================


@pytest.mark.parametrize("engine_cls", ENGINES)
def test_national_default_yields_zero_rows_without_physical_stockout(
    conn, migrated_db, request, engine_cls, monkeypatch
):
    """Env explicitly UNSET → the pilot's arbitrated DEFAULT ('national')
    resolves: same seed as case 1, but no physical stockout → not one
    `shortages` row, in any status. The PROJECTION itself stays computed and
    visible on the PI node — the policy gates DETECTION only."""
    monkeypatch.delenv(_ENV_VAR, raising=False)
    seed = _seed_case(conn, demand_qty=WEAK_DEMAND)
    _register_neutralizer(request, migrated_db, seed)
    conn.commit()

    engine = _build_engine(engine_cls, conn)
    _propagate_seed(engine, conn, seed)

    pi = _pi_row(conn, seed["pi_id"])
    # Projection computed and identical to the per_site case — only the
    # shortages materialization differs between the two scopes.
    assert Decimal(str(pi["closing_stock"])) == ON_HAND - WEAK_DEMAND  # 3
    assert Decimal(str(pi["outflows"])) == WEAK_DEMAND
    assert Decimal(str(pi["opening_stock"])) == ON_HAND

    assert _shortage_rows(conn, seed["location_id"]) == [], (
        f"{engine_cls.__name__}: 'national' scope (default) must not materialize "
        "a below_safety_stock row — per-site safety is not a detection threshold"
    )


# ===========================================================================
# 3. Physical stockout: detected in BOTH scopes — SQL/Python parity
# ===========================================================================


@pytest.mark.parametrize(
    "scope_env",
    [
        pytest.param(None, id="national-default"),
        pytest.param("per_site", id="per_site"),
    ],
)
def test_physical_stockout_detected_in_both_scopes_sql_python_parity(
    conn, migrated_db, request, monkeypatch, scope_env
):
    """demand 20 > on-hand 5 → closing -15, a genuine physical stockout: the
    stockout branch is untouched by the policy in EITHER scope. Twin identical
    seeds, one per engine, business values pinned exactly AND compared
    column-by-column across engines — the parity check on this axis."""
    if scope_env is None:
        monkeypatch.delenv(_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(_ENV_VAR, scope_env)

    seed_sql = _seed_case(conn, demand_qty=STOCKOUT_DEMAND)
    seed_py = _seed_case(conn, demand_qty=STOCKOUT_DEMAND)
    _register_neutralizer(request, migrated_db, seed_sql)
    _register_neutralizer(request, migrated_db, seed_py)
    conn.commit()

    rows_by_engine: dict[str, dict] = {}
    for engine_cls, seed in (
        (SqlPropagationEngine, seed_sql),
        (PropagationEngine, seed_py),
    ):
        engine = _build_engine(engine_cls, conn)
        _propagate_seed(engine, conn, seed)

        pi = _pi_row(conn, seed["pi_id"])
        assert Decimal(str(pi["closing_stock"])) == -(STOCKOUT_DEMAND - ON_HAND)

        # Read (and pin) THIS engine's row immediately: the second engine's
        # resolve_stale legitimately retires the first engine's active row
        # (same scenario, later calc_run) — capture before moving on.
        rows = _shortage_rows(conn, seed["location_id"])
        assert len(rows) == 1, (
            f"{engine_cls.__name__} [{scope_env or 'national-default'}]: expected "
            f"exactly one stockout row, got {rows!r}"
        )
        row = rows[0]
        assert row["status"] == "active"
        assert row["severity_class"] == "stockout"
        assert Decimal(str(row["shortage_qty"])) == STOCKOUT_QTY
        # 1-day bucket x unpriced item (unit-cost proxy 1) → severity == qty.
        assert Decimal(str(row["severity_score"])) == STOCKOUT_QTY
        assert row["shortage_date"] == seed["today"]
        rows_by_engine[engine_cls.__name__] = row

    # Parity: both engines produced the SAME business columns.
    sql_row = rows_by_engine["SqlPropagationEngine"]
    py_row = rows_by_engine["PropagationEngine"]
    for col in ("severity_class", "shortage_qty", "severity_score", "shortage_date"):
        assert sql_row[col] == py_row[col], (
            f"SQL/Python parity broke on '{col}' under scope "
            f"{scope_env or 'national-default'}: {sql_row[col]!r} != {py_row[col]!r}"
        )
