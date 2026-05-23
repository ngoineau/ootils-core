"""
Integration tests for ootils_core.engine.kernel.allocation.engine
against a real PostgreSQL database.

Ported from tests/test_allocation_engine.py (which previously relied on
MagicMock/patch for GraphStore). The "no mocks" rule (CLAUDE.md) means
every branch is exercised by inserting real demand/PI nodes and edges,
running ``AllocationEngine.allocate`` (or its helpers), and reading
back the resulting pegged_to edges and updated closing_stock.

Each test creates unique items/locations/nodes/edges and cleans up at
the end. The function-scoped ``conn`` fixture also rolls back
uncommitted changes, but we use ``commit()`` inside tests so that
GraphStore's row-level locking works against persisted rows.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

from ootils_core.engine.kernel.allocation.engine import (
    AllocationEngine,
    _ZERO,
)
from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.models import Scenario

from .conftest import requires_db

pytestmark = requires_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_item_and_location(conn) -> tuple[UUID, UUID]:
    item_id = uuid4()
    location_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"Alloc Test Item {item_id}"),
    )
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"Alloc Test Loc {location_id}"),
    )
    return item_id, location_id


def _insert_demand_node(
    conn,
    *,
    scenario_id: UUID,
    item_id: UUID,
    location_id: UUID,
    quantity: Decimal | None,
    time_ref: date | None,
    node_type: str = "CustomerOrderDemand",
) -> UUID:
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            quantity, qty_uom, time_grain, time_ref
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, 'EA', 'exact_date', %s
        )
        """,
        (node_id, node_type, scenario_id, item_id, location_id, quantity, time_ref),
    )
    return node_id


def _insert_pi_node(
    conn,
    *,
    scenario_id: UUID,
    item_id: UUID,
    location_id: UUID,
    closing_stock: Decimal | None,
    time_span_start: date = date(2026, 4, 10),
    time_span_end: date = date(2026, 4, 11),
) -> UUID:
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_span_start, time_span_end,
            closing_stock, opening_stock, inflows, outflows
        ) VALUES (
            %s, 'ProjectedInventory', %s, %s, %s,
            'day', %s, %s,
            %s, %s, 0, 0
        )
        """,
        (
            node_id, scenario_id, item_id, location_id,
            time_span_start, time_span_end,
            closing_stock, closing_stock if closing_stock is not None else Decimal("0"),
        ),
    )
    return node_id


def _insert_consumes_edge(
    conn,
    *,
    from_node_id: UUID,
    to_node_id: UUID,
    scenario_id: UUID,
    priority: int = 0,
) -> UUID:
    edge_id = uuid4()
    conn.execute(
        """
        INSERT INTO edges (
            edge_id, edge_type, from_node_id, to_node_id, scenario_id,
            priority, weight_ratio
        ) VALUES (
            %s, 'consumes', %s, %s, %s,
            %s, 1.0
        )
        """,
        (edge_id, from_node_id, to_node_id, scenario_id, priority),
    )
    return edge_id


def _cleanup(conn, *, scenario_id: UUID, node_ids: list[UUID], item_id: UUID, location_id: UUID):
    """Delete every row written during the test so the DB stays clean."""
    if node_ids:
        conn.execute(
            "DELETE FROM edges WHERE scenario_id = %s AND (from_node_id = ANY(%s) OR to_node_id = ANY(%s))",
            (scenario_id, node_ids, node_ids),
        )
        conn.execute("DELETE FROM nodes WHERE node_id = ANY(%s)", (node_ids,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.commit()


# ===========================================================================
# get_demand_nodes
# ===========================================================================


class TestGetDemandNodes:
    def test_returns_sorted_by_priority_time_ref_node_id(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id, location_id = _insert_item_and_location(conn)

        # Insert three demands with controlled priority/time_ref. node_ids are
        # random uuids — we sort by them in the expected list to match the
        # engine's tertiary sort key.
        n_a = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("2"), time_ref=date(2025, 1, 10),
        )
        n_b = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("1"), time_ref=date(2025, 1, 10),
        )
        n_c = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("1"), time_ref=date(2025, 1, 5),
        )
        conn.commit()

        try:
            engine = AllocationEngine()
            result = engine.get_demand_nodes(scenario_id, conn)
            ids = [n.node_id for n in result if n.node_id in (n_a, n_b, n_c)]

            # Expected order: priority ASC, time_ref ASC, node_id ASC.
            # node_c has earliest time_ref (priority=1, Jan 5) → first.
            # Then between n_a (priority=2) and n_b (priority=1) on Jan 10,
            # n_b's priority is lower so it comes before n_a.
            assert ids == [n_c, n_b, n_a]
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[n_a, n_b, n_c],
                     item_id=item_id, location_id=location_id)

    def test_none_time_ref_sorts_last(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id, location_id = _insert_item_and_location(conn)

        n_a = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("1"), time_ref=None,
        )
        n_b = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("1"), time_ref=date(2025, 1, 1),
        )
        conn.commit()

        try:
            engine = AllocationEngine()
            result = engine.get_demand_nodes(scenario_id, conn)
            ids = [n.node_id for n in result if n.node_id in (n_a, n_b)]
            # n_b has real time_ref → first; n_a's None → sentinel last
            assert ids == [n_b, n_a]
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[n_a, n_b],
                     item_id=item_id, location_id=location_id)

    def test_empty_demands(self, conn):
        # Use a fresh scenario with no demand nodes.
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"empty-{scenario_id}"),
        )
        conn.commit()
        try:
            engine = AllocationEngine()
            result = engine.get_demand_nodes(scenario_id, conn)
            assert result == []
        finally:
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()


# ===========================================================================
# _allocate_demand
# ===========================================================================


class TestAllocateDemand:
    """
    Exercise every branch of _allocate_demand by setting up real rows.
    Each test seeds its own item/location, runs _allocate_demand, then
    cleans up.
    """

    def _setup(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id, location_id = _insert_item_and_location(conn)
        return scenario_id, item_id, location_id

    def test_zero_quantity_skips(self, conn):
        scenario_id, item_id, location_id = self._setup(conn)
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("0"), time_ref=date(2026, 4, 10),
        )
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            result = engine._allocate_demand(demand, scenario_id, conn, store)
            assert result == (_ZERO, 0, 0)
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id],
                     item_id=item_id, location_id=location_id)

    def test_negative_quantity_skips(self, conn):
        scenario_id, item_id, location_id = self._setup(conn)
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("-5"), time_ref=date(2026, 4, 10),
        )
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            result = engine._allocate_demand(demand, scenario_id, conn, store)
            assert result == (_ZERO, 0, 0)
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id],
                     item_id=item_id, location_id=location_id)

    def test_none_quantity_treated_as_zero_skips(self, conn):
        scenario_id, item_id, location_id = self._setup(conn)
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=None, time_ref=date(2026, 4, 10),
        )
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            result = engine._allocate_demand(demand, scenario_id, conn, store)
            assert result == (_ZERO, 0, 0)
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id],
                     item_id=item_id, location_id=location_id)

    def test_no_consumes_edges_returns_zero(self, conn):
        scenario_id, item_id, location_id = self._setup(conn)
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("50"), time_ref=date(2026, 4, 10),
        )
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            result = engine._allocate_demand(demand, scenario_id, conn, store)
            assert result == (_ZERO, 0, 0)
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id],
                     item_id=item_id, location_id=location_id)

    def test_pi_node_zero_stock_skips(self, conn):
        scenario_id, item_id, location_id = self._setup(conn)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("0"),
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("50"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi_id,
                              scenario_id=scenario_id)
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            result = engine._allocate_demand(demand, scenario_id, conn, store)
            assert result == (_ZERO, 0, 0)
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi_id],
                     item_id=item_id, location_id=location_id)

    def test_pi_node_negative_stock_skips(self, conn):
        scenario_id, item_id, location_id = self._setup(conn)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("-10"),
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("50"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi_id,
                              scenario_id=scenario_id)
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            result = engine._allocate_demand(demand, scenario_id, conn, store)
            assert result == (_ZERO, 0, 0)
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi_id],
                     item_id=item_id, location_id=location_id)

    def test_pi_node_none_closing_stock_treated_as_zero(self, conn):
        scenario_id, item_id, location_id = self._setup(conn)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=None,
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("50"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi_id,
                              scenario_id=scenario_id)
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            result = engine._allocate_demand(demand, scenario_id, conn, store)
            assert result == (_ZERO, 0, 0)
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi_id],
                     item_id=item_id, location_id=location_id)

    def test_full_allocation_creates_edge(self, conn):
        scenario_id, item_id, location_id = self._setup(conn)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("100"),
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("30"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi_id,
                              scenario_id=scenario_id)
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            alloc, created, updated = engine._allocate_demand(
                demand, scenario_id, conn, store
            )
            assert alloc == Decimal("30")
            assert created == 1
            assert updated == 0
            # PI closing stock should now be 70
            pi_row = conn.execute(
                "SELECT closing_stock FROM nodes WHERE node_id = %s AND scenario_id = %s",
                (pi_id, scenario_id),
            ).fetchone()
            assert Decimal(str(pi_row["closing_stock"])) == Decimal("70")
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi_id],
                     item_id=item_id, location_id=location_id)

    def test_partial_allocation_across_multiple_pi_nodes(self, conn):
        """Demand=100, PI1 has 60, PI2 has 20 → allocated=80, shortage=20."""
        scenario_id, item_id, location_id = self._setup(conn)
        pi1 = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("60"),
            time_span_start=date(2026, 4, 10), time_span_end=date(2026, 4, 11),
        )
        pi2 = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("20"),
            time_span_start=date(2026, 4, 11), time_span_end=date(2026, 4, 12),
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("100"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi1,
                              scenario_id=scenario_id, priority=0)
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi2,
                              scenario_id=scenario_id, priority=1)
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            alloc, created, updated = engine._allocate_demand(
                demand, scenario_id, conn, store
            )
            assert alloc == Decimal("80")
            assert created == 2
            assert updated == 0
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi1, pi2],
                     item_id=item_id, location_id=location_id)

    def test_upsert_edge_updated_not_created(self, conn):
        """
        Running the allocator twice over the same demand/PI re-uses the
        existing pegged_to edge → updated counter increments.
        """
        scenario_id, item_id, location_id = self._setup(conn)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("50"),
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("10"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi_id,
                              scenario_id=scenario_id)
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)

            # First run creates the pegged_to edge.
            demand = store.get_node(demand_id, scenario_id)
            alloc1, created1, updated1 = engine._allocate_demand(
                demand, scenario_id, conn, store
            )
            conn.commit()
            assert created1 == 1 and updated1 == 0
            assert alloc1 == Decimal("10")

            # Reset PI stock back to 50 so the second run can allocate again
            # against the same edge → upsert returns created=False.
            conn.execute(
                "UPDATE nodes SET closing_stock = %s WHERE node_id = %s AND scenario_id = %s",
                (Decimal("50"), pi_id, scenario_id),
            )
            conn.commit()

            demand = store.get_node(demand_id, scenario_id)
            alloc2, created2, updated2 = engine._allocate_demand(
                demand, scenario_id, conn, store
            )
            conn.commit()
            assert alloc2 == Decimal("10")
            assert created2 == 0
            assert updated2 == 1
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi_id],
                     item_id=item_id, location_id=location_id)

    def test_demand_fully_satisfied_stops_early(self, conn):
        """When demand=10 and PI1=50, PI2 should never be looked at."""
        scenario_id, item_id, location_id = self._setup(conn)
        pi1 = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("50"),
            time_span_start=date(2026, 4, 10), time_span_end=date(2026, 4, 11),
        )
        pi2 = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("50"),
            time_span_start=date(2026, 4, 11), time_span_end=date(2026, 4, 12),
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("10"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi1,
                              scenario_id=scenario_id, priority=0)
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi2,
                              scenario_id=scenario_id, priority=1)
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            alloc, created, updated = engine._allocate_demand(
                demand, scenario_id, conn, store
            )
            assert alloc == Decimal("10")
            # pi1 closing_stock should be 40 (50 - 10); pi2 untouched at 50
            pi1_row = conn.execute(
                "SELECT closing_stock FROM nodes WHERE node_id = %s AND scenario_id = %s",
                (pi1, scenario_id),
            ).fetchone()
            pi2_row = conn.execute(
                "SELECT closing_stock FROM nodes WHERE node_id = %s AND scenario_id = %s",
                (pi2, scenario_id),
            ).fetchone()
            assert Decimal(str(pi1_row["closing_stock"])) == Decimal("40")
            assert Decimal(str(pi2_row["closing_stock"])) == Decimal("50")
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi1, pi2],
                     item_id=item_id, location_id=location_id)

    def test_mixed_pi_nodes_some_empty_some_with_stock(self, conn):
        """First PI has zero stock; second PI has stock — second satisfies demand."""
        scenario_id, item_id, location_id = self._setup(conn)
        pi_empty = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("0"),
            time_span_start=date(2026, 4, 10), time_span_end=date(2026, 4, 11),
        )
        pi_full = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("100"),
            time_span_start=date(2026, 4, 11), time_span_end=date(2026, 4, 12),
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("25"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi_empty,
                              scenario_id=scenario_id, priority=0)
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi_full,
                              scenario_id=scenario_id, priority=1)
        conn.commit()
        try:
            engine = AllocationEngine()
            store = GraphStore(conn)
            demand = store.get_node(demand_id, scenario_id)
            alloc, created, updated = engine._allocate_demand(
                demand, scenario_id, conn, store
            )
            assert alloc == Decimal("25")
            assert created == 1
            conn.commit()
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi_empty, pi_full],
                     item_id=item_id, location_id=location_id)


# ===========================================================================
# allocate (top-level)
# ===========================================================================


class TestAllocate:
    def test_empty_demands_returns_zero_result(self, conn):
        # Use a fresh scenario with no demand nodes.
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"alloc-empty-{scenario_id}"),
        )
        conn.commit()
        try:
            engine = AllocationEngine()
            result = engine.allocate(scenario_id, conn)
            conn.commit()

            assert result.scenario_id == scenario_id
            assert result.demands_total == 0
            assert result.demands_fully_allocated == 0
            assert result.demands_partially_allocated == 0
            assert result.demands_unallocated == 0
            assert result.total_qty_demanded == _ZERO
            assert result.total_qty_allocated == _ZERO
            assert result.edges_created == 0
            assert result.edges_updated == 0
            assert result.run_at is not None
            assert result.run_at.tzinfo is not None
        finally:
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_fully_allocated_demand(self, conn):
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"alloc-full-{scenario_id}"),
        )
        conn.commit()
        item_id, location_id = _insert_item_and_location(conn)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("100"),
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("50"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi_id,
                              scenario_id=scenario_id)
        conn.commit()
        try:
            engine = AllocationEngine()
            result = engine.allocate(scenario_id, conn)
            conn.commit()
            assert result.demands_total == 1
            assert result.demands_fully_allocated == 1
            assert result.demands_partially_allocated == 0
            assert result.demands_unallocated == 0
            assert result.total_qty_demanded == Decimal("50")
            assert result.total_qty_allocated == Decimal("50")
            assert result.edges_created == 1
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi_id],
                     item_id=item_id, location_id=location_id)
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_partially_allocated_demand(self, conn):
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"alloc-partial-{scenario_id}"),
        )
        conn.commit()
        item_id, location_id = _insert_item_and_location(conn)
        pi_id = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("40"),
        )
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("100"), time_ref=date(2026, 4, 10),
        )
        _insert_consumes_edge(conn, from_node_id=demand_id, to_node_id=pi_id,
                              scenario_id=scenario_id)
        conn.commit()
        try:
            engine = AllocationEngine()
            result = engine.allocate(scenario_id, conn)
            conn.commit()
            assert result.demands_partially_allocated == 1
            assert result.demands_fully_allocated == 0
            assert result.demands_unallocated == 0
            assert result.total_qty_demanded == Decimal("100")
            assert result.total_qty_allocated == Decimal("40")
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id, pi_id],
                     item_id=item_id, location_id=location_id)
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_unallocated_demand(self, conn):
        """Demand exists but has no consumes edges → unallocated counter."""
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"alloc-un-{scenario_id}"),
        )
        conn.commit()
        item_id, location_id = _insert_item_and_location(conn)
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("100"), time_ref=date(2026, 4, 10),
        )
        conn.commit()
        try:
            engine = AllocationEngine()
            result = engine.allocate(scenario_id, conn)
            conn.commit()
            assert result.demands_total == 1
            assert result.demands_unallocated == 1
            assert result.demands_fully_allocated == 0
            assert result.demands_partially_allocated == 0
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id],
                     item_id=item_id, location_id=location_id)
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_none_quantity_treated_as_zero_in_allocate(self, conn):
        """A demand with quantity=NULL is treated as zero — counts as fully allocated."""
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"alloc-none-{scenario_id}"),
        )
        conn.commit()
        item_id, location_id = _insert_item_and_location(conn)
        demand_id = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=None, time_ref=date(2026, 4, 10),
        )
        conn.commit()
        try:
            engine = AllocationEngine()
            result = engine.allocate(scenario_id, conn)
            conn.commit()
            assert result.demands_total == 1
            assert result.demands_fully_allocated == 1
            assert result.total_qty_demanded == _ZERO
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[demand_id],
                     item_id=item_id, location_id=location_id)
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_multiple_demands_mixed_outcomes(self, conn):
        """Mix of fully allocated, partially allocated, and unallocated demands."""
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"alloc-mix-{scenario_id}"),
        )
        conn.commit()
        item_id, location_id = _insert_item_and_location(conn)

        # PI 1: 50 units (will be fully consumed by d1=50)
        pi1 = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("50"),
            time_span_start=date(2026, 4, 10), time_span_end=date(2026, 4, 11),
        )
        # PI 2: 30 units (will partially serve d2=100 → alloc 30, short 70)
        pi2 = _insert_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("30"),
            time_span_start=date(2026, 4, 11), time_span_end=date(2026, 4, 12),
        )

        d1 = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("50"), time_ref=date(2026, 4, 10),
        )
        d2 = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("100"), time_ref=date(2026, 4, 11),
        )
        # d3: 80 units but no consumes edges → unallocated
        d3 = _insert_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            quantity=Decimal("80"), time_ref=date(2026, 4, 12),
        )
        _insert_consumes_edge(conn, from_node_id=d1, to_node_id=pi1,
                              scenario_id=scenario_id)
        _insert_consumes_edge(conn, from_node_id=d2, to_node_id=pi2,
                              scenario_id=scenario_id)
        conn.commit()
        try:
            engine = AllocationEngine()
            result = engine.allocate(scenario_id, conn)
            conn.commit()
            assert result.demands_total == 3
            assert result.demands_fully_allocated == 1
            assert result.demands_partially_allocated == 1
            assert result.demands_unallocated == 1
            assert result.total_qty_demanded == Decimal("230")
            assert result.total_qty_allocated == Decimal("80")
            assert result.edges_created == 2
            assert result.edges_updated == 0
        finally:
            _cleanup(conn, scenario_id=scenario_id, node_ids=[d1, d2, d3, pi1, pi2],
                     item_id=item_id, location_id=location_id)
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_run_at_and_scenario_id_propagated(self, conn):
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"alloc-runat-{scenario_id}"),
        )
        conn.commit()
        try:
            engine = AllocationEngine()
            result = engine.allocate(scenario_id, conn)
            conn.commit()
            assert result.scenario_id == scenario_id
            assert result.run_at is not None
            assert result.run_at.tzinfo is not None
        finally:
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()
