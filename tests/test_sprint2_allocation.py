"""
test_sprint2_allocation.py — Sprint 2 allocation engine tests.

Sections:
  1. AllocationResult dataclass
  2. AllocationEngine.allocate — greedy priority ordering
  3. Partial allocation (insufficient stock)
  4. Unallocated demand (zero stock)
  5. Determinism: same input → same output
  6. AllocationResult counters correctness
  7. pegged_to edge created with correct quantity
  8. Integration tests (require DATABASE_URL env var)
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.allocation.engine import AllocationEngine
from ootils_core.models import AllocationResult, Edge, Node


# ===========================================================================
# Helpers
# ===========================================================================

_ZERO = Decimal("0")


def _demand(
    qty: Decimal,
    node_type: str = "CustomerOrderDemand",
    time_ref: date | None = None,
    node_id: UUID | None = None,
    scenario_id: UUID | None = None,
) -> Node:
    return Node(
        node_id=node_id or uuid4(),
        node_type=node_type,
        scenario_id=scenario_id or uuid4(),
        quantity=qty,
        time_ref=time_ref or date(2026, 4, 10),
        active=True,
    )


def _pi_node(
    closing_stock: Decimal,
    node_id: UUID | None = None,
    scenario_id: UUID | None = None,
) -> Node:
    return Node(
        node_id=node_id or uuid4(),
        node_type="ProjectedInventory",
        scenario_id=scenario_id or uuid4(),
        closing_stock=closing_stock,
        active=True,
    )


def _consumes_edge(
    from_id: UUID,
    to_id: UUID,
    scenario_id: UUID,
    priority: int = 0,
) -> Edge:
    return Edge(
        edge_id=uuid4(),
        edge_type="consumes",
        from_node_id=from_id,
        to_node_id=to_id,
        scenario_id=scenario_id,
        priority=priority,
        weight_ratio=Decimal("1.0"),
    )


def _make_store_mock(
    demand_nodes: list[Node],
    pi_nodes: dict[UUID, Node],
    consumes_map: dict[UUID, list[Edge]],
) -> MagicMock:
    """
    Build a GraphStore mock wired up for a typical allocation scenario.

    demand_nodes   — returned by get_demand_nodes()
    pi_nodes       — keyed by node_id, returned by get_node()
    consumes_map   — demand_node_id → list[Edge] returned by get_edges_from()
    """
    # Track closing stock mutations so get_node reflects deductions.
    closing_stocks: dict[UUID, Decimal] = {
        nid: (n.closing_stock if n.closing_stock is not None else _ZERO)
        for nid, n in pi_nodes.items()
    }

    store = MagicMock()

    store.get_demand_nodes.return_value = demand_nodes

    def _get_edges_from(node_id, scenario_id, edge_type=None):
        if edge_type == "consumes":
            return consumes_map.get(node_id, [])
        return []

    store.get_edges_from.side_effect = _get_edges_from

    def _get_node(node_id, scenario_id, for_update=False):
        node = pi_nodes.get(node_id)
        if node is None:
            return None
        # Return a copy with updated closing_stock
        import copy
        n = copy.deepcopy(node)
        n.closing_stock = closing_stocks[node_id]
        return n

    store.get_node.side_effect = _get_node

    def _upsert_edge(edge):
        # Always "created" for simplicity
        return edge, True

    store.upsert_edge.side_effect = _upsert_edge

    def _update_closing_stock(node_id, scenario_id, new_stock):
        closing_stocks[node_id] = new_stock

    store.update_node_closing_stock.side_effect = _update_closing_stock

    return store


# ===========================================================================
# 1. AllocationResult dataclass
# ===========================================================================


class TestAllocationResultDataclass:
    """Verify the dataclass exists and carries the expected fields."""

    def test_instantiation(self):
        scenario_id = uuid4()
        result = AllocationResult(
            scenario_id=scenario_id,
            demands_total=5,
            demands_fully_allocated=3,
            demands_partially_allocated=1,
            demands_unallocated=1,
            total_qty_demanded=Decimal("500"),
            total_qty_allocated=Decimal("400"),
            edges_created=3,
            edges_updated=1,
            run_at=datetime.now(timezone.utc),
        )
        assert result.scenario_id == scenario_id
        assert result.demands_total == 5
        assert result.demands_fully_allocated == 3
        assert result.demands_partially_allocated == 1
        assert result.demands_unallocated == 1
        assert result.total_qty_demanded == Decimal("500")
        assert result.total_qty_allocated == Decimal("400")
        assert result.edges_created == 3
        assert result.edges_updated == 1


# ===========================================================================
# 2. Greedy priority ordering
# ===========================================================================


class TestGreedyPriorityOrdering:
    """High-priority demand (lower int) is served first, starving lower priority."""

    def _run(self, stock: Decimal, qty_p1: Decimal, qty_p2: Decimal):
        scenario_id = uuid4()
        pi_id = uuid4()

        # Demand P1 arrives before P2 by time_ref (high priority = served first)
        d_p1 = _demand(qty_p1, time_ref=date(2026, 4, 1), scenario_id=scenario_id)
        d_p2 = _demand(qty_p2, time_ref=date(2026, 4, 5), scenario_id=scenario_id)
        d_p1.scenario_id = scenario_id
        d_p2.scenario_id = scenario_id

        pi = _pi_node(stock, node_id=pi_id, scenario_id=scenario_id)

        store = _make_store_mock(
            demand_nodes=[d_p1, d_p2],  # already priority-ordered
            pi_nodes={pi_id: pi},
            consumes_map={
                d_p1.node_id: [_consumes_edge(d_p1.node_id, pi_id, scenario_id)],
                d_p2.node_id: [_consumes_edge(d_p2.node_id, pi_id, scenario_id)],
            },
        )

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            result = engine.allocate(scenario_id, db)

        return result, store

    def test_high_priority_fully_allocated_when_enough_stock(self):
        result, store = self._run(
            stock=Decimal("300"),
            qty_p1=Decimal("100"),
            qty_p2=Decimal("100"),
        )
        assert result.demands_fully_allocated == 2
        assert result.demands_unallocated == 0
        assert result.total_qty_allocated == Decimal("200")

    def test_high_priority_served_first_starves_lower(self):
        """P1 gets 100, P2 gets 50 (only 50 left)."""
        result, store = self._run(
            stock=Decimal("150"),
            qty_p1=Decimal("100"),
            qty_p2=Decimal("100"),
        )
        assert result.demands_fully_allocated == 1
        assert result.demands_partially_allocated == 1
        assert result.demands_unallocated == 0
        assert result.total_qty_allocated == Decimal("150")

    def test_high_priority_served_first_leaves_zero_for_lower(self):
        """P1 consumes all stock; P2 unallocated."""
        result, store = self._run(
            stock=Decimal("100"),
            qty_p1=Decimal("100"),
            qty_p2=Decimal("100"),
        )
        assert result.demands_fully_allocated == 1
        assert result.demands_unallocated == 1
        assert result.total_qty_allocated == Decimal("100")


# ===========================================================================
# 3. Partial allocation
# ===========================================================================


class TestPartialAllocation:
    """Demand is partially allocated when stock < demanded quantity."""

    def test_partial_allocation_counters(self):
        scenario_id = uuid4()
        pi_id = uuid4()

        demand = _demand(Decimal("200"), scenario_id=scenario_id)
        pi = _pi_node(Decimal("80"), node_id=pi_id, scenario_id=scenario_id)

        store = _make_store_mock(
            demand_nodes=[demand],
            pi_nodes={pi_id: pi},
            consumes_map={
                demand.node_id: [_consumes_edge(demand.node_id, pi_id, scenario_id)]
            },
        )

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            result = engine.allocate(scenario_id, db)

        assert result.demands_total == 1
        assert result.demands_fully_allocated == 0
        assert result.demands_partially_allocated == 1
        assert result.demands_unallocated == 0
        assert result.total_qty_demanded == Decimal("200")
        assert result.total_qty_allocated == Decimal("80")

    def test_partial_allocation_pegged_to_qty(self):
        """The pegged_to edge weight_ratio equals the allocated quantity."""
        scenario_id = uuid4()
        pi_id = uuid4()

        demand = _demand(Decimal("200"), scenario_id=scenario_id)
        pi = _pi_node(Decimal("80"), node_id=pi_id, scenario_id=scenario_id)

        upserted_edges: list[Edge] = []

        store = _make_store_mock(
            demand_nodes=[demand],
            pi_nodes={pi_id: pi},
            consumes_map={
                demand.node_id: [_consumes_edge(demand.node_id, pi_id, scenario_id)]
            },
        )

        def capture_upsert(edge):
            upserted_edges.append(edge)
            return edge, True

        store.upsert_edge.side_effect = capture_upsert

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            engine.allocate(scenario_id, db)

        assert len(upserted_edges) == 1
        assert upserted_edges[0].edge_type == "pegged_to"
        assert upserted_edges[0].weight_ratio == Decimal("80")
        assert upserted_edges[0].from_node_id == demand.node_id
        assert upserted_edges[0].to_node_id == pi_id


# ===========================================================================
# 4. Zero stock — demand unallocated
# ===========================================================================


class TestZeroStock:
    """When closing_stock = 0, the demand receives no allocation."""

    def test_unallocated_when_zero_stock(self):
        scenario_id = uuid4()
        pi_id = uuid4()

        demand = _demand(Decimal("100"), scenario_id=scenario_id)
        pi = _pi_node(Decimal("0"), node_id=pi_id, scenario_id=scenario_id)

        store = _make_store_mock(
            demand_nodes=[demand],
            pi_nodes={pi_id: pi},
            consumes_map={
                demand.node_id: [_consumes_edge(demand.node_id, pi_id, scenario_id)]
            },
        )

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            result = engine.allocate(scenario_id, db)

        assert result.demands_unallocated == 1
        assert result.demands_fully_allocated == 0
        assert result.demands_partially_allocated == 0
        assert result.total_qty_allocated == _ZERO
        # No pegged_to edge should be created
        store.upsert_edge.assert_not_called()

    def test_unallocated_when_no_consumes_edges(self):
        """Demand without any consumes edge is unallocated."""
        scenario_id = uuid4()
        demand = _demand(Decimal("50"), scenario_id=scenario_id)

        store = _make_store_mock(
            demand_nodes=[demand],
            pi_nodes={},
            consumes_map={},  # no edges
        )

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            result = engine.allocate(scenario_id, db)

        assert result.demands_unallocated == 1
        assert result.total_qty_allocated == _ZERO


# ===========================================================================
# 5. Determinism
# ===========================================================================


class TestDeterminism:
    """Same input → same allocation result across multiple runs."""

    def _build_scenario(self):
        scenario_id = uuid4()
        pi_id = uuid4()

        d1 = _demand(Decimal("100"), time_ref=date(2026, 4, 1), scenario_id=scenario_id)
        d2 = _demand(Decimal("200"), time_ref=date(2026, 4, 5), scenario_id=scenario_id)
        d3 = _demand(Decimal("150"), time_ref=date(2026, 4, 8), scenario_id=scenario_id)

        pi = _pi_node(Decimal("350"), node_id=pi_id, scenario_id=scenario_id)

        consumes_map = {
            d1.node_id: [_consumes_edge(d1.node_id, pi_id, scenario_id)],
            d2.node_id: [_consumes_edge(d2.node_id, pi_id, scenario_id)],
            d3.node_id: [_consumes_edge(d3.node_id, pi_id, scenario_id)],
        }

        return scenario_id, [d1, d2, d3], {pi_id: pi}, consumes_map

    def _run_once(self, scenario_id, demands, pi_nodes, consumes_map):
        store = _make_store_mock(demands, pi_nodes, consumes_map)
        engine = AllocationEngine()
        db = MagicMock()
        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            return engine.allocate(scenario_id, db)

    def test_same_result_two_runs(self):
        """Two independent runs on the same input produce identical counters."""
        scenario_id, demands, pi_nodes, consumes_map = self._build_scenario()

        r1 = self._run_once(scenario_id, demands, pi_nodes, consumes_map)
        r2 = self._run_once(scenario_id, demands, pi_nodes, consumes_map)

        assert r1.demands_total == r2.demands_total
        assert r1.demands_fully_allocated == r2.demands_fully_allocated
        assert r1.demands_partially_allocated == r2.demands_partially_allocated
        assert r1.demands_unallocated == r2.demands_unallocated
        assert r1.total_qty_demanded == r2.total_qty_demanded
        assert r1.total_qty_allocated == r2.total_qty_allocated
        assert r1.edges_created == r2.edges_created

    def test_deterministic_counters_known_values(self):
        """
        Known scenario: 3 demands, 350 units available.
        d1=100, d2=200, d3=150 → d1 fully (100), d2 fully (200), d3 partial (50).
        """
        scenario_id, demands, pi_nodes, consumes_map = self._build_scenario()
        result = self._run_once(scenario_id, demands, pi_nodes, consumes_map)

        assert result.demands_fully_allocated == 2
        assert result.demands_partially_allocated == 1
        assert result.demands_unallocated == 0
        assert result.total_qty_demanded == Decimal("450")
        assert result.total_qty_allocated == Decimal("350")


# ===========================================================================
# 6. AllocationResult counters correctness
# ===========================================================================


class TestAllocationResultCounters:
    """Verify all AllocationResult counters are computed correctly."""

    def test_all_counters_mixed_scenario(self):
        """
        3 demands: one fully allocated, one partial, one unallocated.
        """
        scenario_id = uuid4()
        pi_full_id = uuid4()
        pi_partial_id = uuid4()
        pi_empty_id = uuid4()

        d_full = _demand(Decimal("100"), time_ref=date(2026, 4, 1), scenario_id=scenario_id)
        d_partial = _demand(Decimal("100"), time_ref=date(2026, 4, 2), scenario_id=scenario_id)
        d_none = _demand(Decimal("100"), time_ref=date(2026, 4, 3), scenario_id=scenario_id)

        pi_full = _pi_node(Decimal("100"), node_id=pi_full_id, scenario_id=scenario_id)
        pi_partial = _pi_node(Decimal("60"), node_id=pi_partial_id, scenario_id=scenario_id)
        pi_empty = _pi_node(Decimal("0"), node_id=pi_empty_id, scenario_id=scenario_id)

        store = _make_store_mock(
            demand_nodes=[d_full, d_partial, d_none],
            pi_nodes={
                pi_full_id: pi_full,
                pi_partial_id: pi_partial,
                pi_empty_id: pi_empty,
            },
            consumes_map={
                d_full.node_id: [_consumes_edge(d_full.node_id, pi_full_id, scenario_id)],
                d_partial.node_id: [_consumes_edge(d_partial.node_id, pi_partial_id, scenario_id)],
                d_none.node_id: [_consumes_edge(d_none.node_id, pi_empty_id, scenario_id)],
            },
        )

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            result = engine.allocate(scenario_id, db)

        assert result.demands_total == 3
        assert result.demands_fully_allocated == 1
        assert result.demands_partially_allocated == 1
        assert result.demands_unallocated == 1
        assert result.total_qty_demanded == Decimal("300")
        assert result.total_qty_allocated == Decimal("160")
        assert result.edges_created == 2  # full + partial
        assert result.edges_updated == 0

    def test_edges_updated_counter(self):
        """When upsert_edge returns created=False, edges_updated increments."""
        scenario_id = uuid4()
        pi_id = uuid4()

        demand = _demand(Decimal("50"), scenario_id=scenario_id)
        pi = _pi_node(Decimal("50"), node_id=pi_id, scenario_id=scenario_id)

        store = _make_store_mock(
            demand_nodes=[demand],
            pi_nodes={pi_id: pi},
            consumes_map={
                demand.node_id: [_consumes_edge(demand.node_id, pi_id, scenario_id)]
            },
        )
        # Simulate edge already exists → not created
        store.upsert_edge.side_effect = lambda edge: (edge, False)

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            result = engine.allocate(scenario_id, db)

        assert result.edges_created == 0
        assert result.edges_updated == 1


# ===========================================================================
# 7. pegged_to edge — correct content
# ===========================================================================


class TestPeggedToEdge:
    """Verify edge_type, direction, and weight_ratio on created pegged_to edges."""

    def test_pegged_to_edge_direction(self):
        """from_node_id = demand, to_node_id = supply (PI node)."""
        scenario_id = uuid4()
        pi_id = uuid4()

        demand = _demand(Decimal("75"), scenario_id=scenario_id)
        pi = _pi_node(Decimal("100"), node_id=pi_id, scenario_id=scenario_id)

        captured: list[Edge] = []

        store = _make_store_mock(
            demand_nodes=[demand],
            pi_nodes={pi_id: pi},
            consumes_map={
                demand.node_id: [_consumes_edge(demand.node_id, pi_id, scenario_id)]
            },
        )
        store.upsert_edge.side_effect = lambda e: (captured.append(e) or (e, True))

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            engine.allocate(scenario_id, db)

        assert len(captured) == 1
        edge = captured[0]
        assert edge.edge_type == "pegged_to"
        assert edge.from_node_id == demand.node_id
        assert edge.to_node_id == pi_id
        assert edge.scenario_id == scenario_id

    def test_pegged_to_qty_equals_allocated(self):
        """weight_ratio on pegged_to edge equals the allocated quantity."""
        scenario_id = uuid4()
        pi_id = uuid4()

        demand = _demand(Decimal("200"), scenario_id=scenario_id)
        pi = _pi_node(Decimal("120"), node_id=pi_id, scenario_id=scenario_id)  # less than demand

        captured: list[Edge] = []

        store = _make_store_mock(
            demand_nodes=[demand],
            pi_nodes={pi_id: pi},
            consumes_map={
                demand.node_id: [_consumes_edge(demand.node_id, pi_id, scenario_id)]
            },
        )
        store.upsert_edge.side_effect = lambda e: (captured.append(e) or (e, True))

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            engine.allocate(scenario_id, db)

        assert len(captured) == 1
        assert captured[0].weight_ratio == Decimal("120")  # min(200, 120)

    def test_pegged_to_full_allocation_qty(self):
        """When demand < supply, pegged_to qty = demand quantity."""
        scenario_id = uuid4()
        pi_id = uuid4()

        demand = _demand(Decimal("50"), scenario_id=scenario_id)
        pi = _pi_node(Decimal("200"), node_id=pi_id, scenario_id=scenario_id)

        captured: list[Edge] = []

        store = _make_store_mock(
            demand_nodes=[demand],
            pi_nodes={pi_id: pi},
            consumes_map={
                demand.node_id: [_consumes_edge(demand.node_id, pi_id, scenario_id)]
            },
        )
        store.upsert_edge.side_effect = lambda e: (captured.append(e) or (e, True))

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            engine.allocate(scenario_id, db)

        assert len(captured) == 1
        assert captured[0].weight_ratio == Decimal("50")

    def test_multiple_pi_nodes_per_demand(self):
        """Demand can be split across multiple PI nodes (multiple consumes edges)."""
        scenario_id = uuid4()
        pi1_id = uuid4()
        pi2_id = uuid4()

        demand = _demand(Decimal("150"), scenario_id=scenario_id)
        pi1 = _pi_node(Decimal("80"), node_id=pi1_id, scenario_id=scenario_id)
        pi2 = _pi_node(Decimal("100"), node_id=pi2_id, scenario_id=scenario_id)

        captured: list[Edge] = []

        store = _make_store_mock(
            demand_nodes=[demand],
            pi_nodes={pi1_id: pi1, pi2_id: pi2},
            consumes_map={
                demand.node_id: [
                    _consumes_edge(demand.node_id, pi1_id, scenario_id, priority=0),
                    _consumes_edge(demand.node_id, pi2_id, scenario_id, priority=1),
                ]
            },
        )
        store.upsert_edge.side_effect = lambda e: (captured.append(e) or (e, True))

        engine = AllocationEngine()
        db = MagicMock()

        with patch(
            "ootils_core.engine.kernel.allocation.engine.GraphStore",
            return_value=store,
        ):
            result = engine.allocate(scenario_id, db)

        assert result.demands_fully_allocated == 1
        assert result.total_qty_allocated == Decimal("150")
        assert len(captured) == 2

        qtys = {e.to_node_id: e.weight_ratio for e in captured}
        assert qtys[pi1_id] == Decimal("80")
        assert qtys[pi2_id] == Decimal("70")  # 150 - 80


# ===========================================================================
# 8. Integration tests (require DATABASE_URL)
# ===========================================================================


SKIP_INTEGRATION = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="DATABASE_URL not set — skipping integration tests",
)


@SKIP_INTEGRATION
class TestAllocationIntegration:
    """
    Full round-trip integration tests against a real PostgreSQL instance.
    These tests create real nodes/edges and verify the allocation engine
    produces correct pegged_to edges.
    """

    @pytest.fixture()
    def db(self):
        import psycopg
        from psycopg.rows import dict_row

        conn = psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)
        yield conn
        conn.rollback()
        conn.close()

    def test_integration_basic_allocation(self, db):
        """Insert demand + PI node, run allocation, verify pegged_to edge."""
        from ootils_core.models import Scenario

        engine = AllocationEngine()
        scenario_id = Scenario.BASELINE_ID
        item_id = uuid4()
        location_id = uuid4()
        pi_node_id = uuid4()
        demand_node_id = uuid4()

        db.execute(
            "INSERT INTO items (item_id, name) VALUES (%s, %s)",
            (item_id, "Allocation Test Item"),
        )
        db.execute(
            "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
            (location_id, "Allocation Test Location"),
        )

        db.execute(
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
                pi_node_id,
                scenario_id,
                item_id,
                location_id,
                date(2026, 4, 10),
                date(2026, 4, 11),
                Decimal("100"),
                Decimal("100"),
            ),
        )
        db.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                quantity, qty_uom, time_grain, time_ref
            ) VALUES (
                %s, 'CustomerOrderDemand', %s, %s, %s,
                %s, 'EA', 'exact_date', %s
            )
            """,
            (
                demand_node_id,
                scenario_id,
                item_id,
                location_id,
                Decimal("40"),
                date(2026, 4, 10),
            ),
        )
        db.execute(
            """
            INSERT INTO edges (
                edge_id, edge_type, from_node_id, to_node_id, scenario_id,
                priority, weight_ratio
            ) VALUES (
                %s, 'consumes', %s, %s, %s,
                1, 1.0
            )
            """,
            (uuid4(), demand_node_id, pi_node_id, scenario_id),
        )
        db.commit()

        result = engine.allocate(scenario_id, db)
        db.commit()

        pegged_edges = db.execute(
            """
            SELECT * FROM edges
            WHERE scenario_id = %s
              AND edge_type = 'pegged_to'
              AND from_node_id = %s
              AND to_node_id = %s
              AND active = TRUE
            """,
            (scenario_id, demand_node_id, pi_node_id),
        ).fetchall()
        pi_row = db.execute(
            "SELECT closing_stock FROM nodes WHERE node_id = %s AND scenario_id = %s",
            (pi_node_id, scenario_id),
        ).fetchone()

        assert result.demands_total == 1
        assert result.demands_fully_allocated == 1
        assert result.demands_partially_allocated == 0
        assert result.demands_unallocated == 0
        assert result.total_qty_demanded == Decimal("40")
        assert result.total_qty_allocated == Decimal("40")
        assert result.edges_created == 1
        assert result.edges_updated == 0

        assert len(pegged_edges) == 1
        assert Decimal(str(pegged_edges[0]["weight_ratio"])) == Decimal("40")
        assert Decimal(str(pi_row["closing_stock"])) == Decimal("60")

        db.execute("DELETE FROM edges WHERE scenario_id = %s AND from_node_id = %s", (scenario_id, demand_node_id))
        db.execute("DELETE FROM nodes WHERE node_id IN (%s, %s)", (demand_node_id, pi_node_id))
        db.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
        db.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
        db.commit()
