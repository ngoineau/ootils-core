"""
Comprehensive tests for ootils_core.engine.kernel.allocation.engine

Covers:
- AllocationEngine.allocate (happy path, empty demands, partial/full/unallocated)
- AllocationEngine.get_demand_nodes (sorting, None time_ref sentinel)
- AllocationEngine._allocate_demand (all branches)
- _priority_key helper (None, valid int, non-int, TypeError)
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch, call
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.allocation.engine import (
    AllocationEngine,
    _priority_key,
    _ZERO,
    _SENTINEL_DATE,
    _DEMAND_TYPES,
)
from ootils_core.models import AllocationResult, Edge, Node


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_node(
    *,
    node_id: UUID | None = None,
    node_type: str = "ForecastDemand",
    scenario_id: UUID | None = None,
    quantity: Decimal | None = Decimal("100"),
    time_ref: date | None = date(2025, 1, 15),
    closing_stock: Decimal | None = None,
) -> Node:
    return Node(
        node_id=node_id or uuid4(),
        node_type=node_type,
        scenario_id=scenario_id or uuid4(),
        quantity=quantity,
        time_ref=time_ref,
        closing_stock=closing_stock,
    )


def _make_edge(
    *,
    from_node_id: UUID | None = None,
    to_node_id: UUID | None = None,
    scenario_id: UUID | None = None,
    edge_type: str = "consumes",
    priority: int = 0,
    weight_ratio: Decimal = Decimal("1.0"),
) -> Edge:
    return Edge(
        edge_id=uuid4(),
        edge_type=edge_type,
        from_node_id=from_node_id or uuid4(),
        to_node_id=to_node_id or uuid4(),
        scenario_id=scenario_id or uuid4(),
        priority=priority,
        weight_ratio=weight_ratio,
    )


# ---------------------------------------------------------------------------
# _priority_key tests
# ---------------------------------------------------------------------------

class TestPriorityKey:
    def test_none_quantity_returns_zero(self):
        node = _make_node(quantity=None)
        assert _priority_key(node) == 0

    def test_valid_integer_quantity(self):
        node = _make_node(quantity=Decimal("3"))
        assert _priority_key(node) == 3

    def test_zero_quantity(self):
        node = _make_node(quantity=Decimal("0"))
        assert _priority_key(node) == 0

    def test_negative_quantity(self):
        node = _make_node(quantity=Decimal("-1"))
        assert _priority_key(node) == -1

    def test_non_numeric_quantity_falls_back_to_zero(self):
        """If int() raises ValueError, fallback to 0."""
        node = _make_node(quantity=Decimal("0"))
        # Monkey-patch quantity to a string that can't convert
        node.quantity = "not_a_number"  # type: ignore[assignment]
        assert _priority_key(node) == 0

    def test_type_error_falls_back_to_zero(self):
        """If int() raises TypeError, fallback to 0."""
        node = _make_node(quantity=Decimal("0"))
        node.quantity = object()  # type: ignore[assignment]
        assert _priority_key(node) == 0


# ---------------------------------------------------------------------------
# get_demand_nodes tests
# ---------------------------------------------------------------------------

class TestGetDemandNodes:
    def test_returns_sorted_by_priority_time_ref_node_id(self):
        scenario_id = uuid4()
        db = MagicMock()

        id_a = UUID("00000000-0000-0000-0000-000000000001")
        id_b = UUID("00000000-0000-0000-0000-000000000002")
        id_c = UUID("00000000-0000-0000-0000-000000000003")

        node_a = _make_node(node_id=id_a, quantity=Decimal("2"), time_ref=date(2025, 1, 10), scenario_id=scenario_id)
        node_b = _make_node(node_id=id_b, quantity=Decimal("1"), time_ref=date(2025, 1, 10), scenario_id=scenario_id)
        node_c = _make_node(node_id=id_c, quantity=Decimal("1"), time_ref=date(2025, 1, 5), scenario_id=scenario_id)

        with patch("ootils_core.engine.kernel.allocation.engine.GraphStore") as MockStore:
            mock_store = MockStore.return_value
            mock_store.get_demand_nodes.return_value = [node_a, node_b, node_c]

            engine = AllocationEngine()
            result = engine.get_demand_nodes(scenario_id, db)

        # Expected order: priority ASC, then time_ref ASC, then node_id ASC
        # node_c: priority=1, time_ref=Jan5, id=...03
        # node_b: priority=1, time_ref=Jan10, id=...02
        # node_a: priority=2, time_ref=Jan10, id=...01
        assert result == [node_c, node_b, node_a]

    def test_none_time_ref_sorts_last(self):
        scenario_id = uuid4()
        db = MagicMock()

        id_a = UUID("00000000-0000-0000-0000-000000000001")
        id_b = UUID("00000000-0000-0000-0000-000000000002")

        node_a = _make_node(node_id=id_a, quantity=Decimal("1"), time_ref=None, scenario_id=scenario_id)
        node_b = _make_node(node_id=id_b, quantity=Decimal("1"), time_ref=date(2025, 1, 1), scenario_id=scenario_id)

        with patch("ootils_core.engine.kernel.allocation.engine.GraphStore") as MockStore:
            mock_store = MockStore.return_value
            mock_store.get_demand_nodes.return_value = [node_a, node_b]

            engine = AllocationEngine()
            result = engine.get_demand_nodes(scenario_id, db)

        # node_b has real time_ref, node_a has None → sentinel 9999-12-31
        assert result == [node_b, node_a]

    def test_empty_demands(self):
        scenario_id = uuid4()
        db = MagicMock()

        with patch("ootils_core.engine.kernel.allocation.engine.GraphStore") as MockStore:
            mock_store = MockStore.return_value
            mock_store.get_demand_nodes.return_value = []

            engine = AllocationEngine()
            result = engine.get_demand_nodes(scenario_id, db)

        assert result == []


# ---------------------------------------------------------------------------
# _allocate_demand tests
# ---------------------------------------------------------------------------

class TestAllocateDemand:
    def setup_method(self):
        self.engine = AllocationEngine()
        self.scenario_id = uuid4()
        self.db = MagicMock()
        self.store = MagicMock()

    def test_zero_quantity_skips(self):
        demand = _make_node(quantity=Decimal("0"))
        result = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)
        assert result == (_ZERO, 0, 0)

    def test_negative_quantity_skips(self):
        demand = _make_node(quantity=Decimal("-5"))
        result = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)
        assert result == (_ZERO, 0, 0)

    def test_none_quantity_treated_as_zero_skips(self):
        demand = _make_node(quantity=None)
        result = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)
        assert result == (_ZERO, 0, 0)

    def test_no_consumes_edges_returns_zero(self):
        demand = _make_node(quantity=Decimal("50"))
        self.store.get_edges_from.return_value = []
        result = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)
        assert result == (_ZERO, 0, 0)

    def test_pi_node_not_found_skips(self):
        demand = _make_node(quantity=Decimal("50"))
        edge = _make_edge(from_node_id=demand.node_id, scenario_id=self.scenario_id)
        self.store.get_edges_from.return_value = [edge]
        self.store.get_node.return_value = None

        result = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)
        assert result == (_ZERO, 0, 0)

    def test_pi_node_zero_stock_skips(self):
        demand = _make_node(quantity=Decimal("50"))
        pi_node = _make_node(closing_stock=Decimal("0"))
        edge = _make_edge(from_node_id=demand.node_id, to_node_id=pi_node.node_id, scenario_id=self.scenario_id)

        self.store.get_edges_from.return_value = [edge]
        self.store.get_node.return_value = pi_node

        result = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)
        assert result == (_ZERO, 0, 0)

    def test_pi_node_negative_stock_skips(self):
        demand = _make_node(quantity=Decimal("50"))
        pi_node = _make_node(closing_stock=Decimal("-10"))
        edge = _make_edge(from_node_id=demand.node_id, to_node_id=pi_node.node_id, scenario_id=self.scenario_id)

        self.store.get_edges_from.return_value = [edge]
        self.store.get_node.return_value = pi_node

        result = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)
        assert result == (_ZERO, 0, 0)

    def test_pi_node_none_closing_stock_treated_as_zero(self):
        demand = _make_node(quantity=Decimal("50"))
        pi_node = _make_node(closing_stock=None)
        edge = _make_edge(from_node_id=demand.node_id, to_node_id=pi_node.node_id, scenario_id=self.scenario_id)

        self.store.get_edges_from.return_value = [edge]
        self.store.get_node.return_value = pi_node

        result = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)
        assert result == (_ZERO, 0, 0)

    def test_full_allocation_creates_edge(self):
        demand = _make_node(quantity=Decimal("30"))
        pi_node = _make_node(closing_stock=Decimal("100"))
        edge = _make_edge(from_node_id=demand.node_id, to_node_id=pi_node.node_id, scenario_id=self.scenario_id)

        self.store.get_edges_from.return_value = [edge]
        self.store.get_node.return_value = pi_node
        self.store.upsert_edge.return_value = (MagicMock(), True)  # created=True

        alloc, created, updated = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)

        assert alloc == Decimal("30")
        assert created == 1
        assert updated == 0
        self.store.update_node_closing_stock.assert_called_once_with(
            pi_node.node_id, self.scenario_id, Decimal("70")
        )

    def test_partial_allocation_across_multiple_pi_nodes(self):
        """Demand=100, PI1 has 60, PI2 has 20 → allocated=80, shortage=20."""
        demand = _make_node(quantity=Decimal("100"))
        pi1 = _make_node(closing_stock=Decimal("60"))
        pi2 = _make_node(closing_stock=Decimal("20"))

        edge1 = _make_edge(from_node_id=demand.node_id, to_node_id=pi1.node_id, scenario_id=self.scenario_id, priority=0)
        edge2 = _make_edge(from_node_id=demand.node_id, to_node_id=pi2.node_id, scenario_id=self.scenario_id, priority=1)

        self.store.get_edges_from.return_value = [edge1, edge2]
        self.store.get_node.side_effect = [pi1, pi2]
        self.store.upsert_edge.return_value = (MagicMock(), True)

        alloc, created, updated = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)

        assert alloc == Decimal("80")
        assert created == 2
        assert updated == 0

    def test_upsert_edge_updated_not_created(self):
        demand = _make_node(quantity=Decimal("10"))
        pi_node = _make_node(closing_stock=Decimal("50"))
        edge = _make_edge(from_node_id=demand.node_id, to_node_id=pi_node.node_id, scenario_id=self.scenario_id)

        self.store.get_edges_from.return_value = [edge]
        self.store.get_node.return_value = pi_node
        self.store.upsert_edge.return_value = (MagicMock(), False)  # updated, not created

        alloc, created, updated = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)

        assert alloc == Decimal("10")
        assert created == 0
        assert updated == 1

    def test_demand_fully_satisfied_stops_early(self):
        """When demand is fully satisfied by first PI node, second PI is not touched."""
        demand = _make_node(quantity=Decimal("10"))
        pi1 = _make_node(closing_stock=Decimal("50"))
        pi2 = _make_node(closing_stock=Decimal("50"))

        edge1 = _make_edge(from_node_id=demand.node_id, to_node_id=pi1.node_id, scenario_id=self.scenario_id)
        edge2 = _make_edge(from_node_id=demand.node_id, to_node_id=pi2.node_id, scenario_id=self.scenario_id)

        self.store.get_edges_from.return_value = [edge1, edge2]
        self.store.get_node.return_value = pi1
        self.store.upsert_edge.return_value = (MagicMock(), True)

        alloc, created, updated = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)

        assert alloc == Decimal("10")
        # get_node should only be called once (for pi1), pi2 never reached because remaining <= 0
        assert self.store.get_node.call_count == 1

    def test_mixed_pi_nodes_some_missing_some_empty(self):
        """First PI missing, second has zero stock, third has stock."""
        demand = _make_node(quantity=Decimal("25"))
        pi3 = _make_node(closing_stock=Decimal("100"))

        edge1 = _make_edge(from_node_id=demand.node_id, scenario_id=self.scenario_id)
        edge2 = _make_edge(from_node_id=demand.node_id, scenario_id=self.scenario_id)
        edge3 = _make_edge(from_node_id=demand.node_id, to_node_id=pi3.node_id, scenario_id=self.scenario_id)

        self.store.get_edges_from.return_value = [edge1, edge2, edge3]
        self.store.get_node.side_effect = [
            None,  # first PI not found
            _make_node(closing_stock=Decimal("0")),  # second PI empty
            pi3,
        ]
        self.store.upsert_edge.return_value = (MagicMock(), True)

        alloc, created, updated = self.engine._allocate_demand(demand, self.scenario_id, self.db, self.store)

        assert alloc == Decimal("25")
        assert created == 1


# ---------------------------------------------------------------------------
# allocate (top-level) tests
# ---------------------------------------------------------------------------

class TestAllocate:
    def setup_method(self):
        self.engine = AllocationEngine()
        self.scenario_id = uuid4()
        self.db = MagicMock()

    def test_empty_demands_returns_zero_result(self):
        with patch.object(self.engine, "get_demand_nodes", return_value=[]):
            result = self.engine.allocate(self.scenario_id, self.db)

        assert isinstance(result, AllocationResult)
        assert result.demands_total == 0
        assert result.demands_fully_allocated == 0
        assert result.demands_partially_allocated == 0
        assert result.demands_unallocated == 0
        assert result.total_qty_demanded == _ZERO
        assert result.total_qty_allocated == _ZERO
        assert result.edges_created == 0
        assert result.edges_updated == 0

    def test_fully_allocated_demand(self):
        demand = _make_node(quantity=Decimal("50"))

        with patch.object(self.engine, "get_demand_nodes", return_value=[demand]), \
             patch.object(self.engine, "_allocate_demand", return_value=(Decimal("50"), 1, 0)):
            result = self.engine.allocate(self.scenario_id, self.db)

        assert result.demands_fully_allocated == 1
        assert result.demands_partially_allocated == 0
        assert result.demands_unallocated == 0
        assert result.total_qty_demanded == Decimal("50")
        assert result.total_qty_allocated == Decimal("50")
        assert result.edges_created == 1

    def test_partially_allocated_demand(self):
        demand = _make_node(quantity=Decimal("100"))

        with patch.object(self.engine, "get_demand_nodes", return_value=[demand]), \
             patch.object(self.engine, "_allocate_demand", return_value=(Decimal("40"), 1, 0)):
            result = self.engine.allocate(self.scenario_id, self.db)

        assert result.demands_partially_allocated == 1
        assert result.demands_fully_allocated == 0
        assert result.demands_unallocated == 0

    def test_unallocated_demand(self):
        demand = _make_node(quantity=Decimal("100"))

        with patch.object(self.engine, "get_demand_nodes", return_value=[demand]), \
             patch.object(self.engine, "_allocate_demand", return_value=(_ZERO, 0, 0)):
            result = self.engine.allocate(self.scenario_id, self.db)

        assert result.demands_unallocated == 1
        assert result.demands_fully_allocated == 0
        assert result.demands_partially_allocated == 0

    def test_none_quantity_treated_as_zero_in_allocate(self):
        demand = _make_node(quantity=None)

        with patch.object(self.engine, "get_demand_nodes", return_value=[demand]), \
             patch.object(self.engine, "_allocate_demand", return_value=(_ZERO, 0, 0)):
            result = self.engine.allocate(self.scenario_id, self.db)

        # qty_demanded=0, allocated=0 → 0 >= 0 → fully allocated
        assert result.demands_fully_allocated == 1
        assert result.total_qty_demanded == _ZERO

    def test_multiple_demands_mixed_outcomes(self):
        d1 = _make_node(quantity=Decimal("50"))
        d2 = _make_node(quantity=Decimal("100"))
        d3 = _make_node(quantity=Decimal("80"))

        alloc_returns = [
            (Decimal("50"), 1, 0),   # fully allocated
            (Decimal("30"), 0, 1),   # partially
            (_ZERO, 0, 0),           # unallocated
        ]

        with patch.object(self.engine, "get_demand_nodes", return_value=[d1, d2, d3]), \
             patch.object(self.engine, "_allocate_demand", side_effect=alloc_returns):
            result = self.engine.allocate(self.scenario_id, self.db)

        assert result.demands_total == 3
        assert result.demands_fully_allocated == 1
        assert result.demands_partially_allocated == 1
        assert result.demands_unallocated == 1
        assert result.total_qty_demanded == Decimal("230")
        assert result.total_qty_allocated == Decimal("80")
        assert result.edges_created == 1
        assert result.edges_updated == 1

    def test_run_at_is_set(self):
        with patch.object(self.engine, "get_demand_nodes", return_value=[]):
            result = self.engine.allocate(self.scenario_id, self.db)

        assert result.run_at is not None
        assert result.run_at.tzinfo == timezone.utc

    def test_scenario_id_propagated(self):
        with patch.object(self.engine, "get_demand_nodes", return_value=[]):
            result = self.engine.allocate(self.scenario_id, self.db)

        assert result.scenario_id == self.scenario_id
