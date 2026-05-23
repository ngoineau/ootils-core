"""
Pure-function tests for ootils_core.engine.kernel.allocation.engine.

These tests do NOT touch the database. All DB-backed engine tests
(get_demand_nodes, _allocate_demand branches, allocate top-level)
were ported to tests/integration/test_allocation_engine_integration.py
per the "no mocks" rule (CLAUDE.md).

Only ``_priority_key`` remains here — it is a pure helper over a Node
dataclass and never touches a connection.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

from ootils_core.engine.kernel.allocation.engine import _priority_key
from ootils_core.models import Node


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


# ---------------------------------------------------------------------------
# _priority_key tests — pure function, no DB
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
