"""
test_graph_store.py — Pure unit tests for GraphStore row-mapper helpers.

Only the row→domain (and reverse) helpers live here — they are pure
functions that take dicts/dataclasses and return dataclasses/dicts with
no database involvement. Everything that actually touches PostgreSQL
has been moved to ``tests/integration/test_graph_store_integration.py``
in line with the project rule "tests run against real Postgres, no mocks"
(CLAUDE.md).
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

from ootils_core.engine.kernel.graph.store import (
    _edge_to_params,
    _node_to_params,
    _row_to_edge,
    _row_to_node,
    _row_to_series,
)
from ootils_core.models import Edge, Node, ProjectionSeries


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_node(**overrides) -> Node:
    defaults = dict(
        node_id=uuid4(),
        node_type="ProjectedInventory",
        scenario_id=uuid4(),
    )
    defaults.update(overrides)
    return Node(**defaults)


def _make_edge(**overrides) -> Edge:
    defaults = dict(
        edge_id=uuid4(),
        edge_type="feeds_forward",
        from_node_id=uuid4(),
        to_node_id=uuid4(),
        scenario_id=uuid4(),
    )
    defaults.update(overrides)
    return Edge(**defaults)


def _full_node_row(**overrides) -> dict:
    """Return a dict that matches the SELECT * FROM nodes column set."""
    now = datetime.now(timezone.utc)
    row = {
        "node_id": str(uuid4()),
        "node_type": "ProjectedInventory",
        "scenario_id": str(uuid4()),
        "item_id": str(uuid4()),
        "location_id": str(uuid4()),
        "quantity": "100.00",
        "qty_uom": "EA",
        "time_grain": "week",
        "time_ref": date(2025, 3, 1),
        "time_span_start": date(2025, 3, 1),
        "time_span_end": date(2025, 3, 7),
        "is_dirty": False,
        "last_calc_run_id": str(uuid4()),
        "active": True,
        "projection_series_id": str(uuid4()),
        "bucket_sequence": 1,
        "opening_stock": "50.00",
        "inflows": "30.00",
        "outflows": "20.00",
        "closing_stock": "60.00",
        "has_shortage": False,
        "shortage_qty": "0",
        "has_exact_date_inputs": True,
        "has_week_inputs": False,
        "has_month_inputs": False,
        "created_at": now,
        "updated_at": now,
    }
    row.update(overrides)
    return row


def _minimal_node_row(**overrides) -> dict:
    """Return a minimal node row (nullable fields absent or None)."""
    row = {
        "node_id": str(uuid4()),
        "node_type": "OnHandSupply",
        "scenario_id": str(uuid4()),
        "item_id": None,
        "location_id": None,
        "quantity": None,
        "qty_uom": None,
        "time_grain": None,
        "time_ref": None,
        "time_span_start": None,
        "time_span_end": None,
        "is_dirty": False,
        "last_calc_run_id": None,
        "active": True,
        "projection_series_id": None,
        "bucket_sequence": None,
        "opening_stock": None,
        "inflows": None,
        "outflows": None,
        "closing_stock": None,
        "has_shortage": False,
        "shortage_qty": None,
        "has_exact_date_inputs": False,
        "has_week_inputs": False,
        "has_month_inputs": False,
        "created_at": None,
        "updated_at": None,
    }
    row.update(overrides)
    return row


def _full_edge_row(**overrides) -> dict:
    now = datetime.now(timezone.utc)
    row = {
        "edge_id": str(uuid4()),
        "edge_type": "feeds_forward",
        "from_node_id": str(uuid4()),
        "to_node_id": str(uuid4()),
        "scenario_id": str(uuid4()),
        "priority": 1,
        "weight_ratio": "1.0",
        "effective_start": date(2025, 1, 1),
        "effective_end": date(2025, 12, 31),
        "active": True,
        "created_at": now,
    }
    row.update(overrides)
    return row


def _minimal_edge_row(**overrides) -> dict:
    row = {
        "edge_id": str(uuid4()),
        "edge_type": "replenishes",
        "from_node_id": str(uuid4()),
        "to_node_id": str(uuid4()),
        "scenario_id": str(uuid4()),
    }
    row.update(overrides)
    return row


def _series_row(**overrides) -> dict:
    now = datetime.now(timezone.utc)
    row = {
        "series_id": str(uuid4()),
        "item_id": str(uuid4()),
        "location_id": str(uuid4()),
        "scenario_id": str(uuid4()),
        "horizon_start": date(2025, 1, 1),
        "horizon_end": date(2025, 12, 31),
        "created_at": now,
        "updated_at": now,
    }
    row.update(overrides)
    return row


# ===========================================================================
# _row_to_node / _row_to_edge / _row_to_series
# ===========================================================================


class TestRowToNode:
    def test_full_row(self):
        row = _full_node_row()
        node = _row_to_node(row)
        assert isinstance(node, Node)
        assert node.node_type == "ProjectedInventory"
        assert node.quantity == Decimal("100.00")
        assert node.opening_stock == Decimal("50.00")
        assert node.has_shortage is False
        assert node.item_id is not None
        assert node.location_id is not None

    def test_minimal_row_nullable_fields(self):
        row = _minimal_node_row()
        node = _row_to_node(row)
        assert node.item_id is None
        assert node.location_id is None
        assert node.quantity is None
        assert node.last_calc_run_id is None
        assert node.projection_series_id is None
        assert node.opening_stock is None
        assert node.inflows is None
        assert node.outflows is None
        assert node.closing_stock is None
        assert node.shortage_qty == Decimal("0")

    def test_shortage_qty_zero_when_none(self):
        row = _minimal_node_row(shortage_qty=None)
        node = _row_to_node(row)
        assert node.shortage_qty == Decimal("0")

    def test_shortage_qty_nonzero(self):
        row = _full_node_row(shortage_qty="42.5")
        node = _row_to_node(row)
        assert node.shortage_qty == Decimal("42.5")


class TestRowToEdge:
    def test_full_row(self):
        row = _full_edge_row()
        edge = _row_to_edge(row)
        assert isinstance(edge, Edge)
        assert edge.priority == 1
        assert edge.weight_ratio == Decimal("1.0")
        assert edge.active is True

    def test_minimal_row_defaults(self):
        row = _minimal_edge_row()
        edge = _row_to_edge(row)
        assert edge.priority == 0
        assert edge.weight_ratio == Decimal("1.0")
        assert edge.active is True


class TestRowToSeries:
    def test_converts_correctly(self):
        row = _series_row()
        series = _row_to_series(row)
        assert isinstance(series, ProjectionSeries)
        assert series.horizon_start == date(2025, 1, 1)
        assert series.horizon_end == date(2025, 12, 31)


class TestNodeToParams:
    def test_returns_all_fields(self):
        node = _make_node(quantity=Decimal("10"), qty_uom="KG")
        params = _node_to_params(node)
        assert params["node_id"] == node.node_id
        assert params["quantity"] == Decimal("10")
        assert params["qty_uom"] == "KG"
        assert "created_at" in params


class TestEdgeToParams:
    def test_returns_all_fields(self):
        edge = _make_edge(priority=5, weight_ratio=Decimal("0.5"))
        params = _edge_to_params(edge)
        assert params["edge_id"] == edge.edge_id
        assert params["priority"] == 5
        assert params["weight_ratio"] == Decimal("0.5")
