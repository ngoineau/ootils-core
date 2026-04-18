"""
test_graph_store.py — Comprehensive unit tests for GraphStore.

Covers every method, every branch (if/else/for), and all helper functions
(_row_to_node, _row_to_edge, _row_to_series, _node_to_params, _edge_to_params).
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, call
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.graph.store import (
    GraphStore,
    _edge_to_params,
    _node_to_params,
    _row_to_edge,
    _row_to_node,
    _row_to_series,
)
from ootils_core.models import CycleDetectedError, Edge, Node, ProjectionSeries


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


def _mock_conn():
    """Return a mock psycopg.Connection with chainable execute().fetchone/fetchall."""
    conn = MagicMock()
    return conn


def _setup_execute(conn, fetchone=None, fetchall=None):
    """Configure conn.execute(...).fetchone() / .fetchall() returns."""
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone
    cursor.fetchall.return_value = fetchall if fetchall is not None else []
    conn.execute.return_value = cursor
    return cursor


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


# ===========================================================================
# GraphStore — Node reads
# ===========================================================================


class TestGetNode:
    def test_found(self):
        conn = _mock_conn()
        row = _full_node_row()
        _setup_execute(conn, fetchone=row)
        store = GraphStore(conn)
        node = store.get_node(uuid4(), uuid4())
        assert node is not None
        assert node.node_type == "ProjectedInventory"

    def test_not_found_returns_none(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchone=None)
        store = GraphStore(conn)
        assert store.get_node(uuid4(), uuid4()) is None


class TestGetNodesBySeries:
    def test_returns_list(self):
        conn = _mock_conn()
        rows = [_full_node_row(), _full_node_row()]
        _setup_execute(conn, fetchall=rows)
        store = GraphStore(conn)
        result = store.get_nodes_by_series(uuid4())
        assert len(result) == 2
        assert all(isinstance(n, Node) for n in result)

    def test_empty(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[])
        store = GraphStore(conn)
        assert store.get_nodes_by_series(uuid4()) == []


class TestGetAllNodes:
    def test_returns_nodes(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[_full_node_row()])
        store = GraphStore(conn)
        result = store.get_all_nodes(uuid4())
        assert len(result) == 1


# ===========================================================================
# GraphStore — Edge reads
# ===========================================================================


class TestGetEdgesFrom:
    def test_with_edge_type(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[_full_edge_row()])
        store = GraphStore(conn)
        result = store.get_edges_from(uuid4(), uuid4(), edge_type="feeds_forward")
        assert len(result) == 1
        # Verify the SQL included edge_type param
        sql = conn.execute.call_args[0][0]
        assert "edge_type" in sql

    def test_without_edge_type(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[_full_edge_row()])
        store = GraphStore(conn)
        result = store.get_edges_from(uuid4(), uuid4())
        assert len(result) == 1
        sql = conn.execute.call_args[0][0]
        assert "edge_type" not in sql

    def test_empty(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[])
        store = GraphStore(conn)
        assert store.get_edges_from(uuid4(), uuid4()) == []


class TestGetEdgesTo:
    def test_with_edge_type(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[_full_edge_row()])
        store = GraphStore(conn)
        result = store.get_edges_to(uuid4(), uuid4(), edge_type="consumes")
        assert len(result) == 1
        sql = conn.execute.call_args[0][0]
        assert "edge_type" in sql

    def test_without_edge_type(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[_full_edge_row()])
        store = GraphStore(conn)
        result = store.get_edges_to(uuid4(), uuid4())
        assert len(result) == 1
        sql = conn.execute.call_args[0][0]
        assert "edge_type" not in sql


class TestGetAllEdges:
    def test_returns_edges(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[_full_edge_row()])
        store = GraphStore(conn)
        assert len(store.get_all_edges(uuid4())) == 1


class TestGetEdgesByType:
    def test_returns_edges(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[_full_edge_row(), _full_edge_row()])
        store = GraphStore(conn)
        result = store.get_edges_by_type(uuid4(), "pegged_to")
        assert len(result) == 2


# ===========================================================================
# GraphStore — Node writes
# ===========================================================================


class TestUpsertNode:
    def test_calls_execute_and_returns_node(self):
        conn = _mock_conn()
        store = GraphStore(conn)
        node = _make_node()
        result = store.upsert_node(node)
        assert result is node
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "INSERT INTO nodes" in sql
        assert "ON CONFLICT" in sql


# ===========================================================================
# GraphStore — Edge writes + cycle detection
# ===========================================================================


class TestInsertEdge:
    def test_no_cycle_inserts_edge(self):
        conn = _mock_conn()
        # First call: validate_no_cycle fetchall (no existing edges)
        # Second call: the INSERT
        cursor_cycle = MagicMock()
        cursor_cycle.fetchall.return_value = []
        cursor_insert = MagicMock()
        conn.execute.side_effect = [cursor_cycle, cursor_insert]

        store = GraphStore(conn)
        edge = _make_edge()
        result = store.insert_edge(edge)
        assert result is edge
        assert conn.execute.call_count == 2

    def test_cycle_detected_raises(self):
        conn = _mock_conn()
        node_a = uuid4()
        node_b = uuid4()
        scenario = uuid4()

        # Existing edge: B -> A  means adding A -> B would create cycle
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {"from_node_id": str(node_b), "to_node_id": str(node_a)},
        ]
        conn.execute.return_value = cursor

        store = GraphStore(conn)
        edge = _make_edge(
            from_node_id=node_a,
            to_node_id=node_b,
            scenario_id=scenario,
        )
        with pytest.raises(CycleDetectedError):
            store.insert_edge(edge)


class TestValidateNoCycle:
    def test_no_edges_no_cycle(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[])
        store = GraphStore(conn)
        # Should not raise
        store.validate_no_cycle(uuid4(), uuid4(), uuid4())

    def test_direct_cycle(self):
        conn = _mock_conn()
        a, b = uuid4(), uuid4()
        scenario = uuid4()
        # Edge: b -> a  means adding a -> b creates a cycle
        _setup_execute(conn, fetchall=[
            {"from_node_id": str(b), "to_node_id": str(a)},
        ])
        store = GraphStore(conn)
        with pytest.raises(CycleDetectedError) as exc_info:
            store.validate_no_cycle(a, b, scenario)
        assert exc_info.value.from_id == a
        assert exc_info.value.to_id == b

    def test_transitive_cycle(self):
        conn = _mock_conn()
        a, b, c = uuid4(), uuid4(), uuid4()
        scenario = uuid4()
        # b -> c -> a  means adding a -> b creates cycle (a -> b -> c -> a)
        _setup_execute(conn, fetchall=[
            {"from_node_id": str(b), "to_node_id": str(c)},
            {"from_node_id": str(c), "to_node_id": str(a)},
        ])
        store = GraphStore(conn)
        with pytest.raises(CycleDetectedError):
            store.validate_no_cycle(a, b, scenario)

    def test_no_cycle_with_edges(self):
        conn = _mock_conn()
        a, b, c = uuid4(), uuid4(), uuid4()
        # b -> c (but c does NOT lead back to a)
        _setup_execute(conn, fetchall=[
            {"from_node_id": str(b), "to_node_id": str(c)},
        ])
        store = GraphStore(conn)
        store.validate_no_cycle(a, b, uuid4())  # Should not raise

    def test_visited_nodes_are_not_revisited(self):
        """Ensure DFS doesn't revisit nodes (diamond graph)."""
        conn = _mock_conn()
        a, b, c, d = uuid4(), uuid4(), uuid4(), uuid4()
        # b -> c, b -> d, c -> d (diamond, no cycle)
        _setup_execute(conn, fetchall=[
            {"from_node_id": str(b), "to_node_id": str(c)},
            {"from_node_id": str(b), "to_node_id": str(d)},
            {"from_node_id": str(c), "to_node_id": str(d)},
        ])
        store = GraphStore(conn)
        store.validate_no_cycle(a, b, uuid4())  # Should not raise


class TestUpdatePiResult:
    def test_calls_execute(self):
        conn = _mock_conn()
        store = GraphStore(conn)
        store.update_pi_result(
            node_id=uuid4(),
            scenario_id=uuid4(),
            calc_run_id=uuid4(),
            opening_stock=Decimal("10"),
            inflows=Decimal("5"),
            outflows=Decimal("3"),
            closing_stock=Decimal("12"),
            has_shortage=False,
            shortage_qty=Decimal("0"),
        )
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "UPDATE nodes" in sql


class TestUpdateNodeClosingStock:
    def test_calls_execute(self):
        conn = _mock_conn()
        store = GraphStore(conn)
        store.update_node_closing_stock(uuid4(), uuid4(), Decimal("99"))
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "closing_stock" in sql


# ===========================================================================
# GraphStore — UpsertEdge (insert vs update)
# ===========================================================================


class TestUpsertEdge:
    def test_update_existing(self):
        conn = _mock_conn()
        existing_edge_id = uuid4()

        # First execute: SELECT existing -> returns row
        # Second execute: UPDATE
        cursor_select = MagicMock()
        cursor_select.fetchone.return_value = {"edge_id": str(existing_edge_id)}
        cursor_update = MagicMock()
        conn.execute.side_effect = [cursor_select, cursor_update]

        store = GraphStore(conn)
        edge = _make_edge()
        result_edge, created = store.upsert_edge(edge)
        assert created is False
        assert result_edge.edge_id == existing_edge_id
        assert conn.execute.call_count == 2

    def test_insert_new(self):
        conn = _mock_conn()

        # First execute: SELECT existing -> returns None
        # Second execute: validate_no_cycle edge scan
        # Third execute: INSERT
        cursor_select = MagicMock()
        cursor_select.fetchone.return_value = None
        cursor_validate = MagicMock()
        cursor_validate.fetchall.return_value = []
        cursor_insert = MagicMock()
        conn.execute.side_effect = [cursor_select, cursor_validate, cursor_insert]

        store = GraphStore(conn)
        edge = _make_edge()
        result_edge, created = store.upsert_edge(edge)
        assert created is True
        assert result_edge is edge
        assert conn.execute.call_count == 3

    def test_insert_new_non_pegged_edge_validates_cycle(self):
        conn = _mock_conn()

        cursor_select = MagicMock()
        cursor_select.fetchone.return_value = None
        cursor_insert = MagicMock()
        conn.execute.side_effect = [cursor_select, cursor_insert]

        store = GraphStore(conn)
        store.validate_no_cycle = MagicMock()

        edge = _make_edge(edge_type="depends_on")
        store.upsert_edge(edge)

        store.validate_no_cycle.assert_called_once_with(
            edge.from_node_id,
            edge.to_node_id,
            edge.scenario_id,
        )

    def test_insert_new_pegged_edge_skips_cycle_validation(self):
        conn = _mock_conn()

        cursor_select = MagicMock()
        cursor_select.fetchone.return_value = None
        cursor_insert = MagicMock()
        conn.execute.side_effect = [cursor_select, cursor_insert]

        store = GraphStore(conn)
        store.validate_no_cycle = MagicMock()

        edge = _make_edge(edge_type="pegged_to")
        store.upsert_edge(edge)

        store.validate_no_cycle.assert_not_called()


# ===========================================================================
# GraphStore — Projection series
# ===========================================================================


class TestGetProjectionSeries:
    def test_found(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchone=_series_row())
        store = GraphStore(conn)
        result = store.get_projection_series(uuid4(), uuid4(), uuid4())
        assert result is not None
        assert isinstance(result, ProjectionSeries)

    def test_not_found(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchone=None)
        store = GraphStore(conn)
        assert store.get_projection_series(uuid4(), uuid4(), uuid4()) is None


class TestCreateProjectionSeries:
    def test_creates_and_returns(self):
        conn = _mock_conn()
        store = GraphStore(conn)
        result = store.create_projection_series(
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start=date(2025, 1, 1),
            horizon_end=date(2025, 12, 31),
        )
        assert isinstance(result, ProjectionSeries)
        conn.execute.assert_called_once()


class TestGetOrCreateProjectionSeries:
    def test_returns_existing(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchone=_series_row())
        store = GraphStore(conn)
        result = store.get_or_create_projection_series(
            uuid4(), uuid4(), uuid4(), date(2025, 1, 1), date(2025, 12, 31),
        )
        assert isinstance(result, ProjectionSeries)
        # Only the get_projection_series SELECT should have been called
        assert conn.execute.call_count == 1

    def test_creates_when_not_existing(self):
        conn = _mock_conn()
        # First call (get): returns None
        # Second call (create): the INSERT
        cursor_get = MagicMock()
        cursor_get.fetchone.return_value = None
        cursor_create = MagicMock()
        conn.execute.side_effect = [cursor_get, cursor_create]

        store = GraphStore(conn)
        result = store.get_or_create_projection_series(
            uuid4(), uuid4(), uuid4(), date(2025, 1, 1), date(2025, 12, 31),
        )
        assert isinstance(result, ProjectionSeries)
        assert conn.execute.call_count == 2


# ===========================================================================
# GraphStore — Demand nodes
# ===========================================================================


class TestGetDemandNodes:
    def test_default_types(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[_full_node_row(node_type="ForecastDemand")])
        store = GraphStore(conn)
        result = store.get_demand_nodes(uuid4())
        assert len(result) == 1
        # SQL should have 3 placeholders for the 3 default types
        sql = conn.execute.call_args[0][0]
        assert "IN (" in sql

    def test_custom_types(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[])
        store = GraphStore(conn)
        result = store.get_demand_nodes(uuid4(), node_types=("Custom",))
        assert result == []
        # Params: (scenario_id, "Custom")
        params = conn.execute.call_args[0][1]
        assert "Custom" in params

    def test_empty_result(self):
        conn = _mock_conn()
        _setup_execute(conn, fetchall=[])
        store = GraphStore(conn)
        assert store.get_demand_nodes(uuid4()) == []
