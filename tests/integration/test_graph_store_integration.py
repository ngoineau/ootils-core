"""
Integration tests for ootils_core.engine.kernel.graph.store.GraphStore
against a real PostgreSQL database.

Ported from tests/test_graph_store.py — the original used MagicMock to
stand in for a psycopg connection, which the audit flagged as a BLOCK
finding under the "tests run against real Postgres, no mocks" rule
(CLAUDE.md). Every DB-touching test class has been re-implemented here
to insert real rows, call the store method, and assert on behaviour
(returned dataclasses, rows actually written, cycle detection raising
on a real edge set).

Each test creates unique items/locations/nodes/edges and cleans up at
the end via the ``_teardown`` helper. The function-scoped ``conn``
fixture also rolls back uncommitted changes; we use explicit DELETE
in the teardown for the same reason ``test_allocation_engine_integration``
does — keep the test DB clean for inspection between runs.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.models import (
    CycleDetectedError,
    Edge,
    Node,
    ProjectionSeries,
    Scenario,
)

from .conftest import requires_db

pytestmark = requires_db


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _make_item(conn) -> UUID:
    item_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"GS Test Item {item_id}"),
    )
    return item_id


def _make_location(conn) -> UUID:
    location_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"GS Test Loc {location_id}"),
    )
    return location_id


def _make_pi_node(
    conn,
    *,
    scenario_id: UUID,
    item_id: UUID,
    location_id: UUID,
    closing_stock: Decimal | None = Decimal("100"),
    opening_stock: Decimal | None = Decimal("100"),
    time_span_start: date = date(2026, 4, 10),
    time_span_end: date = date(2026, 4, 11),
    projection_series_id: UUID | None = None,
    bucket_sequence: int | None = None,
) -> UUID:
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_span_start, time_span_end,
            closing_stock, opening_stock, inflows, outflows,
            projection_series_id, bucket_sequence
        ) VALUES (
            %s, 'ProjectedInventory', %s, %s, %s,
            'day', %s, %s,
            %s, %s, 0, 0,
            %s, %s
        )
        """,
        (
            node_id, scenario_id, item_id, location_id,
            time_span_start, time_span_end,
            closing_stock, opening_stock,
            projection_series_id, bucket_sequence,
        ),
    )
    return node_id


def _make_demand_node(
    conn,
    *,
    scenario_id: UUID,
    item_id: UUID,
    location_id: UUID,
    quantity: Decimal = Decimal("10"),
    time_ref: date | None = date(2026, 4, 10),
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


def _make_edge_row(
    conn,
    *,
    edge_type: str,
    from_node_id: UUID,
    to_node_id: UUID,
    scenario_id: UUID,
    priority: int = 0,
    weight_ratio: Decimal = Decimal("1.0"),
    active: bool = True,
) -> UUID:
    edge_id = uuid4()
    conn.execute(
        """
        INSERT INTO edges (
            edge_id, edge_type, from_node_id, to_node_id, scenario_id,
            priority, weight_ratio, active
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """,
        (
            edge_id, edge_type, from_node_id, to_node_id, scenario_id,
            priority, weight_ratio, active,
        ),
    )
    return edge_id


def _make_series_row(
    conn,
    *,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
    horizon_start: date = date(2026, 1, 1),
    horizon_end: date = date(2026, 12, 31),
) -> UUID:
    series_id = uuid4()
    conn.execute(
        """
        INSERT INTO projection_series (
            series_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end),
    )
    return series_id


def _teardown(
    conn,
    *,
    edges: list[UUID] | None = None,
    nodes: list[UUID] | None = None,
    series: list[UUID] | None = None,
    locations: list[UUID] | None = None,
    items: list[UUID] | None = None,
) -> None:
    """Delete rows in FK-safe order. Best-effort — swallow per-row failures."""
    if edges:
        # Delete by id AND by from/to to also catch edges the store created.
        conn.execute(
            "DELETE FROM edges WHERE edge_id = ANY(%s) "
            "OR from_node_id = ANY(%s) OR to_node_id = ANY(%s)",
            (edges, nodes or [], nodes or []),
        )
    elif nodes:
        conn.execute(
            "DELETE FROM edges WHERE from_node_id = ANY(%s) OR to_node_id = ANY(%s)",
            (nodes, nodes),
        )
    if nodes:
        conn.execute("DELETE FROM nodes WHERE node_id = ANY(%s)", (nodes,))
    if series:
        conn.execute("DELETE FROM projection_series WHERE series_id = ANY(%s)", (series,))
    if locations:
        conn.execute("DELETE FROM locations WHERE location_id = ANY(%s)", (locations,))
    if items:
        conn.execute("DELETE FROM items WHERE item_id = ANY(%s)", (items,))
    conn.commit()


# ===========================================================================
# GraphStore — Node reads
# ===========================================================================


class TestGetNode:
    def test_found(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        node_id = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            node = store.get_node(node_id, scenario_id)
            assert node is not None
            assert node.node_type == "ProjectedInventory"
            assert node.node_id == node_id
            assert node.scenario_id == scenario_id
        finally:
            _teardown(
                conn,
                nodes=[node_id],
                locations=[location_id],
                items=[item_id],
            )

    def test_not_found_returns_none(self, conn):
        store = GraphStore(conn)
        assert store.get_node(uuid4(), Scenario.BASELINE_ID) is None


class TestGetNodesBySeries:
    def test_returns_list(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        series_id = _make_series_row(
            conn, item_id=item_id, location_id=location_id, scenario_id=scenario_id,
        )
        node_a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            projection_series_id=series_id, bucket_sequence=0,
        )
        node_b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            projection_series_id=series_id, bucket_sequence=1,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_nodes_by_series(series_id)
            assert len(result) == 2
            assert all(isinstance(n, Node) for n in result)
            assert {n.node_id for n in result} == {node_a, node_b}
        finally:
            _teardown(
                conn,
                nodes=[node_a, node_b],
                series=[series_id],
                locations=[location_id],
                items=[item_id],
            )

    def test_empty(self, conn):
        store = GraphStore(conn)
        # series_id that doesn't exist → empty list
        assert store.get_nodes_by_series(uuid4()) == []


class TestGetAllNodes:
    def test_returns_nodes(self, conn):
        scenario_id = uuid4()
        # Insert a fresh non-baseline scenario so other tests' rows do not leak in.
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"GS Test Scenario {scenario_id}"),
        )
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        node_id = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_all_nodes(scenario_id)
            assert len(result) == 1
            assert result[0].node_id == node_id
        finally:
            _teardown(
                conn,
                nodes=[node_id],
                locations=[location_id],
                items=[item_id],
            )
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()


# ===========================================================================
# GraphStore — Edge reads
# ===========================================================================


class TestGetEdgesFrom:
    def test_with_edge_type(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        # One feeds_forward edge, one consumes — get_edges_from(..., 'feeds_forward')
        # should return only the first.
        demand = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        e_ff = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=a, to_node_id=b,
            scenario_id=scenario_id,
        )
        e_c = _make_edge_row(
            conn, edge_type="consumes", from_node_id=a, to_node_id=demand,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_edges_from(a, scenario_id, edge_type="feeds_forward")
            assert len(result) == 1
            assert result[0].edge_id == e_ff
            assert result[0].edge_type == "feeds_forward"
        finally:
            _teardown(
                conn,
                edges=[e_ff, e_c],
                nodes=[a, b, demand],
                locations=[location_id],
                items=[item_id],
            )

    def test_without_edge_type(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        e = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=a, to_node_id=b,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_edges_from(a, scenario_id)
            assert len(result) == 1
            assert result[0].edge_id == e
        finally:
            _teardown(
                conn,
                edges=[e],
                nodes=[a, b],
                locations=[location_id],
                items=[item_id],
            )

    def test_empty(self, conn):
        store = GraphStore(conn)
        assert store.get_edges_from(uuid4(), Scenario.BASELINE_ID) == []


class TestGetEdgesTo:
    def test_with_edge_type(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        pi = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        demand = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        other_pi = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        e_c = _make_edge_row(
            conn, edge_type="consumes", from_node_id=demand, to_node_id=pi,
            scenario_id=scenario_id,
        )
        e_ff = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=other_pi, to_node_id=pi,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_edges_to(pi, scenario_id, edge_type="consumes")
            assert len(result) == 1
            assert result[0].edge_id == e_c
            assert result[0].edge_type == "consumes"
        finally:
            _teardown(
                conn,
                edges=[e_c, e_ff],
                nodes=[pi, demand, other_pi],
                locations=[location_id],
                items=[item_id],
            )

    def test_without_edge_type(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        pi = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        demand = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        e = _make_edge_row(
            conn, edge_type="consumes", from_node_id=demand, to_node_id=pi,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_edges_to(pi, scenario_id)
            assert len(result) == 1
            assert result[0].edge_id == e
        finally:
            _teardown(
                conn,
                edges=[e],
                nodes=[pi, demand],
                locations=[location_id],
                items=[item_id],
            )


class TestGetAllEdges:
    def test_returns_edges(self, conn):
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"GS Test Scenario {scenario_id}"),
        )
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        e = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=a, to_node_id=b,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_all_edges(scenario_id)
            assert len(result) == 1
            assert result[0].edge_id == e
        finally:
            _teardown(
                conn,
                edges=[e],
                nodes=[a, b],
                locations=[location_id],
                items=[item_id],
            )
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()


class TestGetEdgesByType:
    def test_returns_edges(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        pi = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        d1 = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        d2 = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        e1 = _make_edge_row(
            conn, edge_type="pegged_to", from_node_id=d1, to_node_id=pi,
            scenario_id=scenario_id,
        )
        e2 = _make_edge_row(
            conn, edge_type="pegged_to", from_node_id=d2, to_node_id=pi,
            scenario_id=scenario_id,
        )
        # Distractor of a different type — must not be returned.
        e_other = _make_edge_row(
            conn, edge_type="consumes", from_node_id=d1, to_node_id=pi,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_edges_by_type(scenario_id, "pegged_to")
            returned_ids = {e.edge_id for e in result}
            assert {e1, e2}.issubset(returned_ids)
            assert e_other not in returned_ids
        finally:
            _teardown(
                conn,
                edges=[e1, e2, e_other],
                nodes=[pi, d1, d2],
                locations=[location_id],
                items=[item_id],
            )


# ===========================================================================
# GraphStore — Node writes
# ===========================================================================


class TestUpsertNode:
    def test_insert_then_returns_node(self, conn):
        """upsert_node on a fresh node_id inserts a row and returns the node."""
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        conn.commit()

        node_id = uuid4()
        node = Node(
            node_id=node_id,
            node_type="ProjectedInventory",
            scenario_id=scenario_id,
            item_id=item_id,
            location_id=location_id,
            time_grain="day",
            time_span_start=date(2026, 4, 10),
            time_span_end=date(2026, 4, 11),
            opening_stock=Decimal("0"),
            inflows=Decimal("0"),
            outflows=Decimal("0"),
            closing_stock=Decimal("50"),
        )

        try:
            store = GraphStore(conn)
            result = store.upsert_node(node)
            conn.commit()
            assert result is node

            # Verify the row was actually written.
            row = conn.execute(
                "SELECT node_type, closing_stock FROM nodes WHERE node_id = %s",
                (node_id,),
            ).fetchone()
            assert row is not None
            assert row["node_type"] == "ProjectedInventory"
            assert Decimal(str(row["closing_stock"])) == Decimal("50")
        finally:
            _teardown(
                conn,
                nodes=[node_id],
                locations=[location_id],
                items=[item_id],
            )

    def test_upsert_updates_existing(self, conn):
        """A second upsert on the same node_id updates mutable fields."""
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        conn.commit()

        node_id = uuid4()
        node = Node(
            node_id=node_id,
            node_type="ProjectedInventory",
            scenario_id=scenario_id,
            item_id=item_id,
            location_id=location_id,
            time_grain="day",
            time_span_start=date(2026, 4, 10),
            time_span_end=date(2026, 4, 11),
            opening_stock=Decimal("0"),
            inflows=Decimal("0"),
            outflows=Decimal("0"),
            closing_stock=Decimal("50"),
        )

        try:
            store = GraphStore(conn)
            store.upsert_node(node)
            conn.commit()

            node.closing_stock = Decimal("77")
            store.upsert_node(node)
            conn.commit()

            row = conn.execute(
                "SELECT closing_stock FROM nodes WHERE node_id = %s",
                (node_id,),
            ).fetchone()
            assert Decimal(str(row["closing_stock"])) == Decimal("77")
        finally:
            _teardown(
                conn,
                nodes=[node_id],
                locations=[location_id],
                items=[item_id],
            )


# ===========================================================================
# GraphStore — Edge writes + cycle detection
# ===========================================================================


class TestInsertEdge:
    def test_no_cycle_inserts_edge(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        conn.commit()

        edge = Edge(
            edge_id=uuid4(),
            edge_type="feeds_forward",
            from_node_id=a,
            to_node_id=b,
            scenario_id=scenario_id,
        )
        try:
            store = GraphStore(conn)
            result = store.insert_edge(edge)
            conn.commit()
            assert result is edge

            row = conn.execute(
                "SELECT edge_type, from_node_id, to_node_id FROM edges "
                "WHERE edge_id = %s",
                (edge.edge_id,),
            ).fetchone()
            assert row is not None
            assert row["edge_type"] == "feeds_forward"
        finally:
            _teardown(
                conn,
                edges=[edge.edge_id],
                nodes=[a, b],
                locations=[location_id],
                items=[item_id],
            )

    def test_cycle_detected_raises(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        # Existing edge b -> a: adding a -> b would close a cycle.
        existing = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=b, to_node_id=a,
            scenario_id=scenario_id,
        )
        conn.commit()

        edge = Edge(
            edge_id=uuid4(),
            edge_type="feeds_forward",
            from_node_id=a,
            to_node_id=b,
            scenario_id=scenario_id,
        )
        try:
            store = GraphStore(conn)
            with pytest.raises(CycleDetectedError):
                store.insert_edge(edge)
        finally:
            conn.rollback()
            _teardown(
                conn,
                edges=[existing],
                nodes=[a, b],
                locations=[location_id],
                items=[item_id],
            )


class TestValidateNoCycle:
    """Tests for validate_no_cycle.

    The function only reads from the ``edges`` table — never ``nodes`` —
    but the edges FK requires every from/to node to actually exist, so we
    insert PI nodes as scaffolding.
    """

    def test_no_edges_no_cycle(self, conn):
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"GS NoCycle Empty {scenario_id}"),
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            # Empty edge set — must not raise. Any node IDs are fine since
            # validate_no_cycle only reads adjacency from edges.
            store.validate_no_cycle(uuid4(), uuid4(), scenario_id)
        finally:
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_direct_cycle(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        # Edge: b -> a means proposing a -> b would create a cycle.
        e = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=b, to_node_id=a,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            with pytest.raises(CycleDetectedError) as exc_info:
                store.validate_no_cycle(a, b, scenario_id)
            assert exc_info.value.from_id == a
            assert exc_info.value.to_id == b
        finally:
            _teardown(
                conn,
                edges=[e],
                nodes=[a, b],
                locations=[location_id],
                items=[item_id],
            )

    def test_transitive_cycle(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        c = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=2,
        )
        # b -> c -> a; adding a -> b closes the loop a -> b -> c -> a.
        e1 = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=b, to_node_id=c,
            scenario_id=scenario_id,
        )
        e2 = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=c, to_node_id=a,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            with pytest.raises(CycleDetectedError):
                store.validate_no_cycle(a, b, scenario_id)
        finally:
            _teardown(
                conn,
                edges=[e1, e2],
                nodes=[a, b, c],
                locations=[location_id],
                items=[item_id],
            )

    def test_no_cycle_with_edges(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        c = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=2,
        )
        # b -> c (c does NOT lead back to a) — adding a -> b is safe.
        e = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=b, to_node_id=c,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            store.validate_no_cycle(a, b, scenario_id)  # no exception
        finally:
            _teardown(
                conn,
                edges=[e],
                nodes=[a, b, c],
                locations=[location_id],
                items=[item_id],
            )

    def test_visited_nodes_are_not_revisited(self, conn):
        """Diamond graph: b -> c, b -> d, c -> d. Adding a -> b must not raise."""
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        c = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=2,
        )
        d = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=3,
        )
        e1 = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=b, to_node_id=c,
            scenario_id=scenario_id,
        )
        e2 = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=b, to_node_id=d,
            scenario_id=scenario_id,
        )
        e3 = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=c, to_node_id=d,
            scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            store.validate_no_cycle(a, b, scenario_id)  # no exception
        finally:
            _teardown(
                conn,
                edges=[e1, e2, e3],
                nodes=[a, b, c, d],
                locations=[location_id],
                items=[item_id],
            )


class TestUpdatePiResult:
    def test_writes_computation_results(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        node_id = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("0"), opening_stock=Decimal("0"),
        )
        # calc_run row so the FK on last_calc_run_id is satisfied (deferrable but
        # still resolved at commit). Insert a real calc_run.
        calc_run_id = uuid4()
        conn.execute(
            "INSERT INTO calc_runs (calc_run_id, scenario_id, status) "
            "VALUES (%s, %s, 'pending')",
            (calc_run_id, scenario_id),
        )
        # Also mark the row as dirty so we can check is_dirty was cleared.
        conn.execute(
            "UPDATE nodes SET is_dirty = TRUE WHERE node_id = %s",
            (node_id,),
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            store.update_pi_result(
                node_id=node_id,
                scenario_id=scenario_id,
                calc_run_id=calc_run_id,
                opening_stock=Decimal("10"),
                inflows=Decimal("5"),
                outflows=Decimal("3"),
                closing_stock=Decimal("12"),
                has_shortage=False,
                shortage_qty=Decimal("0"),
            )
            conn.commit()

            row = conn.execute(
                "SELECT opening_stock, inflows, outflows, closing_stock, "
                "has_shortage, shortage_qty, is_dirty, last_calc_run_id "
                "FROM nodes WHERE node_id = %s",
                (node_id,),
            ).fetchone()
            assert Decimal(str(row["opening_stock"])) == Decimal("10")
            assert Decimal(str(row["inflows"])) == Decimal("5")
            assert Decimal(str(row["outflows"])) == Decimal("3")
            assert Decimal(str(row["closing_stock"])) == Decimal("12")
            assert row["has_shortage"] is False
            assert Decimal(str(row["shortage_qty"])) == Decimal("0")
            assert row["is_dirty"] is False
            assert UUID(str(row["last_calc_run_id"])) == calc_run_id
        finally:
            _teardown(
                conn,
                nodes=[node_id],
                locations=[location_id],
                items=[item_id],
            )
            conn.execute("DELETE FROM calc_runs WHERE calc_run_id = %s", (calc_run_id,))
            conn.commit()


class TestUpdateNodeClosingStock:
    def test_writes_closing_stock(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        node_id = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            closing_stock=Decimal("100"), opening_stock=Decimal("100"),
        )
        # Mark dirty so we can check that the method clears it.
        conn.execute(
            "UPDATE nodes SET is_dirty = TRUE WHERE node_id = %s",
            (node_id,),
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            store.update_node_closing_stock(node_id, scenario_id, Decimal("99"))
            conn.commit()

            row = conn.execute(
                "SELECT closing_stock, is_dirty FROM nodes WHERE node_id = %s",
                (node_id,),
            ).fetchone()
            assert Decimal(str(row["closing_stock"])) == Decimal("99")
            assert row["is_dirty"] is False
        finally:
            _teardown(
                conn,
                nodes=[node_id],
                locations=[location_id],
                items=[item_id],
            )


# ===========================================================================
# GraphStore — UpsertEdge (insert vs update)
# ===========================================================================


class TestUpsertEdge:
    def test_update_existing(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        existing_edge_id = _make_edge_row(
            conn, edge_type="feeds_forward", from_node_id=a, to_node_id=b,
            scenario_id=scenario_id, priority=5, weight_ratio=Decimal("1.0"),
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            edge = Edge(
                edge_id=uuid4(),  # different ID — the existing row's ID must win
                edge_type="feeds_forward",
                from_node_id=a,
                to_node_id=b,
                scenario_id=scenario_id,
                priority=9,
                weight_ratio=Decimal("0.75"),
            )
            result_edge, created = store.upsert_edge(edge)
            conn.commit()

            assert created is False
            assert result_edge.edge_id == existing_edge_id

            row = conn.execute(
                "SELECT priority, weight_ratio FROM edges WHERE edge_id = %s",
                (existing_edge_id,),
            ).fetchone()
            assert row["priority"] == 9
            assert Decimal(str(row["weight_ratio"])) == Decimal("0.75")
        finally:
            _teardown(
                conn,
                edges=[existing_edge_id],
                nodes=[a, b],
                locations=[location_id],
                items=[item_id],
            )

    def test_insert_new(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        conn.commit()

        edge = Edge(
            edge_id=uuid4(),
            edge_type="feeds_forward",
            from_node_id=a,
            to_node_id=b,
            scenario_id=scenario_id,
        )
        try:
            store = GraphStore(conn)
            result_edge, created = store.upsert_edge(edge)
            conn.commit()
            assert created is True
            assert result_edge is edge

            row = conn.execute(
                "SELECT edge_type FROM edges WHERE edge_id = %s",
                (edge.edge_id,),
            ).fetchone()
            assert row is not None
            assert row["edge_type"] == "feeds_forward"
        finally:
            _teardown(
                conn,
                edges=[edge.edge_id],
                nodes=[a, b],
                locations=[location_id],
                items=[item_id],
            )

    def test_insert_new_non_pegged_edge_validates_cycle(self, conn):
        """Cycle validation runs for non-pegged_to edges: a cycle must raise."""
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        a = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=0,
        )
        b = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            bucket_sequence=1,
        )
        # b -> a exists; proposing a depends_on b would create a cycle.
        existing = _make_edge_row(
            conn, edge_type="depends_on", from_node_id=b, to_node_id=a,
            scenario_id=scenario_id,
        )
        conn.commit()

        edge = Edge(
            edge_id=uuid4(),
            edge_type="depends_on",
            from_node_id=a,
            to_node_id=b,
            scenario_id=scenario_id,
        )
        try:
            store = GraphStore(conn)
            with pytest.raises(CycleDetectedError):
                store.upsert_edge(edge)
        finally:
            conn.rollback()
            _teardown(
                conn,
                edges=[existing],
                nodes=[a, b],
                locations=[location_id],
                items=[item_id],
            )

    def test_insert_new_pegged_edge_skips_cycle_validation(self, conn):
        """pegged_to edges bypass cycle detection: even a back-edge inserts cleanly."""
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        pi = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        demand = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        # Set up a cycle that would otherwise trip validate_no_cycle: pi -> demand.
        existing = _make_edge_row(
            conn, edge_type="consumes", from_node_id=pi, to_node_id=demand,
            scenario_id=scenario_id,
        )
        conn.commit()

        edge = Edge(
            edge_id=uuid4(),
            edge_type="pegged_to",
            from_node_id=demand,
            to_node_id=pi,
            scenario_id=scenario_id,
        )
        try:
            store = GraphStore(conn)
            result_edge, created = store.upsert_edge(edge)
            conn.commit()
            assert created is True
            assert result_edge is edge

            row = conn.execute(
                "SELECT edge_id FROM edges WHERE edge_id = %s",
                (edge.edge_id,),
            ).fetchone()
            assert row is not None
        finally:
            _teardown(
                conn,
                edges=[existing, edge.edge_id],
                nodes=[pi, demand],
                locations=[location_id],
                items=[item_id],
            )


# ===========================================================================
# GraphStore — Projection series
# ===========================================================================


class TestGetProjectionSeries:
    def test_found(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        series_id = _make_series_row(
            conn, item_id=item_id, location_id=location_id, scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_projection_series(item_id, location_id, scenario_id)
            assert result is not None
            assert isinstance(result, ProjectionSeries)
            assert result.series_id == series_id
        finally:
            _teardown(
                conn,
                series=[series_id],
                locations=[location_id],
                items=[item_id],
            )

    def test_not_found(self, conn):
        store = GraphStore(conn)
        # Random IDs that won't be in projection_series — must return None.
        assert store.get_projection_series(uuid4(), uuid4(), Scenario.BASELINE_ID) is None


class TestCreateProjectionSeries:
    def test_creates_and_returns(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.create_projection_series(
                item_id=item_id,
                location_id=location_id,
                scenario_id=scenario_id,
                horizon_start=date(2025, 1, 1),
                horizon_end=date(2025, 12, 31),
            )
            conn.commit()
            assert isinstance(result, ProjectionSeries)
            assert result.item_id == item_id
            assert result.location_id == location_id

            row = conn.execute(
                "SELECT series_id FROM projection_series WHERE series_id = %s",
                (result.series_id,),
            ).fetchone()
            assert row is not None
        finally:
            conn.execute(
                "DELETE FROM projection_series "
                "WHERE item_id = %s AND location_id = %s AND scenario_id = %s",
                (item_id, location_id, scenario_id),
            )
            conn.commit()
            _teardown(
                conn,
                locations=[location_id],
                items=[item_id],
            )


class TestGetOrCreateProjectionSeries:
    def test_returns_existing(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        series_id = _make_series_row(
            conn, item_id=item_id, location_id=location_id, scenario_id=scenario_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_or_create_projection_series(
                item_id, location_id, scenario_id,
                date(2025, 1, 1), date(2025, 12, 31),
            )
            assert isinstance(result, ProjectionSeries)
            assert result.series_id == series_id
        finally:
            _teardown(
                conn,
                series=[series_id],
                locations=[location_id],
                items=[item_id],
            )

    def test_creates_when_not_existing(self, conn):
        scenario_id = Scenario.BASELINE_ID
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_or_create_projection_series(
                item_id, location_id, scenario_id,
                date(2025, 1, 1), date(2025, 12, 31),
            )
            conn.commit()
            assert isinstance(result, ProjectionSeries)

            row = conn.execute(
                "SELECT series_id FROM projection_series WHERE series_id = %s",
                (result.series_id,),
            ).fetchone()
            assert row is not None
        finally:
            conn.execute(
                "DELETE FROM projection_series "
                "WHERE item_id = %s AND location_id = %s AND scenario_id = %s",
                (item_id, location_id, scenario_id),
            )
            conn.commit()
            _teardown(
                conn,
                locations=[location_id],
                items=[item_id],
            )


# ===========================================================================
# GraphStore — Demand nodes
# ===========================================================================


class TestGetDemandNodes:
    def test_default_types(self, conn):
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"GS Demand Default {scenario_id}"),
        )
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        # One node per default demand type.
        n_fc = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            node_type="ForecastDemand",
        )
        n_co = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            node_type="CustomerOrderDemand",
        )
        n_dep = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            node_type="DependentDemand",
        )
        # Distractor: not a demand node — must be excluded.
        n_pi = _make_pi_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_demand_nodes(scenario_id)
            returned_ids = {n.node_id for n in result}
            assert returned_ids == {n_fc, n_co, n_dep}
        finally:
            _teardown(
                conn,
                nodes=[n_fc, n_co, n_dep, n_pi],
                locations=[location_id],
                items=[item_id],
            )
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_custom_types(self, conn):
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"GS Demand Custom {scenario_id}"),
        )
        item_id = _make_item(conn)
        location_id = _make_location(conn)
        # TransferDemand is allowed by the node_type CHECK constraint and is
        # NOT in the default demand set, so we can use it for the override.
        n_td = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            node_type="TransferDemand",
        )
        # Default-type demand node — must NOT show up under the override.
        n_co = _make_demand_node(
            conn, scenario_id=scenario_id, item_id=item_id, location_id=location_id,
            node_type="CustomerOrderDemand",
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            result = store.get_demand_nodes(scenario_id, node_types=("TransferDemand",))
            assert [n.node_id for n in result] == [n_td]
        finally:
            _teardown(
                conn,
                nodes=[n_td, n_co],
                locations=[location_id],
                items=[item_id],
            )
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()

    def test_empty_result(self, conn):
        scenario_id = uuid4()
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
            (scenario_id, f"GS Demand Empty {scenario_id}"),
        )
        conn.commit()

        try:
            store = GraphStore(conn)
            assert store.get_demand_nodes(scenario_id) == []
        finally:
            conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
            conn.commit()
