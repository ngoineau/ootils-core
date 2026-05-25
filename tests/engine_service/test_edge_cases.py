"""
test_edge_cases.py — boundary conditions + malformed inputs.

Validate the engine fails gracefully (proper gRPC error codes) rather
than panicking, and that it handles unusual-but-valid inputs.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import pytest

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def test_propagate_with_malformed_uuid_returns_invalid_argument(engine_session, grpc_module):
    """Phase 6 client converts UUIDs to strings; if a malformed string
    sneaks in, the engine should respond with INVALID_ARGUMENT."""
    _, client = engine_session
    # Bypass the typed client to send bogus strings directly.
    from ootils_core._grpc import engine_pb2

    bogus_req = engine_pb2.PropagateRequest(
        scenario_id="not-a-uuid",
        event_id=str(uuid4()),
        event_type="supply_qty_changed",
        trigger_node_id="also-not-a-uuid",
    )
    with pytest.raises(grpc_module.RpcError) as exc_info:
        client._stub.Propagate(bogus_req, timeout=2.0)
    assert exc_info.value.code() == grpc_module.StatusCode.INVALID_ARGUMENT


def test_get_node_with_malformed_uuid_returns_invalid_argument(engine_session, grpc_module):
    _, client = engine_session
    from ootils_core._grpc import engine_pb2

    bogus_req = engine_pb2.NodeQuery(
        scenario_id=str(BASELINE),
        node_id="bzzt",
    )
    with pytest.raises(grpc_module.RpcError) as exc_info:
        client._stub.GetNode(bogus_req, timeout=2.0)
    assert exc_info.value.code() == grpc_module.StatusCode.INVALID_ARGUMENT


def test_propagate_returns_zero_dirty_for_isolated_trigger(engine_session, dsn, grpc_module):
    """Trigger node whose (item, location) has no PI buckets in the
    baseline scenario should propagate 0 PIs cleanly. Skipped if no
    such orphan node exists in the seeded DB."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        # Pick any non-PI node — its (item, location) usually has no
        # PI buckets in the FG-at-DC projection graph.
        row = conn.execute(
            "SELECT n.node_id FROM nodes n "
            "WHERE n.scenario_id=%s AND n.active=TRUE "
            "AND n.node_type IN ('PurchaseOrderSupply','WorkOrderSupply') "
            "AND NOT EXISTS ( "
            "  SELECT 1 FROM nodes p "
            "  WHERE p.node_type='ProjectedInventory' "
            "    AND p.scenario_id = n.scenario_id "
            "    AND p.item_id = n.item_id "
            "    AND p.location_id = n.location_id "
            "    AND p.active = TRUE "
            ") LIMIT 1",
            (BASELINE,),
        ).fetchone()
    if row is None:
        pytest.skip("no orphan supply node in baseline (all have PI coverage)")

    _, client = engine_session
    trigger = UUID(str(row["node_id"]))
    res = client.propagate(
        scenario_id=BASELINE,
        event_id=uuid4(),
        event_type="supply_qty_changed",
        trigger_node_id=trigger,
    )
    # Orphan trigger → 0 PIs to recompute.
    assert res.nodes_processed == 0
    assert res.shortages_detected == 0


def test_concurrent_propagate_returns_deterministic_state(engine_session, pick_pi_node):
    """Two back-to-back Propagate calls on the same trigger should
    leave the node in the same state (idempotent on a fixed-point
    baseline)."""
    _, client = engine_session
    trigger, _, _ = pick_pi_node()

    # First propagation.
    client.propagate(
        scenario_id=BASELINE,
        event_id=uuid4(),
        event_type="supply_qty_changed",
        trigger_node_id=trigger,
    )
    state_1 = client.get_node(BASELINE, trigger)

    # Second propagation on the same trigger.
    client.propagate(
        scenario_id=BASELINE,
        event_id=uuid4(),
        event_type="supply_qty_changed",
        trigger_node_id=trigger,
    )
    state_2 = client.get_node(BASELINE, trigger)

    # Idempotency — same inputs → same outputs.
    assert state_1.closing_stock == state_2.closing_stock
    assert state_1.has_shortage == state_2.has_shortage
