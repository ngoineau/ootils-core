"""
test_correctness.py — basic correctness of the engine service.

Health, Metrics, GetNode, simple Propagate. All read-only-style tests
can share the module-scoped `engine_session` fixture for speed.
"""
from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def test_health_returns_serving(engine_session):
    _, client = engine_session
    h = client.health()
    # status enum 1 = SERVING
    assert h.status == 1, f"engine not serving: {h.detail}"
    assert "baseline loaded" in h.detail
    assert h.uptime_seconds >= 0


def test_metrics_baseline_loaded(engine_session):
    _, client = engine_session
    m = client.metrics()
    assert m.baseline_graph_bytes > 0
    # Sane upper bound on profile L (we measured 76 MB; allow 3× for safety).
    assert m.baseline_graph_bytes < 300 * 1024 * 1024


def test_get_node_returns_valid_state(engine_session, pick_pi_node):
    _, client = engine_session
    node_id, item_id, loc_id = pick_pi_node()
    state = client.get_node(BASELINE, node_id)
    assert state.node_id == node_id
    assert state.node_type == "ProjectedInventory"
    assert state.item_id == item_id
    assert state.location_id == loc_id
    # Decimal fields parse cleanly + are non-negative-ish (closing_stock
    # can be negative if there's a shortage, that's OK).
    assert isinstance(state.opening_stock, Decimal)
    assert isinstance(state.closing_stock, Decimal)


def test_get_node_unknown_uuid_returns_not_found(engine_session, grpc_module):
    _, client = engine_session
    bogus = UUID("99999999-9999-9999-9999-999999999999")
    with pytest.raises(grpc_module.RpcError) as exc_info:
        client.get_node(BASELINE, bogus)
    assert exc_info.value.code() == grpc_module.StatusCode.NOT_FOUND


def test_propagate_smoke(engine_session, pick_pi_node):
    _, client = engine_session
    trigger, _, _ = pick_pi_node()
    res = client.propagate(
        scenario_id=BASELINE,
        event_id=uuid4(),
        event_type="supply_qty_changed",
        trigger_node_id=trigger,
    )
    # On the seeded baseline, expect a series of 90 PIs to be processed
    # (one PI series = 90 daily buckets).
    assert res.nodes_processed >= 1, "propagation processed nothing"
    assert res.nodes_processed <= 1_000, "propagation processed suspiciously many nodes"
    # Compute is sub-millisecond per propagation.
    assert res.compute_ms < 10.0, f"compute too slow: {res.compute_ms} ms"


def test_propagate_with_unknown_trigger_returns_not_found(engine_session, grpc_module):
    _, client = engine_session
    bogus = UUID("11111111-2222-3333-4444-555555555555")
    with pytest.raises(grpc_module.RpcError) as exc_info:
        client.propagate(
            scenario_id=BASELINE,
            event_id=uuid4(),
            event_type="supply_qty_changed",
            trigger_node_id=bogus,
        )
    assert exc_info.value.code() == grpc_module.StatusCode.NOT_FOUND
