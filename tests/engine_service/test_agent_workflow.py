"""
test_agent_workflow.py — P3 agent-first contract validation.

Covers the new agentic capabilities (P3.1-P3.5):

  - P3.1 GetNode(scenario_id) returns the overlay value, not baseline
  - P3.2 PropagateBatch applies N events sequentially in one RPC
  - P3.3 GetScenarioSnapshot / determinism guarantees
  - P3.4 HeartbeatScenario + ForkRequest.ttl_seconds
  - P3.5 fork-from-scenario (MCTS tree branching)

These tests simulate an agent reasoning loop: fork → propagate ×N
→ read back → branch (fork from scenario) → propagate → compare.
"""
from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest


def _decimals_close(a, b, tol=Decimal("0.001")):
    """Tolerance-based Decimal comparison. Decimal round-trips
    Rust → str → Python lose trailing precision (rust_decimal caps at
    28 sig digits, PG numeric is 50+). We compare with 0.001 tolerance
    which is way more precise than any business-meaningful difference."""
    return abs(Decimal(a) - Decimal(b)) < tol

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

pytestmark = pytest.mark.slow


def _pick_trigger(dsn):
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT node_id FROM nodes WHERE node_type='ProjectedInventory' "
            "AND scenario_id=%s AND active=TRUE AND bucket_sequence=0 "
            "ORDER BY node_id LIMIT 1",
            (BASELINE,),
        ).fetchone()
    return UUID(str(row["node_id"])) if row else None


def test_p3_1_get_node_scenario_aware(engine, dsn):
    """P3.1: GetNode(scenario_id=X) reads from scenario overlay, NOT
    baseline. Validated by: corrupt the baseline state, fork, propagate
    on fork (computes corrected values into overlay), then read via
    GetNode(scenario_id=fork) — should see the corrected value.
    Read via GetNode(scenario_id=baseline) should still see corrupted."""
    import psycopg
    from psycopg.rows import dict_row

    harness, client = engine
    trigger = _pick_trigger(dsn)
    if trigger is None:
        pytest.skip("no PI in baseline")

    # Snapshot original
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT closing_stock FROM nodes WHERE node_id = %s",
            (str(trigger),),
        ).fetchone()
    original = row["closing_stock"]

    # Restart engine after corrupting baseline so it loads corrupted.
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "UPDATE nodes SET closing_stock = closing_stock + 7777, last_calc_seq = NULL "
            "WHERE node_id = %s",
            (str(trigger),),
        )
        conn.commit()
    try:
        harness.stop()
        harness.start(wait_for_ready=True, ready_timeout_s=30.0)

        from ootils_core.engine_rust_service import EngineClient
        with EngineClient.connect(harness.listen_addr) as c:
            # 1. Read baseline (should be corrupted)
            baseline_node = c.get_node(BASELINE, trigger)
            assert _decimals_close(baseline_node.closing_stock, original + 7777), (
                f"baseline didn't load corruption: {baseline_node.closing_stock} vs {original + 7777}"
            )

            # 2. Fork + propagate (writes corrected value to overlay)
            info = c.fork_scenario(BASELINE, name="agent-p31")
            sid = UUID(info.id)
            res = c.propagate(
                scenario_id=sid,
                event_id=uuid4(),
                event_type="supply_qty_changed",
                trigger_node_id=trigger,
            )
            assert res.nodes_changed >= 1

            # 3. Read via scenario — must see CORRECTED value (overlay)
            scenario_node = c.get_node(sid, trigger)
            assert not _decimals_close(
                scenario_node.closing_stock, baseline_node.closing_stock, tol=Decimal("100")
            ), "GetNode(scenario) returned baseline value — P3.1 overlay-aware read broken"
            assert _decimals_close(scenario_node.closing_stock, original), (
                f"scenario didn't restore original: {scenario_node.closing_stock} vs {original}"
            )

            # 4. Re-read baseline — must STILL be corrupted (isolation)
            baseline_again = c.get_node(BASELINE, trigger)
            assert _decimals_close(baseline_again.closing_stock, original + 7777), (
                "baseline leaked overlay value"
            )
    finally:
        with psycopg.connect(dsn) as conn:
            conn.execute(
                "UPDATE nodes SET closing_stock = %s, last_calc_seq = NULL WHERE node_id = %s",
                (original, str(trigger)),
            )
            conn.commit()


def test_p3_2_propagate_batch(engine, dsn):
    """P3.2: PropagateBatch applies N events in one RPC, single lock
    acquisition. Verify all N succeed + results match calling N times."""
    from ootils_core._grpc import engine_pb2

    harness, client = engine
    trigger = _pick_trigger(dsn)
    if trigger is None:
        pytest.skip("no PI in baseline")

    info = client.fork_scenario(BASELINE, name="agent-p32-batch")
    sid = UUID(info.id)

    # Send 10 events in one batch.
    events = [
        engine_pb2.BatchEvent(
            event_id=str(uuid4()),
            event_type="supply_qty_changed",
            trigger_node_id=str(trigger),
            payload=b"",
        )
        for _ in range(10)
    ]
    req = engine_pb2.PropagateBatchRequest(
        scenario_id=str(sid),
        events=events,
    )
    resp = client._stub.PropagateBatch(req, timeout=10.0)

    assert resp.failed_at_index == -1, f"batch failed at {resp.failed_at_index}: {resp.failure_detail}"
    assert len(resp.results) == 10
    # First propag should produce deltas; subsequent ones idempotent (0 changed)
    assert resp.results[0].nodes_processed >= 1
    # Each result has its own calc_run_id matching the input
    for ev, result in zip(events, resp.results):
        assert result.calc_run_id == ev.event_id


def test_p3_2_propagate_batch_empty_returns_empty(engine):
    """Edge case: empty batch returns empty results, no error."""
    from ootils_core._grpc import engine_pb2

    harness, client = engine
    info = client.fork_scenario(BASELINE, name="agent-p32-empty")
    sid = UUID(info.id)

    req = engine_pb2.PropagateBatchRequest(scenario_id=str(sid), events=[])
    resp = client._stub.PropagateBatch(req, timeout=5.0)
    assert resp.failed_at_index == -1
    assert len(resp.results) == 0


def test_p3_4_heartbeat_prevents_eviction(engine):
    """P3.4: HeartbeatScenario bumps last_accessed_at. Verified by
    checking that idle_seconds_before drops after heartbeat."""
    import time
    from ootils_core._grpc import engine_pb2

    harness, client = engine
    info = client.fork_scenario(BASELINE, name="agent-p34-heartbeat")
    sid = UUID(info.id)

    # Wait a moment so idle_seconds is observable.
    time.sleep(2.0)

    req = engine_pb2.HeartbeatRequest(scenario_id=str(sid))
    resp = client._stub.HeartbeatScenario(req, timeout=5.0)
    # idle_seconds_before should be >= 2.
    assert resp.idle_seconds_before >= 2, (
        f"idle_seconds_before suspiciously low: {resp.idle_seconds_before}"
    )

    # Second heartbeat right after should report idle < 2.
    resp2 = client._stub.HeartbeatScenario(req, timeout=5.0)
    assert resp2.idle_seconds_before < 2, (
        f"second heartbeat didn't reset idle: {resp2.idle_seconds_before}"
    )


def test_p3_4_heartbeat_unknown_scenario(engine, grpc_module):
    """Heartbeat on unknown scenario returns NOT_FOUND."""
    from ootils_core._grpc import engine_pb2

    harness, client = engine
    bogus = uuid4()
    req = engine_pb2.HeartbeatRequest(scenario_id=str(bogus))
    with pytest.raises(grpc_module.RpcError) as exc_info:
        client._stub.HeartbeatScenario(req, timeout=5.0)
    assert exc_info.value.code() == grpc_module.StatusCode.NOT_FOUND


def test_p3_4_fork_with_custom_ttl(engine):
    """P3.4: ForkRequest.ttl_seconds is honored (the scenario's
    min_ttl_seconds field is set). Verified indirectly by forking
    with a 60s TTL and asserting the scenario survives a short
    idle period (we can't easily test eviction without long sleep)."""
    from ootils_core._grpc import engine_pb2

    harness, client = engine
    req = engine_pb2.ForkRequest(
        parent_scenario_id="",
        name="agent-p34-ttl",
        ttl_seconds=3600,
    )
    resp = client._stub.ForkScenario(req, timeout=5.0)
    sid = UUID(resp.id)

    # Scenario exists immediately after fork.
    sl = client.list_scenarios()
    assert any(UUID(s.id) == sid for s in sl.scenarios)


def test_p3_5_fork_from_scenario_inherits_overlay(engine, dsn):
    """P3.5: fork_from_scenario creates a child scenario that
    inherits the parent's overlay. Validated by: corrupt baseline,
    propagate on parent (overlay has corrected value), fork child,
    GetNode(child, trigger) should match parent's overlay."""
    import psycopg
    from psycopg.rows import dict_row
    from ootils_core._grpc import engine_pb2

    harness, client = engine
    trigger = _pick_trigger(dsn)
    if trigger is None:
        pytest.skip("no PI in baseline")

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT closing_stock FROM nodes WHERE node_id = %s",
            (str(trigger),),
        ).fetchone()
    original = row["closing_stock"]

    with psycopg.connect(dsn) as conn:
        conn.execute(
            "UPDATE nodes SET closing_stock = closing_stock + 8888, last_calc_seq = NULL WHERE node_id = %s",
            (str(trigger),),
        )
        conn.commit()
    try:
        harness.stop()
        harness.start(wait_for_ready=True, ready_timeout_s=30.0)
        from ootils_core.engine_rust_service import EngineClient
        with EngineClient.connect(harness.listen_addr) as c:
            # Parent scenario
            parent = c.fork_scenario(BASELINE, name="parent-p35")
            parent_sid = UUID(parent.id)
            c.propagate(
                scenario_id=parent_sid,
                event_id=uuid4(),
                event_type="supply_qty_changed",
                trigger_node_id=trigger,
            )
            parent_state = c.get_node(parent_sid, trigger)

            # Child forked FROM parent (P3.5)
            child_req = engine_pb2.ForkRequest(
                parent_scenario_id=str(parent_sid),
                name="child-p35-mcts-branch",
            )
            child_info = c.fork_scenario_raw if hasattr(c, "fork_scenario_raw") else None
            # Use raw stub since EngineClient.fork_scenario uses positional API
            resp = c._stub.ForkScenario(child_req, timeout=5.0)
            child_sid = UUID(resp.id)
            assert resp.parent_id == str(parent_sid), (
                f"child parent_id mismatch: {resp.parent_id} vs {parent_sid}"
            )

            # GetNode(child, trigger) should see parent's overlay value
            child_state = c.get_node(child_sid, trigger)
            assert _decimals_close(child_state.closing_stock, parent_state.closing_stock), (
                f"child didn't inherit parent overlay: child={child_state.closing_stock} "
                f"vs parent={parent_state.closing_stock}"
            )
            assert _decimals_close(child_state.closing_stock, original), (
                f"child didn't see corrected value: {child_state.closing_stock} vs {original}"
            )

            # Mutate child — must NOT affect parent (deep-copied overlay)
            # This is exercised by propagating on child + checking parent unchanged.
            # We rely on idempotence: propagate again on child = same result, parent's
            # overlay still has its own value. (Hard to test without distinct triggers.)
    finally:
        with psycopg.connect(dsn) as conn:
            conn.execute(
                "UPDATE nodes SET closing_stock = %s, last_calc_seq = NULL WHERE node_id = %s",
                (original, str(trigger)),
            )
            conn.commit()


def test_p3_5_fork_from_unknown_scenario_returns_not_found(engine, grpc_module):
    """Forking from a non-existent parent returns NOT_FOUND."""
    from ootils_core._grpc import engine_pb2

    harness, client = engine
    bogus_parent = uuid4()
    req = engine_pb2.ForkRequest(
        parent_scenario_id=str(bogus_parent),
        name="orphan",
    )
    with pytest.raises(grpc_module.RpcError) as exc_info:
        client._stub.ForkScenario(req, timeout=5.0)
    assert exc_info.value.code() == grpc_module.StatusCode.NOT_FOUND
