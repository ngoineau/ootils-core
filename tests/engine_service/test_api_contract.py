"""
test_api_contract.py — strict API-contract tests (Cluster C of REVIEW
audit response).

Covers:
- F-006: Propagate rejects non-baseline scenario_id with UNIMPLEMENTED
  (until ADR-018 lands).
- F-015: Propagate rejects malformed event_id with INVALID_ARGUMENT
  (no silent fallback to a random UUID that breaks the audit chain).
- F-016: server-side gRPC message size limits are explicitly set to
  match the Python client (256 MB) — sanity-check via a moderately
  large field.

Each test uses raw protobuf when needed to bypass the typed client's
UUID validation (we WANT to feed bad input here).
"""
from __future__ import annotations

from uuid import UUID, uuid4

import pytest

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def test_propagate_rejects_non_baseline_scenario_id(engine_session, grpc_module, pick_pi_node):
    """F-006: a scenario_id that isn't baseline must NOT silently mutate
    the baseline. Until per-scenario propagation lands (ADR-018), the
    engine must reject the call with Unimplemented."""
    _, client = engine_session
    trigger, _, _ = pick_pi_node()
    non_baseline = UUID("11111111-1111-1111-1111-111111111111")
    with pytest.raises(grpc_module.RpcError) as exc_info:
        client.propagate(
            scenario_id=non_baseline,
            event_id=uuid4(),
            event_type="supply_qty_changed",
            trigger_node_id=trigger,
        )
    assert exc_info.value.code() == grpc_module.StatusCode.UNIMPLEMENTED
    assert "ADR-018" in exc_info.value.details()


def test_propagate_accepts_baseline_scenario_id_explicitly(engine_session, pick_pi_node):
    """F-006 sanity: when scenario_id IS baseline, propagation proceeds
    normally (this is the only currently-supported path)."""
    _, client = engine_session
    trigger, _, _ = pick_pi_node()
    res = client.propagate(
        scenario_id=BASELINE,
        event_id=uuid4(),
        event_type="supply_qty_changed",
        trigger_node_id=trigger,
    )
    assert res.nodes_processed >= 1


def test_propagate_rejects_malformed_event_id(engine_session, grpc_module, pick_pi_node):
    """F-015: bad event_id must surface as INVALID_ARGUMENT, NOT
    silently substitute a fresh UUID (which would break audit chain
    between events table and calc_runs table)."""
    from ootils_core._grpc import engine_pb2

    _, client = engine_session
    trigger, _, _ = pick_pi_node()
    # Bypass the typed client to send a bad event_id string.
    req = engine_pb2.PropagateRequest(
        scenario_id=str(BASELINE),
        event_id="not-a-uuid",
        event_type="supply_qty_changed",
        trigger_node_id=str(trigger),
        payload=b"",
    )
    with pytest.raises(grpc_module.RpcError) as exc_info:
        client._stub.Propagate(req, timeout=5.0)
    assert exc_info.value.code() == grpc_module.StatusCode.INVALID_ARGUMENT
    assert "event_id" in exc_info.value.details()


def test_propagate_empty_event_id_generates_calc_run_id(engine_session, pick_pi_node):
    """F-015: an empty event_id is the documented 'engine, generate one
    for me' shortcut. The response's calc_run_id must be populated with
    the generated value — including on no-op propagations
    (reviewer B2: audit chain must hold even when nothing was dirty)."""
    from ootils_core._grpc import engine_pb2

    _, client = engine_session
    trigger, _, _ = pick_pi_node()
    req = engine_pb2.PropagateRequest(
        scenario_id=str(BASELINE),
        event_id="",  # ask engine to generate
        event_type="supply_qty_changed",
        trigger_node_id=str(trigger),
        payload=b"",
    )
    resp = client._stub.Propagate(req, timeout=5.0)
    # B2 contract: calc_run_id ALWAYS populated when the engine
    # generated one, regardless of nodes_processed.
    assert resp.calc_run_id, "engine generated no calc_run_id"
    UUID(resp.calc_run_id)  # parse-able


def test_propagate_event_id_round_trips_when_valid(engine_session, pick_pi_node):
    """F-015 + reviewer B2: a valid event_id is echoed verbatim as
    calc_run_id regardless of whether the propagation produced
    deltas. The audit chain (event → calc_run) must hold for no-op
    events too."""
    _, client = engine_session
    trigger, _, _ = pick_pi_node()
    event_id = uuid4()
    res = client.propagate(
        scenario_id=BASELINE,
        event_id=event_id,
        event_type="supply_qty_changed",
        trigger_node_id=trigger,
    )
    assert UUID(res.calc_run_id) == event_id, (
        f"calc_run_id changed: expected {event_id}, got {res.calc_run_id}"
    )


def test_propagate_large_payload_accepted(engine_session, pick_pi_node):
    """F-016: the engine accepts payloads up to ~256 MB (matching the
    client's grpc.max_send_message_length). 8 MB is a comfortable
    over-the-default test value — would be rejected by tonic's default
    4 MB cap if the server-side limit wasn't lifted in main.rs."""
    from ootils_core._grpc import engine_pb2

    _, client = engine_session
    trigger, _, _ = pick_pi_node()
    big_payload = b"x" * (8 * 1024 * 1024)  # 8 MB
    req = engine_pb2.PropagateRequest(
        scenario_id=str(BASELINE),
        event_id=str(uuid4()),
        event_type="supply_qty_changed",
        trigger_node_id=str(trigger),
        payload=big_payload,
    )
    # No raise = server accepted the message.
    resp = client._stub.Propagate(req, timeout=15.0)
    assert resp.nodes_processed >= 1
