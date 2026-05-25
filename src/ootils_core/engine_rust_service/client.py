"""
client.py — thin synchronous gRPC client around the engine service.

Wraps the generated stub with:
- typed Python returns (NamedTuples) instead of raw protobuf messages
- explicit UUID + Decimal conversions at the boundary
- a sane default 5-second deadline (the engine commits to < 100 ms on
  full propagation; if we ever wait 5 s something is very wrong).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from uuid import UUID

import grpc
from google.protobuf import empty_pb2

from ootils_core._grpc import engine_pb2, engine_pb2_grpc

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S = 5.0


@dataclass
class PropagationResult:
    calc_run_id: str
    nodes_processed: int
    nodes_changed: int
    shortages_detected: int
    total_ms: float
    compute_ms: float
    wal_fsync_ms: float


@dataclass
class NodeState:
    node_id: UUID
    node_type: str
    item_id: Optional[UUID]
    location_id: Optional[UUID]
    opening_stock: Decimal
    inflows: Decimal
    outflows: Decimal
    closing_stock: Decimal
    has_shortage: bool
    shortage_qty: Decimal


class EngineClient:
    """Synchronous client for the ootils-engine gRPC service.

    Holds one gRPC channel; safe to share across threads (grpc Channel
    is thread-safe by design)."""

    def __init__(self, channel: grpc.Channel) -> None:
        self._channel = channel
        self._stub = engine_pb2_grpc.EngineStub(channel)

    @classmethod
    def connect(cls, addr: str = "127.0.0.1:50051") -> "EngineClient":
        """Open an insecure channel — phase 6 ships local dev/test only.
        Phase 8 production rollout adds TLS + auth."""
        channel = grpc.insecure_channel(
            addr,
            options=[
                ("grpc.max_receive_message_length", 256 * 1024 * 1024),
                ("grpc.max_send_message_length", 256 * 1024 * 1024),
            ],
        )
        return cls(channel)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> "EngineClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # -- Health / Metrics ---------------------------------------------

    def health(self, timeout: float = DEFAULT_TIMEOUT_S) -> engine_pb2.HealthStatus:
        return self._stub.Health(empty_pb2.Empty(), timeout=timeout)

    def metrics(self, timeout: float = DEFAULT_TIMEOUT_S) -> engine_pb2.EngineMetrics:
        return self._stub.Metrics(empty_pb2.Empty(), timeout=timeout)

    # -- Reads ---------------------------------------------------------

    def get_node(self, scenario_id: UUID, node_id: UUID, timeout: float = DEFAULT_TIMEOUT_S) -> NodeState:
        req = engine_pb2.NodeQuery(
            scenario_id=str(scenario_id),
            node_id=str(node_id),
        )
        resp = self._stub.GetNode(req, timeout=timeout)
        return NodeState(
            node_id=UUID(resp.node_id),
            node_type=resp.node_type,
            item_id=UUID(resp.item_id) if resp.item_id else None,
            location_id=UUID(resp.location_id) if resp.location_id else None,
            opening_stock=Decimal(resp.opening_stock),
            inflows=Decimal(resp.inflows),
            outflows=Decimal(resp.outflows),
            closing_stock=Decimal(resp.closing_stock),
            has_shortage=bool(resp.has_shortage),
            shortage_qty=Decimal(resp.shortage_qty),
        )

    # -- Mutations -----------------------------------------------------

    def propagate(
        self,
        scenario_id: UUID,
        event_id: UUID,
        event_type: str,
        trigger_node_id: UUID,
        payload: bytes = b"",
        timeout: float = DEFAULT_TIMEOUT_S,
    ) -> PropagationResult:
        req = engine_pb2.PropagateRequest(
            scenario_id=str(scenario_id),
            event_id=str(event_id),
            event_type=event_type,
            trigger_node_id=str(trigger_node_id),
            payload=payload,
        )
        resp = self._stub.Propagate(req, timeout=timeout)
        timing = resp.timing
        return PropagationResult(
            calc_run_id=resp.calc_run_id,
            nodes_processed=resp.nodes_processed,
            nodes_changed=resp.nodes_changed,
            shortages_detected=resp.shortages_detected,
            total_ms=timing.total_us / 1000.0 if timing else 0.0,
            compute_ms=timing.compute_us / 1000.0 if timing else 0.0,
            wal_fsync_ms=timing.wal_fsync_us / 1000.0 if timing else 0.0,
        )

    def fork_scenario(
        self,
        parent_scenario_id: UUID,
        name: str = "",
        timeout: float = DEFAULT_TIMEOUT_S,
    ) -> engine_pb2.ScenarioInfo:
        req = engine_pb2.ForkRequest(
            parent_scenario_id=str(parent_scenario_id),
            name=name,
        )
        return self._stub.ForkScenario(req, timeout=timeout)

    def list_scenarios(self, timeout: float = DEFAULT_TIMEOUT_S) -> engine_pb2.ScenarioList:
        return self._stub.ListScenarios(empty_pb2.Empty(), timeout=timeout)
