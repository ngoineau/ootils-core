"""
engine_rust_service — Python client + lifecycle helpers for the
standalone Rust engine service (ADR-017 Architecture B).

Use it like this:

    from ootils_core.engine_rust_service import EngineClient
    client = EngineClient.connect("127.0.0.1:50051")
    health = client.health()
    print(health.detail)

    result = client.propagate(
        scenario_id=BASELINE_SCENARIO_ID,
        event_id=event_id,
        event_type="supply_qty_changed",
        trigger_node_id=trigger_id,
    )
    print(result.nodes_processed, result.shortages_detected)

If you want to drive the service from tests, see `EngineHarness` which
starts the engine binary as a subprocess + tears it down cleanly.
"""

from .client import EngineClient
from .harness import EngineHarness

__all__ = ["EngineClient", "EngineHarness"]
