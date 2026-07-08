"""
Fleet-event emission (chantier AN-1, #401).

emit: the single ``emit_stream_event`` helper — ONE typed ``events`` INSERT on
    the caller's connection, in the caller's transaction, for the five
    fleet-emission types (migration 071). North Star "Streamable": a
    state-changing capability that writes no ``events`` row is invisible to
    ``GET /v1/stream``. Granularity is RUN, never per-item (ADR-027).
"""
from ootils_core.engine.events.emit import (
    FLEET_EVENT_TYPES,
    emit_recommendation_created_for_run,
    emit_stream_event,
)

__all__ = [
    "FLEET_EVENT_TYPES",
    "emit_recommendation_created_for_run",
    "emit_stream_event",
]
