"""
temporal/ — Temporal Bridge and Zone Transition Engine.

Components:
  - TemporalBridge: read-only presentation layer for aggregate/disaggregate views.
  - ZoneTransitionEngine: calendar-triggered zone roll-forward jobs.

Neither component writes directly to the DB — all writes go through GraphStore.
"""
from ootils_core.engine.kernel.temporal.bridge import TemporalBridge
from ootils_core.engine.kernel.temporal.zone_transition import ZoneTransitionEngine

__all__ = ["TemporalBridge", "ZoneTransitionEngine"]
