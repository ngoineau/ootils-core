"""
temporal/ — Temporal Bridge (presentation layer).

Components:
  - TemporalBridge: read-only presentation layer for aggregate/disaggregate views.

The bridge never writes directly to the DB — all writes go through GraphStore.

Note: ZoneTransitionEngine (elastic-time calendar roll-forward) was buried
2026-07-19 — the elastic-time model was never shipped. See ADR-002d and
docs/CARTE-CODE.md. bridge.py itself is frozen (candidate #433 re-aggregation).
"""
from ootils_core.engine.kernel.temporal.bridge import TemporalBridge

__all__ = ["TemporalBridge"]
