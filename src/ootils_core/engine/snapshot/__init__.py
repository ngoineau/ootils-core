"""
Inventory snapshot engine — the proof-machine historisation backbone
(chantier #393 A3-PR1, ADR-030).

capture: SELECT-only per-(item, location) on-hand capture (``capture_snapshot``,
    pure/DB-read) + the single idempotent upsert writer (``persist_snapshot``)
    into ``inventory_snapshots`` (migration 067). Per-site, never pooled (the
    DRP lesson) — the snapshot is a site-level stock fact.
"""
from ootils_core.engine.snapshot.capture import (
    VALID_SOURCES,
    SnapshotRow,
    capture_snapshot,
    persist_snapshot,
)

__all__ = [
    "VALID_SOURCES",
    "SnapshotRow",
    "capture_snapshot",
    "persist_snapshot",
]
