"""
Clock injection for the kernel (ADR-003).

ADR-003 makes determinism a hard constraint. Wall-clock reads inside the
kernel (`datetime.now()`) break that contract because two runs over the
same input state then produce different `created_at` / `updated_at` /
`run_at` values. Tests that diff or replay engine output can't rely on
equality.

The kernel classes accept an optional ``clock`` in their constructor.
Production code passes nothing — `SystemClock` (wall-clock UTC) is the
default. Tests pass `FrozenClock(frozen_at=...)` to pin time.

Usage:
    from ootils_core.engine.kernel._clock import FrozenClock
    from datetime import datetime, timezone

    clock = FrozenClock(datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc))
    engine = AllocationEngine(clock=clock)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    """A source of the current time. All implementations return UTC-aware datetimes."""

    def now(self) -> datetime: ...


class SystemClock:
    """Real wall-clock — the production default."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class FrozenClock:
    """Returns a fixed datetime — for deterministic tests."""

    def __init__(self, frozen_at: datetime) -> None:
        if frozen_at.tzinfo is None:
            raise ValueError("FrozenClock requires a tz-aware datetime")
        self._frozen_at = frozen_at

    def now(self) -> datetime:
        return self._frozen_at
