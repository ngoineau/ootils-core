"""
Transparent SCD2 helpers (ADR-014 D3).

Pattern: the client pushes its current state for an entity that has
`effective_from` / `effective_to` columns. The API:

  - looks up the currently active row (`effective_to IS NULL`),
  - if every pushed field matches that row → no-op (idempotent),
  - if any field differs and the active row was created TODAY →
        UPDATE in place (cannot rotate within the same day — the
        DB constraint forbids `effective_to <= effective_from`),
  - if any field differs and the active row predates today →
        UPDATE active SET effective_to = today - 1 day
        INSERT new row WITH effective_from = today, effective_to = NULL

Pure-function logic; no DB access. The caller orchestrates the SQL.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, Mapping


class Scd2Action(str, Enum):
    """What the caller should do given the current state."""

    CREATED = "created"             # no prior active row; INSERT
    NOOP = "noop"                   # incoming matches active; do nothing
    UPDATED_INPLACE = "updated_inplace"  # same-day change; UPDATE active row
    ROTATED = "rotated"             # cross-day change; close active + INSERT new


@dataclass(frozen=True)
class Scd2Decision:
    """Result of deciding what to do with one incoming row."""

    action: Scd2Action
    changed_fields: dict[str, Any]
    """Subset of `tracked` keys whose new value differs from the active row.
    Empty when action == NOOP or CREATED."""


def diff_tracked_fields(
    active_row: Mapping[str, Any] | None,
    incoming: Mapping[str, Any],
    tracked: list[str],
) -> dict[str, Any]:
    """Return {field: incoming_value} for every tracked field that the
    client pushed AND whose value differs from `active_row`.

    Fields the client did NOT push (key absent from `incoming`) are
    ignored: SCD2 transparent treats omission as "keep current value".

    If `active_row` is None, every present tracked field is returned —
    nothing to compare against, so anything pushed is a change.
    """
    diff: dict[str, Any] = {}
    for key in tracked:
        if key not in incoming:
            continue
        if active_row is None:
            diff[key] = incoming[key]
            continue
        if active_row.get(key) != incoming[key]:
            diff[key] = incoming[key]
    return diff


def decide_action(
    active_row: Mapping[str, Any] | None,
    incoming: Mapping[str, Any],
    tracked: list[str],
    today: date,
) -> Scd2Decision:
    """Pick the SCD2 action for one incoming row.

    Args:
        active_row: The currently active row from DB
            (with `effective_from` key), or None if no history exists.
        incoming: The fields the client pushed (excludes effective_*).
        tracked: List of field names that participate in change
            detection. Typically every business value field; NOT
            metadata like created_at / updated_at / param_id.
        today: The date to use as `effective_from` / cut-off. Tests
            pass a fixed date; production passes `date.today()`.

    Returns:
        Scd2Decision with the action and the changed fields.
    """
    if active_row is None:
        # No prior version: every present tracked field is fresh content
        return Scd2Decision(
            action=Scd2Action.CREATED,
            changed_fields=diff_tracked_fields(None, incoming, tracked),
        )

    changed = diff_tracked_fields(active_row, incoming, tracked)
    if not changed:
        return Scd2Decision(action=Scd2Action.NOOP, changed_fields={})

    # Cross-day vs same-day rollover decision (ADR-014 D3 trade-off)
    active_from = active_row.get("effective_from")
    if active_from == today:
        return Scd2Decision(action=Scd2Action.UPDATED_INPLACE, changed_fields=changed)

    return Scd2Decision(action=Scd2Action.ROTATED, changed_fields=changed)
