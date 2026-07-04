"""
l4_rules.py — DQ Level 4 (cross-batch / cross-entity rules).

L4 catches errors that are invisible to L1/L2/L3 because they require
either looking at the WHOLE batch at once (duplicates) or joining
against canonical tables (orphans, inactive references).

Examples L4 catches but L1-L3 don't:
  - same external_id appears twice in the same uploaded file
  - external_id collides with a still-pending batch from another upload
  - supplier_items references a supplier whose status is now 'blocked'
  - items being introduced without any matching item_planning_params

Unlike L3 (pure, DB-free), L4 rules DO touch Postgres. They are still
restricted to rows that passed L1 + L2 + L3 (no error issues) to avoid
cascading noise.

Adding a new rule:
  1. Write a function `_l4_<name>(rows, batch_id, db) -> list[DQIssue]`
  2. Register it under the relevant entity_type(s) in `_L4_RULES`
  3. Add a test in tests/test_dq_l4_rules.py
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from uuid import UUID

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.dq.engine import DQIssue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _issue(
    row_id: UUID,
    row_number: int,
    rule_code: str,
    severity: str,
    field_name: str | None,
    raw_value,
    message: str,
) -> DQIssue:
    return DQIssue(
        row_id=row_id,
        row_number=row_number,
        dq_level=4,
        rule_code=rule_code,
        severity=severity,
        field_name=field_name,
        raw_value=None if raw_value is None else str(raw_value)[:255],
        message=message,
    )


# ---------------------------------------------------------------------------
# Rule: intra-batch duplicate external_id
# ---------------------------------------------------------------------------


def _l4_duplicate_external_id(
    rows: list[tuple[UUID, int, dict]],
    batch_id: UUID,  # noqa: ARG001 (signature uniform across rules)
    db: DictRowConnection,  # noqa: ARG001
    *,
    field: str = "external_id",
) -> list[DQIssue]:
    """Same external_id appears 2+ times in the rows of this batch.

    Severity: error (every row with the dup external_id is flagged).
    The file must be deduplicated before re-upload.
    """
    # Count occurrences of each external_id (ignoring missing/empty,
    # already caught by L1_MISSING_FIELD)
    counts: Counter[str] = Counter()
    for _, _, content in rows:
        val = content.get(field)
        if val:
            counts[val] += 1

    duplicates = {v for v, n in counts.items() if n >= 2}
    if not duplicates:
        return []

    out: list[DQIssue] = []
    for row_id, row_number, content in rows:
        val = content.get(field)
        if val and val in duplicates:
            out.append(_issue(
                row_id, row_number, "L4_DUPLICATE_EXTERNAL_ID", "error",
                field, val,
                f"{field}={val!r} appears multiple times in this batch ({counts[val]} occurrences)",
            ))
    return out


# ---------------------------------------------------------------------------
# Rule: cross-batch collision (same external_id in another pending batch)
# ---------------------------------------------------------------------------


def _l4_inter_batch_collision(
    rows: list[tuple[UUID, int, dict]],
    batch_id: UUID,
    db: DictRowConnection,
    *,
    field: str = "external_id",
) -> list[DQIssue]:
    """An external_id in this batch also appears in another batch
    that is still pending / validated (i.e. not yet imported or rejected).

    Severity: warning. Approving this batch will supersede the other —
    the operator should know.
    """
    external_ids = {
        content.get(field)
        for _, _, content in rows
        if content.get(field)
    }
    if not external_ids:
        return []

    # Look in ingest_rows of OTHER batches that are not terminal.
    # We match by col_01 since that's the positional column the staging
    # loader puts external_id into (it's always the first column per the
    # template convention).
    rows_in_db = db.execute(
        """
        SELECT DISTINCT r.col_01
        FROM ingest_rows r
        JOIN ingest_batches b ON b.batch_id = r.batch_id
        WHERE r.batch_id != %s
          AND b.status NOT IN ('imported', 'rejected')
          AND r.col_01 = ANY(%s)
        """,
        (batch_id, list(external_ids)),
    ).fetchall()
    colliding = {r["col_01"] if isinstance(r, dict) else r[0] for r in rows_in_db}
    if not colliding:
        return []

    out: list[DQIssue] = []
    for row_id, row_number, content in rows:
        val = content.get(field)
        if val and val in colliding:
            out.append(_issue(
                row_id, row_number, "L4_INTER_BATCH_COLLISION", "warning",
                field, val,
                f"{field}={val!r} also present in another open batch; "
                "approving this one will supersede the previous",
            ))
    return out


# ---------------------------------------------------------------------------
# Rule: supplier_items pointing at non-active supplier
# ---------------------------------------------------------------------------


def _l4_supplier_inactive(
    rows: list[tuple[UUID, int, dict]],
    batch_id: UUID,  # noqa: ARG001
    db: DictRowConnection,
) -> list[DQIssue]:
    """A supplier_items row references a supplier whose status is not
    'active' (i.e. inactive or blocked). Approving would create a link
    to a supplier that can't be used for planning."""
    sup_ids = {
        content.get("supplier_external_id")
        for _, _, content in rows
        if content.get("supplier_external_id")
    }
    if not sup_ids:
        return []

    bad_sups = db.execute(
        """
        SELECT external_id, status FROM suppliers
        WHERE external_id = ANY(%s) AND status != 'active'
        """,
        (list(sup_ids),),
    ).fetchall()
    bad_map = {
        (r["external_id"] if isinstance(r, dict) else r[0]):
        (r["status"] if isinstance(r, dict) else r[1])
        for r in bad_sups
    }
    if not bad_map:
        return []

    out: list[DQIssue] = []
    for row_id, row_number, content in rows:
        sup_ext = content.get("supplier_external_id")
        if sup_ext and sup_ext in bad_map:
            out.append(_issue(
                row_id, row_number, "L4_SUPPLIER_INACTIVE", "error",
                "supplier_external_id", sup_ext,
                f"supplier_external_id={sup_ext!r} has status={bad_map[sup_ext]!r} "
                "(only 'active' suppliers can be linked)",
            ))
    return out


# ---------------------------------------------------------------------------
# Rule: items being introduced without item_planning_params (orphan)
# ---------------------------------------------------------------------------


def _l4_orphan_item_no_planning(
    rows: list[tuple[UUID, int, dict]],
    batch_id: UUID,  # noqa: ARG001
    db: DictRowConnection,
) -> list[DQIssue]:
    """An item in this batch has zero item_planning_params rows in the DB.

    Severity: warning. A brand-new item without planning params is valid
    (the params often arrive in a follow-up batch) but worth flagging so
    the operator can verify the planning team has the work scheduled.
    """
    ext_ids = {
        content.get("external_id")
        for _, _, content in rows
        if content.get("external_id")
    }
    if not ext_ids:
        return []

    # Items that EXIST in canonical and have zero item_planning_params rows.
    # A brand-new item (not yet in `items`) is NOT an orphan — it's just
    # being introduced; its planning rows will arrive in a later batch.
    rows_db = db.execute(
        """
        SELECT i.external_id
        FROM items i
        LEFT JOIN item_planning_params ipp ON ipp.item_id = i.item_id
        WHERE i.external_id = ANY(%s)
        GROUP BY i.external_id
        HAVING COUNT(ipp.param_id) = 0
        """,
        (list(ext_ids),),
    ).fetchall()
    orphans = {
        (r["external_id"] if isinstance(r, dict) else r[0]) for r in rows_db
    }
    if not orphans:
        return []

    out: list[DQIssue] = []
    for row_id, row_number, content in rows:
        val = content.get("external_id")
        if val and val in orphans:
            out.append(_issue(
                row_id, row_number, "L4_ORPHAN_ITEM_NO_PLANNING", "warning",
                "external_id", val,
                f"item {val!r} has no item_planning_params yet; planning team "
                "should follow up with a planning_params batch",
            ))
    return out


# ---------------------------------------------------------------------------
# Registry — entity_type -> list of rule functions
# ---------------------------------------------------------------------------


L4RuleFn = Callable[[list[tuple[UUID, int, dict]], UUID, DictRowConnection], list[DQIssue]]


# Rules that apply to every entity_type carrying an external_id
_GENERIC_RULES: list[L4RuleFn] = [
    _l4_duplicate_external_id,
    _l4_inter_batch_collision,
]

# Entity-specific rule extensions
_L4_RULES: dict[str, list[L4RuleFn]] = {
    "items":           _GENERIC_RULES + [_l4_orphan_item_no_planning],
    "locations":       _GENERIC_RULES,
    "suppliers":       _GENERIC_RULES,
    "supplier_items":  [_l4_supplier_inactive],  # no external_id at row level
    "purchase_orders": _GENERIC_RULES,
    "work_orders":     _GENERIC_RULES,
    "customer_orders": _GENERIC_RULES,
    "forecasts":       [],   # forecasts don't have a stable external_id by row
    "transfers":       _GENERIC_RULES,
    "on_hand":         [],   # OH snapshots are inherently "current state", no external_id
}


def check_l4(
    rows: list[tuple[UUID, int, dict]],
    entity_type: str,
    batch_id: UUID,
    db: DictRowConnection,
) -> list[DQIssue]:
    """Run all L4 rules for `entity_type` over the given rows.

    Each rule runs in turn; rules don't depend on each other so order
    doesn't matter except for diagnostic logging.

    Unknown entity_type returns no issues (same opt-in convention as L3).
    """
    rule_fns = _L4_RULES.get(entity_type)
    if not rule_fns:
        return []
    out: list[DQIssue] = []
    for fn in rule_fns:
        out.extend(fn(rows, batch_id, db))
    return out
