"""
approve.py — execute the full-reload transform + load (ADR-013 D3 + D4).

Implements `POST /v1/staging/batches/{id}/approve`. Given a batch in
status='validated' (i.e. DQ-clean) plus an optional `force=true` flag
for crossing the 20% deletion threshold, applies the entire batch to
the canonical tables in a single atomic transaction:

  1. UPSERT each batch row into the canonical table (insert if the
     external_id is new, update if it exists; the row carries the
     full new state).
  2. Insert/update the corresponding `external_references` mapping so
     subsequent diffs / approvals stay source-scoped.
  3. Soft-delete: for each canonical row that belongs to this
     (entity_type, source_system) but is absent from the batch, mark
     it inactive in the canonical table (status='obsolete' on items,
     'inactive' on suppliers) AND remove the mapping. Locations have
     no status field — only the mapping is removed; the canonical
     row stays in case another source references it.
  4. Bookkeeping: ingest_batches.status='imported', staging.transform_runs
     gets a row with counters + approver identity + outcome.

If any step fails, the whole transaction rolls back. The batch stays
in status='validated' and the operator can retry after fixing the
underlying issue.

Currently supports items / locations / suppliers (the same 3 master-data
entity_types as the diff endpoint). Transactional entities follow when
they have their canonical-table writers in place.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from uuid import UUID, uuid4

import psycopg

from ootils_core.staging.diff import (
    DELETION_RATIO_THRESHOLD,
    DiffError,
    DiffResult,
    compute_diff,
)

logger = logging.getLogger(__name__)


class ApprovalError(ValueError):
    """Raised when the approval is rejected (bad state, threshold guard, etc.)."""


@dataclass
class ApprovalResult:
    """Outcome of /approve. Counts are populated only on success."""
    batch_id: UUID
    run_id: UUID
    entity_type: str
    source_system: str
    approved_by: str
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_soft_deleted: int = 0
    rows_noop: int = 0
    forced_approval: bool = False
    duration_seconds: float = 0.0
    deletion_ratio: float = 0.0
    samples: dict[str, list[str]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Per-entity approval specs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ApprovalSpec:
    """How to UPSERT + soft-delete this entity_type's canonical rows."""
    canonical_table: str
    pk_column: str
    ref_entity_type: str          # singular for external_references
    fields: tuple[str, ...]       # columns to write (external_id always first)
    soft_delete_column: str | None  # e.g., 'status' for items
    soft_delete_value: str | None   # e.g., 'obsolete' for items


_SPECS: dict[str, _ApprovalSpec] = {
    "items": _ApprovalSpec(
        canonical_table="items",
        pk_column="item_id",
        ref_entity_type="item",
        fields=("external_id", "name", "item_type", "uom", "status"),
        soft_delete_column="status",
        soft_delete_value="obsolete",
    ),
    "locations": _ApprovalSpec(
        canonical_table="locations",
        pk_column="location_id",
        ref_entity_type="location",
        fields=("external_id", "name", "location_type", "country", "timezone"),
        # locations has no status/active column — soft-delete only via
        # removing the external_references row.
        soft_delete_column=None,
        soft_delete_value=None,
    ),
    "suppliers": _ApprovalSpec(
        canonical_table="suppliers",
        pk_column="supplier_id",
        ref_entity_type="supplier",
        fields=("external_id", "name", "country", "lead_time_days",
                "reliability_score", "status"),
        soft_delete_column="status",
        soft_delete_value="inactive",
    ),
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def approve_batch(
    conn: psycopg.Connection,
    batch_id: UUID,
    approved_by: str,
    notes: str | None = None,
    force: bool = False,
) -> ApprovalResult:
    """Apply the batch to canonical tables. All-or-nothing transaction.

    Raises:
        ApprovalError if the batch isn't in 'validated' status, the
        entity_type isn't supported, or the deletion threshold is
        crossed without force=true.
    """
    import time as _time
    started = _time.perf_counter()

    # ---------- 1. Load batch + validate state ----------
    batch_row = conn.execute(
        "SELECT entity_type, source_system, status, dq_status "
        "FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    if batch_row is None:
        raise ApprovalError(f"batch {batch_id} not found")

    entity_type = batch_row["entity_type"]
    source_system = batch_row["source_system"]
    status = batch_row["status"]
    dq_status = batch_row.get("dq_status")

    if status != "validated":
        raise ApprovalError(
            f"batch is in status {status!r}; only batches in 'validated' state can be approved"
        )
    if dq_status not in (None, "validated", "warning"):
        # 'rejected' means DQ found errors — must not be approvable
        raise ApprovalError(
            f"batch dq_status is {dq_status!r}; only DQ-validated batches can be approved"
        )

    spec = _SPECS.get(entity_type)
    if spec is None:
        raise ApprovalError(
            f"entity_type {entity_type!r} does not yet have approval support — "
            "currently implemented for items / locations / suppliers"
        )

    # ---------- 2. Compute the diff ----------
    try:
        diff = compute_diff(conn, batch_id)
    except DiffError as e:
        raise ApprovalError(f"could not compute diff: {e}") from e

    if not diff.supported:
        raise ApprovalError(diff.unsupported_reason or "diff not supported")

    # ---------- 3. Deletion guard ----------
    if diff.exceeds_deletion_threshold and not force:
        raise ApprovalError(
            f"deletion ratio {diff.deletion_ratio:.1%} exceeds "
            f"threshold {DELETION_RATIO_THRESHOLD:.0%}; "
            f"would soft-delete {diff.will_soft_delete_count} of "
            f"{diff.total_in_canonical_for_source} canonical rows. "
            f"Pass force=true to override (and include a justification in notes)."
        )

    # ---------- 4. Open transform_run + perform the load ----------
    run_id = uuid4()
    conn.execute(
        """
        INSERT INTO staging.transform_runs
            (run_id, batch_id, status, approved_by, approval_notes, forced_approval)
        VALUES (%s, %s, 'running', %s, %s, %s)
        """,
        (run_id, batch_id, approved_by, notes,
         force and diff.exceeds_deletion_threshold),
    )
    conn.commit()

    try:
        # All canonical-write steps run within a savepoint so a failure
        # rolls them back without losing the transform_run audit row.
        conn.execute("SAVEPOINT staging_approve")

        # 4a. Load batch rows (parsed from raw_content)
        batch_records = _load_batch_records(conn, batch_id, spec)

        # 4b. Upsert canonical + external_references
        n_inserted, n_updated = _upsert_canonical_rows(
            conn, spec, source_system, batch_records, diff,
        )

        # 4c. Soft-delete the rows not in the batch (for this source)
        n_soft_deleted = _soft_delete_missing(
            conn, spec, source_system, diff.will_soft_delete_sample
            if diff.will_soft_delete_count <= 10
            else _all_soft_delete_targets(conn, spec, source_system,
                                          set(batch_records.keys())),
        )

        # 4d. Mark batch as imported
        conn.execute(
            """
            UPDATE ingest_batches
            SET status = 'imported', imported_at = now()
            WHERE batch_id = %s
            """,
            (batch_id,),
        )

        # 4e. Close out the transform_run record with counters
        duration = _time.perf_counter() - started
        conn.execute(
            """
            UPDATE staging.transform_runs
            SET status = 'completed',
                completed_at = now(),
                rows_inserted = %s,
                rows_updated = %s,
                rows_soft_deleted = %s,
                rows_skipped = %s
            WHERE run_id = %s
            """,
            (n_inserted, n_updated, n_soft_deleted,
             diff.will_noop_count, run_id),
        )

        conn.execute("RELEASE SAVEPOINT staging_approve")
        conn.commit()

    except Exception as exc:
        # On failure, rollback the canonical writes but PRESERVE the
        # transform_run row with status='failed' for audit.
        conn.execute("ROLLBACK TO SAVEPOINT staging_approve")
        conn.execute(
            """
            UPDATE staging.transform_runs
            SET status = 'failed',
                completed_at = now(),
                error_message = %s
            WHERE run_id = %s
            """,
            (str(exc)[:2000], run_id),
        )
        conn.commit()
        logger.exception(
            "staging.approve failed: batch=%s run=%s entity=%s err=%s",
            batch_id, run_id, entity_type, exc,
        )
        raise ApprovalError(f"approval failed during write: {exc}") from exc

    duration = _time.perf_counter() - started
    logger.info(
        "staging.approve OK: batch=%s entity=%s source=%s "
        "ins=%d upd=%d soft_del=%d noop=%d duration=%.2fs",
        batch_id, entity_type, source_system,
        n_inserted, n_updated, n_soft_deleted, diff.will_noop_count, duration,
    )

    return ApprovalResult(
        batch_id=batch_id,
        run_id=run_id,
        entity_type=entity_type,
        source_system=source_system,
        approved_by=approved_by,
        rows_inserted=n_inserted,
        rows_updated=n_updated,
        rows_soft_deleted=n_soft_deleted,
        rows_noop=diff.will_noop_count,
        forced_approval=force and diff.exceeds_deletion_threshold,
        duration_seconds=round(duration, 3),
        deletion_ratio=diff.deletion_ratio,
        samples={
            "inserted": diff.will_insert_sample,
            "updated": diff.will_update_sample,
            "soft_deleted": diff.will_soft_delete_sample,
        },
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_batch_records(
    conn: psycopg.Connection,
    batch_id: UUID,
    spec: _ApprovalSpec,
) -> dict[str, dict]:
    """Parse raw_content JSON keyed by external_id (last-wins on dups —
    L4 caught those already, but defensively the writer wins)."""
    rows = conn.execute(
        "SELECT row_number, raw_content FROM ingest_rows "
        "WHERE batch_id = %s ORDER BY row_number",
        (batch_id,),
    ).fetchall()
    out: dict[str, dict] = {}
    for row in rows:
        raw = row["raw_content"]
        try:
            content = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(content, dict):
            continue
        ext_id = content.get("external_id")
        if not ext_id:
            continue
        # Coerce to the field set the spec expects; missing fields become None
        normalised = {f: content.get(f) for f in spec.fields}
        out[ext_id] = normalised
    return out


def _upsert_canonical_rows(
    conn: psycopg.Connection,
    spec: _ApprovalSpec,
    source_system: str,
    batch_records: dict[str, dict],
    diff: DiffResult,
) -> tuple[int, int]:
    """UPSERT every batch record into the canonical table + register the
    external_references mapping. Returns (inserted, updated).

    Counts come from `diff` (computed in the same approval transaction,
    so the numbers reflect this batch's intent). We don't try to derive
    insert-vs-update from each RETURNING — the SQL plan is the same
    either way, the diff already classified.
    """
    if not batch_records:
        return 0, 0

    fields = spec.fields
    table = spec.canonical_table
    cols_csv = ", ".join(fields)
    placeholders = ", ".join("%s" for _ in fields)
    update_set = ", ".join(f"{f} = EXCLUDED.{f}" for f in fields if f != "external_id")
    upsert_sql = (
        f"INSERT INTO {table} ({spec.pk_column}, {cols_csv}) "
        f"VALUES (gen_random_uuid(), {placeholders}) "
        f"ON CONFLICT (external_id) DO UPDATE SET {update_set} "
        f"RETURNING {spec.pk_column}"
    )

    for ext_id, record in batch_records.items():
        params = tuple(record.get(f) for f in fields)
        row = conn.execute(upsert_sql, params).fetchone()
        internal_id = (
            row[spec.pk_column] if isinstance(row, dict) else row[0]
        )
        conn.execute(
            """
            INSERT INTO external_references
                (entity_type, external_id, source_system, internal_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (entity_type, external_id, source_system) DO UPDATE
                SET internal_id = EXCLUDED.internal_id, updated_at = now()
            """,
            (spec.ref_entity_type, ext_id, source_system, internal_id),
        )

    return diff.will_insert_count, diff.will_update_count


def _all_soft_delete_targets(
    conn: psycopg.Connection,
    spec: _ApprovalSpec,
    source_system: str,
    batch_external_ids: set[str],
) -> list[str]:
    """Fallback path when the diff sample was truncated. Recomputes the
    full set of external_ids that need soft-delete from canonical state."""
    rows = conn.execute(
        f"""
        SELECT c.external_id
        FROM {spec.canonical_table} c
        JOIN external_references r
          ON r.internal_id = c.{spec.pk_column}
        WHERE r.entity_type = %s
          AND r.source_system = %s
        """,
        (spec.ref_entity_type, source_system),
    ).fetchall()
    canonical_ids = {r["external_id"] for r in rows}
    return sorted(canonical_ids - batch_external_ids)


def _soft_delete_missing(
    conn: psycopg.Connection,
    spec: _ApprovalSpec,
    source_system: str,
    soft_delete_ids: list[str],
) -> int:
    """For each external_id in `soft_delete_ids`:
      - if the spec has a soft_delete_column, set it on the canonical row
      - always remove the external_references mapping for this (source, id)
    Returns the count of distinct external_ids processed (may differ
    from rows_updated if the canonical column was already at the target value).
    """
    if not soft_delete_ids:
        return 0

    if spec.soft_delete_column and spec.soft_delete_value:
        conn.execute(
            f"""
            UPDATE {spec.canonical_table} c
            SET {spec.soft_delete_column} = %s
            FROM external_references r
            WHERE r.internal_id = c.{spec.pk_column}
              AND r.entity_type = %s
              AND r.source_system = %s
              AND c.external_id = ANY(%s)
            """,
            (spec.soft_delete_value, spec.ref_entity_type, source_system,
             soft_delete_ids),
        )

    # Always remove the source's claim on the row (other sources may still
    # reference it via their own external_references entries)
    conn.execute(
        """
        DELETE FROM external_references
        WHERE entity_type = %s
          AND source_system = %s
          AND external_id = ANY(%s)
        """,
        (spec.ref_entity_type, source_system, soft_delete_ids),
    )
    return len(soft_delete_ids)
