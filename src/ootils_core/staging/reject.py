"""
⚠️  DEPRECATED (ADR-042, 2026-07-18) — reachable only from the unmounted
`api/routers/staging.py` (see that module's banner). The rejection audit
shape below (`rejected_by`/`rejection_reason`) is the pattern the governed
daily-run pipeline's own audit trail follows, but this module itself is not
called from any live path. Module + `staging.*` tables are kept, not
dropped; do not wire this back behind a live endpoint.

reject.py — close out a batch as 'rejected' without writing to canonical.

POST /v1/staging/batches/{id}/reject is the operator's way to decline
a batch (typo'd CSV, wrong source_system, scope error spotted in /diff,
DQ-warning batch deemed too risky to approve, etc.).

Outcome:
  - ingest_batches.status -> 'rejected'
  - a staging.transform_runs row is created with status='rolled_back'
    holding the rejector identity + reason for audit
  - ingest_rows are NOT deleted; the raw upload stays in the staging
    zone so the operator can investigate later, and the file's sha256
    in staging.uploads remains queryable.

A rejected batch is TERMINAL — the lifecycle does not allow moving
back to 'validated' or 'imported' from here. To re-attempt the import
the source must be re-uploaded as a new batch.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)


class RejectionError(ValueError):
    """Raised when the rejection cannot be applied (bad batch state)."""


@dataclass
class RejectionResult:
    """Outcome of /reject."""
    batch_id: UUID
    run_id: UUID
    entity_type: str
    source_system: str
    rejected_by: str
    rejection_reason: str
    prior_status: str  # the status the batch was in before this call


def reject_batch(
    conn: DictRowConnection,
    batch_id: UUID,
    rejected_by: str,
    reason: str,
) -> RejectionResult:
    """Mark a batch as rejected with a free-text reason.

    Raises:
        RejectionError if the batch doesn't exist or is already in a
        terminal state ('imported' or 'rejected'). Re-rejecting an
        already-rejected batch is a no-op semantically but we surface
        it as an error so the caller knows the rejection didn't take
        effect this time (and to avoid duplicate audit rows).
    """
    if not reason or not reason.strip():
        raise RejectionError("reason is required and cannot be empty")

    row = conn.execute(
        "SELECT entity_type, source_system, status FROM ingest_batches "
        "WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    if row is None:
        raise RejectionError(f"batch {batch_id} not found")

    entity_type = row["entity_type"]
    source_system = row["source_system"]
    prior_status = row["status"]

    if prior_status in ("imported", "rejected"):
        raise RejectionError(
            f"batch is in terminal status {prior_status!r}; rejection refused"
        )

    run_id = uuid4()
    conn.execute(
        """
        INSERT INTO staging.transform_runs
            (run_id, batch_id, status, approved_by, approval_notes,
             started_at, completed_at)
        VALUES (%s, %s, 'rolled_back', %s, %s, now(), now())
        """,
        (run_id, batch_id, rejected_by, reason.strip()),
    )
    conn.execute(
        """
        UPDATE ingest_batches
        SET status = 'rejected',
            processed_at = now(),
            notes = COALESCE(notes || E'\n', '') || %s
        WHERE batch_id = %s
        """,
        (f"[REJECTED by {rejected_by}] {reason.strip()}", batch_id),
    )
    conn.commit()

    logger.info(
        "staging.reject OK: batch=%s entity=%s source=%s rejected_by=%s "
        "prior_status=%s",
        batch_id, entity_type, source_system, rejected_by, prior_status,
    )

    return RejectionResult(
        batch_id=batch_id,
        run_id=run_id,
        entity_type=entity_type,
        source_system=source_system,
        rejected_by=rejected_by,
        rejection_reason=reason.strip(),
        prior_status=prior_status,
    )
