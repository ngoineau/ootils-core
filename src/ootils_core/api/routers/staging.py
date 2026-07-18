"""
⚠️  DEPRECATED (ADR-042, 2026-07-18) — this router is UNMOUNTED, not deleted.

ADR-042 decision 2.1 "enterre" the staging pipeline: `approve_batch`
(`staging/approve.py`) requires `status == 'validated'`, a status nothing in
the repo ever drove automatically outside a human clicking
`POST /v1/staging/batches/{id}/approve` after `POST /v1/staging/upload` — in
practice this path was never wired into production. It is superseded by the
governed daily-run pipeline (Dropbox inbox ->
`engine/ingest/daily_orchestrator.py`, ADR-042 §3). `staging.router` is no
longer imported/included in `api/app.py` (PR-1) — every route below is
unreachable in the running app.

Kept intentionally, not dropped:
  * the module and the `staging.*` tables stay (ADR-042 decision 2.1 — no
    data/schema deletion);
  * the two value-bearing guards this pipeline introduced were RELOGGED into
    the new pipeline rather than lost — the 20% deletion-ratio guard
    (`staging/diff.py`'s `DELETION_RATIO_THRESHOLD`) and the rejection audit
    shape (`staging/reject.py`'s `rejected_by`/`rejection_reason`);
  * `staging.parser`/`staging.diff` are still imported directly (not via
    HTTP) by tests kept in the suite (`tests/test_staging_parser.py`,
    `tests/test_daily_run_guards.py`).

Do not re-mount this router — see docs/ADR-042-interface-doctrine.md §2
point 1 ("Refus explicites en V1": "réanimation du pipeline staging servi en
production").

POST /v1/staging/* — file-upload entry point for the staging pipeline.

This router is the HTTP surface of ADR-013. It orchestrates the
parser (step 2) + the loader (step 3) into a single transactional
operation, exposed as a multipart file-upload endpoint.

Endpoint:
    POST /v1/staging/upload
        multipart form fields:
            file:           the file content (required)
            entity_type:    one of the values supported by
                            ingest_batches.entity_type CHECK constraint
            source_system:  free-text identifier of the upstream system
            notes:          optional free-text annotation
            format_hint:    optional ('tsv','csv','xlsx','json') to
                            skip auto-detection
            sheet_name:     optional XLSX sheet override
            delimiter:      optional CSV delimiter override

        Returns 202 Accepted with the upload_id + batch_id and the
        parse + load diagnostics. The batch status is 'pending' — the
        DQ pipeline (L1..L4) runs asynchronously or via a follow-up
        endpoint (TBD in ADR-013 step 5+).

ADR-013 D4 mandates approval; this endpoint only enqueues the upload —
batches sit in `pending` until /v1/staging/batches/{id}/approve lands.

Authorization (#392 security-review follow-up): every staging write
(upload, diff-preview, reject, approve) requires the `ingest` scope — the
legacy token holds `admin` (a superset) so nothing pre-#392 regresses.
`approve` additionally applies the same UPSERT + soft-delete to canonical
master data (items/locations/suppliers) that `staging/approve.py` describes
below — an apply-to-baseline L3 action — so it ALSO requires the Decision
Ladder human gate on top of the scope: a non-human principal (agent/service)
is refused with 403 regardless of scope. The approver identity written to
`staging.transform_runs.approved_by` is the request body's `approved_by`
field (kept for the human-readable name a bearer token doesn't carry), but
the *governance decision* (human vs not) is the authenticated principal's
`actor_kind`, never self-declared.
"""
from __future__ import annotations

import logging
from typing import Annotated, Optional
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
)

from pydantic import BaseModel, Field

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db
from ootils_core.db.types import DictRowConnection
from ootils_core.staging.approve import ApprovalError, approve_batch
from ootils_core.staging.diff import DiffError, compute_diff
from ootils_core.staging.loader import LoaderError, load_to_staging
from ootils_core.staging.parser import ParseError, ParseOptions, parse
from ootils_core.staging.reject import RejectionError, reject_batch

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/staging", tags=["staging"])


# Allowed values for entity_type — must stay in sync with the CHECK
# constraint on ingest_batches.entity_type (see migration 007).
_VALID_ENTITY_TYPES = frozenset({
    "items",
    "locations",
    "suppliers",
    "supplier_items",
    "purchase_orders",
    "customer_orders",
    "forecasts",
    "work_orders",
    "transfers",
    "on_hand",
})

# Allowed format hints (parser SUPPORTED_FORMATS — kept here too to
# validate early and return a 400 rather than letting the parser raise)
_VALID_FORMAT_HINTS = frozenset({"tsv", "csv", "xlsx", "json"})

# Reasonable upper bound on upload size — 50 MB covers an ERP item
# master export (5K items × ~200 bytes = 1MB) with plenty of headroom
# but blocks accidental dumps of entire DBs.
_MAX_UPLOAD_BYTES = 50 * 1024 * 1024


@router.post(
    "/upload",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_scope("ingest"))],
)
def upload_file(
    file: Annotated[UploadFile, File(description="Source file (TSV/CSV/XLSX/JSON)")],
    entity_type: Annotated[str, Form(description="Target entity (items, locations, etc.)")],
    source_system: Annotated[str, Form(description="Upstream system identifier (SAP-EU, KINAXIS, ...)")],
    notes: Annotated[Optional[str], Form(description="Free-text annotation")] = None,
    format_hint: Annotated[Optional[str], Form(description="tsv/csv/xlsx/json — skip auto-detect")] = None,
    sheet_name: Annotated[Optional[str], Form(description="XLSX sheet override")] = None,
    delimiter: Annotated[Optional[str], Form(description="CSV delimiter override")] = None,
    db: DictRowConnection = Depends(get_db),
) -> dict:
    """Upload a file into the staging zone (ADR-013 step 1 of the lifecycle)."""

    # ---------- 1. Input validation ----------
    if entity_type not in _VALID_ENTITY_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"unknown entity_type {entity_type!r}; "
                f"expected one of {sorted(_VALID_ENTITY_TYPES)}"
            ),
        )
    if format_hint is not None and format_hint not in _VALID_FORMAT_HINTS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"unknown format_hint {format_hint!r}; "
                f"expected one of {sorted(_VALID_FORMAT_HINTS)}"
            ),
        )
    if not source_system or not source_system.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source_system is required and cannot be empty",
        )

    # ---------- 2. Read bytes ----------
    raw = file.file.read()
    if not raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="uploaded file is empty",
        )
    if len(raw) > _MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"file exceeds {_MAX_UPLOAD_BYTES} bytes",
        )

    # ---------- 3. Parse ----------
    options = ParseOptions(
        sheet_name=sheet_name,
        delimiter=delimiter,
    )
    try:
        parse_result = parse(
            data=raw,
            filename=file.filename,
            format_hint=format_hint,
            options=options,
        )
    except ParseError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"parse error: {e}",
        ) from e

    # ---------- 4. Load into staging tables ----------
    try:
        load_result = load_to_staging(
            db,
            parse_result=parse_result,
            entity_type=entity_type,
            source_system=source_system.strip(),
            raw_bytes_size=len(raw),
            filename=file.filename or "upload",
            content_type=file.content_type,
            submitted_by=_extract_submitter(),
            notes=notes,
        )
        db.commit()
    except LoaderError as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"load error: {e}",
        ) from e
    except Exception as e:
        db.rollback()
        logger.exception("staging upload failed unexpectedly: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="internal error while persisting upload",
        ) from e

    logger.info(
        "staging.upload OK: batch=%s entity=%s source=%s rows=%d format=%s",
        load_result.batch_id, entity_type, source_system,
        load_result.rows_inserted, load_result.format,
    )

    return {
        "upload_id": str(load_result.upload_id),
        "batch_id": str(load_result.batch_id),
        "status": "pending",
        "entity_type": entity_type,
        "source_system": source_system.strip(),
        "rows_inserted": load_result.rows_inserted,
        "format": load_result.format,
        "encoding": load_result.encoding,
        "delimiter": load_result.delimiter,
        "headers": load_result.headers,
        "sha256": load_result.sha256,
        "file_size_bytes": load_result.file_size_bytes,
    }


def _extract_submitter() -> str | None:
    """Placeholder for pulling the authenticated identity off the request.

    `resolve_principal` (#392) now extracts a name/actor_kind from a minted
    token, but `upload_file` above only depends on the `ingest` scope check
    (not the Principal itself) — wiring `principal.name` in here as the
    submitter is a small follow-up, not yet done. This function stays the
    single place to do it.
    """
    return None


@router.get(
    "/batches/{batch_id}/diff",
    dependencies=[Depends(require_scope("ingest"))],
)
def get_batch_diff(
    batch_id: UUID,
    db: DictRowConnection = Depends(get_db),
) -> dict:
    """Preview the impact of approving this batch (ADR-013 D4).

    Returns counts + samples of what `POST /approve` would do:
      - will_insert     external_ids new to canonical
      - will_update     external_ids in both; canonical values differ
      - will_noop       external_ids in both; identical
      - will_soft_delete external_ids in canonical for this
                         (entity_type, source_system) but absent from
                         the batch

    The `deletion_ratio` is a fraction of soft-deletes over the current
    canonical footprint. When > 20%, `exceeds_deletion_threshold=true` —
    approval must be called with `force=true` (validated by /approve
    in step 8). This is the principal protection against destructive
    imports (truncated ERP exports, scope-reduction accidents, etc.).
    """
    try:
        diff = compute_diff(db, batch_id)
    except DiffError as e:
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e

    return {
        "batch_id": str(diff.batch_id),
        "entity_type": diff.entity_type,
        "source_system": diff.source_system,
        "supported": diff.supported,
        "unsupported_reason": diff.unsupported_reason,
        "counts": {
            "in_batch": diff.total_in_batch,
            "in_canonical_for_source": diff.total_in_canonical_for_source,
            "will_insert": diff.will_insert_count,
            "will_update": diff.will_update_count,
            "will_noop": diff.will_noop_count,
            "will_soft_delete": diff.will_soft_delete_count,
        },
        "samples": {
            "will_insert": diff.will_insert_sample,
            "will_update": diff.will_update_sample,
            "will_soft_delete": diff.will_soft_delete_sample,
        },
        "deletion_guard": {
            "ratio": diff.deletion_ratio,
            "threshold": diff.deletion_ratio_threshold,
            "exceeds_threshold": diff.exceeds_deletion_threshold,
        },
    }


class ApproveRequest(BaseModel):
    """Body of POST /v1/staging/batches/{id}/approve.

    `force` is required (set to True) when the diff's deletion ratio
    exceeds the 20% threshold — see ADR-013 D4. `notes` is optional but
    strongly recommended when forcing, as it becomes the audit trail
    rationale in staging.transform_runs.approval_notes.
    """
    notes: Optional[str] = Field(default=None, max_length=2000)
    force: bool = Field(default=False)
    # Identity of the approver. The current auth layer doesn't extract
    # subjects from the bearer token, so callers pass it explicitly for
    # now. When auth.py grows subject extraction, this becomes optional.
    approved_by: str = Field(..., min_length=1, max_length=200)


@router.post(
    "/batches/{batch_id}/approve",
)
def approve(
    batch_id: UUID,
    body: ApproveRequest,
    db: DictRowConnection = Depends(get_db),
    principal: Principal = Depends(require_scope("ingest")),
) -> dict:
    """Apply the validated batch to canonical tables (ADR-013 D3+D4).

    Returns counts (rows_inserted / rows_updated / rows_soft_deleted /
    rows_noop), the new run_id, and a list of samples per category.
    The batch transitions from 'validated' to 'imported'; a new
    `staging.transform_runs` row is created in 'completed' state.

    Failure mode: if the canonical write fails for any reason, the
    transaction is rolled back and the transform_runs row is marked
    'failed' with the error message (audit trail preserved). The
    batch stays in 'validated' state so the operator can retry.

    Decision Ladder gate (#392 security-review follow-up): this endpoint
    UPSERTs + soft-deletes canonical master data (items/locations/suppliers)
    — an apply-to-baseline L3 action, exactly the class of decision the #392
    token-truth model exists to gate. A non-human principal (actor_kind
    'agent' or 'service') is refused with 403 even if it holds the `ingest`
    scope: scope says "may operate staging", the human gate says "may commit
    to baseline". This is a NEW gate (staging/approve.py had none before
    #392) — there is no pre-#392 self-declared actor_kind to preserve here
    (contrast with recommendations/scenarios, which already gated on a body
    field and need the #392-9 legacy fallback).
    """
    if principal.actor_kind != "human":
        logger.warning(
            "staging.approve.human_gate_rejected batch=%s actor_kind=%s token=%s",
            batch_id,
            principal.actor_kind,
            principal.token_id,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "Approving a staging batch commits to canonical baseline data "
                "and is an L3/L4 decision reserved to human actors (Decision "
                "Ladder, strategy doc §5)."
            ),
        )

    try:
        result = approve_batch(
            db,
            batch_id=batch_id,
            approved_by=body.approved_by,
            notes=body.notes,
            force=body.force,
        )
    except ApprovalError as e:
        # Roll back any open transaction state so the pooled connection
        # comes back clean for the next request.
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e

    return {
        "batch_id": str(result.batch_id),
        "run_id": str(result.run_id),
        "entity_type": result.entity_type,
        "source_system": result.source_system,
        "approved_by": result.approved_by,
        "forced_approval": result.forced_approval,
        "duration_seconds": result.duration_seconds,
        "counts": {
            "rows_inserted": result.rows_inserted,
            "rows_updated": result.rows_updated,
            "rows_soft_deleted": result.rows_soft_deleted,
            "rows_noop": result.rows_noop,
        },
        "deletion_ratio": result.deletion_ratio,
        "samples": result.samples,
    }


class RejectRequest(BaseModel):
    """Body of POST /v1/staging/batches/{id}/reject.

    A rejection is permanent — the batch transitions to 'rejected' and
    cannot be revived. The `reason` is stored in
    `staging.transform_runs.approval_notes` AND appended to
    `ingest_batches.notes` so operators can see the rationale either
    way.

    To retry the import after correction, the source must be
    re-uploaded as a new batch (the rejected batch's ingest_rows stay
    intact for forensics).
    """
    rejected_by: str = Field(..., min_length=1, max_length=200)
    reason: str = Field(..., min_length=1, max_length=2000)


@router.post(
    "/batches/{batch_id}/reject",
    dependencies=[Depends(require_scope("ingest"))],
)
def reject(
    batch_id: UUID,
    body: RejectRequest,
    db: DictRowConnection = Depends(get_db),
) -> dict:
    """Close out a batch as rejected without writing to canonical."""
    try:
        result = reject_batch(
            db,
            batch_id=batch_id,
            rejected_by=body.rejected_by,
            reason=body.reason,
        )
    except RejectionError as e:
        try:
            db.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        ) from e

    return {
        "batch_id": str(result.batch_id),
        "run_id": str(result.run_id),
        "entity_type": result.entity_type,
        "source_system": result.source_system,
        "rejected_by": result.rejected_by,
        "rejection_reason": result.rejection_reason,
        "prior_status": result.prior_status,
        "new_status": "rejected",
    }
