"""
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
"""
from __future__ import annotations

import logging
from typing import Annotated, Optional

import psycopg
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
)

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db
from ootils_core.staging.loader import LoaderError, load_to_staging
from ootils_core.staging.parser import ParseError, ParseOptions, parse

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
    dependencies=[Depends(require_auth)],
)
def upload_file(
    file: Annotated[UploadFile, File(description="Source file (TSV/CSV/XLSX/JSON)")],
    entity_type: Annotated[str, Form(description="Target entity (items, locations, etc.)")],
    source_system: Annotated[str, Form(description="Upstream system identifier (SAP-EU, KINAXIS, ...)")],
    notes: Annotated[Optional[str], Form(description="Free-text annotation")] = None,
    format_hint: Annotated[Optional[str], Form(description="tsv/csv/xlsx/json — skip auto-detect")] = None,
    sheet_name: Annotated[Optional[str], Form(description="XLSX sheet override")] = None,
    delimiter: Annotated[Optional[str], Form(description="CSV delimiter override")] = None,
    db: psycopg.Connection = Depends(get_db),
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

    The current `require_auth` dependency only validates the bearer
    token (no subject extraction yet). When ADR-013's audit story
    needs real identities (OIDC subject, API-key owner, etc.) this
    function is the single place to wire it up.
    """
    return None
