"""
/v1/audit — read the API request audit trail (PROD-QW, #192-adjacent).

``api_request_log`` (migration 023, widened by 064) is written by the request
middleware in ``api/app.py`` on every ``/health`` and ``/v1/*`` call, but until
now NOTHING read it back — "Audit is a feature, not telemetry" (North Star)
stayed aspirational. This router exposes it:

  GET /v1/audit   paginated, filtered (actor_kind, path prefix, status, date
                  window), newest first, ADMIN scope.

NOT scenario-scoped: this is a cross-cutting INFRASTRUCTURE audit surface (who
called what, when, with which status/latency), not a business read path — the
North Star "scenario_id on every read path" rule targets graph/plan reads, not
the request log. Access is therefore gated on the ``admin`` scope instead: the
trail can reveal call patterns across every scenario and actor, so it is an
operator-only view.

NO SECRET IN THE RESPONSE: the table never stored the raw token or its hash —
only ``token_prefix`` (the non-secret leading slice minted for correlation, see
auth.py:token_prefix) and the denormalised ``actor_kind``/``token_id``. The
response model surfaces exactly those; there is no token_hash column to leak.
"""
from __future__ import annotations

import datetime as _dt
import logging
from typing import Any, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import VALID_ACTOR_KINDS, Principal, require_scope
from ootils_core.api.dependencies import get_db
from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/audit", tags=["audit"])

# Upper bound on a single page — mirrors the recommendations router (le=200) so
# an admin cannot pull the whole (unbounded, ever-growing) log in one query.
_MAX_LIMIT = 200


class AuditLogEntry(BaseModel):
    """One ``api_request_log`` row. Carries the non-secret ``token_prefix`` and
    the denormalised ``actor_kind``/``token_id`` for attribution — never a token
    or a token hash (the table stores neither)."""

    request_id: UUID
    correlation_id: Optional[str] = None
    token_prefix: Optional[str] = None
    token_id: Optional[UUID] = None
    actor_kind: Optional[str] = None
    method: str
    path: str
    status_code: int
    latency_ms: int
    client_ip: Optional[str] = None
    created_at: _dt.datetime


class AuditLogListResponse(BaseModel):
    entries: List[AuditLogEntry]
    total: int
    limit: int
    offset: int


@router.get("", response_model=AuditLogListResponse, summary="Read the API request audit trail")
def list_audit_log(
    _principal: Principal = Depends(require_scope("admin")),
    actor_kind: Optional[str] = Query(
        default=None,
        description="Filter by the denormalised actor_kind ('agent' | 'human' | 'service').",
    ),
    path_prefix: Optional[str] = Query(
        default=None,
        description="Return only rows whose request path starts with this prefix (e.g. '/v1/recommendations').",
    ),
    status_code: Optional[int] = Query(
        default=None, ge=100, le=599, description="Filter by exact HTTP status code."
    ),
    from_: Optional[_dt.datetime] = Query(
        default=None,
        alias="from",
        description="Window start (inclusive) on created_at (ISO 8601 date or datetime).",
    ),
    to: Optional[_dt.datetime] = Query(
        default=None,
        description="Window end (inclusive) on created_at (ISO 8601 date or datetime).",
    ),
    limit: int = Query(default=50, ge=1, le=_MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
    db: DictRowConnection = Depends(get_db),
) -> AuditLogListResponse:
    """List audit rows newest-first. All filters are optional and combine with
    AND; every value is bound as a parameter (never interpolated into SQL text).
    The ``path_prefix`` filter uses ``starts_with`` (not LIKE) so a caller's
    ``%``/``_`` are treated literally, not as wildcards."""
    if actor_kind is not None and actor_kind not in VALID_ACTOR_KINDS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unknown actor_kind '{actor_kind}'. Valid: {sorted(VALID_ACTOR_KINDS)}",
        )

    conditions: list[str] = []
    params: list[Any] = []
    if actor_kind is not None:
        conditions.append("actor_kind = %s")
        params.append(actor_kind)
    if path_prefix is not None:
        conditions.append("starts_with(path, %s)")
        params.append(path_prefix)
    if status_code is not None:
        conditions.append("status_code = %s")
        params.append(status_code)
    if from_ is not None:
        conditions.append("created_at >= %s")
        params.append(from_)
    if to is not None:
        conditions.append("created_at <= %s")
        params.append(to)

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    count_row = db.execute(
        f"SELECT COUNT(*) AS total FROM api_request_log {where_clause}",  # noqa: S608 — static text, parameterized values
        params,
    ).fetchone()
    total = int(count_row["total"]) if count_row else 0

    rows = db.execute(
        f"""
        SELECT request_id, correlation_id, token_prefix, token_id, actor_kind,
               method, path, status_code, latency_ms, client_ip, created_at
        FROM api_request_log
        {where_clause}
        ORDER BY created_at DESC, request_id
        LIMIT %s OFFSET %s
        """,  # noqa: S608 — static text, parameterized values
        params + [limit, offset],
    ).fetchall()

    return AuditLogListResponse(
        entries=[_row_to_entry(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


def _row_to_entry(row: dict[str, Any]) -> AuditLogEntry:
    token_id = row.get("token_id")
    return AuditLogEntry(
        request_id=UUID(str(row["request_id"])),
        correlation_id=row.get("correlation_id"),
        token_prefix=row.get("token_prefix"),
        token_id=UUID(str(token_id)) if token_id is not None else None,
        actor_kind=row.get("actor_kind"),
        method=row["method"],
        path=row["path"],
        status_code=int(row["status_code"]),
        latency_ms=int(row["latency_ms"]),
        client_ip=row.get("client_ip"),
        created_at=row["created_at"],
    )
