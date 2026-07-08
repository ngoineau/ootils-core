"""
/v1/tokens — API-token lifecycle over HTTP (#392 AN-2 PR2b, ADR-029).

The admin surface that replaces the hand-rolled INSERTs and the
``scripts/issue_agent_token.py`` CLI as the primary way to mint / list / revoke
credentials. Every verb requires the ``admin`` scope: issuing or killing a
credential is itself the most privileged operation in the system (a mis-issued
token can mint an agent that walks a governed action), so it is gated on the
superset scope, never a narrower one.

  * POST   /v1/tokens             — mint a credential; returns the CLEARTEXT ONCE.
  * GET    /v1/tokens             — list credentials WITHOUT any secret material.
  * DELETE /v1/tokens/{token_id}  — soft-revoke; 204 idempotent, 404 if unknown.

The cleartext token is shown exactly once, in the POST response, and is never
retrievable again (only its SHA-256 hash lives in the DB — see migration 064 /
``token_service``). GET and the list rows deliberately expose neither the hash
nor the cleartext, only the non-secret ``prefix``.

Validation (invalid ``actor_kind`` / ``scopes``) is raised by ``token_service``
as ``ValueError`` and mapped here to a hand-authored 422 — the message names the
offending value and the whitelist, never a raw psycopg / DSN string (same
carve-out discipline as ``param_overrides.py`` / ``staging.py``).
"""
from __future__ import annotations

import datetime as _dt
import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db
from ootils_core.api.token_service import mint_token, revoke_token
from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/tokens", tags=["tokens"])


class TokenCreateRequest(BaseModel):
    """Body of POST /v1/tokens. ``actor_kind`` and ``scopes`` are validated in
    ``token_service.mint_token`` against the auth whitelists — an invalid value
    surfaces as a hand-authored 422, not a Pydantic-enum 422, so the message can
    name the full valid set."""

    name: str = Field(min_length=1, description="Human label, e.g. 'shortage-watcher'.")
    actor_kind: str = Field(description="agent | human | service.")
    scopes: list[str] = Field(
        default_factory=list, description="Granted scopes (subset of the auth whitelist)."
    )
    rate_per_min: Optional[int] = Field(
        default=None, gt=0, description="Per-token request budget; null = uncapped."
    )
    expires_at: Optional[_dt.datetime] = Field(
        default=None, description="Expiry timestamp (UTC); null = no expiry."
    )


class TokenCreateResponse(BaseModel):
    """Response of a mint. ``token`` is the CLEARTEXT, shown ONCE and never
    retrievable again — store it now or re-mint."""

    token_id: UUID
    token: str
    prefix: str
    name: str
    actor_kind: str
    scopes: list[str]
    rate_per_min: Optional[int]
    created_at: _dt.datetime
    expires_at: Optional[_dt.datetime]


class TokenOut(BaseModel):
    """One registry row for listing. Carries NO secret material — neither the
    cleartext (never stored) nor the ``token_hash`` (a lookup key we do not
    surface); only the non-secret ``prefix``."""

    token_id: UUID
    name: str
    prefix: str
    actor_kind: str
    scopes: list[str]
    rate_per_min: Optional[int]
    created_at: _dt.datetime
    last_used_at: Optional[_dt.datetime]
    expires_at: Optional[_dt.datetime]
    revoked_at: Optional[_dt.datetime]


class TokenListResponse(BaseModel):
    tokens: list[TokenOut]
    total: int


def _row_to_out(row: dict) -> TokenOut:
    return TokenOut(
        token_id=UUID(str(row["token_id"])),
        name=row["name"],
        prefix=row["token_prefix"],
        actor_kind=row["actor_kind"],
        scopes=list(row["scopes"] or []),
        rate_per_min=row["rate_per_min"],
        created_at=row["created_at"],
        last_used_at=row["last_used_at"],
        expires_at=row["expires_at"],
        revoked_at=row["revoked_at"],
    )


@router.post(
    "",
    response_model=TokenCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Mint an API token",
    description=(
        "Issue a new credential. Returns the cleartext token EXACTLY ONCE — it "
        "is never stored (only its SHA-256 hash) and cannot be recovered later. "
        "Requires the `admin` scope."
    ),
)
def create_token(
    body: TokenCreateRequest,
    _principal: Principal = Depends(require_scope("admin")),
    db: DictRowConnection = Depends(get_db),
) -> TokenCreateResponse:
    """Mint one token. ``get_db`` owns commit/rollback; ``mint_token`` only
    executes the INSERT. The follow-up SELECT reads the persisted row (visible on
    the same connection before commit) so the response reflects the DB defaults
    (created_at) alongside the once-shown cleartext."""
    try:
        token_id, cleartext = mint_token(
            db,
            name=body.name,
            actor_kind=body.actor_kind,
            scopes=body.scopes,
            rate_per_min=body.rate_per_min,
            expires_at=body.expires_at,
        )
    except ValueError as exc:
        # Hand-authored message from token_service (names the offending value +
        # the whitelist). Safe to echo: no DSN, no psycopg text.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        )

    row = db.execute(
        """
        SELECT token_id, name, token_prefix, actor_kind, scopes,
               rate_per_min, created_at, expires_at
        FROM api_tokens
        WHERE token_id = %s
        """,
        (token_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("create_token: minted row not found on read-back")

    return TokenCreateResponse(
        token_id=token_id,
        token=cleartext,
        prefix=row["token_prefix"],
        name=row["name"],
        actor_kind=row["actor_kind"],
        scopes=list(row["scopes"] or []),
        rate_per_min=row["rate_per_min"],
        created_at=row["created_at"],
        expires_at=row["expires_at"],
    )


@router.get(
    "",
    response_model=TokenListResponse,
    summary="List API tokens",
    description=(
        "List issued credentials WITHOUT any secret material (no cleartext, no "
        "hash). By default only live tokens are returned; pass "
        "`include_revoked=true` to include revoked rows. Requires the `admin` scope."
    ),
)
def list_tokens(
    _principal: Principal = Depends(require_scope("admin")),
    include_revoked: bool = Query(
        default=False, description="Include revoked tokens in the listing."
    ),
    db: DictRowConnection = Depends(get_db),
) -> TokenListResponse:
    """List tokens. The optional ``include_revoked`` toggles a single static
    ``revoked_at IS NULL`` predicate — no caller data ever reaches the SQL text."""
    if include_revoked:
        where_clause = ""
    else:
        where_clause = "WHERE revoked_at IS NULL"

    rows = db.execute(
        f"""
        SELECT token_id, name, token_prefix, actor_kind, scopes,
               rate_per_min, created_at, last_used_at, expires_at, revoked_at
        FROM api_tokens
        {where_clause}
        ORDER BY created_at DESC
        """,  # noqa: S608 — static predicate, no interpolated caller data
    ).fetchall()

    out = [_row_to_out(r) for r in rows]
    return TokenListResponse(tokens=out, total=len(out))


@router.delete(
    "/{token_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke an API token",
    description=(
        "Soft-revoke a credential (sets `revoked_at`; the row is kept for audit). "
        "Idempotent: revoking an already-revoked token still returns 204. Returns "
        "404 only when the token_id is unknown. Requires the `admin` scope."
    ),
)
def delete_token(
    token_id: UUID,
    _principal: Principal = Depends(require_scope("admin")),
    db: DictRowConnection = Depends(get_db),
) -> Response:
    """Soft-revoke a token.

    404 vs 204 is decided by an existence SELECT (revoke_token's bool cannot tell
    "unknown" from "already revoked" apart): unknown token_id → 404; known token
    (live or already revoked) → 204 idempotent. ``revoke_token`` clears the
    principal cache on the flip so a live token stops authenticating at once."""
    exists = db.execute(
        "SELECT 1 FROM api_tokens WHERE token_id = %s",
        (token_id,),
    ).fetchone()
    if exists is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"token {token_id} not found",
        )

    revoke_token(db, token_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
