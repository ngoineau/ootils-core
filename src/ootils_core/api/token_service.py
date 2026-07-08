"""
token_service.py — API-token lifecycle helpers (mint / revoke) for #392 AN-2.

The single, shared implementation of "create a credential" and "kill a
credential", used by BOTH the REST surface (``api/routers/tokens.py``) and the
demo runbook (``scripts/demo_e2e.py``) so there is exactly one place that knows
how a token is generated, hashed and persisted — no more hand-rolled INSERTs
scattered across scripts.

Design invariants (mirrors migration 064's header + ``api/auth.py``):
  * The CLEARTEXT token is generated here and returned to the caller ONCE; it is
    NEVER stored. Only its SHA-256 hex (``token_hash``, lookup/uniqueness) and a
    non-secret leading ``token_prefix`` are persisted. A leak of ``api_tokens``
    leaks no usable credential.
  * ``actor_kind`` and ``scopes`` are validated in APPLICATION code against the
    auth layer's whitelists (``VALID_ACTOR_KINDS`` / ``VALID_SCOPES``) — the DB
    has no CHECK on scope contents by design (scopes evolve with the code that
    enforces them). A bad value raises ``ValueError`` (the router maps it to a
    hand-authored 422; never a raw psycopg error string to the client).
  * 256 bits of entropy (``secrets.token_urlsafe(32)`` → 32 os.urandom bytes),
    rendered ``ootk_<base64url>``. A fast hash (SHA-256, no KDF) is correct for
    high-entropy keys — see migration 064.

Transaction ownership: these helpers only ``execute`` on the connection they are
given; they NEVER commit or rollback. The caller owns the transaction (the REST
path via ``Depends(get_db)``, the demo via its own ``with connect()`` block).
"""
from __future__ import annotations

import logging
import secrets
from typing import Optional
from uuid import UUID

from datetime import datetime

from ootils_core.api.auth import (
    VALID_ACTOR_KINDS,
    VALID_SCOPES,
    _MINTED_PREFIX,
    hash_token,
    invalidate_token_cache,
    token_prefix,
)
from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

# 32 bytes → 256 bits of entropy, the size migration 064's header commits to.
_TOKEN_ENTROPY_BYTES = 32


def _generate_cleartext() -> str:
    """``ootk_`` + 256 bits of URL-safe randomness. The only place a cleartext
    token is born; it is returned to the caller once and never persisted."""
    return f"{_MINTED_PREFIX}{secrets.token_urlsafe(_TOKEN_ENTROPY_BYTES)}"


def mint_token(
    conn: DictRowConnection,
    *,
    name: str,
    actor_kind: str,
    scopes: list[str],
    rate_per_min: Optional[int] = None,
    expires_at: Optional[datetime] = None,
) -> tuple[UUID, str]:
    """Insert one live ``api_tokens`` row and return ``(token_id, cleartext)``.

    The cleartext is shown ONCE (this return value) and never stored. Validates
    ``actor_kind`` and every scope against the auth-layer whitelists BEFORE the
    INSERT so an invalid grant fails loudly (``ValueError``) instead of writing a
    half-valid row. Does not commit — the caller owns the transaction.
    """
    if actor_kind not in VALID_ACTOR_KINDS:
        raise ValueError(
            f"unknown actor_kind {actor_kind!r}; "
            f"valid actor kinds are {sorted(VALID_ACTOR_KINDS)}"
        )
    invalid = sorted(set(scopes) - VALID_SCOPES)
    if invalid:
        raise ValueError(
            f"unknown scope(s) {invalid}; valid scopes are {sorted(VALID_SCOPES)}"
        )
    if rate_per_min is not None and rate_per_min <= 0:
        raise ValueError(f"rate_per_min must be a positive integer, got {rate_per_min}")

    cleartext = _generate_cleartext()
    row = conn.execute(
        """
        INSERT INTO api_tokens (
            name, actor_kind, token_hash, token_prefix,
            scopes, rate_per_min, expires_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING token_id
        """,
        (
            name,
            actor_kind,
            hash_token(cleartext),
            token_prefix(cleartext),
            scopes,
            rate_per_min,
            expires_at,
        ),
    ).fetchone()
    if row is None:
        raise RuntimeError("mint_token: INSERT ... RETURNING yielded no row")

    token_id = UUID(str(row["token_id"]))
    logger.info(
        "token.minted token_id=%s name=%s actor_kind=%s scopes=%s",
        token_id, name, actor_kind, sorted(scopes),
    )
    return token_id, cleartext


def revoke_token(conn: DictRowConnection, token_id: UUID) -> bool:
    """Soft-revoke a token (set ``api_tokens.revoked_at = now()``).

    Returns True if THIS call flipped a live token to revoked; False if the token
    is unknown OR was already revoked (the UPDATE matched no live row). The
    caller distinguishes "unknown" from "already revoked" itself if it needs to
    (the router does an existence SELECT for its 404) — this helper only reports
    whether it changed anything.

    Invalidates the in-process principal cache on a successful flip so the revoke
    takes effect on the very next request instead of lingering up to the cache
    TTL. Does not commit — the caller owns the transaction; the cache clear is
    done here (not deferred to commit) because a global clear is cheap and idempotent,
    and a caller that rolls back simply re-populates the cache on the next lookup.
    """
    result = conn.execute(
        """
        UPDATE api_tokens
        SET revoked_at = now()
        WHERE token_id = %s
          AND revoked_at IS NULL
        """,
        (token_id,),
    )
    changed = result.rowcount > 0
    if changed:
        invalidate_token_cache()
        logger.info("token.revoked token_id=%s", token_id)
    else:
        logger.info("token.revoke_noop token_id=%s (unknown or already revoked)", token_id)
    return changed
