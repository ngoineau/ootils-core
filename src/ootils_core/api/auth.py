"""
auth.py — Bearer token authentication + principal resolution for Ootils Core.

Two token flavours coexist (chantier #392 PR1):

  * **Legacy single token** — the value of ``OOTILS_API_TOKEN``. Compared with
    ``hmac.compare_digest`` exactly as before. Resolves to a synthetic
    ``Principal`` with ``actor_kind='human'`` and the ``admin`` superset scope,
    so every pre-#392 caller keeps working byte-for-byte. Deprecated but not
    removed.
  * **Minted token** (``ootk_`` prefix) — looked up in the ``api_tokens`` table
    (migration 064). The row is the SINGLE source of truth for the caller's
    ``actor_kind`` and ``scopes``. This closes the #350 gap where ``actor_kind``
    was self-declared by the request body: a caller can no longer claim to be a
    human to clear an L3 human gate — the actor kind now comes from the token.

The API stays FAIL-CLOSED and there is deliberately NO optional-auth path:
  * ``OOTILS_API_TOKEN`` is validated at import time (``_expected_token``);
    the process refuses to start without it.
  * A missing / malformed / unknown / revoked / expired token → 401.
  * A minted-token lookup that cannot reach the DB → 503 (never 200).

The minted-token lookup is memoised in-process for ``_CACHE_TTL_SECONDS`` (30 s)
so the hot path stays pool-free: a token is looked up in the DB at most once
per 30 s FOR A GIVEN TOKEN VALUE, and negative results (unknown token) are
cached too — so a flood of *repeated* bogus tokens (the same wrong value
retried) cannot turn into a flood of DB lookups. That guarantee does NOT
extend to a flood of *distinct* bogus tokens (e.g. ``ootk_<random>`` probed
once each): every unique value is still a genuine cache miss and a genuine
lookup. The cache is therefore also SIZE-bounded (``_CACHE_MAX_ENTRIES``,
LRU-ish eviction on overflow) so that scenario degrades to bounded memory use
and steady-state lookup pressure, never unbounded growth, rather than being
mistaken for a flood defense it cannot provide. ``last_used_at`` is bumped
best-effort on a rare cache-miss and MUST NEVER fail authentication — the bump
runs in its own try/except, isolated from the SELECT that resolves the
Principal (see ``_lookup_minted_token_sync``): a read-only standby or a
concurrent lock on ``api_tokens`` degrades the bump silently, never auth.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional
from uuid import UUID

from anyio import to_thread

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

_bearer = HTTPBearer(auto_error=False)

# Minted tokens carry this prefix; anything else is treated as the legacy
# single token. The prefix is part of the presented secret, not stripped.
_MINTED_PREFIX = "ootk_"

# Number of leading characters stored as a non-secret token_prefix (for audit
# correlation) — kept in sync with the length the token-minting path records.
_PREFIX_LEN = 12

# In-process lookup memo TTL. A minted token is resolved against the DB at most
# once per this many seconds FOR A GIVEN token value; negative lookups are
# cached with the same TTL to blunt brute-force RE-ENUMERATION of the same
# value. It does not blunt a flood of distinct values — see _CACHE_MAX_ENTRIES.
_CACHE_TTL_SECONDS = 30.0

# Hard cap on the number of memoised token hashes (positive + negative
# combined), pre-auth reachable via _resolve_minted_principal (an attacker
# needs no valid credential to populate a negative entry). Without a cap, a
# flood of distinct bogus `ootk_<random>` probes — each a genuine cache miss,
# since TTL memoisation only helps on a REPEATED value — would grow the dict
# unboundedly (multi-GB) and, on every miss, dispatch a lookup to the shared
# worker threadpool (the same pool sync `def` handlers use), starving it.
# 10k entries is generously above realistic legitimate-token-churn levels
# (single/low-hundreds of live tokens) while bounding worst-case memory to a
# few MB. Eviction is oldest-inserted/least-recently-touched (see
# _TokenCache.get moving an entry to the end on hit) — a plain LRU, not a
# security control in itself, just a memory/lookup-pressure ceiling.
_CACHE_MAX_ENTRIES = 10_000

# Superset scope: a principal holding 'admin' satisfies every require_scope
# check. The legacy token maps to this so nothing regresses.
_ADMIN_SCOPE = "admin"

# The only actor kinds a credential may carry. Kept in sync with the
# api_tokens.actor_kind CHECK constraint (migration 064) — the token-minting
# path (issue_agent_token.py, PR2) validates against this before insert.
VALID_ACTOR_KINDS: frozenset[str] = frozenset({"agent", "human", "service"})

_TRUTHY = {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Principal:
    """The authenticated caller behind one request.

    ``token_id`` is None for the legacy single token (it has no DB row).
    ``is_legacy`` distinguishes that synthetic principal from a minted one.
    ``scopes`` is the authoritative capability set; ``'admin'`` is a superset.
    """

    token_id: Optional[UUID]
    name: str
    actor_kind: str  # 'agent' | 'human' | 'service'
    scopes: frozenset[str]
    is_legacy: bool

    def has_scope(self, scope: str) -> bool:
        return _ADMIN_SCOPE in self.scopes or scope in self.scopes


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def _expected_token() -> str:
    """Return the configured legacy API token or raise loudly if unset."""
    token = os.environ.get("OOTILS_API_TOKEN")
    if not token:
        raise RuntimeError(
            "OOTILS_API_TOKEN environment variable is not set. "
            "The API cannot start without an explicit token, "
            "set OOTILS_API_TOKEN to a strong secret before launching."
        )
    return token


def _agents_enabled() -> bool:
    """Fleet kill switch, default ON. Falsy OOTILS_AGENTS_ENABLED disables
    every ``actor_kind='agent'`` principal (503), leaving humans/service through."""
    return os.environ.get("OOTILS_AGENTS_ENABLED", "1").strip().lower() in _TRUTHY


# ---------------------------------------------------------------------------
# Pure helpers (no IO, no clock of their own) — the test-writer wave targets
# these directly.
# ---------------------------------------------------------------------------


def is_minted_token(presented: str) -> bool:
    """True if the presented secret uses the minted ``ootk_`` scheme."""
    return presented.startswith(_MINTED_PREFIX)


def hash_token(presented: str) -> str:
    """SHA-256 hex digest of the presented secret — the ``api_tokens.token_hash``
    lookup key. Never store or log the raw token; the hash is what the DB holds."""
    return hashlib.sha256(presented.encode("utf-8")).hexdigest()


def token_prefix(presented: str) -> str:
    """Non-secret leading slice used for audit correlation (``token_prefix``)."""
    return presented[:_PREFIX_LEN]


def principal_from_row(row: dict) -> Principal:
    """Build a Principal from an ``api_tokens`` row (already filtered on
    revoked_at IS NULL AND not-expired by the SELECT). ``scopes`` may arrive as
    a Python list (psycopg maps ``TEXT[]``) or None → empty frozenset."""
    raw_scopes = row.get("scopes") or []
    return Principal(
        token_id=UUID(str(row["token_id"])),
        name=row["name"],
        actor_kind=row["actor_kind"],
        scopes=frozenset(raw_scopes),
        is_legacy=False,
    )


def legacy_principal() -> Principal:
    """The synthetic principal for the legacy single token: a human admin with
    no DB identity. Preserves every pre-#392 behaviour (human gate passes, all
    scopes satisfied)."""
    return Principal(
        token_id=None,
        name="legacy",
        actor_kind="human",
        scopes=frozenset({_ADMIN_SCOPE}),
        is_legacy=True,
    )


def resolve_gate_kind(
    principal: Principal,
    declared_actor_kind: Optional[str],
) -> str:
    """Return the actor_kind the Decision Ladder human gate should decide on.

    #392 security-review fix (defect 9 — "the legacy window weakens the gate
    for honest agents"): pre-#392, EVERY caller self-declared its actor_kind
    in the request body, and the gate ran on that declared value — an agent
    that honestly declared ``actor_kind="agent"`` got 403 on a human-only
    transition. Post-#392, the LEGACY token resolves to a synthetic
    ``human``/``admin`` Principal (so nothing about the single shared token
    regresses for genuinely human callers) — but taken naively, that means
    the SAME agent, still on the shared legacy token, now sails through
    the same call with 200: a governance regression opened BY the fix meant
    to close one, for the transition window before PR2 mints per-agent
    tokens.

    The fix: for a LEGACY principal ONLY, if the request body still declares
    an ``actor_kind``, the gate decides on THAT declared value — exactly the
    pre-#392 behaviour, preserved until the legacy token is retired. A MINTED
    token's Principal is never second-guessed by the body: the token IS the
    truth there, full stop; ``declared_actor_kind`` is ignored for it, which
    is the entire point of #392.

    ``declared_actor_kind`` is the request body's self-declared field
    (deprecated) — pass ``None`` when the endpoint's body carries no such
    field (e.g. staging's ``ApproveRequest``, which never had one — that
    site has no pre-#392 gate to preserve, so it always decides on
    ``principal.actor_kind``, non-legacy or not)."""
    if principal.is_legacy and declared_actor_kind is not None:
        return declared_actor_kind
    return principal.actor_kind


# ---------------------------------------------------------------------------
# TTL cache for minted-token lookups (clock injectable for tests)
# ---------------------------------------------------------------------------


class _TokenCache:
    """Small TTL + size-bounded memo mapping ``token_hash -> (Principal|None,
    expiry)``.

    Caches negatives (unknown token → None) as well as positives, which blunts
    RE-PROBING of the same wrong value — it does NOT blunt a flood of distinct
    wrong values (see ``_CACHE_MAX_ENTRIES``'s docstring at the module level),
    each of which is a genuine cache miss. This class is reachable PRE-AUTH
    (``_resolve_minted_principal`` populates a negative entry before any
    credential has been validated), so it is also SIZE-bounded: ``put`` evicts
    the oldest/least-recently-touched entry once ``max_entries`` is exceeded
    (a plain LRU via ``OrderedDict``, not a security control by itself — the
    scope check / hmac comparisons are what actually authenticate).

    Guarded by a plain ``threading.Lock`` because the DB lookup on a miss runs
    in a worker thread (``to_thread.run_sync``), so ``get``/``put`` are
    reachable from threads other than the event loop.

    The clock is injected (``time.monotonic`` by default) so tests can drive
    TTL expiry deterministically without sleeping.
    """

    __slots__ = ("_store", "_lock", "_ttl", "_clock", "_max_entries")

    def __init__(
        self,
        ttl_seconds: float = _CACHE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        max_entries: int = _CACHE_MAX_ENTRIES,
    ) -> None:
        self._store: OrderedDict[str, tuple[Optional[Principal], float]] = OrderedDict()
        self._lock = threading.Lock()
        self._ttl = ttl_seconds
        self._clock = clock
        self._max_entries = max_entries

    def get(self, key: str) -> tuple[bool, Optional[Principal]]:
        """Return (hit, principal). ``hit`` False means miss/expired — the
        caller must do a DB lookup. A cached negative is a hit with None.

        A hit moves the entry to the most-recently-used end, so a token under
        active (legitimate) traffic is the last one evicted under pressure."""
        now = self._clock()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return False, None
            principal, expiry = entry
            if now >= expiry:
                # Lazily evict the stale entry.
                del self._store[key]
                return False, None
            self._store.move_to_end(key)
            return True, principal

    def put(self, key: str, principal: Optional[Principal]) -> None:
        """Insert/refresh one entry, then evict oldest entries (FIFO/LRU order)
        while the store exceeds ``max_entries`` — the size ceiling that keeps a
        flood of distinct bogus tokens from growing this dict unboundedly."""
        expiry = self._clock() + self._ttl
        with self._lock:
            self._store[key] = (principal, expiry)
            self._store.move_to_end(key)
            while len(self._store) > self._max_entries:
                self._store.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


# Module-level cache. Not a mutable-config anti-pattern: it is an internal
# memo with no external configuration surface, guarded by its own lock.
_token_cache = _TokenCache()


# ---------------------------------------------------------------------------
# Minted-token DB lookup (rare — cache-miss only)
# ---------------------------------------------------------------------------


class _TokenLookupUnavailable(Exception):
    """The DB could not be reached to resolve a minted token → surfaces as 503
    (fail-closed: an unreachable auth backend must never fall through to 200)."""


def _lookup_minted_token_sync(token_hash: str) -> Optional[Principal]:
    """Resolve a minted token against ``api_tokens`` (SELECT-only).

    Runs in a worker thread (see ``resolve_principal``), so it may borrow a
    sync pool connection without blocking the event loop. Called at most once
    per token per cache-TTL window — the hot path never reaches here.

    Returns None for an unknown/revoked/expired token (→ caller raises 401).
    Raises ``_TokenLookupUnavailable`` if the SELECT itself cannot be
    completed (→ caller raises 503), so a down auth backend is never a
    silent allow.

    ``last_used_at`` bump-best-effort security fix: the bump runs in its OWN
    connection/transaction (``_bump_last_used_best_effort``), AFTER this
    function has already resolved and is about to return a Principal from a
    SUCCESSFUL SELECT. Earlier code ran the UPDATE inside the same
    try/except AND the same transaction as the SELECT — on a read-only
    standby (failover) or a row lock held by a concurrent un-committed
    revoke, the UPDATE raised, the blanket ``except Exception`` mapped it to
    ``_TokenLookupUnavailable`` -> 503, and because a 503 is never cached
    (see ``_resolve_minted_principal``), EVERY subsequent request for EVERY
    minted token re-hit the same failing UPDATE: a read-only failover turned
    into a total minted-token auth outage, even though the SELECT — the part
    that actually decides "is this caller who they say they are" — never
    stopped working. Isolating the bump means the read-only-standby case now
    degrades to "auth succeeds, last_used_at silently stops advancing",
    which is the correct blast radius for a housekeeping side-effect.
    """
    from ootils_core.api.dependencies import _get_ootils_db

    try:
        db = _get_ootils_db()
        with db.conn() as conn:
            row = _select_token_row(conn, token_hash)
    except Exception as exc:
        # Any driver/connection failure resolving the SELECT is treated as
        # "auth backend down" → 503, never a fall-through to allow. Logged
        # without the token.
        logger.warning("auth.token_lookup_failed error=%s", exc)
        raise _TokenLookupUnavailable from exc

    if row is None:
        return None

    _bump_last_used_best_effort(row["token_id"])
    return principal_from_row(row)


def _bump_last_used_best_effort(token_id: UUID) -> None:
    """Update ``api_tokens.last_used_at`` in its OWN connection/transaction,
    isolated from the SELECT that authenticates the caller.

    MUST NEVER raise and MUST NEVER affect the auth decision: any failure
    here (read-only standby, a row lock from a concurrent revoke, a pool
    exhaustion blip) is swallowed and logged at DEBUG — a bookkeeping
    timestamp is not worth trading away authentication availability for.
    Runs on the same worker thread as the caller (already off the event
    loop via ``to_thread.run_sync``), so a second short-lived pool
    connection here is the same cost class as the SELECT's.
    """
    try:
        from ootils_core.api.dependencies import _get_ootils_db

        db = _get_ootils_db()
        with db.conn() as conn:
            conn.execute(
                "UPDATE api_tokens SET last_used_at = now() WHERE token_id = %s",
                (token_id,),
            )
    except Exception as exc:
        logger.debug("auth.last_used_at_bump_failed token_id=%s error=%s", token_id, exc)


def _select_token_row(conn: DictRowConnection, token_hash: str) -> Optional[dict]:
    """SELECT the live token row (not revoked, not expired) for a hash."""
    return conn.execute(
        """
        SELECT token_id, name, actor_kind, scopes, token_prefix
        FROM api_tokens
        WHERE token_hash = %s
          AND revoked_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())
        """,
        (token_hash,),
    ).fetchone()


async def _resolve_minted_principal(presented: str) -> Principal:
    """Cache-first resolution of a minted token; DB lookup off the event loop.

    Cache hit (positive or negative) is pure and instant. A miss dispatches the
    SELECT to the bounded worker threadpool (same pool the sync ``def`` handlers
    use, capped to the DB pool size in ``app.py``'s lifespan) so the event loop
    is never blocked on DB IO.
    """
    token_hash = hash_token(presented)
    hit, cached = _token_cache.get(token_hash)
    if hit:
        principal = cached
    else:
        try:
            principal = await to_thread.run_sync(_lookup_minted_token_sync, token_hash)
        except _TokenLookupUnavailable:
            # Do NOT cache a transient unavailability as a negative — that would
            # pin a 401/allow decision for 30 s off one blip. Surface 503.
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Authentication backend unavailable",
                headers={"WWW-Authenticate": "Bearer"},
            )
        _token_cache.put(token_hash, principal)

    if principal is None:
        logger.warning("auth.invalid_token kind=minted")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return principal


# ---------------------------------------------------------------------------
# Principal resolution (the core dependency) + kill-switch
# ---------------------------------------------------------------------------


def _enforce_fleet_kill_switch(principal: Principal, client_id: str) -> None:
    """503 when the agent fleet is disabled and the caller is an agent.

    Humans and service principals pass. Pool-free: a disabled fleet answers 503
    without ever touching the DB (the decision is env-only).

    ``client_id`` (the token_prefix, or ``"global_token"`` for legacy) is
    passed in explicitly rather than read off ``request.state`` so this
    function has no ordering dependency on WHEN the caller poses request.state
    — it always logs identity regardless of call order."""
    if principal.actor_kind == "agent" and not _agents_enabled():
        # #392 security-review fix: log the BLOCKED agent's identity (name +
        # token_id + non-secret prefix — never the raw token) — an operator
        # who trips this kill switch mid-incident needs to see WHICH agents
        # are still knocking, not just "some agent got a 503". The caller
        # (resolve_principal) also poses request.state.principal/client_id
        # BEFORE invoking this check, so the audit row for this very 503
        # carries the same attribution.
        logger.warning(
            "auth.agent_fleet_disabled name=%s token_id=%s prefix=%s",
            principal.name,
            principal.token_id,
            client_id,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Agent fleet is disabled (OOTILS_AGENTS_ENABLED).",
        )


async def resolve_principal(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
    request: Request = None,  # type: ignore[assignment]  # FastAPI injects; Optional confuses dep resolver
) -> Principal:
    """FastAPI dependency — authenticate the Bearer token and resolve the caller.

    Legacy token → synthetic human/admin Principal (byte-identical to the old
    ``require_auth`` acceptance). Minted ``ootk_`` token → the ``api_tokens``
    row is the truth for actor_kind + scopes.

    Fail-closed: missing/invalid/unknown/revoked/expired → 401; DB unreachable
    on a minted-token cache-miss → 503. Sets ``request.state.principal`` and
    ``request.state.client_id`` (token_prefix, or ``"global_token"`` for the
    legacy token — preserves the existing audit key in ``app.py``).
    """
    if credentials is None:
        logger.warning("auth.missing_token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    presented = credentials.credentials

    if is_minted_token(presented):
        principal = await _resolve_minted_principal(presented)
        client_id = token_prefix(presented)
    else:
        expected = _expected_token()
        # hmac.compare_digest prevents timing-based token enumeration — same
        # comparison the pre-#392 code used, unchanged.
        if not hmac.compare_digest(presented, expected):
            logger.warning("auth.invalid_token kind=legacy")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # DEBUG, not INFO: a legacy token is valid but deprecated — flag it once
        # per request at debug level, never spam INFO on the hot path.
        logger.debug("auth.legacy_token accepted (DEPRECATED — migrate to a minted ootk_ token)")
        principal = legacy_principal()
        client_id = "global_token"

    # #392 security-review fix: pose request.state BEFORE the kill-switch
    # check, not after. The kill switch can itself raise (503, agent fleet
    # disabled) — if it ran first, that 503 would be audited/logged with a
    # blank principal/client_id, so an operator who just tripped the switch
    # mid-incident couldn't see WHICH agent the blocked call belonged to.
    # Posing state first means every downstream consumer (the audit INSERT
    # in app.py, the kill-switch's own log line) has attribution regardless
    # of whether this request goes on to succeed or gets 503'd right here.
    if request is not None:
        request.state.principal = principal
        request.state.client_id = client_id

    _enforce_fleet_kill_switch(principal, client_id)

    return principal


async def require_auth(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
    request: Request = None,  # type: ignore[assignment]
) -> str:
    """Backward-compatible dependency — resolves the principal (populating
    ``request.state``) and RETURNS THE TOKEN STRING, exactly as before #392.

    Every one of the ~24 ``_token: str = Depends(require_auth)`` routers keeps
    its return contract; the new capability (principal on request.state) rides
    alongside it. Kept as a thin alias over ``resolve_principal`` so there is a
    single authentication implementation.
    """
    await resolve_principal(credentials=credentials, request=request)
    # credentials is non-None here (resolve_principal raises 401 otherwise).
    return credentials.credentials  # type: ignore[union-attr]


def require_scope(scope: str) -> Callable[..., Awaitable[Principal]]:
    """Dependency factory — require ``scope`` (or the ``admin`` superset).

    Depends on ``resolve_principal`` so authentication is guaranteed to have run
    (and ``request.state.principal`` to be set) before the scope is checked —
    FastAPI resolves the sub-dependency first, so there is no ordering race. 401
    if no principal could be resolved; 403 if the principal lacks the scope. The
    legacy token holds ``admin`` and therefore satisfies every scope — no
    regression for pre-#392 callers.

    An overridden ``resolve_principal`` (as tests do for ``require_auth``) that
    returns a Principal flows straight through; only a None/absent principal
    yields 401.
    """

    async def _dependency(
        principal: Optional[Principal] = Depends(resolve_principal),
    ) -> Principal:
        if principal is None:
            logger.warning("auth.scope_check_without_principal scope=%s", scope)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing Authorization header",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if not principal.has_scope(scope):
            logger.warning(
                "auth.missing_scope scope=%s actor_kind=%s token=%s",
                scope,
                principal.actor_kind,
                principal.token_id,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"missing scope '{scope}'",
            )
        return principal

    return _dependency
