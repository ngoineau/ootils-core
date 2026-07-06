"""
Unit tests for auth.py principal resolution + the enterprise-floor helpers
(chantier #392 PR1, plus the #392 security-review fixes). PURE — no
PostgreSQL, no live pool.

Layers exercised:
  * Pure helpers (is_minted_token / hash_token / token_prefix /
    principal_from_row / legacy_principal / Principal.has_scope /
    resolve_gate_kind — security-review defect 9).
  * _TokenCache with an INJECTED clock (deterministic TTL expiry — no sleep)
    AND a size bound (defect 2 — LRU eviction via OrderedDict).
  * resolve_principal, with the minted-token DB lookup monkeypatched out
    (_lookup_minted_token_sync) and the env driven via monkeypatch: legacy
    vs minted, unknown/revoked/expired -> 401, DB-down -> 503 (and NOT cached),
    the OOTILS_AGENTS_ENABLED fleet kill switch.
  * _bump_last_used_best_effort (defects 3+4): must never raise, and a
    SELECT-ok/bump-ko lookup must still yield a Principal.
  * require_scope, driven through a file-local FastAPI app + TestClient so the
    401/403 mapping and the exact `missing scope '<scope>'` detail are covered
    end to end.

No pytest-asyncio in this repo: async coroutines are driven with
``asyncio.run()`` inside sync tests (same pattern as test_mps_approve.py).
"""
from __future__ import annotations

import asyncio
import os
from uuid import UUID, uuid4

import pytest

# auth.py validates OOTILS_API_TOKEN at IMPORT time — set it before importing.
os.environ.setdefault("OOTILS_API_TOKEN", "unit-legacy-token")

import ootils_core.api.auth as auth  # noqa: E402
from ootils_core.api.auth import (  # noqa: E402
    Principal,
    _CACHE_TTL_SECONDS,
    _MINTED_PREFIX,
    _TokenCache,
    _TokenLookupUnavailable,
    VALID_ACTOR_KINDS,
    hash_token,
    is_minted_token,
    legacy_principal,
    principal_from_row,
    require_scope,
    resolve_gate_kind,
    resolve_principal,
    token_prefix,
)

_LEGACY = "unit-legacy-token-value-abc123"


@pytest.fixture(autouse=True)
def _isolate_auth(monkeypatch):
    """Every test gets a known legacy token, the fleet enabled, and a clean
    module-level token cache (so cross-test state never leaks)."""
    monkeypatch.setenv("OOTILS_API_TOKEN", _LEGACY)
    monkeypatch.delenv("OOTILS_AGENTS_ENABLED", raising=False)
    auth._token_cache.clear()
    yield
    auth._token_cache.clear()


# ---------------------------------------------------------------------------
# Helpers for driving the async dependency from sync tests
# ---------------------------------------------------------------------------


class _Creds:
    """Stand-in for HTTPAuthorizationCredentials (only .credentials is read)."""

    def __init__(self, token: str) -> None:
        self.scheme = "Bearer"
        self.credentials = token


class _FakeState:
    pass


class _FakeRequest:
    def __init__(self) -> None:
        self.state = _FakeState()


def _resolve(token: str | None):
    """Run resolve_principal for a bearer token (or None) and return the
    Principal. Raises the HTTPException verbatim for the caller to inspect."""
    creds = _Creds(token) if token is not None else None
    request = _FakeRequest()
    return asyncio.run(resolve_principal(credentials=creds, request=request)), request


# ===========================================================================
# 1. Pure helpers
# ===========================================================================


class TestIsMintedToken:
    def test_minted_prefix_is_minted(self):
        assert is_minted_token(_MINTED_PREFIX + "abc") is True

    def test_legacy_value_is_not_minted(self):
        assert is_minted_token("plain-legacy-token") is False

    def test_empty_string_is_not_minted(self):
        assert is_minted_token("") is False

    def test_prefix_must_be_leading(self):
        # The prefix only counts when it leads the string.
        assert is_minted_token("x" + _MINTED_PREFIX) is False


class TestHashToken:
    def test_is_deterministic(self):
        assert hash_token("ootk_abc") == hash_token("ootk_abc")

    def test_is_64_hex_chars(self):
        digest = hash_token("ootk_whatever")
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)

    def test_differs_from_the_raw_token(self):
        raw = "ootk_secret"
        assert hash_token(raw) != raw

    def test_distinct_inputs_distinct_hashes(self):
        assert hash_token("ootk_a") != hash_token("ootk_b")

    def test_matches_hashlib_sha256(self):
        import hashlib

        raw = "ootk_reference"
        assert hash_token(raw) == hashlib.sha256(raw.encode("utf-8")).hexdigest()


class TestTokenPrefix:
    def test_length_capped(self):
        # Long token -> a short, fixed-length non-secret slice.
        pfx = token_prefix("ootk_" + "z" * 100)
        assert len(pfx) == auth._PREFIX_LEN

    def test_stable_for_same_input(self):
        assert token_prefix("ootk_stable_abc") == token_prefix("ootk_stable_abc")

    def test_is_a_leading_slice(self):
        token = "ootk_abcdefghijklmnop"
        assert token.startswith(token_prefix(token))

    def test_short_token_returned_whole(self):
        assert token_prefix("ootk") == "ootk"


class TestPrincipalFromRow:
    def test_scopes_list_becomes_frozenset(self):
        tid = uuid4()
        p = principal_from_row(
            {
                "token_id": tid,
                "name": "shortage-watcher",
                "actor_kind": "agent",
                "scopes": ["read", "recommend:draft"],
                "token_prefix": "ootk_abc",
            }
        )
        assert p.token_id == tid
        assert p.name == "shortage-watcher"
        assert p.actor_kind == "agent"
        assert p.scopes == frozenset({"read", "recommend:draft"})
        assert isinstance(p.scopes, frozenset)
        assert p.is_legacy is False

    def test_none_scopes_becomes_empty_frozenset(self):
        p = principal_from_row(
            {
                "token_id": uuid4(),
                "name": "svc",
                "actor_kind": "service",
                "scopes": None,
                "token_prefix": "ootk_svc",
            }
        )
        assert p.scopes == frozenset()

    def test_empty_scopes_list_becomes_empty_frozenset(self):
        p = principal_from_row(
            {
                "token_id": uuid4(),
                "name": "svc",
                "actor_kind": "service",
                "scopes": [],
                "token_prefix": "ootk_svc",
            }
        )
        assert p.scopes == frozenset()

    def test_token_id_string_is_coerced_to_uuid(self):
        tid = uuid4()
        p = principal_from_row(
            {
                "token_id": str(tid),
                "name": "n",
                "actor_kind": "human",
                "scopes": ["admin"],
                "token_prefix": "ootk_n",
            }
        )
        assert p.token_id == tid
        assert isinstance(p.token_id, UUID)

    def test_actor_kind_carried_through(self):
        for kind in VALID_ACTOR_KINDS:
            p = principal_from_row(
                {
                    "token_id": uuid4(),
                    "name": "n",
                    "actor_kind": kind,
                    "scopes": [],
                    "token_prefix": "ootk_n",
                }
            )
            assert p.actor_kind == kind


class TestLegacyPrincipal:
    def test_is_human_admin_legacy(self):
        p = legacy_principal()
        assert p.actor_kind == "human"
        assert p.is_legacy is True
        assert p.token_id is None
        assert p.name == "legacy"

    def test_holds_admin_scope(self):
        assert legacy_principal().has_scope("admin")

    def test_admin_satisfies_any_scope(self):
        p = legacy_principal()
        assert p.has_scope("read")
        assert p.has_scope("recommend:approve")
        assert p.has_scope("anything:at:all")


class TestHasScope:
    def _agent(self, scopes):
        return Principal(
            token_id=uuid4(),
            name="a",
            actor_kind="agent",
            scopes=frozenset(scopes),
            is_legacy=False,
        )

    def test_exact_scope_present(self):
        assert self._agent({"read"}).has_scope("read") is True

    def test_absent_scope_is_false(self):
        assert self._agent({"read"}).has_scope("recommend:approve") is False

    def test_admin_is_superset(self):
        assert self._agent({"admin"}).has_scope("recommend:approve") is True

    def test_empty_scopes_never_satisfy(self):
        assert self._agent(set()).has_scope("read") is False


# ===========================================================================
# 2. _TokenCache — injected clock, deterministic TTL
# ===========================================================================


class _FakeClock:
    """Manually advanced monotonic-like clock."""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _principal(token_id=None):
    return Principal(
        token_id=token_id or uuid4(),
        name="cached",
        actor_kind="agent",
        scopes=frozenset({"read"}),
        is_legacy=False,
    )


class TestTokenCache:
    def test_miss_then_put_then_hit(self):
        clock = _FakeClock()
        cache = _TokenCache(ttl_seconds=30.0, clock=clock)

        hit, value = cache.get("h1")
        assert hit is False and value is None

        p = _principal()
        cache.put("h1", p)

        hit, value = cache.get("h1")
        assert hit is True
        assert value is p

    def test_expiry_is_strict_after_ttl(self):
        clock = _FakeClock()
        cache = _TokenCache(ttl_seconds=30.0, clock=clock)
        cache.put("h1", _principal())

        # Just before TTL: still a hit.
        clock.advance(29.999)
        hit, _ = cache.get("h1")
        assert hit is True

        # AT the TTL boundary the entry is expired (get uses now >= expiry).
        clock.advance(0.001)  # total 30.0
        hit, value = cache.get("h1")
        assert hit is False
        assert value is None

    def test_negative_entry_is_cached(self):
        clock = _FakeClock()
        cache = _TokenCache(ttl_seconds=30.0, clock=clock)

        cache.put("bogus", None)  # cache a NEGATIVE lookup

        hit, value = cache.get("bogus")
        assert hit is True  # a cached negative is a HIT, not a miss
        assert value is None

    def test_negative_entry_also_expires(self):
        clock = _FakeClock()
        cache = _TokenCache(ttl_seconds=10.0, clock=clock)
        cache.put("bogus", None)

        clock.advance(10.0)
        hit, value = cache.get("bogus")
        assert hit is False
        assert value is None

    def test_clear_drops_all_entries(self):
        cache = _TokenCache(ttl_seconds=30.0, clock=_FakeClock())
        cache.put("h1", _principal())
        cache.put("h2", None)

        cache.clear()

        assert cache.get("h1") == (False, None)
        assert cache.get("h2") == (False, None)

    def test_default_ttl_is_module_constant(self):
        # The parameter defaults to the documented module constant.
        cache = _TokenCache(clock=_FakeClock())
        assert cache._ttl == _CACHE_TTL_SECONDS


# ===========================================================================
# 2b. _TokenCache size bound (#392 security-review defect 2) — LRU eviction
# ===========================================================================


class TestTokenCacheSizeBound:
    """A flood of DISTINCT bogus ``ootk_<random>`` tokens is, by definition,
    all cache MISSES (TTL memoisation only helps on a REPEATED value) — so the
    size bound, not the TTL, is what keeps the store from growing unboundedly.
    Reachable pre-auth: ``_resolve_minted_principal`` populates a negative
    entry before any credential has been validated."""

    def test_put_beyond_max_entries_evicts_oldest(self):
        clock = _FakeClock()
        cache = _TokenCache(ttl_seconds=30.0, clock=clock, max_entries=3)

        cache.put("k1", _principal())
        cache.put("k2", _principal())
        cache.put("k3", _principal())
        cache.put("k4", _principal())  # over budget -> evicts k1 (oldest)

        assert cache.get("k1") == (False, None)  # evicted
        assert cache.get("k2")[0] is True
        assert cache.get("k3")[0] is True
        assert cache.get("k4")[0] is True

    def test_store_never_exceeds_max_entries(self):
        clock = _FakeClock()
        cache = _TokenCache(ttl_seconds=30.0, clock=clock, max_entries=5)

        for i in range(50):  # N + K distinct keys, N=5
            cache.put(f"flood-{i}", None)  # a flood of NEGATIVE entries
            assert len(cache._store) <= 5

        assert len(cache._store) == 5

    def test_negative_entries_count_toward_the_bound(self):
        """A flood of unknown ootk_ tokens (all negative lookups) must evict
        just like positive entries — the bound is on ENTRY COUNT, not on
        'real' (positive) principals only."""
        clock = _FakeClock()
        cache = _TokenCache(ttl_seconds=30.0, clock=clock, max_entries=2)

        cache.put("bogus-1", None)
        cache.put("bogus-2", None)
        cache.put("bogus-3", None)  # evicts bogus-1

        assert cache.get("bogus-1") == (False, None)
        assert cache.get("bogus-2") == (True, None)
        assert cache.get("bogus-3") == (True, None)

    def test_get_hit_moves_entry_to_end_protecting_it_from_eviction(self):
        """A `get` hit must move the entry to the MRU end, so a token under
        active legitimate traffic is not the one evicted when the cache is
        under flood pressure from distinct bogus tokens."""
        clock = _FakeClock()
        cache = _TokenCache(ttl_seconds=30.0, clock=clock, max_entries=3)

        cache.put("k1", _principal())
        cache.put("k2", _principal())
        cache.put("k3", _principal())
        # Touch k1 (a "real" hit) — it becomes the MRU entry.
        hit, _ = cache.get("k1")
        assert hit is True

        cache.put("k4", _principal())  # over budget: evicts the NOW-oldest, k2

        assert cache.get("k1")[0] is True  # protected by the touch above
        assert cache.get("k2") == (False, None)  # evicted instead of k1
        assert cache.get("k3")[0] is True
        assert cache.get("k4")[0] is True

    def test_default_max_entries_is_module_constant(self):
        cache = _TokenCache(clock=_FakeClock())
        assert cache._max_entries == auth._CACHE_MAX_ENTRIES


# ===========================================================================
# 3. resolve_principal — legacy / minted / kill switch, lookup monkeypatched
# ===========================================================================


class TestResolveLegacy:
    def test_correct_legacy_token_is_human_admin(self):
        principal, request = _resolve(_LEGACY)
        assert principal.is_legacy is True
        assert principal.actor_kind == "human"
        assert principal.has_scope("admin")
        # client_id audit key preserved for the legacy token.
        assert request.state.client_id == "global_token"
        assert request.state.principal is principal

    def test_wrong_legacy_token_is_401(self):
        with pytest.raises(auth.HTTPException) as ei:
            _resolve("not-the-legacy-token")
        assert ei.value.status_code == 401

    def test_missing_header_is_401(self):
        with pytest.raises(auth.HTTPException) as ei:
            _resolve(None)
        assert ei.value.status_code == 401


class TestResolveMinted:
    def _install_lookup(self, monkeypatch, mapping, *, raises=None, counter=None):
        """Replace the DB lookup with an in-memory dict; optionally raise, and
        optionally count invocations."""

        def _fake(token_hash):
            if counter is not None:
                counter.append(token_hash)
            if raises is not None:
                raise raises
            return mapping.get(token_hash)

        monkeypatch.setattr(auth, "_lookup_minted_token_sync", _fake)

    def test_valid_minted_token_reflects_row(self, monkeypatch):
        token = _MINTED_PREFIX + "agentsecret"
        row_principal = Principal(
            token_id=uuid4(),
            name="shortage-watcher",
            actor_kind="agent",
            scopes=frozenset({"read", "recommend:draft"}),
            is_legacy=False,
        )
        self._install_lookup(monkeypatch, {hash_token(token): row_principal})

        principal, request = _resolve(token)
        assert principal is row_principal
        assert principal.actor_kind == "agent"
        assert principal.scopes == frozenset({"read", "recommend:draft"})
        assert principal.is_legacy is False
        # client_id audit key is the non-secret token prefix, not the raw token.
        assert request.state.client_id == token_prefix(token)

    def test_unknown_minted_token_is_401(self, monkeypatch):
        token = _MINTED_PREFIX + "ghost"
        self._install_lookup(monkeypatch, {})  # lookup returns None
        with pytest.raises(auth.HTTPException) as ei:
            _resolve(token)
        assert ei.value.status_code == 401

    def test_revoked_or_expired_lookup_none_is_401(self, monkeypatch):
        # The SELECT filters revoked/expired -> returns None -> 401, identical
        # to the unknown-token path (auth cannot tell them apart, by design).
        token = _MINTED_PREFIX + "revoked"
        self._install_lookup(monkeypatch, {})
        with pytest.raises(auth.HTTPException) as ei:
            _resolve(token)
        assert ei.value.status_code == 401

    def test_db_down_is_503(self, monkeypatch):
        token = _MINTED_PREFIX + "dbdown"
        self._install_lookup(monkeypatch, {}, raises=_TokenLookupUnavailable())
        with pytest.raises(auth.HTTPException) as ei:
            _resolve(token)
        assert ei.value.status_code == 503

    def test_db_down_503_is_not_cached(self, monkeypatch):
        """A transient 503 must NOT be memoised as a negative — the very next
        request re-attempts the DB lookup instead of serving a cached failure."""
        token = _MINTED_PREFIX + "flaky"
        calls: list[str] = []
        self._install_lookup(
            monkeypatch, {}, raises=_TokenLookupUnavailable(), counter=calls
        )

        with pytest.raises(auth.HTTPException) as ei1:
            _resolve(token)
        assert ei1.value.status_code == 503

        with pytest.raises(auth.HTTPException) as ei2:
            _resolve(token)
        assert ei2.value.status_code == 503

        # Two independent lookups: the failure was never cached.
        assert len(calls) == 2

    def test_positive_lookup_is_cached_single_db_hit(self, monkeypatch):
        """Sanity counterpart to the 503 test: a SUCCESSFUL lookup is memoised,
        so a second resolve within the TTL does not re-hit the DB."""
        token = _MINTED_PREFIX + "cached"
        calls: list[str] = []
        row_principal = _principal()
        self._install_lookup(
            monkeypatch, {hash_token(token): row_principal}, counter=calls
        )

        p1, _ = _resolve(token)
        p2, _ = _resolve(token)
        assert p1 is row_principal and p2 is row_principal
        assert len(calls) == 1  # second call served from the process cache


class TestFleetKillSwitch:
    def _install_agent(self, monkeypatch, actor_kind, scopes=("read",)):
        token = _MINTED_PREFIX + "k-" + actor_kind
        principal = Principal(
            token_id=uuid4(),
            name=actor_kind,
            actor_kind=actor_kind,
            scopes=frozenset(scopes),
            is_legacy=False,
        )
        monkeypatch.setattr(
            auth,
            "_lookup_minted_token_sync",
            lambda h, _p=principal: _p,
        )
        return token

    def test_agent_blocked_when_fleet_disabled(self, monkeypatch):
        monkeypatch.setenv("OOTILS_AGENTS_ENABLED", "0")
        token = self._install_agent(monkeypatch, "agent")
        with pytest.raises(auth.HTTPException) as ei:
            _resolve(token)
        assert ei.value.status_code == 503

    def test_human_passes_when_fleet_disabled(self, monkeypatch):
        monkeypatch.setenv("OOTILS_AGENTS_ENABLED", "0")
        token = self._install_agent(monkeypatch, "human")
        principal, _ = _resolve(token)
        assert principal.actor_kind == "human"

    def test_service_passes_when_fleet_disabled(self, monkeypatch):
        monkeypatch.setenv("OOTILS_AGENTS_ENABLED", "0")
        token = self._install_agent(monkeypatch, "service")
        principal, _ = _resolve(token)
        assert principal.actor_kind == "service"

    def test_legacy_human_passes_when_fleet_disabled(self, monkeypatch):
        # The legacy token maps to actor_kind='human' -> the agent kill switch
        # never touches it.
        monkeypatch.setenv("OOTILS_AGENTS_ENABLED", "0")
        principal, _ = _resolve(_LEGACY)
        assert principal.actor_kind == "human"
        assert principal.is_legacy is True

    def test_agent_passes_when_fleet_enabled(self, monkeypatch):
        monkeypatch.setenv("OOTILS_AGENTS_ENABLED", "1")
        token = self._install_agent(monkeypatch, "agent")
        principal, _ = _resolve(token)
        assert principal.actor_kind == "agent"


# ===========================================================================
# 3b. resolve_gate_kind — #392 security-review defect 9
#
# Pre-#392: EVERY caller self-declared actor_kind, and the gate decided on
# that declared value. Post-#392: a MINTED token's Principal is the sole
# truth (declared value ignored) — but the LEGACY token resolves to a
# synthetic human/admin Principal, so naively gating on principal.actor_kind
# would let an agent that HONESTLY declares 'agent' on the shared legacy
# token now sail past a human-only gate it used to get 403 on. The fix: for
# a LEGACY principal ONLY, an explicit declared_actor_kind overrides;
# declared_actor_kind is IGNORED for every minted (non-legacy) principal.
# ===========================================================================


class TestResolveGateKind:
    def _legacy(self) -> Principal:
        return legacy_principal()

    def _minted(self, actor_kind: str = "agent") -> Principal:
        return Principal(
            token_id=uuid4(),
            name="minted",
            actor_kind=actor_kind,
            scopes=frozenset({"read"}),
            is_legacy=False,
        )

    def test_legacy_with_declared_agent_returns_agent(self):
        # (a) pre-#392 behaviour preserved: an honest agent declaration on the
        # shared legacy token still gates as 'agent'.
        assert resolve_gate_kind(self._legacy(), "agent") == "agent"

    def test_legacy_with_no_declaration_returns_principal_kind(self):
        # (b) no body field at all (e.g. staging's ApproveRequest) -> falls
        # back to the legacy principal's synthetic 'human'.
        assert resolve_gate_kind(self._legacy(), None) == "human"

    def test_minted_with_declared_human_lie_returns_token_kind(self):
        # (c) THE #392 closure: a minted token's body claiming 'human' is
        # ignored outright — the token's real kind always wins.
        minted = self._minted("agent")
        assert resolve_gate_kind(minted, "human") == "agent"

    def test_minted_with_no_declaration_returns_principal_kind(self):
        # (d) minted + no body field -> principal.actor_kind, same as (c).
        minted = self._minted("service")
        assert resolve_gate_kind(minted, None) == "service"

    def test_legacy_with_declared_human_returns_human(self):
        # Sanity: an explicit 'human' declaration on legacy is a no-op change
        # (still 'human'), not a special case that skips the fallback branch.
        assert resolve_gate_kind(self._legacy(), "human") == "human"

    def test_minted_human_token_with_declared_agent_lie_returns_human(self):
        # Symmetric to (c): the token wins regardless of which direction the
        # body's lie points.
        minted = self._minted("human")
        assert resolve_gate_kind(minted, "agent") == "human"


# ===========================================================================
# 3c. _bump_last_used_best_effort — #392 security-review defects 3+4
#
# The last_used_at UPDATE must be isolated from the SELECT that authenticates
# the caller: a read-only standby / a concurrent row lock from a revoke must
# degrade to "auth succeeds, last_used_at silently stops advancing", never to
# an auth outage.
# ===========================================================================


class TestBumpLastUsedBestEffort:
    def test_never_raises_when_the_update_connection_fails(self, monkeypatch):
        class _ExplodingConn:
            def __enter__(self):
                raise RuntimeError("read-only standby: cannot UPDATE")

            def __exit__(self, *exc_info):
                return False

        class _FakeDb:
            def conn(self):
                return _ExplodingConn()

        monkeypatch.setattr(
            "ootils_core.api.dependencies._get_ootils_db",
            lambda: _FakeDb(),
        )

        # Must swallow the failure and simply return — no exception escapes.
        auth._bump_last_used_best_effort(uuid4())

    def test_lookup_returns_principal_even_when_bump_fails(self, monkeypatch):
        """SELECT-ok + bump-ko (a REAL failure inside the bump's own DB call,
        not a stand-in that raises past it) must still resolve a Principal —
        the bump is isolated INSIDE _bump_last_used_best_effort's own
        try/except (auth.py), not by the caller catching anything: note that
        _lookup_minted_token_sync's own try/except wraps ONLY the SELECT, so
        this only holds together because the bump swallows its own failure
        (see test_never_raises_when_the_update_connection_fails above) —
        this test proves the end-to-end consequence of that contract."""
        token_id = uuid4()
        row = {
            "token_id": token_id,
            "name": "watcher",
            "actor_kind": "agent",
            "scopes": ["read"],
            "token_prefix": "ootk_watch",
        }

        select_calls: list[str] = []

        class _SelectCursorResult:
            def fetchone(self_inner):
                return row

        class _SelectConn:
            def execute(self_inner, *args, **kwargs):
                select_calls.append("select")
                return _SelectCursorResult()

        class _SelectConnCtx:
            def __enter__(self_inner):
                return _SelectConn()

            def __exit__(self_inner, *exc_info):
                return False

        class _BumpConnCtx:
            """The bump's OWN connection — opening it fails, exactly the
            read-only-standby scenario _bump_last_used_best_effort's
            docstring describes."""

            def __enter__(self_inner):
                raise RuntimeError("read-only standby: cannot UPDATE")

            def __exit__(self_inner, *exc_info):
                return False

        class _FakeDb:
            """Same SINGLETON instance serves BOTH call sites (SELECT then
            bump — each site calls _get_ootils_db() independently, so the
            patched function must return the SAME object both times); the
            SELECT succeeds, the bump's connection acquisition fails."""

            def __init__(self_inner):
                self_inner._calls = 0

            def conn(self_inner):
                self_inner._calls += 1
                return _SelectConnCtx() if self_inner._calls == 1 else _BumpConnCtx()

        shared_db = _FakeDb()
        monkeypatch.setattr(
            "ootils_core.api.dependencies._get_ootils_db",
            lambda: shared_db,
        )

        # No exception escapes — the bump swallows its own connection failure
        # (real bump code path, not a stand-in), and a Principal comes back.
        principal = auth._lookup_minted_token_sync("irrelevant-hash")
        assert select_calls == ["select"]
        assert principal is not None
        assert principal.token_id == token_id
        assert principal.actor_kind == "agent"


# ===========================================================================
# 4. require_scope — driven through a file-local FastAPI app
# ===========================================================================


def _scope_app():
    """A tiny app whose one route requires the 'recommend:approve' scope.

    resolve_principal is overridden per-test via dependency_overrides so the
    scope check runs against a Principal we control, with no DB and no bearer
    parsing."""
    from fastapi import Depends, FastAPI

    app = FastAPI()

    @app.get("/guarded")
    async def guarded(
        principal: Principal = Depends(require_scope("recommend:approve")),
    ):
        return {"actor_kind": principal.actor_kind}

    return app


def _override_principal(app, principal):
    # require_scope depends on resolve_principal; override THAT so the sub-dep
    # returns our principal (or None) without any bearer/DB work.
    async def _fake():
        return principal

    app.dependency_overrides[resolve_principal] = _fake


class TestRequireScope:
    def _client(self, principal):
        from fastapi.testclient import TestClient

        app = _scope_app()
        _override_principal(app, principal)
        return TestClient(app)

    def test_no_principal_is_401(self):
        client = self._client(None)
        resp = client.get("/guarded")
        assert resp.status_code == 401

    def test_missing_scope_is_403_with_exact_detail(self):
        agent = Principal(
            token_id=uuid4(),
            name="watcher",
            actor_kind="agent",
            scopes=frozenset({"read"}),  # lacks recommend:approve
            is_legacy=False,
        )
        client = self._client(agent)
        resp = client.get("/guarded")
        assert resp.status_code == 403
        assert resp.json()["detail"] == "missing scope 'recommend:approve'"

    def test_present_scope_passes(self):
        human = Principal(
            token_id=uuid4(),
            name="ngo",
            actor_kind="human",
            scopes=frozenset({"recommend:approve"}),
            is_legacy=False,
        )
        client = self._client(human)
        resp = client.get("/guarded")
        assert resp.status_code == 200
        assert resp.json()["actor_kind"] == "human"

    def test_admin_superset_passes(self):
        admin = Principal(
            token_id=None,
            name="legacy",
            actor_kind="human",
            scopes=frozenset({"admin"}),
            is_legacy=True,
        )
        client = self._client(admin)
        resp = client.get("/guarded")
        assert resp.status_code == 200
