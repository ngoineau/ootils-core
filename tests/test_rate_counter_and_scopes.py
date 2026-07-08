"""
tests/test_rate_counter_and_scopes.py — PURE unit tests (no PostgreSQL) for the
AN-2 (#392 PR2a) per-token rate counter and scope-enforcement helpers.

Complements test_auth_principal.py (which already covers the pure helpers,
_TokenCache, resolve_principal, resolve_gate_kind, the last_used bump and the
require_scope 401/403 mapping through a file-local app). This file targets the
pieces PR2a added that that file does NOT exercise:

  * ``_RateCounter`` — the per-token sliding-window counter, driven with an
    INJECTED clock so the 60 s window rolls over deterministically (no sleep):
    under-limit allow, over-limit refuse + Retry-After, a REFUSED request that
    does NOT consume a slot, window rollover, LRU size-bound eviction, and a
    lock-serialised concurrency smoke test.
  * ``require_scope`` FACTORY validation — an unknown scope raises ValueError at
    factory-call time (import time for the real routers), the ``admin`` superset
    satisfies every scope in VALID_SCOPES, and the 403 detail names the scope.
  * ``Principal.rate_per_min`` / ``_enforce_rate_limit`` exemptions — a NULL
    budget (and the legacy token's None token_id) never even consults the
    counter (fail-open "uncapped", never "blocked").

No pytest-asyncio in this repo: the async require_scope dependency is driven
with ``asyncio.run()`` inside sync tests (same pattern as test_auth_principal.py).
"""
from __future__ import annotations

import asyncio
import os
import threading
from uuid import uuid4

import pytest

# auth.py validates OOTILS_API_TOKEN at IMPORT time — set it before importing.
os.environ.setdefault("OOTILS_API_TOKEN", "unit-legacy-token")

from fastapi import HTTPException  # noqa: E402

import ootils_core.api.auth as auth  # noqa: E402
from ootils_core.api.auth import (  # noqa: E402
    Principal,
    VALID_SCOPES,
    _RATE_MAX_ENTRIES,
    _RATE_WINDOW_SECONDS,
    _RateCounter,
    _enforce_rate_limit,
    legacy_principal,
    require_scope,
)


class _FakeClock:
    """Manually advanced monotonic-like clock (mirrors test_auth_principal)."""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class _SpyCounter:
    """Stand-in for the module-level _rate_counter that records every call, so a
    test can prove an exempt principal never consults the counter at all."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def check(self, token_id, limit):
        self.calls.append((token_id, limit))
        return None


def _minted(scopes, *, rate_per_min=None, token_id=None):
    return Principal(
        token_id=token_id or uuid4(),
        name="agent",
        actor_kind="agent",
        scopes=frozenset(scopes),
        is_legacy=False,
        rate_per_min=rate_per_min,
    )


# ===========================================================================
# 1. _RateCounter — under the limit
# ===========================================================================


class TestRateCounterUnderLimit:
    def test_first_n_calls_at_the_limit_are_allowed(self):
        rc = _RateCounter(window_seconds=60.0, clock=_FakeClock())
        tid = uuid4()
        # Exactly `limit` allowed calls, each returns None (admitted).
        for _ in range(5):
            assert rc.check(tid, 5) is None

    def test_calls_strictly_below_limit_never_refuse(self):
        rc = _RateCounter(window_seconds=60.0, clock=_FakeClock())
        tid = uuid4()
        assert rc.check(tid, 3) is None
        assert rc.check(tid, 3) is None  # 2 < 3 → still admitted


# ===========================================================================
# 2. _RateCounter — over the limit + Retry-After semantics
# ===========================================================================


class TestRateCounterOverLimit:
    def test_n_plus_one_refused_with_positive_retry(self):
        clock = _FakeClock(1000.0)
        rc = _RateCounter(window_seconds=60.0, clock=clock)
        tid = uuid4()
        assert rc.check(tid, 3) is None
        assert rc.check(tid, 3) is None
        assert rc.check(tid, 3) is None
        retry = rc.check(tid, 3)  # 4th → refused
        assert retry is not None
        assert retry >= 1
        # All three landed at t=1000; the oldest ages out at t=1060 → retry=60.
        assert retry == pytest.approx(60.0)

    def test_refused_request_does_not_consume_a_slot(self):
        clock = _FakeClock(1000.0)
        rc = _RateCounter(window_seconds=60.0, clock=clock)
        tid = uuid4()
        for _ in range(3):
            rc.check(tid, 3)
        assert len(rc._store[tid]) == 3
        # A refused call must NOT append a 4th timestamp (it would extend the
        # window and punish a caller that then backs off exactly Retry-After).
        assert rc.check(tid, 3) is not None
        assert len(rc._store[tid]) == 3

    def test_backing_off_exactly_retry_after_is_admitted(self):
        clock = _FakeClock(1000.0)
        rc = _RateCounter(window_seconds=60.0, clock=clock)
        tid = uuid4()
        rc.check(tid, 3)                      # t=1000
        clock.advance(10.0)
        rc.check(tid, 3)                      # t=1010
        clock.advance(10.0)
        rc.check(tid, 3)                      # t=1020 → full [1000, 1010, 1020]
        retry = rc.check(tid, 3)             # refused at t=1020
        assert retry == pytest.approx(40.0)  # 1000 + 60 − 1020
        clock.advance(retry)                 # back off EXACTLY retry → t=1060
        # The oldest (t=1000) has aged out → one slot freed → admitted.
        assert rc.check(tid, 3) is None

    def test_full_window_rollover_resets_allowance(self):
        clock = _FakeClock(1000.0)
        rc = _RateCounter(window_seconds=60.0, clock=clock)
        tid = uuid4()
        for _ in range(3):
            assert rc.check(tid, 3) is None
        assert rc.check(tid, 3) is not None  # window full → refused
        clock.advance(60.0)                  # the entire window elapses
        # Every timestamp (all at t=1000) is now <= cutoff (1000) → dropped.
        assert rc.check(tid, 3) is None
        assert rc.check(tid, 3) is None
        assert rc.check(tid, 3) is None


# ===========================================================================
# 3. _RateCounter — LRU size bound (memory ceiling, not a security control)
# ===========================================================================


class TestRateCounterSizeBound:
    def test_eviction_at_the_bound_drops_the_oldest_token(self):
        rc = _RateCounter(window_seconds=60.0, clock=_FakeClock(), max_entries=2)
        t1, t2, t3 = uuid4(), uuid4(), uuid4()
        rc.check(t1, 5)
        rc.check(t2, 5)
        rc.check(t3, 5)  # over budget → evicts t1 (least-recently-seen)
        assert t1 not in rc._store
        assert t2 in rc._store and t3 in rc._store
        assert len(rc._store) == 2

    def test_store_never_exceeds_max_entries(self):
        rc = _RateCounter(window_seconds=60.0, clock=_FakeClock(), max_entries=5)
        for _ in range(50):  # a flood of distinct token_ids
            rc.check(uuid4(), 5)
            assert len(rc._store) <= 5
        assert len(rc._store) == 5

    def test_recently_seen_token_survives_eviction(self):
        rc = _RateCounter(window_seconds=60.0, clock=_FakeClock(), max_entries=2)
        t1, t2, t3 = uuid4(), uuid4(), uuid4()
        rc.check(t1, 5)
        rc.check(t2, 5)
        rc.check(t1, 5)   # touch t1 → moves it to the MRU end
        rc.check(t3, 5)   # over budget → evicts t2 (now oldest), NOT t1
        assert t2 not in rc._store
        assert t1 in rc._store and t3 in rc._store

    def test_default_window_and_max_are_the_module_constants(self):
        rc = _RateCounter(clock=_FakeClock())
        assert rc._window == _RATE_WINDOW_SECONDS
        assert rc._max_entries == _RATE_MAX_ENTRIES

    def test_clear_drops_all_buckets(self):
        rc = _RateCounter(window_seconds=60.0, clock=_FakeClock())
        rc.check(uuid4(), 5)
        rc.check(uuid4(), 5)
        rc.clear()
        assert len(rc._store) == 0


# ===========================================================================
# 4. _RateCounter — basic thread safety (lock present + no lost updates)
# ===========================================================================


class TestRateCounterThreadSafety:
    def test_has_a_lock(self):
        rc = _RateCounter()
        # A real threading.Lock exposes acquire/release and context-manager use.
        assert hasattr(rc._lock, "acquire")
        assert hasattr(rc._lock, "release")

    def test_concurrent_checks_do_not_lose_updates(self):
        # A constant clock keeps every append inside the window, and a huge
        # limit keeps every request allowed — so the surviving count MUST equal
        # the total number of allowed calls IFF the lock serialises the append
        # (a lost update under a race would leave fewer than that).
        rc = _RateCounter(window_seconds=60.0, clock=lambda: 1000.0)
        tid = uuid4()
        n_threads, per_thread = 16, 50

        def worker():
            for _ in range(per_thread):
                rc.check(tid, 10_000)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(rc._store[tid]) == n_threads * per_thread


# ===========================================================================
# 5. require_scope — factory-time validation + admin superset + 403 detail
# ===========================================================================


class TestRequireScopeFactory:
    def test_unknown_scope_raises_valueerror_at_the_factory(self):
        # Routers wire Depends(require_scope("...")) at import, so a typo fails
        # LOUDLY at app import, never silently at request time.
        with pytest.raises(ValueError) as ei:
            require_scope("scope-inconnu")
        assert "unknown scope" in str(ei.value).lower()

    @pytest.mark.parametrize("scope", sorted(VALID_SCOPES))
    def test_every_valid_scope_is_constructible(self, scope):
        dep = require_scope(scope)
        assert callable(dep)

    @pytest.mark.parametrize("scope", sorted(VALID_SCOPES))
    def test_admin_principal_satisfies_every_valid_scope(self, scope):
        admin = Principal(
            token_id=None,
            name="legacy",
            actor_kind="human",
            scopes=frozenset({"admin"}),
            is_legacy=True,
        )
        dep = require_scope(scope)
        # Passing principal= explicitly bypasses Depends(resolve_principal); the
        # body runs against our controlled principal.
        result = asyncio.run(dep(principal=admin))
        assert result is admin  # cleared the scope check → returns the principal

    def test_missing_scope_403_names_the_scope(self):
        agent = Principal(
            token_id=uuid4(),
            name="watcher",
            actor_kind="agent",
            scopes=frozenset({"read"}),  # lacks recommend:approve
            is_legacy=False,
        )
        dep = require_scope("recommend:approve")
        with pytest.raises(HTTPException) as ei:
            asyncio.run(dep(principal=agent))
        assert ei.value.status_code == 403
        assert ei.value.detail == "missing scope 'recommend:approve'"


# ===========================================================================
# 6. Principal.rate_per_min / _enforce_rate_limit — fail-open exemptions
# ===========================================================================


class TestRatePerMinExemptions:
    def test_legacy_principal_has_no_rate_cap(self):
        assert legacy_principal().rate_per_min is None

    def test_null_rate_never_consults_the_counter(self, monkeypatch):
        spy = _SpyCounter()
        monkeypatch.setattr(auth, "_rate_counter", spy)
        p = _minted({"read"}, rate_per_min=None)  # capped budget absent
        _enforce_rate_limit(p, "ootk_prefix")     # must be a pure no-op
        assert spy.calls == []                     # the counter was never touched

    def test_legacy_token_id_none_never_consults_the_counter(self, monkeypatch):
        spy = _SpyCounter()
        monkeypatch.setattr(auth, "_rate_counter", spy)
        _enforce_rate_limit(legacy_principal(), "global_token")
        assert spy.calls == []

    def test_capped_token_over_budget_raises_429_with_retry_after(self, monkeypatch):
        fake = _RateCounter(window_seconds=60.0, clock=_FakeClock(1000.0))
        monkeypatch.setattr(auth, "_rate_counter", fake)
        p = _minted({"read"}, rate_per_min=2)
        _enforce_rate_limit(p, "c")  # 1st — under budget
        _enforce_rate_limit(p, "c")  # 2nd — at budget
        with pytest.raises(HTTPException) as ei:
            _enforce_rate_limit(p, "c")  # 3rd — over budget → 429
        assert ei.value.status_code == 429
        # Whole-seconds Retry-After (HTTP spec), floored at 1.
        assert int(ei.value.headers["Retry-After"]) >= 1

    def test_capped_token_under_budget_passes(self, monkeypatch):
        fake = _RateCounter(window_seconds=60.0, clock=_FakeClock(1000.0))
        monkeypatch.setattr(auth, "_rate_counter", fake)
        p = _minted({"read"}, rate_per_min=5)
        for _ in range(5):  # exactly the budget — none refused
            _enforce_rate_limit(p, "c")
