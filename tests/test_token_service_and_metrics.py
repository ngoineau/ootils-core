"""
Unit tests for the AN-2 PR2b token/metrics surface (#392) — PURE: no PostgreSQL,
no live pool. Three modules under test:

  * ``api/token_service.py`` — mint_token / revoke_token. Validation
    (actor_kind / scopes / rate_per_min) is asserted to run BEFORE any DB access
    via a conn-spy that must record ZERO execute() calls on the failure paths;
    revoke_token's cache-invalidation side-effect is asserted to fire ONLY on a
    real flip (rowcount>0), never on a no-op re-revoke.
  * ``api/metrics.py`` — the process-global Prometheus collectors + helpers.
    Counters are process-GLOBAL singletons (constructed once at import), so every
    increment assertion reads a DELTA around the call, and uses a UNIQUE label
    value so the pre-call baseline is a clean 0.
  * ``api/routers/tokens.py`` — driven through a real ``create_app()`` +
    TestClient with ``resolve_principal`` and ``get_db`` overridden. The DB is a
    duck-typed fake conn dispatching canned results on the SQL text (no Postgres).
    Proves: POST returns the cleartext ONCE and never a hash; GET/list carries no
    secret material; DELETE is 204; every token verb AND the root /metrics route
    are 403 without the admin scope; an invalid scope is a hand-authored 422.

No pytest-asyncio in this repo; the router is exercised synchronously through
TestClient. auth.py validates OOTILS_API_TOKEN at import time and app.py builds a
module-level app at import — so the env token is set before the first import.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

# auth.py validates OOTILS_API_TOKEN at IMPORT time, and importing app.py builds
# a module-level `app = create_app()` (which calls _expected_token()) — set the
# token before any ootils import below.
os.environ.setdefault("OOTILS_API_TOKEN", "unit-legacy-token")

import ootils_core.api.auth as auth  # noqa: E402
import ootils_core.api.metrics as metrics  # noqa: E402
import ootils_core.api.token_service as token_service  # noqa: E402
from ootils_core.api.auth import (  # noqa: E402
    Principal,
    resolve_principal,
)
from ootils_core.api.token_service import mint_token, revoke_token  # noqa: E402


# ===========================================================================
# Shared fakes / helpers
# ===========================================================================


class _Result:
    """Duck-typed psycopg cursor-result: fetchone / fetchall / rowcount."""

    def __init__(self, *, one=None, many=None, rowcount=0) -> None:
        self._one = one
        self._many = [] if many is None else many
        self.rowcount = rowcount

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _SpyConn:
    """Minimal duck-typed stand-in for a dict_row connection.

    Records every execute() (``calls``) so the validation-before-IO contract can
    be asserted (zero calls on a rejected mint). Dispatches canned results on the
    SQL text so it can back mint_token, revoke_token AND the router's read-backs
    without a database.
    """

    def __init__(
        self,
        *,
        mint_token_id: UUID | None = None,
        revoke_rowcount: int = 1,
        exists: bool = True,
        list_rows=None,
        readback=None,
    ) -> None:
        self.calls: list[tuple[str, object]] = []
        self.mint_token_id = mint_token_id or uuid4()
        self.revoke_rowcount = revoke_rowcount
        self.exists = exists
        self.list_rows = [] if list_rows is None else list_rows
        self.readback = readback

    def execute(self, sql, params=None):
        s = " ".join(str(sql).split())
        self.calls.append((s, params))
        if "INSERT INTO api_tokens" in s:
            return _Result(one={"token_id": self.mint_token_id})
        if "UPDATE api_tokens SET revoked_at" in s:
            return _Result(rowcount=self.revoke_rowcount)
        if "SELECT 1 FROM api_tokens" in s:
            return _Result(one={"?column?": 1} if self.exists else None)
        if "last_used_at" in s:  # list_tokens SELECT (has last_used_at column)
            return _Result(many=self.list_rows)
        if "SELECT token_id, name, token_prefix" in s:  # create read-back
            return _Result(one=self.readback)
        raise AssertionError(f"unexpected SQL: {s}")


def _principal(actor_kind: str = "agent", scopes=("read",)) -> Principal:
    return Principal(
        token_id=uuid4(),
        name="unit",
        actor_kind=actor_kind,
        scopes=frozenset(scopes),
        is_legacy=False,
    )


def _admin() -> Principal:
    """The legacy-style admin principal (superset scope) the token router gates on."""
    return Principal(
        token_id=None,
        name="admin",
        actor_kind="human",
        scopes=frozenset({"admin"}),
        is_legacy=True,
    )


@pytest.fixture(autouse=True)
def _clear_token_cache():
    """token_service.revoke_token clears the module-level auth._token_cache on a
    flip; keep it clean around every test so nothing leaks between them."""
    auth._token_cache.clear()
    yield
    auth._token_cache.clear()


# ===========================================================================
# 1. mint_token — cleartext + entropy, validation BEFORE any DB access
# ===========================================================================


class TestMintToken:
    def test_returns_ootk_cleartext_with_entropy(self):
        conn = _SpyConn()
        token_id, cleartext = mint_token(
            conn, name="watcher", actor_kind="agent", scopes=["read"]
        )
        assert token_id == UUID(str(conn.mint_token_id))
        assert cleartext.startswith("ootk_")
        # 256 bits of urlsafe entropy → well beyond the 5-char prefix.
        assert len(cleartext) > len("ootk_") + 30
        # Exactly one INSERT reached the DB.
        assert sum("INSERT INTO api_tokens" in c[0] for c in conn.calls) == 1

    def test_two_mints_have_distinct_cleartext(self):
        conn = _SpyConn()
        _, a = mint_token(conn, name="n", actor_kind="agent", scopes=["read"])
        _, b = mint_token(conn, name="n", actor_kind="agent", scopes=["read"])
        assert a != b

    def test_unknown_actor_kind_raises_before_any_db_access(self):
        conn = _SpyConn()
        with pytest.raises(ValueError) as ei:
            mint_token(conn, name="n", actor_kind="robot", scopes=["read"])
        assert "actor_kind" in str(ei.value)
        assert conn.calls == []  # conn-spy: zero DB access on rejection

    def test_unknown_scope_raises_before_any_db_access(self):
        conn = _SpyConn()
        with pytest.raises(ValueError) as ei:
            mint_token(conn, name="n", actor_kind="agent", scopes=["bogus:scope"])
        assert "bogus:scope" in str(ei.value)
        assert "scope" in str(ei.value).lower()
        assert conn.calls == []

    def test_non_positive_rate_per_min_raises_before_any_db_access(self):
        conn = _SpyConn()
        with pytest.raises(ValueError) as ei:
            mint_token(
                conn, name="n", actor_kind="agent", scopes=["read"], rate_per_min=0
            )
        assert "rate_per_min" in str(ei.value)
        assert conn.calls == []


# ===========================================================================
# 2. revoke_token — flip semantics + cache invalidation ONLY on flip
# ===========================================================================


class TestRevokeToken:
    def test_first_flip_returns_true_and_invalidates_cache(self, monkeypatch):
        calls: list[int] = []
        monkeypatch.setattr(
            token_service, "invalidate_token_cache", lambda: calls.append(1)
        )
        conn = _SpyConn(revoke_rowcount=1)

        assert revoke_token(conn, uuid4()) is True
        assert calls == [1]  # invalidated exactly once, on the flip

    def test_re_revoke_or_unknown_returns_false_and_does_not_invalidate(
        self, monkeypatch
    ):
        calls: list[int] = []
        monkeypatch.setattr(
            token_service, "invalidate_token_cache", lambda: calls.append(1)
        )
        conn = _SpyConn(revoke_rowcount=0)  # UPDATE matched no live row

        assert revoke_token(conn, uuid4()) is False
        assert calls == []  # no-op flip → cache untouched


# ===========================================================================
# 3. invalidate_token_cache — the real cache API round-trips to a miss
# ===========================================================================


class TestInvalidateTokenCache:
    def test_cached_entry_then_clear_is_a_miss(self):
        key = "unit-" + uuid4().hex
        auth._token_cache.put(key, _principal())
        assert auth._token_cache.get(key)[0] is True  # cached

        token_service.invalidate_token_cache()

        hit, value = auth._token_cache.get(key)
        assert hit is False and value is None  # cleared → miss


# ===========================================================================
# 4. metrics — route_template, collector deltas, render content-type
# ===========================================================================


class _FakeRoute:
    def __init__(self, path) -> None:
        self.path = path


class _FakeState:
    pass


class _FakeReq:
    """Duck-typed Request: only .scope / .method / .state.principal are read."""

    def __init__(self, route_path=None, method="GET", actor_kind=None) -> None:
        self.scope = {"route": _FakeRoute(route_path)} if route_path is not None else {}
        self.method = method
        self.state = _FakeState()
        if actor_kind is not None:
            self.state.principal = _principal(actor_kind=actor_kind)


class TestRouteTemplate:
    def test_matched_route_returns_template(self):
        req = _FakeReq("/v1/tokens/{token_id}")
        assert metrics.route_template(req) == "/v1/tokens/{token_id}"

    def test_unmatched_request_returns_literal_unmatched(self):
        req = _FakeReq(route_path=None)  # no route on the scope
        assert metrics.route_template(req) == "unmatched"

    def test_empty_path_collapses_to_unmatched(self):
        req = _FakeReq(route_path="")  # a route object with an empty path
        assert metrics.route_template(req) == "unmatched"


class TestCollectorDeltas:
    def test_observe_request_increments_counter(self):
        # Unique route → the pre-call baseline for this label set is a clean 0.
        route = f"/unit/{uuid4().hex}/{{id}}"
        req = _FakeReq(route, method="GET", actor_kind="agent")
        labels = dict(route=route, method="GET", status="201", actor_kind="agent")

        before = metrics.http_requests_total.labels(**labels)._value.get()
        metrics.observe_request(req, status_code=201, duration_seconds=0.02)
        after = metrics.http_requests_total.labels(**labels)._value.get()
        assert after - before == 1

    def test_observe_request_records_latency_sample(self):
        route = f"/unit/{uuid4().hex}/{{id}}"
        req = _FakeReq(route, method="POST", actor_kind="agent")
        hist = metrics.http_request_duration_seconds.labels(route=route, method="POST")

        before = hist._sum.get()
        metrics.observe_request(req, status_code=200, duration_seconds=0.25)
        after = hist._sum.get()
        assert after - before == pytest.approx(0.25)

    def test_no_principal_labels_actor_kind_none(self):
        route = f"/unit/{uuid4().hex}/{{id}}"
        req = _FakeReq(route, method="GET", actor_kind=None)  # no principal posed
        labels = dict(route=route, method="GET", status="200", actor_kind="none")

        before = metrics.http_requests_total.labels(**labels)._value.get()
        metrics.observe_request(req, status_code=200, duration_seconds=0.01)
        after = metrics.http_requests_total.labels(**labels)._value.get()
        assert after - before == 1

    def test_record_rate_limited_increments(self):
        kind = "unit-" + uuid4().hex
        before = metrics.rate_limited_total.labels(actor_kind=kind)._value.get()
        metrics.record_rate_limited(kind)
        after = metrics.rate_limited_total.labels(actor_kind=kind)._value.get()
        assert after - before == 1

    def test_record_fleet_killswitch_increments(self):
        before = metrics.fleet_killswitch_total._value.get()
        metrics.record_fleet_killswitch()
        after = metrics.fleet_killswitch_total._value.get()
        assert after - before == 1


class TestRenderLatest:
    def test_content_type_and_known_metric_present(self):
        from prometheus_client import CONTENT_TYPE_LATEST

        body, content_type = metrics.render_latest()
        assert content_type == CONTENT_TYPE_LATEST
        assert isinstance(body, bytes)
        assert b"ootils_http_requests_total" in body


# ===========================================================================
# 5. /v1/tokens router — TestClient + dependency overrides (no Postgres)
# ===========================================================================


def _make_client(principal: Principal, conn: _SpyConn):
    """A TestClient over the REAL create_app() app with resolve_principal + get_db
    overridden. resolve_principal is the sub-dependency of require_scope('admin'),
    so overriding it drives the scope gate off ``principal``; get_db yields the
    fake conn so no Postgres is touched."""
    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db

    app = create_app()

    async def _fake_principal():
        return principal

    def _fake_db():
        yield conn

    app.dependency_overrides[resolve_principal] = _fake_principal
    app.dependency_overrides[get_db] = _fake_db
    return TestClient(app)


def _readback_row(token_id: UUID) -> dict:
    return {
        "token_id": token_id,
        "name": "unit-token",
        "token_prefix": "ootk_unittes",
        "actor_kind": "agent",
        "scopes": ["read"],
        "rate_per_min": None,
        "created_at": datetime(2026, 7, 8, tzinfo=timezone.utc),
        "expires_at": None,
    }


def _list_row(token_id: UUID) -> dict:
    return {
        "token_id": token_id,
        "name": "unit-token",
        "token_prefix": "ootk_unittes",
        "actor_kind": "agent",
        "scopes": ["read"],
        "rate_per_min": None,
        "created_at": datetime(2026, 7, 8, tzinfo=timezone.utc),
        "last_used_at": None,
        "expires_at": None,
        "revoked_at": None,
    }


class TestTokensRouterAdmin:
    def test_post_returns_cleartext_once_and_never_a_hash(self):
        conn = _SpyConn()
        conn.readback = _readback_row(conn.mint_token_id)
        with _make_client(_admin(), conn) as client:
            resp = client.post(
                "/v1/tokens",
                json={"name": "unit-token", "actor_kind": "agent", "scopes": ["read"]},
            )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        # The cleartext is present, exactly once, under `token`; and it is a
        # real minted ootk_ secret, not the stored hash.
        assert body["token"].startswith("ootk_")
        assert len(body["token"]) > len("ootk_") + 30
        assert "token_hash" not in body
        assert body["prefix"] == "ootk_unittes"
        assert body["token_id"] == str(conn.mint_token_id)

    def test_get_list_carries_no_secret_material(self):
        tid = uuid4()
        conn = _SpyConn(list_rows=[_list_row(tid)])
        with _make_client(_admin(), conn) as client:
            resp = client.get("/v1/tokens")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["total"] == 1
        tok = body["tokens"][0]
        # Neither the cleartext (never stored) nor the hash (a lookup key we do
        # not surface) appears — only the non-secret prefix.
        assert "token" not in tok
        assert "token_hash" not in tok
        assert tok["prefix"] == "ootk_unittes"

    def test_delete_revokes_with_204(self):
        conn = _SpyConn(exists=True, revoke_rowcount=1)
        with _make_client(_admin(), conn) as client:
            resp = client.delete(f"/v1/tokens/{uuid4()}")
        assert resp.status_code == 204, resp.text
        assert resp.content == b""

    def test_post_invalid_scope_is_hand_authored_422_before_any_write(self):
        conn = _SpyConn()
        with _make_client(_admin(), conn) as client:
            resp = client.post(
                "/v1/tokens",
                json={
                    "name": "x",
                    "actor_kind": "agent",
                    "scopes": ["bogus:scope"],
                },
            )
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert isinstance(detail, str)  # hand-authored string, not a Pydantic list
        assert "bogus:scope" in detail
        assert "scope" in detail.lower()
        # Validation happened in mint_token BEFORE the INSERT — no DB write.
        assert conn.calls == []


class TestTokensRouterScopeFloor:
    def test_all_token_verbs_and_metrics_are_403_without_admin(self):
        # A read-only agent lacks the admin superset the whole surface requires.
        conn = _SpyConn()
        with _make_client(_principal(actor_kind="agent", scopes=("read",)), conn) as client:
            post = client.post(
                "/v1/tokens",
                json={"name": "x", "actor_kind": "agent", "scopes": ["read"]},
            )
            get = client.get("/v1/tokens")
            dele = client.delete(f"/v1/tokens/{uuid4()}")
            metr = client.get("/metrics")

        for resp in (post, get, dele, metr):
            assert resp.status_code == 403, resp.text
            assert resp.json()["detail"] == "missing scope 'admin'"
        # Auth stopped every call at the scope floor → no DB access.
        assert conn.calls == []
