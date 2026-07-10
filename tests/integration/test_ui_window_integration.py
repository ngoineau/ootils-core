"""
tests/integration/test_ui_window_integration.py — DB-backed tests for the
EXP-1 PR1 human window (ADR-036, issue #445) against a real Postgres:
``GET /v1/whoami`` resolved through the REAL token path (legacy env token AND
minted ``api_tokens`` rows via ``token_service.mint_token`` — no mocks,
CLAUDE.md), plus the ``/ui`` shell under both kill-switch states.

App fixtures follow the #392 pattern (test_agent_floor_integration.py /
test_snapshot_integration.py): TestClient with ``get_db`` overridden onto the
test DB. The minted-token AUTH lookup does not go through ``get_db`` — it
resolves via the module singleton bound to DATABASE_URL, which the fixtures
point at the migrated test DB, exactly as production resolves its pool.

The ``OOTILS_UI_ENABLED`` kill switch is evaluated ONCE at ``create_app()``
time (not per request), so the on/off cases each get their OWN app: the env
value only matters while the app is being built, and is restored immediately
after so neither fixture leaks its switch state into the other (fixture
instantiation order is test-order dependent).

Locked contracts:
  1. whoami + legacy env token -> the synthetic admin-equivalent principal
     (actor_kind='human', scopes=['admin'], is_legacy=True, token_prefix=None
     — its client_id is the 'global_token' sentinel, not a minted prefix).
  2. whoami + minted reduced-scope token -> the api_tokens row is the truth:
     actor_kind/scopes echo the mint, scopes come back SORTED, token_prefix is
     the non-secret 12-char prefix, and the response NEVER carries the
     cleartext (key absent AND raw value absent from the bytes).
  3. whoami floor: 401 unknown minted token, 401 revoked token (revocation is
     immediate — token_service.revoke_token invalidates the in-process cache),
     403 for a minted token WITHOUT the read scope.
  4. /ui default OFF (404, static too); ON -> 200 dataless shell (a seeded
     item's external_id never appears in the HTML; byte-identical across
     requests) + strict CSP without 'unsafe-inline'; the SAME app serves
     /v1/whoami — one window, one auth path.
"""
from __future__ import annotations

import os
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db

pytestmark = requires_db

LEGACY_TOKEN = "integration-test-token"


# ---------------------------------------------------------------------------
# App fixtures (the #392 pattern of test_agent_floor_integration.py)
# ---------------------------------------------------------------------------


def _build_app(migrated_db, ui_enabled_value: str | None):
    """create_app() with the UI kill switch pinned ONLY for the build (it is
    read once, at registration time), then restored so the two client
    fixtures never see each other's switch state. get_db is overridden onto
    the test DB (which also suppresses the api_request_log audit write —
    _should_audit_request returns False when the override is present)."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = LEGACY_TOKEN

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    previous = os.environ.get("OOTILS_UI_ENABLED")
    if ui_enabled_value is None:
        os.environ.pop("OOTILS_UI_ENABLED", None)
    else:
        os.environ["OOTILS_UI_ENABLED"] = ui_enabled_value
    try:
        app = create_app()
    finally:
        if previous is None:
            os.environ.pop("OOTILS_UI_ENABLED", None)
        else:
            os.environ["OOTILS_UI_ENABLED"] = previous

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    return app


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """TestClient on an app built with the kill switch at its DEFAULT (unset):
    the /ui window must NOT exist on this client."""
    from fastapi.testclient import TestClient

    app = _build_app(migrated_db, None)
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def ui_client(migrated_db):
    """TestClient on an app built with OOTILS_UI_ENABLED=1: the /ui window
    exists, and the SAME app serves /v1/whoami (one auth path)."""
    from fastapi.testclient import TestClient

    app = _build_app(migrated_db, "1")
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def _clear_token_cache():
    """The minted-token lookup is memoised in-process and the per-token rate
    counter accumulates in-process; clear BOTH around every test so a
    mint/revoke in one test never leaks a cached auth decision or a spent
    rate slot into another (pattern: test_agent_floor_integration.py)."""
    import ootils_core.api.auth as auth

    auth._token_cache.clear()
    auth._rate_counter.clear()
    yield
    auth._token_cache.clear()
    auth._rate_counter.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _mint(dsn, *, actor_kind: str, scopes: list[str], name: str | None = None):
    """Mint one real api_tokens row through the SINGLE lifecycle seam
    (token_service.mint_token) — never a hand-rolled INSERT. Returns
    (token_id, cleartext, name). autocommit=True: each execute commits
    (mint_token itself never commits — the caller owns the transaction)."""
    from ootils_core.api.token_service import mint_token

    token_name = name or f"uiwin-{actor_kind}-{uuid4().hex[:8]}"
    with _db_conn(dsn) as conn:
        token_id, cleartext = mint_token(
            conn, name=token_name, actor_kind=actor_kind, scopes=scopes
        )
    return token_id, cleartext, token_name


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ===========================================================================
# 1. GET /v1/whoami — legacy env token (admin-equivalent synthetic principal)
# ===========================================================================


class TestWhoAmILegacy:
    def test_legacy_token_is_admin_equivalent_human(self, api_client):
        resp = api_client.get("/v1/whoami", headers=_bearer(LEGACY_TOKEN))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["name"] == "legacy"
        assert body["actor_kind"] == "human"
        assert body["scopes"] == ["admin"], (
            "legacy resolves to the admin superset — admin-equivalent, "
            "not an enumeration of every scope"
        )
        assert body["is_legacy"] is True
        assert body["token_prefix"] is None, (
            "legacy has no minted prefix — its client_id is the "
            "'global_token' sentinel, deliberately not surfaced as one"
        )

    def test_legacy_response_never_carries_the_token(self, api_client):
        resp = api_client.get("/v1/whoami", headers=_bearer(LEGACY_TOKEN))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert "token" not in body
        assert "token_hash" not in body
        assert LEGACY_TOKEN not in resp.text

    def test_whoami_401_without_token(self, api_client):
        resp = api_client.get("/v1/whoami")
        assert resp.status_code == 401, resp.text


# ===========================================================================
# 2. GET /v1/whoami — minted tokens (the api_tokens row is the truth)
# ===========================================================================


class TestWhoAmIMinted:
    def test_minted_reduced_scopes_echoed_from_the_row(self, api_client, migrated_db):
        """A token minted with scopes=['read'] introspects as EXACTLY that —
        actor_kind and scopes come from the DB row, never self-declared."""
        _tid, cleartext, name = _mint(migrated_db, actor_kind="agent", scopes=["read"])

        resp = api_client.get("/v1/whoami", headers=_bearer(cleartext))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["name"] == name
        assert body["actor_kind"] == "agent"
        assert body["scopes"] == ["read"]
        assert body["is_legacy"] is False

    def test_minted_token_prefix_is_nonsecret_slice_never_the_token(
        self, api_client, migrated_db
    ):
        """token_prefix is the 12-char audit-correlation slice — present,
        equal to what auth.token_prefix computes, and NEVER the cleartext."""
        from ootils_core.api.auth import token_prefix

        _tid, cleartext, _name = _mint(migrated_db, actor_kind="human", scopes=["read"])

        resp = api_client.get("/v1/whoami", headers=_bearer(cleartext))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["token_prefix"] == token_prefix(cleartext)
        assert body["token_prefix"] != cleartext
        assert len(body["token_prefix"]) < len(cleartext)
        assert "token" not in body
        assert cleartext not in resp.text, "the cleartext must never be echoed"

    def test_minted_scopes_come_back_sorted(self, api_client, migrated_db):
        granted = ["scenario:write", "read", "calc:run"]
        _tid, cleartext, _name = _mint(migrated_db, actor_kind="service", scopes=granted)

        resp = api_client.get("/v1/whoami", headers=_bearer(cleartext))
        assert resp.status_code == 200, resp.text
        assert resp.json()["scopes"] == sorted(granted)

    def test_403_for_minted_token_without_read_scope(self, api_client, migrated_db):
        """whoami sits behind require_scope('read') — a token that can ingest
        but not read is refused on the scope floor, not on authentication."""
        _tid, cleartext, _name = _mint(migrated_db, actor_kind="agent", scopes=["ingest"])

        resp = api_client.get("/v1/whoami", headers=_bearer(cleartext))
        assert resp.status_code == 403, resp.text
        assert "read" in resp.json()["detail"].lower()

    def test_401_for_unknown_minted_token(self, api_client):
        resp = api_client.get(
            "/v1/whoami", headers=_bearer(f"ootk_unknown_{uuid4().hex}")
        )
        assert resp.status_code == 401, resp.text

    def test_401_for_revoked_token_immediately(self, api_client, migrated_db):
        """revoke_token invalidates the in-process cache — a revoked token
        stops introspecting at once, no TTL window."""
        from ootils_core.api.token_service import revoke_token

        token_id, cleartext, _name = _mint(
            migrated_db, actor_kind="agent", scopes=["read"]
        )
        # Prove it works first (and prime the auth cache with a positive).
        ok = api_client.get("/v1/whoami", headers=_bearer(cleartext))
        assert ok.status_code == 200, ok.text

        with _db_conn(migrated_db) as conn:
            assert revoke_token(conn, token_id) is True

        resp = api_client.get("/v1/whoami", headers=_bearer(cleartext))
        assert resp.status_code == 401, resp.text


# ===========================================================================
# 3. GET /ui — kill switch off (DEFAULT) and on, against the real app stack
# ===========================================================================


class TestUiWindowOff:
    def test_ui_404_by_default(self, api_client):
        assert api_client.get("/ui").status_code == 404

    def test_ui_static_404_by_default(self, api_client):
        assert api_client.get("/ui/static/app.js").status_code == 404

    def test_whoami_still_served_with_window_off(self, api_client):
        """/v1/whoami is NOT gated by the UI switch — agents introspect their
        Principal regardless of whether the human window exists."""
        resp = api_client.get("/v1/whoami", headers=_bearer(LEGACY_TOKEN))
        assert resp.status_code == 200, resp.text


class TestUiWindowOn:
    def test_ui_shell_200_html(self, ui_client):
        resp = ui_client.get("/ui")
        assert resp.status_code == 200, resp.text
        assert resp.headers["content-type"].startswith("text/html")

    def test_ui_static_js_served(self, ui_client):
        resp = ui_client.get("/ui/static/app.js")
        assert resp.status_code == 200
        assert "sessionStorage" in resp.text

    def test_ui_csp_strict_without_unsafe_inline(self, ui_client):
        resp = ui_client.get("/ui")
        csp = resp.headers["Content-Security-Policy"]
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "unsafe-inline" not in csp

    def test_shell_carries_no_business_data_from_the_db(self, ui_client, migrated_db):
        """Seed a distinctively named item in the REAL DB, then fetch the
        shell: the marker must NOT appear — the shell is server-rendered from
        an empty context, all data arrives client-side via the operator's own
        Bearer calls. Also byte-identical across requests (dataless = static)."""
        marker = f"UIWIN-NEVER-IN-SHELL-{uuid4().hex}"
        with _db_conn(migrated_db) as conn:
            conn.execute(
                "INSERT INTO items (item_id, external_id, name) VALUES (%s, %s, %s)",
                (uuid4(), marker, "ui-window-marker-item"),
            )

        first = ui_client.get("/ui")
        second = ui_client.get("/ui")
        assert first.status_code == second.status_code == 200
        assert marker not in first.text
        assert first.text == second.text

    def test_shell_carries_no_token_material(self, ui_client):
        html = ui_client.get("/ui").text
        assert LEGACY_TOKEN not in html
        assert "ootk_" not in html

    def test_same_app_serves_whoami_one_auth_path(self, ui_client, migrated_db):
        """One window, one auth path: the ui-enabled app answers /v1/whoami
        with BOTH token flavours, through the same resolve_principal the rest
        of the API uses."""
        legacy = ui_client.get("/v1/whoami", headers=_bearer(LEGACY_TOKEN))
        assert legacy.status_code == 200, legacy.text
        assert legacy.json()["is_legacy"] is True

        _tid, cleartext, _name = _mint(migrated_db, actor_kind="human", scopes=["read"])
        minted = ui_client.get("/v1/whoami", headers=_bearer(cleartext))
        assert minted.status_code == 200, minted.text
        assert minted.json()["is_legacy"] is False
