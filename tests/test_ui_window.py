"""
tests/test_ui_window.py — DB-free tests for the EXP-1 PR1 human window
(ADR-036, issue #445): the ``GET /ui`` shell (``api/routers/ui.py``), its
``OOTILS_UI_ENABLED`` kill switch (DEFAULT OFF, evaluated ONCE at
``create_app()`` time — a disabled window is a clean 404 because neither the
route nor the static mount exist), the ``/ui`` CSP branch in ``api/app.py``
(``default-src 'self'`` and NEVER ``'unsafe-inline'``), and ``GET /v1/whoami``
(``api/routers/me.py``).

Locked contracts:
  1. Kill switch DEFAULT OFF: with OOTILS_UI_ENABLED unset, /ui and
     /ui/static/* are 404 and the routes are NOT registered at all (clean
     non-existence, not a route that exists and refuses).
  2. Enabled shell: 200 text/html, rendered from an EMPTY Jinja2 context —
     byte-identical across requests, zero business data, zero token material,
     no prefilled input values, no inline <script>/<style>/on*= handlers
     (required by the CSP without 'unsafe-inline').
  3. CSP: /ui and /ui/static/* get "default-src 'self'; frame-ancestors
     'none'" WITHOUT 'unsafe-inline'; every other path keeps the strict
     "default-src 'none'" default (including a /ui-PREFIX-lookalike like
     /uix — the branch matches "/ui" exactly or "/ui/").
  4. GET /v1/whoami: 401 without/with a wrong token; 200 with the legacy
     token WITHOUT ever touching the DB (the legacy path is env-only); the
     response NEVER carries the token — key absent from the body, raw value
     absent from the bytes; scopes/actor_kind present; the shell route stays
     out of the OpenAPI schema while /v1/whoami is in it.

DB-free by construction (the test_atp_api.py pattern): ``get_db`` is
overridden with a sentinel that raises on ANY attribute access, proving the
whole surface under test resolves without a database (the override's mere
presence also suppresses the api_request_log audit write — see
``api/app.py:_should_audit_request``). The DB-backed cases (minted tokens via
token_service, reduced scopes -> 403) live in
tests/integration/test_ui_window_integration.py.
"""
from __future__ import annotations

import os
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

# create_app() validates OOTILS_API_TOKEN — set it before importing the app.
os.environ.setdefault("OOTILS_API_TOKEN", "unit-ui-token")

from ootils_core.api.app import create_app  # noqa: E402
from ootils_core.api.dependencies import get_db  # noqa: E402
from ootils_core.api.routers.ui import ui_enabled  # noqa: E402

LEGACY_TOKEN = "unit-ui-legacy-token-value"
AUTH_HEADERS = {"Authorization": f"Bearer {LEGACY_TOKEN}"}


@pytest.fixture(autouse=True)
def _pin_env(monkeypatch):
    """Pin the env for the WHOLE test (not just create_app time): the legacy
    token is compared against OOTILS_API_TOKEN at REQUEST time, and the
    security-headers/CSP middleware reads its own switch per request. The kill
    switch starts UNSET so every test states its own value explicitly."""
    monkeypatch.setenv("OOTILS_API_TOKEN", LEGACY_TOKEN)
    monkeypatch.delenv("OOTILS_DISABLE_SECURITY_HEADERS", raising=False)
    monkeypatch.delenv("OOTILS_UI_ENABLED", raising=False)


@contextmanager
def _env(**overrides: str | None):
    """Temporarily set env vars; restore on exit. Pass None to unset.
    (Pattern: tests/test_security_headers_and_cors.py.)"""
    previous: dict[str, str | None] = {}
    for k, v in overrides.items():
        previous[k] = os.environ.get(k)
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    try:
        yield
    finally:
        for k, v in previous.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class _DBAccessForbidden(AssertionError):
    """Raised if anything under test reaches for the DB."""


class _ForbiddenDB:
    """Sentinel: any attribute access raises — proves the path is DB-free."""

    def __getattr__(self, name: str):
        raise _DBAccessForbidden(
            f"DB-free test reached the DB (accessed {name!r})"
        )


def _make_client(ui_enabled_value: str | None) -> TestClient:
    """Build a TestClient with the kill switch pinned BEFORE create_app() —
    the switch is evaluated once, at registration time, so what matters is
    its value while the app is being built."""
    with _env(OOTILS_UI_ENABLED=ui_enabled_value):
        app = create_app()

    def override_db():
        yield _ForbiddenDB()

    app.dependency_overrides[get_db] = override_db
    return TestClient(app)


# ---------------------------------------------------------------------------
# 1. Kill switch — DEFAULT OFF (the default is the contract, test it as such)
# ---------------------------------------------------------------------------


class TestUiKillSwitch:
    def test_ui_disabled_by_default(self):
        """OOTILS_UI_ENABLED unset -> /ui does not exist. THE default."""
        client = _make_client(None)
        assert client.get("/ui").status_code == 404

    def test_ui_static_disabled_by_default(self):
        """The static mount is gated by the SAME switch — no orphan asset
        route on a disabled window."""
        client = _make_client(None)
        assert client.get("/ui/static/app.js").status_code == 404

    def test_ui_disabled_with_explicit_zero(self):
        client = _make_client("0")
        assert client.get("/ui").status_code == 404

    def test_disabled_means_unregistered_not_refusing(self):
        """Disabled -> neither the /ui route nor the /ui/static mount are in
        app.routes at all: a clean 404 by non-existence, never a registered
        route answering 403/503."""
        with _env(OOTILS_UI_ENABLED=None):
            app = create_app()
        paths = [getattr(r, "path", "") for r in app.routes]
        assert not any(p == "/ui" or p.startswith("/ui/") for p in paths)

    def test_ui_enabled_serves_shell(self):
        client = _make_client("1")
        resp = client.get("/ui")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/html")

    def test_ui_enabled_registers_route_and_mount(self):
        with _env(OOTILS_UI_ENABLED="1"):
            app = create_app()
        paths = [getattr(r, "path", "") for r in app.routes]
        assert "/ui" in paths
        # The Mount must land on the APP (include_router does not copy Mount
        # routes from a sub-APIRouter — the caveat ui.py documents).
        assert "/ui/static" in paths

    def test_ui_enabled_serves_static_js(self):
        client = _make_client("1")
        resp = client.get("/ui/static/app.js")
        assert resp.status_code == 200
        # The client script keeps the token in sessionStorage — its defining
        # trait (never a cookie, never a URL).
        assert "sessionStorage" in resp.text

    def test_ui_enabled_truthy_variants(self):
        for value in ("1", "true", "yes", "on", "TRUE", " On "):
            with _env(OOTILS_UI_ENABLED=value):
                assert ui_enabled() is True, value

    def test_ui_disabled_falsy_variants(self):
        for value in (None, "0", "false", "off", "no", ""):
            with _env(OOTILS_UI_ENABLED=value):
                assert ui_enabled() is False, repr(value)

    def test_ui_shell_not_in_openapi_schema(self):
        """The shell is an ops/demo surface, not part of the versioned API
        contract; /v1/whoami IS part of it."""
        with _env(OOTILS_UI_ENABLED="1"):
            app = create_app()
        paths = app.openapi()["paths"]
        assert "/ui" not in paths
        assert "/v1/whoami" in paths


# ---------------------------------------------------------------------------
# 2. Shell content — NO business data, NO token, CSP-compatible markup
# ---------------------------------------------------------------------------


class TestUiShellContent:
    def _shell(self) -> str:
        client = _make_client("1")
        resp = client.get("/ui")
        assert resp.status_code == 200
        return resp.text

    def test_shell_is_static_byte_identical(self):
        """Rendered from an EMPTY Jinja2 context: two requests produce the
        same bytes — there is no per-request (hence no business) data in the
        server-rendered shell."""
        client = _make_client("1")
        first = client.get("/ui")
        second = client.get("/ui")
        assert first.status_code == second.status_code == 200
        assert first.text == second.text

    def test_shell_contains_no_token_material(self):
        html = self._shell()
        assert LEGACY_TOKEN not in html
        assert "ootk_" not in html, "no minted-token material in the shell"

    def test_shell_has_empty_panels_only(self):
        """The three read panels exist and are EMPTY placeholders — the data
        arrives client-side, through the operator's own Bearer calls."""
        html = self._shell()
        assert 'id="recommendations-body"' in html
        assert 'id="kpi-body"' in html
        assert 'id="compare-body"' in html
        assert html.count("Not loaded.") == 3

    def test_shell_prefills_no_input_value(self):
        """No <input value="..."> — nothing (least of all a token) is ever
        server-rendered into a form field."""
        html = self._shell()
        assert 'value="' not in html
        assert "value='" not in html

    def test_shell_has_no_inline_script_or_style(self):
        """CSP without 'unsafe-inline' requires it: the ONLY script is the
        external /ui/static/app.js, and there are no inline handlers."""
        html = self._shell()
        assert html.count("<script") == 1
        assert '<script src="/ui/static/app.js"' in html
        assert "<style" not in html
        assert "javascript:" not in html
        for handler in ("onclick=", "onload=", "onsubmit=", "onchange=", "onerror="):
            assert handler not in html, handler

    def test_static_js_never_logs_or_urlises_the_token(self):
        """The client script keeps the token out of console.log and out of
        URLs (it goes ONLY into the Authorization header), and renders via
        textContent/createElement — never innerHTML."""
        client = _make_client("1")
        js = client.get("/ui/static/app.js").text
        assert "console.log" not in js
        assert "innerHTML" not in js, "DOM rendering must go through textContent"
        assert "Authorization" in js


# ---------------------------------------------------------------------------
# 3. CSP — /ui branch strict, no 'unsafe-inline', default branch untouched
# ---------------------------------------------------------------------------


class TestUiCsp:
    def test_csp_present_on_ui_without_unsafe_inline(self):
        client = _make_client("1")
        resp = client.get("/ui")
        csp = resp.headers["Content-Security-Policy"]
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "unsafe-inline" not in csp
        assert "unsafe-eval" not in csp

    def test_csp_applies_to_ui_static(self):
        client = _make_client("1")
        resp = client.get("/ui/static/app.js")
        csp = resp.headers["Content-Security-Policy"]
        assert "default-src 'self'" in csp
        assert "unsafe-inline" not in csp

    def test_csp_default_branch_still_strict(self):
        """Adding the /ui branch must not loosen the default: /health keeps
        default-src 'none'."""
        client = _make_client("1")
        resp = client.get("/health")
        assert "default-src 'none'" in resp.headers["Content-Security-Policy"]

    def test_csp_ui_prefix_lookalike_gets_default_branch(self):
        """/uix is NOT the window (the branch is "/ui" exact or "/ui/..."):
        a lookalike path falls through to the strict default CSP."""
        client = _make_client("1")
        resp = client.get("/uix")
        assert resp.status_code == 404
        assert "default-src 'none'" in resp.headers["Content-Security-Policy"]

    def test_csp_on_ui_even_when_disabled_404(self):
        """The CSP branch is path-based middleware — it stamps the strict /ui
        policy on the 404 of a disabled window too; disabling the window never
        loosens a header."""
        client = _make_client(None)
        resp = client.get("/ui")
        assert resp.status_code == 404
        assert "unsafe-inline" not in resp.headers["Content-Security-Policy"]


# ---------------------------------------------------------------------------
# 4. GET /v1/whoami — 401 floor, never the token, DB-free legacy resolution
# ---------------------------------------------------------------------------


class TestWhoAmI:
    def test_whoami_401_without_token(self):
        client = _make_client(None)
        resp = client.get("/v1/whoami")
        assert resp.status_code == 401
        assert resp.headers.get("WWW-Authenticate") == "Bearer"

    def test_whoami_401_with_wrong_token(self):
        client = _make_client(None)
        resp = client.get("/v1/whoami", headers={"Authorization": "Bearer nope"})
        assert resp.status_code == 401

    def test_whoami_legacy_token_resolves_without_db(self):
        """The legacy token is env-only: a 200 through the _ForbiddenDB
        sentinel proves no DB was touched. Legacy -> synthetic human/admin
        (api/auth.py:legacy_principal), token_prefix None (its client_id is
        the 'global_token' sentinel, not a real minted prefix)."""
        client = _make_client(None)
        resp = client.get("/v1/whoami", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "legacy"
        assert body["actor_kind"] == "human"
        assert body["scopes"] == ["admin"]
        assert body["is_legacy"] is True
        assert body["token_prefix"] is None

    def test_whoami_response_never_carries_the_token(self):
        """NEVER the token: no 'token' key in the body, no raw value in the
        bytes — the only token-ish field is the non-secret token_prefix."""
        client = _make_client(None)
        resp = client.get("/v1/whoami", headers=AUTH_HEADERS)
        assert resp.status_code == 200
        body = resp.json()
        assert "token" not in body
        assert "token_hash" not in body
        assert set(body.keys()) == {
            "name",
            "actor_kind",
            "scopes",
            "is_legacy",
            "token_prefix",
        }
        assert LEGACY_TOKEN not in resp.text

    def test_whoami_out_model_has_no_secret_field(self):
        """Model-level guard: WhoAmIOut's only token-named field is the
        non-secret token_prefix — nobody can 'accidentally' serialise the
        secret without changing the schema under review."""
        from ootils_core.api.routers.me import WhoAmIOut

        fields = set(WhoAmIOut.model_fields)
        assert fields == {"name", "actor_kind", "scopes", "is_legacy", "token_prefix"}

    def test_whoami_available_even_when_ui_disabled(self):
        """/v1/whoami is a plain API endpoint, NOT gated by the UI kill
        switch — an agent may introspect its Principal with the window off."""
        client = _make_client(None)
        resp = client.get("/v1/whoami", headers=AUTH_HEADERS)
        assert resp.status_code == 200
