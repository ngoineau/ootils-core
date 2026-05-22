"""
tests/test_security_headers_and_cors.py — verify CORS + security-headers middleware.

Resolves R3 of REVIEW-2026-05. Tests the wiring added in api/app.py.

These tests build a fresh FastAPI app per test by calling create_app() with
the environment already configured — no importlib.reload (which would
re-execute the module-level `app = create_app()` and double initialization).
"""
from __future__ import annotations

import os
from contextlib import contextmanager

from fastapi.testclient import TestClient


@contextmanager
def _env(**overrides: str | None):
    """Temporarily set env vars; restore on exit. Pass None to unset."""
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


def _make_app():
    os.environ.setdefault("OOTILS_API_TOKEN", "test-token-headers")
    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db

    app = create_app()
    # Suppress the per-request DB audit log path (see api/app.py:_log_api_request).
    # Overriding get_db short-circuits _should_audit_request so the test client
    # does not try to open a real DB connection on /health.
    app.dependency_overrides[get_db] = lambda: None
    return app


# ---------------------------------------------------------------------------
# Security headers
# ---------------------------------------------------------------------------


def test_security_headers_present_by_default():
    with _env(OOTILS_DISABLE_SECURITY_HEADERS=None, OOTILS_CORS_ALLOWED_ORIGINS=None):
        client = TestClient(_make_app())
        response = client.get("/health")
    assert response.status_code == 200
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
    assert "Content-Security-Policy" in response.headers


def test_csp_strict_on_non_docs_paths():
    with _env(OOTILS_DISABLE_SECURITY_HEADERS=None):
        client = TestClient(_make_app())
        response = client.get("/health")
    csp = response.headers["Content-Security-Policy"]
    assert "default-src 'none'" in csp
    assert "frame-ancestors 'none'" in csp


def test_hsts_only_on_https_via_forwarded_proto():
    with _env(OOTILS_DISABLE_SECURITY_HEADERS=None):
        client = TestClient(_make_app())
        plain = client.get("/health")
        secure = client.get("/health", headers={"X-Forwarded-Proto": "https"})
    assert "Strict-Transport-Security" not in plain.headers
    assert "max-age=31536000" in secure.headers["Strict-Transport-Security"]


def test_security_headers_disabled_when_opted_out():
    with _env(OOTILS_DISABLE_SECURITY_HEADERS="1"):
        client = TestClient(_make_app())
        response = client.get("/health")
    assert response.status_code == 200
    assert "X-Content-Type-Options" not in response.headers
    assert "Content-Security-Policy" not in response.headers


# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------


def test_cors_disabled_by_default():
    with _env(OOTILS_CORS_ALLOWED_ORIGINS=None):
        client = TestClient(_make_app())
        response = client.get(
            "/health",
            headers={"Origin": "https://example.com"},
        )
    assert response.status_code == 200
    assert "Access-Control-Allow-Origin" not in response.headers


def test_cors_allows_configured_origin():
    with _env(OOTILS_CORS_ALLOWED_ORIGINS="https://app.example.com,https://other.example.com"):
        client = TestClient(_make_app())
        response = client.get(
            "/health",
            headers={"Origin": "https://app.example.com"},
        )
    assert response.headers.get("Access-Control-Allow-Origin") == "https://app.example.com"


def test_cors_rejects_unlisted_origin():
    with _env(OOTILS_CORS_ALLOWED_ORIGINS="https://app.example.com"):
        client = TestClient(_make_app())
        response = client.get(
            "/health",
            headers={"Origin": "https://evil.example.com"},
        )
    assert "Access-Control-Allow-Origin" not in response.headers
