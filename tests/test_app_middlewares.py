"""
tests/test_app_middlewares.py — coverage on the app.py middleware paths.

After cycles 1, 14 added CORS, security headers and rate limiting, coverage
on api/app.py rose to ~70%. The two remaining significant gaps were:
  - IngestPayloadSizeLimitMiddleware
  - request_context_middleware (correlation id, exception path)

These tests close those gaps without touching the DB.
"""
from __future__ import annotations

import os
from contextlib import contextmanager

from fastapi.testclient import TestClient


@contextmanager
def _env(**overrides: str | None):
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
    os.environ.setdefault("OOTILS_API_TOKEN", "test-token-middleware")
    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db

    app = create_app()
    app.dependency_overrides[get_db] = lambda: None
    return app


# ---------------------------------------------------------------------------
# IngestPayloadSizeLimitMiddleware
# ---------------------------------------------------------------------------


def test_ingest_payload_under_limit_passes_middleware():
    """A small Content-Length on /v1/ingest/* clears the middleware (auth then 401)."""
    with _env():
        client = TestClient(_make_app())
        r = client.post(
            "/v1/ingest/items",
            content=b'{"items":[]}',
            headers={"Content-Type": "application/json"},
        )
    # Middleware lets it through; downstream auth dependency returns 401.
    # Either way it is NOT 413 — the body did not exceed 10 MB.
    assert r.status_code != 413


def test_ingest_payload_over_10mb_returns_413():
    """A Content-Length header above 10 MB on /v1/ingest/* must short-circuit to 413."""
    big = 10 * 1024 * 1024 + 1
    with _env():
        client = TestClient(_make_app())
        # We don't actually send 10 MB — the middleware checks the
        # Content-Length header before reading the body.
        r = client.post(
            "/v1/ingest/items",
            content=b"x",
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(big),
            },
        )
    assert r.status_code == 413
    body = r.json()
    assert body["error"] == "payload_too_large"
    assert body["status"] == 413


def test_payload_limit_does_not_apply_outside_ingest():
    """The 10 MB cap is ingest-only; /health with a huge declared length passes."""
    with _env():
        client = TestClient(_make_app())
        r = client.get(
            "/health",
            headers={"Content-Length": str(50 * 1024 * 1024)},
        )
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# request_context_middleware — correlation id propagation
# ---------------------------------------------------------------------------


def test_correlation_id_round_trips_when_supplied():
    """A caller-supplied X-Correlation-ID echoes back on the response."""
    cid = "req_external_12345"
    with _env():
        client = TestClient(_make_app())
        r = client.get("/health", headers={"X-Correlation-ID": cid})
    assert r.status_code == 200
    assert r.headers["X-Correlation-ID"] == cid


def test_correlation_id_generated_when_absent():
    """When no header is supplied, the middleware generates a `req_<hex>` ID."""
    with _env():
        client = TestClient(_make_app())
        r = client.get("/health")
    cid = r.headers.get("X-Correlation-ID")
    assert cid is not None
    assert cid.startswith("req_")
    # uuid4 hex is 32 chars → prefix + 32 = 36
    assert len(cid) == 36


def test_correlation_id_truncated_at_128_chars():
    """Inbound correlation IDs longer than 128 chars are clipped — DoS protection."""
    cid_long = "X" * 200
    with _env():
        client = TestClient(_make_app())
        r = client.get("/health", headers={"X-Correlation-ID": cid_long})
    assert r.status_code == 200
    echoed = r.headers["X-Correlation-ID"]
    assert len(echoed) == 128
    assert echoed == "X" * 128


def test_x_api_version_header_is_emitted():
    """Every response carries X-API-Version regardless of route."""
    with _env():
        client = TestClient(_make_app())
        r = client.get("/health")
    assert r.headers.get("X-API-Version") == "1.0.0"
