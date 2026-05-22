"""
tests/test_rate_limit.py — verify the opt-in rate-limit middleware.

slowapi-backed limiter, configured via OOTILS_RATE_LIMIT_PER_MIN.
Resolves the remaining part of REVIEW-2026-05 R3.
"""
from __future__ import annotations

import os
from contextlib import contextmanager

from fastapi.testclient import TestClient


@contextmanager
def _env(**overrides: str | None):
    """Temporarily set env vars; restore on exit. None unsets."""
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
    os.environ.setdefault("OOTILS_API_TOKEN", "test-token-rate-limit")
    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db

    app = create_app()
    app.dependency_overrides[get_db] = lambda: None
    return app


def test_no_rate_limit_by_default():
    """OOTILS_RATE_LIMIT_PER_MIN unset → unlimited requests pass."""
    with _env(OOTILS_RATE_LIMIT_PER_MIN=None):
        client = TestClient(_make_app())
        for _ in range(20):
            r = client.get("/health")
            assert r.status_code == 200
        # No slowapi headers when limiter is off
        assert "X-RateLimit-Limit" not in r.headers


def test_rate_limit_emits_headers_when_enabled():
    """OOTILS_RATE_LIMIT_PER_MIN=60 → 200 response carries X-RateLimit-* headers."""
    with _env(OOTILS_RATE_LIMIT_PER_MIN="60"):
        client = TestClient(_make_app())
        r = client.get("/health")
    assert r.status_code == 200
    # slowapi adds these when headers_enabled=True
    assert "X-RateLimit-Limit" in r.headers
    assert int(r.headers["X-RateLimit-Limit"]) == 60


def test_rate_limit_blocks_burst():
    """A tight burst above the limit returns 429 once exhausted."""
    with _env(OOTILS_RATE_LIMIT_PER_MIN="3"):
        client = TestClient(_make_app())
        statuses = [client.get("/health").status_code for _ in range(6)]
    # First 3 must succeed; subsequent ones must be 429.
    assert statuses[:3] == [200, 200, 200], statuses
    assert 429 in statuses[3:], statuses


def test_rate_limit_429_payload_mentions_the_rate():
    """When rate-limited, the 429 response carries slowapi's error envelope.

    We do not over-specify the body shape here — slowapi's
    SlowAPIMiddleware processes RateLimitExceeded itself before our custom
    exception_handler can run. Asserting the rate appears in the body
    is enough proof that the limiter is the one returning the 429.
    """
    with _env(OOTILS_RATE_LIMIT_PER_MIN="1"):
        client = TestClient(_make_app())
        client.get("/health")  # consume the quota
        r = client.get("/health")
    assert r.status_code == 429
    body_text = r.text.lower()
    assert "rate limit" in body_text or "1 per" in body_text
