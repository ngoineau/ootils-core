"""Unit tests for auth.py — Bearer token authentication."""
from __future__ import annotations

import os

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

# Ensure the env var is set before importing require_auth
os.environ.setdefault("OOTILS_API_TOKEN", "test-token-auth")

from ootils_core.api.auth import require_auth  # noqa: E402

_TEST_TOKEN = "test-token-auth-unique-value-xyz"


def make_app():
    app = FastAPI()

    @app.get("/protected")
    async def protected(_token: str = Depends(require_auth)):
        return {"ok": True}

    return app


@pytest.fixture(autouse=True)
def _set_token(monkeypatch):
    """Ensure a known token is set for every test in this module."""
    monkeypatch.setenv("OOTILS_API_TOKEN", _TEST_TOKEN)


class TestRequireAuth:
    def test_valid_token_passes(self):
        client = TestClient(make_app())
        resp = client.get("/protected", headers={"Authorization": f"Bearer {_TEST_TOKEN}"})
        assert resp.status_code == 200

    def test_valid_token_returns_ok_body(self):
        client = TestClient(make_app())
        resp = client.get("/protected", headers={"Authorization": f"Bearer {_TEST_TOKEN}"})
        assert resp.json() == {"ok": True}

    def test_missing_token_returns_401(self):
        client = TestClient(make_app())
        resp = client.get("/protected")
        assert resp.status_code == 401

    def test_invalid_token_returns_401(self):
        client = TestClient(make_app())
        resp = client.get("/protected", headers={"Authorization": "Bearer wrong-token"})
        assert resp.status_code == 401

    def test_invalid_token_has_www_authenticate_header(self):
        client = TestClient(make_app())
        resp = client.get("/protected", headers={"Authorization": "Bearer wrong-token"})
        assert "WWW-Authenticate" in resp.headers

    def test_missing_token_has_www_authenticate_header(self):
        client = TestClient(make_app())
        resp = client.get("/protected")
        assert "WWW-Authenticate" in resp.headers

    def test_missing_env_var_raises(self, monkeypatch):
        """_expected_token() must raise RuntimeError when env var is unset."""
        monkeypatch.delenv("OOTILS_API_TOKEN", raising=False)

        import ootils_core.api.auth as auth_module

        with pytest.raises(RuntimeError, match="OOTILS_API_TOKEN"):
            auth_module._expected_token()
