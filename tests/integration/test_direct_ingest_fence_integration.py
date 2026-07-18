"""Integration tests for the direct-ingest fence (ADR-042 PR-1, decision 2.2).

``require_direct_ingest`` (api/routers/ingest.py) composes on top of
``require_scope("ingest")`` and adds the ``OOTILS_DIRECT_INGEST_ENABLED``
kill switch: when the switch is falsy, every direct ``POST /v1/ingest/*``
answers 503 for every caller EXCEPT the legacy principal (the bootstrap
``OOTILS_API_TOKEN`` credential — the one the governed daily-run
orchestrator, ``scripts/ingest_file.py`` and the seed/demo TestClients
actually present).

THE CONTRACT under test, one case per class:

  1. Switch at DEFAULT (env var absent) -> ON: a token MINTED over
     POST /v1/tokens with the `ingest` scope passes -> 200 (dry_run).
     Nothing regresses out of the box.
  2. Switch OFF (``OOTILS_DIRECT_INGEST_ENABLED=0``): the same minted
     `ingest` token -> 503, detail names the governed pipeline so a caller
     hitting the fence can self-correct.
  3. Switch OFF + the LEGACY root token -> 200. THE orchestrator case:
     the governed daily-run pipeline itself ingests through this exemption
     (``interfaces/ingest_exec.py:call_api``), so the fence must never
     lock out the very pipeline it funnels callers towards.
  4. The scope floor still fires FIRST: switch OFF + a minted token
     WITHOUT `ingest` -> 403 (missing scope), never a 503 — auth semantics
     are unchanged by the fence.
  5. The staging pipeline is buried (ADR-042 decision 2.1):
     POST /v1/staging/upload -> 404 (router unmounted, clean not-found).

The switch is read PER REQUEST (``_direct_ingest_enabled``), so tests flip
it with monkeypatch against a module-scoped TestClient — no app rebuild.

Same fixture pattern as test_ingest_retraction_integration.py /
test_agent_floor_integration.py: real Postgres via ``migrated_db``,
TestClient with get_db overridden, legacy auth via OOTILS_API_TOKEN.
"""
from __future__ import annotations

import io
import os
from uuid import uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

LEGACY_TOKEN = "integration-test-token"


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped TestClient bound to the real test DB (same pattern as
    test_ingest_retraction_integration.py)."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = LEGACY_TOKEN

    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def _mint_token(api_client, *, scopes: list[str]) -> str:
    """Mint a real token over POST /v1/tokens with the LEGACY admin token;
    return the once-shown cleartext. A minted token is exactly the kind of
    principal the fence is meant to stop (``is_legacy=False``)."""
    resp = api_client.post(
        "/v1/tokens",
        headers=_bearer(LEGACY_TOKEN),
        json={
            "name": f"fence-{uuid4().hex[:8]}",
            "actor_kind": "service",
            "scopes": scopes,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["token"]


def _post_items_dry_run(api_client, token: str):
    """One representative direct-ingest call: POST /v1/ingest/items with
    dry_run=True — validation runs, nothing is persisted, 200 when the
    fence lets it through."""
    return api_client.post(
        "/v1/ingest/items",
        headers=_bearer(token),
        json={
            "items": [
                {"external_id": f"FENCE-{uuid4().hex[:8]}", "name": "Fence Item"}
            ],
            "dry_run": True,
        },
    )


# ===========================================================================
# 1. Default ON — a minted `ingest` token passes (nothing regresses).
# ===========================================================================


class TestFenceDefaultOn:
    def test_minted_ingest_token_passes_when_env_absent(
        self, api_client, monkeypatch
    ):
        monkeypatch.delenv("OOTILS_DIRECT_INGEST_ENABLED", raising=False)
        token = _mint_token(api_client, scopes=["ingest"])

        resp = _post_items_dry_run(api_client, token)
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "dry_run"


# ===========================================================================
# 2. Switch OFF — the same minted `ingest` token is fenced with 503 and the
#    detail names the governed pipeline.
# ===========================================================================


class TestFenceOffMintedToken:
    def test_minted_ingest_token_gets_503_naming_the_governed_pipeline(
        self, api_client, monkeypatch
    ):
        monkeypatch.setenv("OOTILS_DIRECT_INGEST_ENABLED", "0")
        token = _mint_token(api_client, scopes=["ingest"])

        resp = _post_items_dry_run(api_client, token)
        assert resp.status_code == 503, resp.text
        detail = resp.json()["detail"]
        assert "direct ingest disabled" in detail
        assert "governed daily-run pipeline" in detail


# ===========================================================================
# 3. Switch OFF + LEGACY root token — 200. THE orchestrator case: the
#    governed pipeline itself presents the legacy credential and must
#    keep writing when direct ingest is fenced for everyone else.
# ===========================================================================


class TestFenceOffLegacyExempt:
    def test_legacy_root_token_still_passes_when_fence_is_down(
        self, api_client, monkeypatch
    ):
        monkeypatch.setenv("OOTILS_DIRECT_INGEST_ENABLED", "0")

        resp = _post_items_dry_run(api_client, LEGACY_TOKEN)
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "dry_run"


# ===========================================================================
# 4. Scope floor precedes the fence — a token WITHOUT `ingest` is 403
#    (missing scope), never 503, switch position irrelevant.
# ===========================================================================


class TestScopeFloorPrecedesFence:
    def test_read_only_token_is_403_not_503_when_fence_is_down(
        self, api_client, monkeypatch
    ):
        monkeypatch.setenv("OOTILS_DIRECT_INGEST_ENABLED", "0")
        token = _mint_token(api_client, scopes=["read"])

        resp = _post_items_dry_run(api_client, token)
        assert resp.status_code == 403, resp.text
        assert resp.json()["detail"] == "missing scope 'ingest'"


# ===========================================================================
# 5. Staging is buried (ADR-042 decision 2.1) — the router is unmounted,
#    so its entry point is a clean 404 even for the legacy admin.
# ===========================================================================


class TestStagingUnmounted:
    def test_staging_upload_is_404(self, api_client):
        resp = api_client.post(
            "/v1/staging/upload",
            headers=_bearer(LEGACY_TOKEN),
            files={
                "file": ("items.tsv", io.BytesIO(b"external_id\tname\nX\tY\n"), "text/plain")
            },
            data={"entity_type": "items", "source_system": "FENCE-TEST"},
        )
        assert resp.status_code == 404, resp.text
