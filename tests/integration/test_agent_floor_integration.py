"""
Integration tests for the agent enterprise floor (chantier #392 PR1) against a
real PostgreSQL database (no mocks). Migration 064 (api_tokens + the
api_request_log.token_id/actor_kind audit columns) is applied by the
``migrated_db`` fixture the same way production applies it (OotilsDB startup).

THE SECURITY CONTRACT under test — a minted token's ``actor_kind`` comes from
the ``api_tokens`` row, NOT the request body, so the #341 human-only approval
gate becomes genuinely enforceable:

  1. An AGENT token cannot approve an L3 (APPROVED) — 403 on the scope floor,
     and ALSO 403 on the human gate even when the agent is (abnormally) minted
     with recommend:approve AND the body lies actor_kind="human". This is the
     defence-in-depth that proves the self-declaration hole is closed.
  2. A HUMAN token approves — 200; the audit row records the TOKEN's actor_kind.
  3. Revocation takes effect within the cache TTL (forced here by clearing the
     process cache, never by a wall-clock sleep) — a revoked token -> 401.
  4. Legacy global-token behaviour is byte-unchanged (admin, approves, and its
     api_request_log.token_id stays NULL).
  5. Audit attributability: a minted call stamps token_id + actor_kind on the
     last api_request_log row.
  6. Stream requires the read scope (403 without, 200 with; ``once=true`` for a
     bounded response).
  7. Generic missing-scope: a {read}-only token cannot even move to REVIEWED.

Plus the #392 security-review fixes:
  8. Staging L3 gate (defect: staging/approve.py had NO gate at all before
     #392 — an agent token with the `ingest` scope could apply canonical
     master-data changes). An agent token WITH `ingest` -> 403 on the human
     gate; an agent token WITHOUT `ingest` -> 403 on the scope floor first
     (never reaches the gate); a human token WITH `ingest` clears both floors.
  9. The legacy-window gate fallback (defect 9, ``resolve_gate_kind``) proven
     end-to-end: the shared legacy token honours a body-declared actor_kind
     for the HUMAN GATE ONLY (pre-#392 behaviour, preserved on purpose) —
     legacy + body declares 'agent' -> 403; legacy + no declaration/'human'
     -> 200. This is the transition-window compromise, not a reopened hole:
     a MINTED token never gets this fallback (see TestAgentCannotApproveL3
     above, which already proves the token always wins for minted callers).
  10. Audit FK fallback (defect 5): a hard-DELETEd api_tokens row must not
      erase the audit trail of calls the (still process-cached) token
      authenticated in its final ≤TTL window — the INSERT retries with
      token_id=NULL, keeping actor_kind/prefix.
  11. A `service` token (defect 6: migration 064's CHECK now allows it end to
      end) can draft-transition a recommendation with no CHECK violation, and
      the audit trail correctly attributes actor_kind='service'.

Plus the AN-2 (#392 PR2a) scope-floor + rate-limit rollout:
  12. THE SCOPE MATRIX (roadmap acceptance criterion): one representative write
      per scope family — a {read}-only token is denied with the missing scope
      NAMED, the family-scoped token clears the auth floor.
  13. The per-token rate limit: rate_per_min=3 → 3×200 then 429 + integer
      Retry-After ≥ 1, the sliding window driven by the injected counter clock
      (no wall-clock sleep) so the token recovers after the window.
  14. Legacy non-regression: the shared env token clears every family write and
      is never rate-limited (token_id is None → exempt by design).
  15. Kill-switch precedes rate: a disabled fleet 503s an agent BEFORE the rate
      counter is ever touched (observable — no bucket for that token_id).

Tokens are inserted DIRECTLY in SQL: only the test knows the cleartext; the DB
holds ``hash_token(clear)``. Each test seeds its own uuid4-suffixed tokens so
there are no inter-test collisions. No wall-clock timing assertions anywhere.
"""
from __future__ import annotations

import io
import os
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

BASELINE = "00000000-0000-0000-0000-000000000001"
LEGACY_TOKEN = "integration-test-token"


# ---------------------------------------------------------------------------
# App fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """TestClient with get_db overridden onto the test DB.

    Mirrors test_recommendations_api_integration.py. NOTE: because get_db is
    overridden, _should_audit_request() returns False, so this client does NOT
    write api_request_log rows — the audit-attribution assertions use the
    ``audit_client`` fixture below instead.
    """
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


@pytest.fixture(scope="module")
def audit_client(migrated_db):
    """TestClient WITHOUT a get_db override so the api_request_log middleware
    actually runs (audit attribution tests). Binds to the real DB via
    DATABASE_URL, exactly as the app resolves its pool in production."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = LEGACY_TOKEN

    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app

    app = create_app()
    with TestClient(app) as client:
        yield client


@pytest.fixture(autouse=True)
def _clear_token_cache():
    """The minted-token lookup is memoised in-process AND the per-token rate
    counter accumulates in-process; clear BOTH around every test so a
    seed/revoke in one test never leaks a cached decision into another (and so
    revocation is observable without a TTL sleep), and so a token that spent
    rate slots in one test never carries them into the next. Both are
    module-level singletons in ootils_core.api.auth, shared by the app the
    api_client fixture drives."""
    import ootils_core.api.auth as auth

    auth._token_cache.clear()
    auth._rate_counter.clear()
    yield
    auth._token_cache.clear()
    auth._rate_counter.clear()


# ---------------------------------------------------------------------------
# Direct DB helpers
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _mint_token(
    dsn,
    *,
    actor_kind: str,
    scopes: list[str],
    expires_at: str | None = None,
    revoked_at: str | None = None,
    rate_per_min: int | None = None,
) -> tuple[str, str]:
    """Insert one api_tokens row; return (cleartext_token, token_id).

    The cleartext exists ONLY here in the test — the DB stores its SHA-256 via
    the same hash_token the auth layer uses on lookup.

    ``rate_per_min`` defaults to None (NULL = no per-token cap), so every token
    minted by the pre-existing tests stays UNCAPPED and can never 429 — the
    AN-2 rate counter is exercised only by the tokens that explicitly ask for a
    budget (see TestRateLimit429 / TestKillSwitchBeforeRate below).
    """
    from ootils_core.api.auth import hash_token, token_prefix

    clear = f"ootk_{actor_kind}_{uuid4().hex}"
    token_id = uuid4()
    with _db_conn(dsn) as conn:
        conn.execute(
            """
            INSERT INTO api_tokens (
                token_id, name, actor_kind, token_hash, token_prefix,
                scopes, expires_at, revoked_at, rate_per_min
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                token_id,
                f"test-{actor_kind}-{token_id}",
                actor_kind,
                hash_token(clear),
                token_prefix(clear),
                scopes,
                expires_at,
                revoked_at,
                rate_per_min,
            ),
        )
    return clear, str(token_id)


def _revoke(dsn, token_id: str) -> None:
    with _db_conn(dsn) as conn:
        conn.execute(
            "UPDATE api_tokens SET revoked_at = now() WHERE token_id = %s",
            (token_id,),
        )


def _insert_recommendation(
    dsn,
    *,
    scenario_id: str = BASELINE,
    status: str = "DRAFT",
    action: str = "EXPEDITE",
    agent_name: str = "shortage_watcher",
) -> tuple[str, str]:
    run_id = uuid4()
    reco_id = uuid4()
    with _db_conn(dsn) as conn:
        conn.execute(
            "INSERT INTO agent_runs (agent_run_id, agent_name, scenario_id, status) "
            "VALUES (%s, %s, %s, 'COMPLETED')",
            (run_id, agent_name, scenario_id),
        )
        conn.execute(
            """
            INSERT INTO recommendations (
                recommendation_id, agent_name, agent_run_id, scenario_id,
                item_id, item_external_id, shortage_date,
                deficit_qty, recommended_qty, estimated_cost, currency,
                lead_time_days, runway_days, margin_days,
                action, status, confidence
            ) VALUES (
                %s, %s, %s, %s,
                %s, 'PUMP-01', '2026-08-15',
                100, 120, 4800, 'EUR',
                14, 30, 16,
                %s, %s, 'HIGH'
            )
            """,
            (reco_id, agent_name, run_id, scenario_id, uuid4(), action, status),
        )
    return str(reco_id), str(run_id)


def _upload_and_validate_batch(api_client, source_system: str) -> str:
    """Upload a minimal single-row items TSV via the LEGACY (admin) token,
    then force the batch into 'validated' so it is approve-ready — mirrors
    tests/integration/test_staging_approve.py::_upload_and_validate.

    Deliberately a PURE INSERT (0% deletion ratio) so approval never needs
    force=true — the point of these tests is the auth floor, not the diff
    guard. Uses the LEGACY token for setup (admin holds every scope,
    including `ingest`) so the fabricated test tokens are reserved for the
    actual assertions under test.
    """
    external_id = f"AGENTFLOOR-{uuid4().hex[:8]}"
    lines = [
        "external_id\tname\titem_type\tuom\tstatus",
        f"{external_id}\tAgent Floor Item\tcomponent\tEA\tactive",
    ]
    data = ("\n".join(lines) + "\n").encode("utf-8")
    resp = api_client.post(
        "/v1/staging/upload",
        headers=_bearer(LEGACY_TOKEN),
        files={"file": ("items.tsv", io.BytesIO(data), "text/plain")},
        data={"entity_type": "items", "source_system": source_system},
    )
    assert resp.status_code == 202, resp.text
    batch_id = resp.json()["batch_id"]
    with _db_conn(os.environ["DATABASE_URL"]) as conn:
        conn.execute(
            "UPDATE ingest_batches SET status = 'validated', dq_status = 'validated' "
            "WHERE batch_id = %s",
            (batch_id,),
        )
    return batch_id


# ---------------------------------------------------------------------------
# Per-test cleanup tracker
# ---------------------------------------------------------------------------


@pytest.fixture
def tracker(migrated_db):
    created = {"recos": [], "runs": [], "tokens": []}

    def _reco(**kwargs) -> str:
        reco_id, run_id = _insert_recommendation(migrated_db, **kwargs)
        created["recos"].append(reco_id)
        created["runs"].append(run_id)
        return reco_id

    def _token(**kwargs) -> tuple[str, str]:
        clear, token_id = _mint_token(migrated_db, **kwargs)
        created["tokens"].append(token_id)
        return clear, token_id

    ns = type("NS", (), {"reco": staticmethod(_reco), "token": staticmethod(_token)})
    yield ns

    with _db_conn(migrated_db) as conn:
        if created["recos"]:
            conn.execute(
                "DELETE FROM recommendation_transitions WHERE recommendation_id = ANY(%s::uuid[])",
                (created["recos"],),
            )
            conn.execute(
                "DELETE FROM recommendations WHERE recommendation_id = ANY(%s::uuid[])",
                (created["recos"],),
            )
        if created["runs"]:
            conn.execute(
                "DELETE FROM agent_runs WHERE agent_run_id = ANY(%s::uuid[])",
                (created["runs"],),
            )
        if created["tokens"]:
            # Null the audit FK first is unnecessary (ON DELETE SET NULL), but
            # scrub the audit rows we generated so the table stays tidy.
            conn.execute(
                "DELETE FROM api_request_log WHERE token_id = ANY(%s::uuid[])",
                (created["tokens"],),
            )
            conn.execute(
                "DELETE FROM api_tokens WHERE token_id = ANY(%s::uuid[])",
                (created["tokens"],),
            )
        conn.execute("DELETE FROM events WHERE event_type = 'recommendation_transition'")


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ===========================================================================
# 1. Agent token cannot approve an L3 (both floors)
# ===========================================================================


class TestAgentCannotApproveL3:
    def _reviewed_reco(self, tracker) -> str:
        """A recommendation moved DRAFT -> REVIEWED (via legacy admin), so the
        only thing standing between it and APPROVED is the caller's identity."""
        return tracker.reco(status="REVIEWED")

    def test_scope_floor_blocks_agent_without_approve(self, api_client, tracker):
        reco_id = self._reviewed_reco(tracker)
        clear, _ = tracker.token(actor_kind="agent", scopes=["read", "recommend:draft"])

        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPROVED", "actor": "shortage_watcher", "actor_kind": "agent"},
            headers=_bearer(clear),
        )
        assert resp.status_code == 403, resp.text
        # It failed on the SCOPE floor, not the human gate.
        assert "scope" in resp.json()["detail"].lower()

    def test_human_gate_blocks_agent_even_with_approve_scope_and_lying_body(
        self, api_client, tracker
    ):
        """DEFENCE IN DEPTH: an agent token abnormally minted WITH
        recommend:approve clears the scope floor — but the human gate reads the
        TOKEN's actor_kind ('agent'), not the body, so a body claiming
        actor_kind='human' still gets 403. This is the test that proves the
        self-declaration hole is closed."""
        reco_id = self._reviewed_reco(tracker)
        clear, _ = tracker.token(actor_kind="agent", scopes=["recommend:approve"])

        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPROVED", "actor": "rogue", "actor_kind": "human"},
            headers=_bearer(clear),
        )
        assert resp.status_code == 403, resp.text
        # The human gate message (not the scope message) — the body lie was ignored.
        assert "human" in resp.json()["detail"].lower()

        # And the recommendation was NOT approved.
        with _db_conn(os.environ["DATABASE_URL"]) as conn:
            row = conn.execute(
                "SELECT status FROM recommendations WHERE recommendation_id = %s",
                (reco_id,),
            ).fetchone()
        assert row["status"] == "REVIEWED"


# ===========================================================================
# 2. Human token approves; audit records the token's actor_kind
# ===========================================================================


class TestHumanApproves:
    def test_human_token_approves_and_audit_kind_is_from_token(self, api_client, tracker):
        reco_id = tracker.reco(status="REVIEWED")
        # Body deliberately claims 'agent' — the TOKEN is human, and the token wins.
        clear, _ = tracker.token(actor_kind="human", scopes=["recommend:approve"])

        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPROVED", "actor": "ngoineau", "actor_kind": "agent"},
            headers=_bearer(clear),
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["actor_kind"] == "human"  # response echoes the token's kind

        with _db_conn(os.environ["DATABASE_URL"]) as conn:
            reco = conn.execute(
                "SELECT status FROM recommendations WHERE recommendation_id = %s",
                (reco_id,),
            ).fetchone()
            assert reco["status"] == "APPROVED"
            audit = conn.execute(
                "SELECT actor, actor_kind FROM recommendation_transitions "
                "WHERE recommendation_id = %s ORDER BY created_at DESC LIMIT 1",
                (reco_id,),
            ).fetchone()
        # The audit trail records the TOKEN's kind, not the body's.
        assert audit["actor_kind"] == "human"
        assert audit["actor"] == "ngoineau"


# ===========================================================================
# 3. Revocation observed within the cache TTL (cache cleared, no sleep)
# ===========================================================================


class TestRevocationWithinTtl:
    def test_valid_then_revoked_is_401(self, api_client, tracker):
        clear, token_id = tracker.token(actor_kind="agent", scopes=["read"])

        ok = api_client.get("/v1/recommendations", headers=_bearer(clear))
        assert ok.status_code == 200, ok.text

        _revoke(os.environ["DATABASE_URL"], token_id)
        # Purge the process cache so the next lookup re-hits the DB (models the
        # <=TTL window elapsing, without a 30 s wall-clock wait).
        import ootils_core.api.auth as auth

        auth._token_cache.clear()

        revoked = api_client.get("/v1/recommendations", headers=_bearer(clear))
        assert revoked.status_code == 401


# ===========================================================================
# 4. Legacy global token unchanged
# ===========================================================================


class TestLegacyUnchanged:
    def test_legacy_reads_and_approves_as_admin(self, api_client, tracker):
        reco_id = tracker.reco(status="REVIEWED")
        headers = _bearer(LEGACY_TOKEN)

        listed = api_client.get("/v1/recommendations", headers=headers)
        assert listed.status_code == 200, listed.text

        approved = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPROVED", "actor": "ngoineau", "actor_kind": "human"},
            headers=headers,
        )
        assert approved.status_code == 200, approved.text

    def test_legacy_audit_row_has_null_token_id(self, audit_client, tracker):
        # audit_client writes api_request_log (no get_db override).
        resp = audit_client.get("/v1/recommendations", headers=_bearer(LEGACY_TOKEN))
        assert resp.status_code == 200, resp.text

        with _db_conn(os.environ["DATABASE_URL"]) as conn:
            row = conn.execute(
                """
                SELECT token_id, token_prefix, actor_kind
                FROM api_request_log
                WHERE path = '/v1/recommendations'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchone()
        assert row is not None
        assert row["token_id"] is None  # legacy token has no DB identity
        assert row["token_prefix"] == "global_token"


# ===========================================================================
# 5. Audit attributability for a minted call
# ===========================================================================


class TestAuditAttribution:
    def test_minted_call_stamps_token_id_and_actor_kind(self, audit_client, tracker):
        clear, token_id = tracker.token(actor_kind="agent", scopes=["read"])

        resp = audit_client.get("/v1/recommendations", headers=_bearer(clear))
        assert resp.status_code == 200, resp.text

        with _db_conn(os.environ["DATABASE_URL"]) as conn:
            row = conn.execute(
                """
                SELECT token_id, actor_kind
                FROM api_request_log
                WHERE token_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (token_id,),
            ).fetchone()
        assert row is not None
        assert str(row["token_id"]) == token_id
        assert row["actor_kind"] == "agent"


# ===========================================================================
# 6. Stream scope
# ===========================================================================


class TestStreamScope:
    def test_stream_without_read_scope_is_403(self, api_client, tracker):
        # A token with a non-read scope only — stream requires 'read'.
        clear, _ = tracker.token(actor_kind="agent", scopes=["recommend:draft"])
        resp = api_client.get("/v1/stream?once=true", headers=_bearer(clear))
        assert resp.status_code == 403, resp.text
        assert resp.json()["detail"] == "missing scope 'read'"

    def test_stream_with_read_scope_is_200(self, api_client, tracker):
        clear, _ = tracker.token(actor_kind="agent", scopes=["read"])
        # once=true -> bounded catch-up drain, so the TestClient does not hang
        # waiting on an open-ended SSE stream.
        resp = api_client.get("/v1/stream?once=true", headers=_bearer(clear))
        assert resp.status_code == 200, resp.text
        assert resp.headers["content-type"].startswith("text/event-stream")


# ===========================================================================
# 7. Generic missing-scope on a transition
# ===========================================================================


class TestGenericMissingScope:
    def test_read_only_token_cannot_review(self, api_client, tracker):
        reco_id = tracker.reco(status="DRAFT")
        clear, _ = tracker.token(actor_kind="agent", scopes=["read"])

        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "REVIEWED", "actor": "shortage_watcher", "actor_kind": "agent"},
            headers=_bearer(clear),
        )
        assert resp.status_code == 403, resp.text
        # DRAFT -> REVIEWED needs recommend:draft, which this token lacks.
        assert resp.json()["detail"] == "missing scope 'recommend:draft'"


# ===========================================================================
# 8. Staging L3 gate (#392 security-review — staging/approve.py had NO gate
#    at all before #392)
# ===========================================================================


class TestStagingApproveGate:
    def test_agent_with_ingest_scope_blocked_by_human_gate(self, api_client, tracker):
        """An agent token holding `ingest` clears the scope floor -> reaches
        the NEW human gate added by the security review -> 403."""
        batch_id = _upload_and_validate_batch(
            api_client, source_system=f"AGENTFLOOR-GATE-{uuid4().hex[:6]}"
        )
        clear, _ = tracker.token(actor_kind="agent", scopes=["ingest"])

        resp = api_client.post(
            f"/v1/staging/batches/{batch_id}/approve",
            headers=_bearer(clear),
            json={"approved_by": "shortage_watcher"},
        )
        assert resp.status_code == 403, resp.text
        assert "human" in resp.json()["detail"].lower()

        # Nothing was committed to canonical: the batch is still 'validated'.
        with _db_conn(os.environ["DATABASE_URL"]) as conn:
            row = conn.execute(
                "SELECT status FROM ingest_batches WHERE batch_id = %s",
                (batch_id,),
            ).fetchone()
        assert row["status"] == "validated"

    @pytest.mark.parametrize("endpoint_suffix", ["approve", "reject", "diff"])
    def test_agent_without_ingest_scope_blocked_by_scope_floor(
        self, api_client, tracker, endpoint_suffix
    ):
        """Without `ingest` at all, the SCOPE floor fires first — the request
        never even reaches the (approve-only) human gate."""
        batch_id = _upload_and_validate_batch(
            api_client, source_system=f"AGENTFLOOR-NOSCOPE-{uuid4().hex[:6]}"
        )
        clear, _ = tracker.token(actor_kind="agent", scopes=["read"])

        if endpoint_suffix == "diff":
            resp = api_client.get(
                f"/v1/staging/batches/{batch_id}/diff", headers=_bearer(clear)
            )
        elif endpoint_suffix == "approve":
            resp = api_client.post(
                f"/v1/staging/batches/{batch_id}/approve",
                headers=_bearer(clear),
                json={"approved_by": "shortage_watcher"},
            )
        else:  # reject
            resp = api_client.post(
                f"/v1/staging/batches/{batch_id}/reject",
                headers=_bearer(clear),
                json={"rejected_by": "shortage_watcher", "reason": "test"},
            )
        assert resp.status_code == 403, resp.text
        assert resp.json()["detail"] == "missing scope 'ingest'"

    def test_human_with_ingest_scope_clears_both_floors(self, api_client, tracker):
        """A human token holding `ingest` passes the scope floor AND the human
        gate — the request reaches the real approval logic. Assert it is NOT
        an auth/gate 403 (the batch is a clean pure-insert, so this resolves
        as 200; asserting 'not 403' first keeps the test robust to any
        unrelated business-logic wrinkle in approve_batch)."""
        batch_id = _upload_and_validate_batch(
            api_client, source_system=f"AGENTFLOOR-HUMANOK-{uuid4().hex[:6]}"
        )
        clear, _ = tracker.token(actor_kind="human", scopes=["ingest"])

        resp = api_client.post(
            f"/v1/staging/batches/{batch_id}/approve",
            headers=_bearer(clear),
            json={"approved_by": "ngoineau"},
        )
        assert resp.status_code != 403, resp.text
        assert resp.status_code == 200, resp.text
        assert resp.json()["counts"]["rows_inserted"] == 1


# ===========================================================================
# 9. Legacy-window gate fallback (#392 defect 9, resolve_gate_kind)
#    proven end-to-end on the recommendations transition endpoint.
# ===========================================================================


class TestLegacyGateFallbackEndToEnd:
    def test_legacy_with_declared_agent_is_403(self, api_client, tracker):
        """The shared legacy token, with the body still declaring
        actor_kind='agent', is gated on THAT declared value (pre-#392
        behaviour preserved on purpose for the transition window) -> 403,
        exactly as an honestly-declaring agent got before #392."""
        reco_id = tracker.reco(status="REVIEWED")
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPROVED", "actor": "shortage_watcher", "actor_kind": "agent"},
            headers=_bearer(LEGACY_TOKEN),
        )
        assert resp.status_code == 403, resp.text
        assert "human" in resp.json()["detail"].lower()

        with _db_conn(os.environ["DATABASE_URL"]) as conn:
            row = conn.execute(
                "SELECT status FROM recommendations WHERE recommendation_id = %s",
                (reco_id,),
            ).fetchone()
        assert row["status"] == "REVIEWED"  # not approved

    def test_legacy_with_declared_human_is_200(self, api_client, tracker):
        """Same shared legacy token, body now declares (or defaults to)
        'human' -> the gate decides on that declared value -> 200. Proves the
        transition window does not regress the ordinary human-approves path."""
        reco_id = tracker.reco(status="REVIEWED")
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPROVED", "actor": "ngoineau", "actor_kind": "human"},
            headers=_bearer(LEGACY_TOKEN),
        )
        assert resp.status_code == 200, resp.text

    def test_legacy_with_no_declared_actor_kind_is_200(self, api_client, tracker):
        """actor_kind omitted entirely -> TransitionRequest defaults it to
        'human' -> resolve_gate_kind's legacy fallback sees 'human' -> 200.
        Distinct from the explicit-'human' case above: this exercises the
        Pydantic default path, not an explicit client declaration."""
        reco_id = tracker.reco(status="REVIEWED")
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPROVED", "actor": "ngoineau"},
            headers=_bearer(LEGACY_TOKEN),
        )
        assert resp.status_code == 200, resp.text


# ===========================================================================
# 10. Audit FK fallback (#392 defect 5) — a hard-DELETEd api_tokens row must
#     not erase the audit trail of a call it authenticated while cached.
# ===========================================================================


class TestAuditFkFallback:
    def test_hard_deleted_token_audit_row_falls_back_to_null_token_id(
        self, audit_client, tracker
    ):
        """audit_client (no get_db override) so _log_api_request actually
        runs. Sequence: mint -> authenticate once (populates the process
        cache) -> hard-DELETE the api_tokens row WITHOUT clearing the cache
        (models the token's final ≤TTL window after a hard delete) ->
        authenticate again -> the SELECT the cache still vouches for
        succeeds, but the audit INSERT's token_id FK now points at nothing.
        _log_api_request must retry with token_id=NULL rather than losing the
        row (migration-064 ForeignKeyViolation carve-out in app.py)."""
        clear, token_id = tracker.token(actor_kind="agent", scopes=["read"])

        warm = audit_client.get("/v1/recommendations", headers=_bearer(clear))
        assert warm.status_code == 200, warm.text  # populates the process cache

        with _db_conn(os.environ["DATABASE_URL"]) as conn:
            conn.execute("DELETE FROM api_tokens WHERE token_id = %s", (token_id,))
        # Deliberately NOT clearing auth._token_cache — the whole point is
        # that the in-process cache still vouches for the (now-deleted) token
        # for the remainder of its TTL window.

        resp = audit_client.get("/v1/recommendations", headers=_bearer(clear))
        assert resp.status_code == 200, resp.text  # cache hit -> still authenticates

        with _db_conn(os.environ["DATABASE_URL"]) as conn:
            row = conn.execute(
                """
                SELECT token_id, actor_kind, token_prefix
                FROM api_request_log
                WHERE token_prefix = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (clear[:12],),
            ).fetchone()
        assert row is not None
        assert row["token_id"] is None  # FK target gone -> NULL fallback, row kept
        assert row["actor_kind"] == "agent"  # denormalised kind still stamped
        assert row["token_prefix"] == clear[:12]
        # tracker's own teardown DELETEs api_tokens WHERE token_id = ANY(...);
        # this row is already gone, so that DELETE is a harmless no-op here.


# ===========================================================================
# 11. service token (#392 defect 6 — migration 064's CHECK now permits it
#     end to end, no CheckViolation on transition_one's INSERT)
# ===========================================================================


class TestServiceActorKind:
    def test_service_token_drafts_transition_with_no_check_violation(
        self, api_client, tracker
    ):
        reco_id = tracker.reco(status="DRAFT")
        clear, _ = tracker.token(actor_kind="service", scopes=["recommend:draft"])

        # No actor_kind in the body: the field is a deprecated Literal['human','agent']
        # and is ignored for minted tokens. 'service' comes from the TOKEN — that is
        # exactly the contract this test proves (the widened migration-064 CHECK lets
        # the transition record actor_kind='service' without a CheckViolation).
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "REVIEWED", "actor": "billing-sync"},
            headers=_bearer(clear),
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["actor_kind"] == "service"

        with _db_conn(os.environ["DATABASE_URL"]) as conn:
            audit = conn.execute(
                "SELECT actor_kind FROM recommendation_transitions "
                "WHERE recommendation_id = %s ORDER BY created_at DESC LIMIT 1",
                (reco_id,),
            ).fetchone()
        assert audit["actor_kind"] == "service"


# ===========================================================================
# 12. AN-2 (#392 PR2a) — the scope-enforcement MATRIX (roadmap acceptance
#     criterion): one representative WRITE per scope family, both floors.
#
#     The read-only token is denied with the missing scope NAMED (the
#     acceptance criterion); the family-scoped token clears the auth floor (a
#     2xx, a business/validation 422, or a 404 all prove auth let it through —
#     only a 403 would be an auth block, scope or human gate).
#
#     PREREQUISITE / minimal-valid-payload notes, per family:
#       * ingest         POST /v1/ingest/items   — dry_run=True + a valid ItemRow
#                        (external_id + name non-empty) → 200 with NO DB writes.
#       * calc:run       POST /v1/calc/run        — {full_recompute: False} on
#                        baseline → deterministic recompute (200; an engine 500
#                        is still != 403).
#       * graph:write    POST /v1/nodes/{id}/firm — the require_scope ROUTE
#                        dependency fires before the body, so a read-only token
#                        is 403 regardless of node existence; the granted token
#                        reaches the body → 404 (random node_id), != 403.
#       * scenario:write POST /v1/simulate        — {scenario_name, overrides:[]}
#                        forks baseline → 201.
#       * recommend:approve POST /v1/recommendations/{id}/transition APPROVED —
#                        the scope check is IN-BODY (runtime target → runtime
#                        scope), so the body MUST be valid (else a 422 pre-empts
#                        the scope check); a REVIEWED reco is seeded. The GRANTED
#                        token here is a HUMAN: APPROVED is a HUMAN_ONLY target,
#                        so an agent+approve token would clear the scope floor
#                        but hit the human GATE (403) — only a human clears both.
#       * admin          POST /v1/demo/phase1/run — the ONLY admin-scoped write.
#                        DENIED hits the real endpoint (require_scope('admin')
#                        403s BEFORE the body → the demo never runs). GRANTED /
#                        legacy stub the demo body (_stub_phase1_demo) so the
#                        test isolates the AUTH-FLOOR grant from the heavy nested
#                        Phase-1 chain (covered by test_phase1_e2e.py).
# ===========================================================================


_MATRIX_FAMILIES = [
    "ingest",
    "calc:run",
    "graph:write",
    "scenario:write",
    "recommend:approve",
    "admin",
]


def _grant_actor_kind(family: str) -> str:
    """The actor_kind a GRANTED token must carry to clear BOTH auth floors for
    a family. recommend:approve targets APPROVED (a HUMAN_ONLY target), so an
    agent+approve token clears the scope floor but is stopped by the human GATE
    — only a human clears both. Every other family's write carries no human
    gate, so an agent (the ordinary fleet caller) is representative."""
    return "human" if family == "recommend:approve" else "agent"


def _do_family_write(api_client, tracker, family: str, token: str):
    """Perform ONE representative write for ``family`` with ``token``; return
    the raw response. Seeds only the prerequisite that family needs."""
    headers = _bearer(token)
    if family == "ingest":
        return api_client.post(
            "/v1/ingest/items",
            headers=headers,
            json={
                "items": [
                    {"external_id": f"FLOOR-{uuid4().hex[:8]}", "name": "Floor Item"}
                ],
                "dry_run": True,
            },
        )
    if family == "calc:run":
        return api_client.post(
            "/v1/calc/run", headers=headers, json={"full_recompute": False}
        )
    if family == "graph:write":
        return api_client.post(
            f"/v1/nodes/{uuid4()}/firm", headers=headers, json={"actor": "floor-test"}
        )
    if family == "scenario:write":
        return api_client.post(
            "/v1/simulate",
            headers=headers,
            json={"scenario_name": f"floor-{uuid4().hex[:8]}", "overrides": []},
        )
    if family == "recommend:approve":
        reco_id = tracker.reco(status="REVIEWED")
        return api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            headers=headers,
            json={
                "to_status": "APPROVED",
                "actor": "floor-approver",
                "actor_kind": "human",
            },
        )
    if family == "admin":
        return api_client.post("/v1/demo/phase1/run", headers=headers)
    raise AssertionError(f"unknown matrix family {family!r}")


def _stub_phase1_demo(monkeypatch):
    """Replace the heavy Phase-1 demo body with a cheap stub for the GRANTED
    admin cases. The scope-enforcement test proves the ADMIN SCOPE grants access
    to the admin-only endpoint — NOT that the demo chain runs (that is
    test_phase1_e2e.py's job). Left unstubbed, the granted case would execute a
    full nested-TestClient demo run (create_app + its own TestClient + the whole
    Forecast→…→ATP chain) with no integration precedent. The DENIED admin case
    needs no stub: require_scope('admin') is a route dependency that 403s BEFORE
    the body, so the real demo body is never reached there.

    The endpoint calls ``run_phase1_demo_from_env`` as a module global of
    ``routers.demo`` resolved at call time, so rebinding that name reaches the
    already-constructed api_client app."""
    import ootils_core.api.routers.demo as demo_router

    monkeypatch.setattr(
        demo_router, "run_phase1_demo_from_env", lambda: {"stubbed": True}
    )


class TestScopeMatrix:
    """The roadmap acceptance criterion, both floors of the matrix."""

    @pytest.mark.parametrize("family", _MATRIX_FAMILIES)
    def test_read_only_token_denied_with_named_scope(self, api_client, tracker, family):
        # A single {read}-only agent token attempts each family's write.
        clear, _ = tracker.token(actor_kind="agent", scopes=["read"])
        resp = _do_family_write(api_client, tracker, family, clear)
        assert resp.status_code == 403, resp.text
        assert resp.json()["detail"] == f"missing scope '{family}'"

    @pytest.mark.parametrize("family", _MATRIX_FAMILIES)
    def test_family_scoped_token_clears_the_auth_floor(
        self, api_client, tracker, family, monkeypatch
    ):
        if family == "admin":
            _stub_phase1_demo(monkeypatch)
        clear, _ = tracker.token(
            actor_kind=_grant_actor_kind(family), scopes=[family]
        )
        resp = _do_family_write(api_client, tracker, family, clear)
        # 2xx / business-422 / 404 all mean the auth floor let the request
        # THROUGH; only a 403 would be an auth (scope or gate) block.
        assert resp.status_code != 403, resp.text


# ===========================================================================
# 13. AN-2 per-token rate limit — 429 + integer Retry-After, window driven by
#     the injected clock (no wall-clock sleep).
# ===========================================================================


class TestRateLimit429:
    def test_capped_token_429s_after_its_budget_then_recovers_after_the_window(
        self, api_client, tracker, monkeypatch
    ):
        """rate_per_min=3 → 3×200, then the 4th → 429 with an integer
        Retry-After ≥ 1. The counter's clock IS injectable from the test:
        ``_RateCounter._clock`` is an instance slot on the module-level
        ``auth._rate_counter`` singleton the app uses, so the 60 s sliding
        window is advanced HERE without sleeping — after crossing the window
        the same token is admitted again."""
        import ootils_core.api.auth as auth

        clock = {"t": 1000.0}
        monkeypatch.setattr(auth._rate_counter, "_clock", lambda: clock["t"])

        clear, _ = tracker.token(
            actor_kind="agent", scopes=["read"], rate_per_min=3
        )
        headers = _bearer(clear)

        # 3 requests inside the budget, same instant → 200.
        for _ in range(3):
            r = api_client.get("/v1/recommendations", headers=headers)
            assert r.status_code == 200, r.text

        # 4th, still the same instant → over budget → 429 + Retry-After.
        blocked = api_client.get("/v1/recommendations", headers=headers)
        assert blocked.status_code == 429, blocked.text
        assert "Retry-After" in blocked.headers
        assert int(blocked.headers["Retry-After"]) >= 1

        # Advance PAST the window (injected clock, no sleep): the three
        # in-window timestamps age out → the token is admitted again.
        clock["t"] += 61.0
        after = api_client.get("/v1/recommendations", headers=headers)
        assert after.status_code == 200, after.text


# ===========================================================================
# 14. Legacy non-regression — the shared env token clears every family write
#     and is NEVER rate-limited (token_id is None → exempt by design).
# ===========================================================================


class TestLegacyNonRegression:
    @pytest.mark.parametrize("family", _MATRIX_FAMILIES)
    def test_legacy_token_clears_every_family_write(
        self, api_client, tracker, family, monkeypatch
    ):
        # The legacy token is admin (superset scope) + human (clears the human
        # gate), so it must pass every family's auth floor unchanged.
        if family == "admin":
            _stub_phase1_demo(monkeypatch)
        resp = _do_family_write(api_client, tracker, family, LEGACY_TOKEN)
        assert resp.status_code != 403, resp.text

    def test_legacy_token_is_never_rate_limited(self, api_client):
        """A tight burst on the shared legacy token never 429s — token_id is
        None, so _enforce_rate_limit returns before ever consulting the
        counter (pre-#392 behaviour preserved)."""
        for _ in range(5):
            r = api_client.get("/v1/recommendations", headers=_bearer(LEGACY_TOKEN))
            assert r.status_code == 200, r.text
            assert r.status_code != 429


# ===========================================================================
# 15. Kill-switch precedes rate — a disabled fleet 503s an agent BEFORE the
#     rate counter is ever touched (resolve_principal ordering: kill switch,
#     then rate limit).
# ===========================================================================


class TestKillSwitchBeforeRate:
    def test_disabled_fleet_503s_agent_and_never_spends_a_rate_slot(
        self, api_client, tracker, monkeypatch
    ):
        import ootils_core.api.auth as auth

        clear, token_id = tracker.token(
            actor_kind="agent", scopes=["read"], rate_per_min=3
        )
        # Start from a clean counter (the autouse fixture already cleared it),
        # then disable the fleet for THIS request only (monkeypatch restores).
        monkeypatch.setenv("OOTILS_AGENTS_ENABLED", "false")

        resp = api_client.get("/v1/recommendations", headers=_bearer(clear))
        assert resp.status_code == 503, resp.text

        # OBSERVABLE proof the counter never moved: the kill switch runs before
        # _enforce_rate_limit, so this token_id has NO bucket in the counter.
        assert UUID(token_id) not in auth._rate_counter._store


# ===========================================================================
# AN-2 (#392 PR2b) — the /v1/tokens REST lifecycle + /metrics scrape, driven
# END TO END (mint / list / revoke through HTTP, then USE the returned
# cleartext against the real auth path). Distinct from sections 1-15, which
# fabricate tokens with direct SQL: here the credential is BORN over the API
# (token_service.mint_token behind POST /v1/tokens) and every field of the
# lifecycle is observed through the same TestClient the fleet would use.
#
#   16. A token minted via POST works IN its granted scope (200) and is denied
#       OUT of it (403) — and an agent minted with recommend:approve is still
#       stopped by the human GATE on APPROVED (actor_kind from the token).
#   17. DELETE (revoke) takes effect on the VERY NEXT call → 401, with no TTL
#       sleep: revoke_token clears the in-process cache, which is the point.
#   18. last_used_at is bumped after a real authenticated call (cache-miss path).
#   19. GET ?include_revoked shows a revoked row (revoked_at set); the default
#       listing hides it.
#   20. /metrics (admin Bearer) exposes ootils_http_requests_total with a REAL
#       route-template label, and ootils_rate_limited_total climbs after a 429
#       (deltas — the collectors are process-global).
#   21. DELETE is idempotent (2nd → 204) and 404s an unknown token_id.
#
# Tokens minted here are NOT tracked by the `tracker` fixture (which only knows
# its own direct-SQL mints); the `api_token_ids` fixture below scrubs them.
# ===========================================================================


@pytest.fixture
def api_token_ids(migrated_db):
    """Collect token_ids minted THROUGH the API during a test and scrub them
    (and any audit rows) at teardown — the parallel of `tracker` for the
    HTTP-minted credentials the REST-lifecycle tests create."""
    ids: list[str] = []
    yield ids
    with _db_conn(migrated_db) as conn:
        if ids:
            conn.execute(
                "DELETE FROM api_request_log WHERE token_id = ANY(%s::uuid[])", (ids,)
            )
            conn.execute(
                "DELETE FROM api_tokens WHERE token_id = ANY(%s::uuid[])", (ids,)
            )


def _api_mint(
    api_client,
    *,
    actor_kind: str,
    scopes: list[str],
    rate_per_min: int | None = None,
    register: list[str] | None = None,
) -> dict:
    """Mint a token over POST /v1/tokens with the LEGACY admin token; return the
    parsed 201 body (carries the once-shown ``token`` cleartext + ``token_id``).
    Registers the token_id for teardown when ``register`` is supplied."""
    body: dict = {
        "name": f"api-{actor_kind}-{uuid4().hex[:8]}",
        "actor_kind": actor_kind,
        "scopes": scopes,
    }
    if rate_per_min is not None:
        body["rate_per_min"] = rate_per_min
    resp = api_client.post("/v1/tokens", headers=_bearer(LEGACY_TOKEN), json=body)
    assert resp.status_code == 201, resp.text
    data = resp.json()
    if register is not None:
        register.append(data["token_id"])
    return data


def _metric_value(exposition_text: str, needle: str) -> float:
    """Value of the first non-comment sample line containing ``needle``, or 0.0
    when no such line exists yet (an untouched counter emits no series). Used to
    read DELTAS around a call, since the Prometheus collectors are process-global
    and accumulate across every test in the run."""
    for line in exposition_text.splitlines():
        if line.startswith("#") or needle not in line:
            continue
        return float(line.rsplit(" ", 1)[1])
    return 0.0


# ===========================================================================
# 16. A minted token is usable in-scope, denied out-of-scope; the gate reads
#     the TOKEN's actor_kind even for an API-minted agent.
# ===========================================================================


class TestApiMintedTokenIsUsable:
    def test_in_scope_200_and_out_of_scope_403(self, api_client, tracker, api_token_ids):
        data = _api_mint(
            api_client, actor_kind="agent", scopes=["read"], register=api_token_ids
        )
        assert data["actor_kind"] == "agent"
        clear = data["token"]

        # IN scope: reading recommendations only needs `read`.
        ok = api_client.get("/v1/recommendations", headers=_bearer(clear))
        assert ok.status_code == 200, ok.text

        # OUT of scope: DRAFT -> REVIEWED needs recommend:draft, which this
        # read-only token lacks.
        reco_id = tracker.reco(status="DRAFT")
        denied = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "REVIEWED", "actor": "watcher", "actor_kind": "agent"},
            headers=_bearer(clear),
        )
        assert denied.status_code == 403, denied.text
        assert denied.json()["detail"] == "missing scope 'recommend:draft'"

    def test_api_minted_agent_is_stopped_by_the_human_gate_on_approve(
        self, api_client, tracker, api_token_ids
    ):
        """Even minted (abnormally) WITH recommend:approve, an API-born AGENT
        token clears the scope floor but is stopped by the human GATE on
        APPROVED — the actor_kind is the token's, not the lying body's. The
        end-to-end proof that minting over the API grants no self-declaration
        loophole."""
        data = _api_mint(
            api_client,
            actor_kind="agent",
            scopes=["recommend:approve"],
            register=api_token_ids,
        )
        clear = data["token"]
        reco_id = tracker.reco(status="REVIEWED")

        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPROVED", "actor": "rogue", "actor_kind": "human"},
            headers=_bearer(clear),
        )
        assert resp.status_code == 403, resp.text
        assert "human" in resp.json()["detail"].lower()


# ===========================================================================
# 17. DELETE (revoke) is observed on the NEXT call — cache clear, not TTL sleep.
# ===========================================================================


class TestApiRevokeTakesEffectImmediately:
    def test_revoke_then_next_call_is_401(self, api_client, api_token_ids):
        data = _api_mint(
            api_client, actor_kind="agent", scopes=["read"], register=api_token_ids
        )
        clear, token_id = data["token"], data["token_id"]

        # Warm the process cache with one successful auth.
        warm = api_client.get("/v1/recommendations", headers=_bearer(clear))
        assert warm.status_code == 200, warm.text

        # Revoke over HTTP: revoke_token flips revoked_at (committed by get_db)
        # AND clears the in-process principal cache — so the very next request
        # re-resolves against the DB, with no 30 s TTL wait.
        deleted = api_client.delete(
            f"/v1/tokens/{token_id}", headers=_bearer(LEGACY_TOKEN)
        )
        assert deleted.status_code == 204, deleted.text

        revoked = api_client.get("/v1/recommendations", headers=_bearer(clear))
        assert revoked.status_code == 401, revoked.text


# ===========================================================================
# 18. last_used_at bumped after a real call (the cache-miss bump path).
# ===========================================================================


class TestLastUsedAtBumped:
    def test_last_used_at_advances_after_one_authenticated_call(
        self, api_client, api_token_ids, migrated_db
    ):
        data = _api_mint(
            api_client, actor_kind="agent", scopes=["read"], register=api_token_ids
        )
        clear, token_id = data["token"], data["token_id"]

        with _db_conn(migrated_db) as conn:
            before = conn.execute(
                "SELECT last_used_at FROM api_tokens WHERE token_id = %s", (token_id,)
            ).fetchone()
        assert before["last_used_at"] is None  # never presented yet

        # First presentation of this token is a cache miss -> DB lookup ->
        # _bump_last_used_best_effort runs in its own connection.
        r = api_client.get("/v1/recommendations", headers=_bearer(clear))
        assert r.status_code == 200, r.text

        with _db_conn(migrated_db) as conn:
            after = conn.execute(
                "SELECT last_used_at FROM api_tokens WHERE token_id = %s", (token_id,)
            ).fetchone()
        assert after["last_used_at"] is not None  # bumped


# ===========================================================================
# 19. GET ?include_revoked toggles the visibility of a revoked row.
# ===========================================================================


class TestListIncludeRevoked:
    def test_revoked_row_visible_only_with_include_revoked(
        self, api_client, api_token_ids
    ):
        data = _api_mint(
            api_client, actor_kind="agent", scopes=["read"], register=api_token_ids
        )
        token_id = data["token_id"]

        deleted = api_client.delete(
            f"/v1/tokens/{token_id}", headers=_bearer(LEGACY_TOKEN)
        )
        assert deleted.status_code == 204, deleted.text

        # Default listing hides the revoked row.
        default = api_client.get("/v1/tokens", headers=_bearer(LEGACY_TOKEN))
        assert default.status_code == 200, default.text
        default_ids = {t["token_id"] for t in default.json()["tokens"]}
        assert token_id not in default_ids

        # include_revoked=true surfaces it, with revoked_at stamped.
        incl = api_client.get(
            "/v1/tokens?include_revoked=true", headers=_bearer(LEGACY_TOKEN)
        )
        assert incl.status_code == 200, incl.text
        by_id = {t["token_id"]: t for t in incl.json()["tokens"]}
        assert token_id in by_id
        assert by_id[token_id]["revoked_at"] is not None


# ===========================================================================
# 20. /metrics scrape — request counter with a real route template, and the
#     rate-limited counter climbing on a provoked 429 (deltas).
# ===========================================================================


class TestMetricsScrape:
    def test_http_requests_total_carries_a_real_route_template(self, api_client):
        # A couple of real requests so the counter has a labelled series.
        for _ in range(2):
            r = api_client.get("/v1/recommendations", headers=_bearer(LEGACY_TOKEN))
            assert r.status_code == 200, r.text

        scrape = api_client.get("/metrics", headers=_bearer(LEGACY_TOKEN))
        assert scrape.status_code == 200, scrape.text
        assert scrape.headers["content-type"].startswith("text/plain")
        body = scrape.text
        assert "ootils_http_requests_total" in body
        # A matched route TEMPLATE, never a raw path or "unmatched".
        assert 'route="/v1/recommendations"' in body

    def test_rate_limited_total_increments_on_a_429(self, api_client, api_token_ids):
        needle = 'ootils_rate_limited_total{actor_kind="agent"}'
        before = _metric_value(
            api_client.get("/metrics", headers=_bearer(LEGACY_TOKEN)).text, needle
        )

        # rate_per_min=1 → the 2nd call in the same window is a 429 (the
        # per-test autouse fixture cleared the rate counter, so the bucket
        # starts empty).
        data = _api_mint(
            api_client,
            actor_kind="agent",
            scopes=["read"],
            rate_per_min=1,
            register=api_token_ids,
        )
        clear = data["token"]

        first = api_client.get("/v1/recommendations", headers=_bearer(clear))
        assert first.status_code == 200, first.text
        blocked = api_client.get("/v1/recommendations", headers=_bearer(clear))
        assert blocked.status_code == 429, blocked.text

        after = _metric_value(
            api_client.get("/metrics", headers=_bearer(LEGACY_TOKEN)).text, needle
        )
        assert after - before >= 1


# ===========================================================================
# 21. DELETE idempotency + 404 on an unknown token_id.
# ===========================================================================


class TestDeleteIdempotent:
    def test_second_delete_is_204_and_unknown_is_404(self, api_client, api_token_ids):
        data = _api_mint(
            api_client, actor_kind="agent", scopes=["read"], register=api_token_ids
        )
        token_id = data["token_id"]

        first = api_client.delete(
            f"/v1/tokens/{token_id}", headers=_bearer(LEGACY_TOKEN)
        )
        assert first.status_code == 204, first.text

        # Idempotent: revoking an already-revoked token still returns 204.
        second = api_client.delete(
            f"/v1/tokens/{token_id}", headers=_bearer(LEGACY_TOKEN)
        )
        assert second.status_code == 204, second.text

        # Unknown token_id → 404 (existence SELECT misses).
        unknown = api_client.delete(
            f"/v1/tokens/{uuid4()}", headers=_bearer(LEGACY_TOKEN)
        )
        assert unknown.status_code == 404, unknown.text
