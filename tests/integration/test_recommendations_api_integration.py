"""
Integration tests for the /v1/recommendations router against a real
PostgreSQL database (no mocks) — chantier #341a.

Covers:
  - list pagination + filters (status/action/agent_name) + scenario scoping
  - detail with evidence + transition history
  - state-machine transitions: valid (200), invalid (409, allowed list in
    the message), unknown recommendation (404)
  - Decision Ladder guard: APPROVED/APPLIED require actor_kind='human' (403)
  - Streamable principle: each transition emits a 'recommendation_transition'
    event row (migration 051)
  - CLI/API share one machine: transition rows written by the API satisfy
    the migration 040 FK + actor_kind CHECK

Rows are inserted directly (agent_runs + recommendations) — no seed script
needed; the baseline scenario is seeded by migration 002.
"""
from __future__ import annotations

import os
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}
BASELINE = "00000000-0000-0000-0000-000000000001"


# ---------------------------------------------------------------------------
# Fixtures (mirror tests/integration/test_forecasting_api_integration.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from fastapi.testclient import TestClient

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
def auth():
    return AUTH_HEADERS


# ---------------------------------------------------------------------------
# Helpers — direct DB access for setup/teardown
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _insert_recommendation(
    dsn,
    *,
    scenario_id: str = BASELINE,
    status: str = "DRAFT",
    action: str = "EXPEDITE",
    agent_name: str = "shortage_watcher",
    evidence: str | None = None,
) -> tuple[str, str]:
    """Insert an agent_run + one recommendation; return (reco_id, run_id)."""
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
                action, status, confidence, evidence
            ) VALUES (
                %s, %s, %s, %s,
                %s, 'PUMP-01', '2026-08-15',
                100, 120, 4800, 'EUR',
                14, 30, 16,
                %s, %s, 'HIGH', %s
            )
            """,
            (reco_id, agent_name, run_id, scenario_id, uuid4(), action, status, evidence),
        )
    return str(reco_id), str(run_id)


def _cleanup(dsn, reco_ids: list[str], run_ids: list[str]):
    with _db_conn(dsn) as conn:
        if reco_ids:
            conn.execute(
                "DELETE FROM recommendation_transitions WHERE recommendation_id = ANY(%s::uuid[])",
                (reco_ids,),
            )
            conn.execute(
                "DELETE FROM recommendations WHERE recommendation_id = ANY(%s::uuid[])",
                (reco_ids,),
            )
        if run_ids:
            conn.execute(
                "DELETE FROM agent_runs WHERE agent_run_id = ANY(%s::uuid[])",
                (run_ids,),
            )
        conn.execute("DELETE FROM events WHERE event_type = 'recommendation_transition'")


@pytest.fixture
def tracker(migrated_db):
    """Collects created ids and cleans them up after each test."""
    created = {"recos": [], "runs": []}

    def _make(**kwargs) -> str:
        reco_id, run_id = _insert_recommendation(migrated_db, **kwargs)
        created["recos"].append(reco_id)
        created["runs"].append(run_id)
        return reco_id

    yield _make
    _cleanup(migrated_db, created["recos"], created["runs"])


# ---------------------------------------------------------------------------
# GET /v1/recommendations — list, pagination, filters, scenario scoping
# ---------------------------------------------------------------------------


class TestListRecommendations:
    def test_list_requires_auth(self, api_client):
        resp = api_client.get("/v1/recommendations")
        assert resp.status_code in (401, 403)

    def test_list_defaults_to_baseline_scenario(self, api_client, auth, tracker):
        reco_id = tracker()
        other_scenario_reco = tracker(scenario_id=str(uuid4()))

        resp = api_client.get("/v1/recommendations", headers=auth)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        ids = [r["recommendation_id"] for r in data["recommendations"]]
        assert reco_id in ids
        assert other_scenario_reco not in ids  # scenario-scoped by default
        for r in data["recommendations"]:
            assert r["scenario_id"] == BASELINE

    def test_list_filters_by_scenario_id(self, api_client, auth, tracker):
        fork_id = str(uuid4())
        fork_reco = tracker(scenario_id=fork_id)
        baseline_reco = tracker()

        resp = api_client.get(
            f"/v1/recommendations?scenario_id={fork_id}", headers=auth
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        ids = [r["recommendation_id"] for r in data["recommendations"]]
        assert fork_reco in ids
        assert baseline_reco not in ids
        assert data["total"] == 1

    def test_list_pagination(self, api_client, auth, tracker):
        scenario = str(uuid4())  # isolated scenario so totals are deterministic
        for _ in range(3):
            tracker(scenario_id=scenario)

        page1 = api_client.get(
            f"/v1/recommendations?scenario_id={scenario}&limit=2&offset=0",
            headers=auth,
        ).json()
        page2 = api_client.get(
            f"/v1/recommendations?scenario_id={scenario}&limit=2&offset=2",
            headers=auth,
        ).json()
        assert page1["total"] == 3 and page2["total"] == 3
        assert len(page1["recommendations"]) == 2
        assert len(page2["recommendations"]) == 1
        all_ids = {
            r["recommendation_id"]
            for r in page1["recommendations"] + page2["recommendations"]
        }
        assert len(all_ids) == 3  # no overlap between pages

    def test_list_filters_status_action_agent(self, api_client, auth, tracker):
        scenario = str(uuid4())
        draft = tracker(scenario_id=scenario, status="DRAFT", action="EXPEDITE")
        approved = tracker(
            scenario_id=scenario, status="APPROVED", action="ORDER_NOW",
            agent_name="material_watcher",
        )

        by_status = api_client.get(
            f"/v1/recommendations?scenario_id={scenario}&status=APPROVED",
            headers=auth,
        ).json()
        assert [r["recommendation_id"] for r in by_status["recommendations"]] == [approved]

        by_action = api_client.get(
            f"/v1/recommendations?scenario_id={scenario}&action=EXPEDITE",
            headers=auth,
        ).json()
        assert [r["recommendation_id"] for r in by_action["recommendations"]] == [draft]

        by_agent = api_client.get(
            f"/v1/recommendations?scenario_id={scenario}&agent_name=material_watcher",
            headers=auth,
        ).json()
        assert [r["recommendation_id"] for r in by_agent["recommendations"]] == [approved]

    def test_list_rejects_unknown_status_and_action(self, api_client, auth):
        assert (
            api_client.get("/v1/recommendations?status=BOGUS", headers=auth).status_code
            == 422
        )
        assert (
            api_client.get("/v1/recommendations?action=BOGUS", headers=auth).status_code
            == 422
        )


# ---------------------------------------------------------------------------
# GET /v1/recommendations/{id} — detail
# ---------------------------------------------------------------------------


class TestGetRecommendation:
    def test_detail_not_found(self, api_client, auth):
        resp = api_client.get(f"/v1/recommendations/{uuid4()}", headers=auth)
        assert resp.status_code == 404

    def test_detail_includes_evidence_and_history(self, api_client, auth, tracker):
        reco_id = tracker(evidence='{"deficit": 100, "reason": "supplier late"}')

        # Produce one transition so history is non-empty
        t = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "REVIEWED", "actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert t.status_code == 200, t.text

        resp = api_client.get(f"/v1/recommendations/{reco_id}", headers=auth)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["recommendation_id"] == reco_id
        assert data["evidence"] == {"deficit": 100, "reason": "supplier late"}
        assert len(data["transitions"]) == 1
        trans = data["transitions"][0]
        assert trans["from_status"] == "DRAFT"
        assert trans["to_status"] == "REVIEWED"
        assert trans["actor"] == "ngoineau"
        assert trans["actor_kind"] == "human"


# ---------------------------------------------------------------------------
# POST /v1/recommendations/{id}/transition — state machine + guards + event
# ---------------------------------------------------------------------------


class TestTransitionEndpoint:
    def test_valid_transition_updates_status_and_audits(
        self, api_client, auth, tracker, migrated_db
    ):
        reco_id = tracker()
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={
                "to_status": "REVIEWED",
                "actor": "shortage_watcher",
                "actor_kind": "agent",
                "reason": "auto-triage",
            },
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["from_status"] == "DRAFT"
        assert data["to_status"] == "REVIEWED"
        assert UUID(data["transition_id"])
        assert UUID(data["event_id"])

        with _db_conn(migrated_db) as conn:
            row = conn.execute(
                "SELECT status FROM recommendations WHERE recommendation_id = %s",
                (reco_id,),
            ).fetchone()
            assert row["status"] == "REVIEWED"
            audit = conn.execute(
                "SELECT from_status, to_status, actor, actor_kind, reason "
                "FROM recommendation_transitions WHERE recommendation_id = %s",
                (reco_id,),
            ).fetchall()
            assert len(audit) == 1
            assert audit[0]["actor_kind"] == "agent"
            assert audit[0]["reason"] == "auto-triage"

    def test_invalid_transition_is_409_with_allowed_list(self, api_client, auth, tracker):
        reco_id = tracker()  # DRAFT
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPLIED", "actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 409, resp.text
        detail = resp.json()["detail"]
        # Hand-authored message lists what IS allowed from DRAFT
        assert "DRAFT" in detail and "APPLIED" in detail
        for allowed in ("APPROVED", "REJECTED", "REVIEWED"):
            assert allowed in detail

    def test_terminal_status_is_409(self, api_client, auth, tracker):
        reco_id = tracker(status="REJECTED")
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "APPROVED", "actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 409
        assert "terminal" in resp.json()["detail"]

    def test_transition_unknown_recommendation_is_404(self, api_client, auth):
        resp = api_client.post(
            f"/v1/recommendations/{uuid4()}/transition",
            json={"to_status": "REVIEWED", "actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 404

    @pytest.mark.parametrize("target", ["APPROVED", "APPLIED"])
    def test_agent_cannot_approve_or_apply(self, api_client, auth, tracker, target):
        """Decision Ladder guard: L3/L4 targets are human-only (403 for agents)."""
        status = "APPROVED" if target == "APPLIED" else "DRAFT"
        reco_id = tracker(status=status)
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": target, "actor": "shortage_watcher", "actor_kind": "agent"},
            headers=auth,
        )
        assert resp.status_code == 403, resp.text
        assert "human" in resp.json()["detail"].lower()

    def test_human_can_approve(self, api_client, auth, tracker, migrated_db):
        reco_id = tracker()
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={
                "to_status": "APPROVED",
                "actor": "ngoineau",
                "actor_kind": "human",
                "reason": "Q3 ramp",
            },
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        with _db_conn(migrated_db) as conn:
            row = conn.execute(
                "SELECT status FROM recommendations WHERE recommendation_id = %s",
                (reco_id,),
            ).fetchone()
            assert row["status"] == "APPROVED"

    def test_transition_emits_event(self, api_client, auth, tracker, migrated_db):
        """Streamable principle: one 'recommendation_transition' event per transition."""
        reco_id = tracker()
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "REVIEWED", "actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        event_id = resp.json()["event_id"]

        with _db_conn(migrated_db) as conn:
            ev = conn.execute(
                "SELECT event_type, scenario_id, field_changed, old_text, new_text, "
                "source, user_ref FROM events WHERE event_id = %s",
                (event_id,),
            ).fetchone()
        assert ev is not None
        assert ev["event_type"] == "recommendation_transition"
        assert str(ev["scenario_id"]) == BASELINE
        assert ev["field_changed"] == "status"
        assert ev["old_text"] == "DRAFT"
        assert ev["new_text"] == "REVIEWED"
        assert ev["source"] == "api"
        assert ev["user_ref"] == "ngoineau"

        # Event is also visible on the read path agents use
        listed = api_client.get(
            "/v1/events?event_type=recommendation_transition", headers=auth
        )
        assert listed.status_code == 200
        assert event_id in [e["event_id"] for e in listed.json()["events"]]

    def test_full_lifecycle_history(self, api_client, auth, tracker):
        """DRAFT → REVIEWED → APPROVED → APPLIED, then terminal (409)."""
        reco_id = tracker()
        steps = [
            ("REVIEWED", "agent", "shortage_watcher"),
            ("APPROVED", "human", "ngoineau"),
            ("APPLIED", "human", "ngoineau"),
        ]
        for to_status, kind, actor in steps:
            resp = api_client.post(
                f"/v1/recommendations/{reco_id}/transition",
                json={"to_status": to_status, "actor": actor, "actor_kind": kind},
                headers=auth,
            )
            assert resp.status_code == 200, resp.text

        # Terminal now
        resp = api_client.post(
            f"/v1/recommendations/{reco_id}/transition",
            json={"to_status": "REJECTED", "actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 409

        detail = api_client.get(f"/v1/recommendations/{reco_id}", headers=auth).json()
        assert detail["status"] == "APPLIED"
        assert [t["to_status"] for t in detail["transitions"]] == [
            "REVIEWED",
            "APPROVED",
            "APPLIED",
        ]
