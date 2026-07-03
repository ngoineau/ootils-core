"""
Integration tests for GET /v1/scenarios/{id}/diff and
POST /v1/scenarios/{id}/promote against a real PostgreSQL database
(no mocks) — chantier #341b.

Covers:
  - promote without conflict: 200, baseline patched, scenario archived,
    scenario_promotions audit row (migration 052), scenario_merge event
  - promote with a diverged baseline: 409 + typed conflict list, and
    NOTHING written (no patch, no archive, no audit row, no event)
  - Decision Ladder guard: promote is L3+ → 403 for actor_kind='agent'
  - promote guards: baseline (400), unknown scenario (404), non-active (409)
  - sibling invalidation count reported on success
  - diff endpoint: exposes scenario_diffs entries; 409 when no calc_run

Rows are inserted directly (items, nodes, scenarios, scenario_overrides,
calc_runs) — the baseline scenario is seeded by migration 002.
"""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}
BASELINE = "00000000-0000-0000-0000-000000000001"
DAY = date(2026, 8, 1)


# ---------------------------------------------------------------------------
# Fixtures (mirror tests/integration/test_recommendations_api_integration.py)
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


def _insert_node(
    conn,
    *,
    scenario_id: str,
    item_id: str,
    quantity="100",
    closing_stock=None,
) -> str:
    """Insert an active PurchaseOrderSupply node; returns node_id.

    Business key for cross-scenario matching:
    (node_type, item_id, location_id=NULL, time_span_start, bucket_sequence).
    """
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id,
            quantity, time_grain, time_ref,
            time_span_start, bucket_sequence,
            closing_stock, active
        ) VALUES (%s, 'PurchaseOrderSupply', %s, %s, %s, 'day', %s, %s, 0, %s, TRUE)
        """,
        (node_id, scenario_id, item_id, quantity, DAY, DAY, closing_stock),
    )
    return str(node_id)


def _insert_calc_run(conn, scenario_id: str) -> str:
    calc_run_id = uuid4()
    conn.execute(
        "INSERT INTO calc_runs (calc_run_id, scenario_id, status, started_at, completed_at) "
        "VALUES (%s, %s, 'completed', now(), now())",
        (calc_run_id, scenario_id),
    )
    return str(calc_run_id)


class _Fixture:
    """A baseline node + a forked scenario carrying one 'quantity' override."""

    def __init__(self, dsn):
        self.dsn = dsn
        self.item_id = str(uuid4())
        self.scenario_id = str(uuid4())
        with _db_conn(dsn) as conn:
            conn.execute(
                "INSERT INTO items (item_id, name) VALUES (%s, %s)",
                (self.item_id, f"ITEM-{self.item_id[:8]}"),
            )
            conn.execute(
                "INSERT INTO scenarios (scenario_id, name, parent_scenario_id, is_baseline, status) "
                "VALUES (%s, %s, %s, FALSE, 'active')",
                (self.scenario_id, f"fork-{self.scenario_id[:8]}", BASELINE),
            )
            self.baseline_node_id = _insert_node(
                conn, scenario_id=BASELINE, item_id=self.item_id, quantity="100"
            )
            # Deep-copy twin in the scenario, then apply_override semantics:
            # old_value = value read from the (still pristine) copy = the
            # baseline value at fork time; node carries new_value afterwards.
            self.scenario_node_id = _insert_node(
                conn, scenario_id=self.scenario_id, item_id=self.item_id, quantity="150"
            )
            conn.execute(
                "INSERT INTO scenario_overrides "
                "(scenario_id, node_id, field_name, old_value, new_value, applied_by) "
                "VALUES (%s, %s, 'quantity', '100', '150', 'ngoineau')",
                (self.scenario_id, self.scenario_node_id),
            )

    def add_sibling(self) -> str:
        sibling_id = str(uuid4())
        with _db_conn(self.dsn) as conn:
            conn.execute(
                "INSERT INTO scenarios (scenario_id, name, parent_scenario_id, is_baseline, status) "
                "VALUES (%s, %s, %s, FALSE, 'active')",
                (sibling_id, f"sibling-{sibling_id[:8]}", BASELINE),
            )
        return sibling_id

    def diverge_baseline(self, new_quantity: str) -> None:
        with _db_conn(self.dsn) as conn:
            conn.execute(
                "UPDATE nodes SET quantity = %s WHERE node_id = %s",
                (new_quantity, self.baseline_node_id),
            )

    def baseline_quantity(self) -> Decimal:
        with _db_conn(self.dsn) as conn:
            row = conn.execute(
                "SELECT quantity FROM nodes WHERE node_id = %s",
                (self.baseline_node_id,),
            ).fetchone()
        return row["quantity"]

    def scenario_status(self) -> str:
        with _db_conn(self.dsn) as conn:
            row = conn.execute(
                "SELECT status FROM scenarios WHERE scenario_id = %s",
                (self.scenario_id,),
            ).fetchone()
        return row["status"]

    def promotion_rows(self) -> list[dict]:
        with _db_conn(self.dsn) as conn:
            return conn.execute(
                "SELECT * FROM scenario_promotions WHERE scenario_id = %s",
                (self.scenario_id,),
            ).fetchall()

    def merge_events(self) -> list[dict]:
        with _db_conn(self.dsn) as conn:
            return conn.execute(
                "SELECT * FROM events WHERE event_type = 'scenario_merge' AND old_text = %s",
                (self.scenario_id,),
            ).fetchall()

    def cleanup(self, extra_scenarios: list[str] | None = None) -> None:
        scenario_ids = [self.scenario_id] + (extra_scenarios or [])
        with _db_conn(self.dsn) as conn:
            conn.execute(
                "DELETE FROM scenario_diffs WHERE scenario_id = ANY(%s::uuid[])",
                (scenario_ids,),
            )
            conn.execute(
                "DELETE FROM scenario_promotions WHERE scenario_id = ANY(%s::uuid[])",
                (scenario_ids,),
            )
            conn.execute(
                "DELETE FROM scenario_overrides WHERE scenario_id = ANY(%s::uuid[])",
                (scenario_ids,),
            )
            conn.execute(
                "DELETE FROM events WHERE event_type = 'scenario_merge' AND old_text = ANY(%s)",
                (scenario_ids,),
            )
            # Real apply_override calls (test_re_override_...) emit
            # policy_changed events whose trigger_node_id references our
            # nodes — purge them before the nodes or the FK blocks teardown.
            conn.execute(
                "DELETE FROM events WHERE trigger_node_id IN "
                "(SELECT node_id FROM nodes WHERE item_id = %s)",
                (self.item_id,),
            )
            conn.execute(
                "DELETE FROM calc_runs WHERE scenario_id = ANY(%s::uuid[]) OR scenario_id = %s",
                (scenario_ids, BASELINE),
            )
            conn.execute("DELETE FROM nodes WHERE item_id = %s", (self.item_id,))
            conn.execute(
                "DELETE FROM scenarios WHERE scenario_id = ANY(%s::uuid[])",
                (scenario_ids,),
            )
            conn.execute("DELETE FROM items WHERE item_id = %s", (self.item_id,))


@pytest.fixture
def fx(migrated_db):
    fixture = _Fixture(migrated_db)
    extra: list[str] = []
    fixture._extra = extra  # collected sibling ids for cleanup
    yield fixture
    fixture.cleanup(extra_scenarios=extra)


# ---------------------------------------------------------------------------
# POST /v1/scenarios/{id}/promote — success path
# ---------------------------------------------------------------------------


class TestPromoteSuccess:
    def test_promote_requires_auth(self, api_client, fx):
        resp = api_client.post(
            f"/v1/scenarios/{fx.scenario_id}/promote",
            json={"actor": "ngoineau", "actor_kind": "human"},
        )
        assert resp.status_code in (401, 403)

    def test_re_override_does_not_false_conflict_at_promote(
        self, api_client, auth, fx, migrated_db
    ):
        """Regression guard for the old_value upsert (#341 part 2 review):
        a SECOND apply_override on the same (scenario, node, field) must
        preserve the ORIGINAL baseline capture in old_value. Before the fix,
        ON CONFLICT overwrote old_value with the scenario's current value
        (= the first override's new_value), so any re-overridden field was
        a guaranteed false 409 at promote."""
        from uuid import UUID as _UUID

        from ootils_core.engine.scenario.manager import ScenarioManager

        manager = ScenarioManager()
        with _db_conn(migrated_db) as conn:
            # Two real apply_override calls — the path that owns the upsert.
            for new_value in ("160", "175"):
                manager.apply_override(
                    scenario_id=_UUID(fx.scenario_id),
                    node_id=_UUID(fx.scenario_node_id),
                    field_name="quantity",
                    new_value=new_value,
                    applied_by="test-double-override",
                    db=conn,
                )
            row = conn.execute(
                "SELECT old_value, new_value FROM scenario_overrides "
                "WHERE scenario_id = %s AND node_id = %s AND field_name = 'quantity'",
                (fx.scenario_id, fx.scenario_node_id),
            ).fetchone()
            # old_value must still be the FORK-TIME baseline capture (150 was
            # the scenario copy's pristine value in this fixture — see
            # _Fixture: the direct-INSERT override row uses 100; apply_override
            # reads the node's current value on first call).
            assert row["new_value"] == "175"

        resp = api_client.post(
            f"/v1/scenarios/{fx.scenario_id}/promote",
            json={"actor": "ngoineau", "actor_kind": "human", "reason": "re-override"},
            headers=auth,
        )
        assert resp.status_code == 200, (
            f"re-overridden field must not false-conflict at promote: {resp.text}"
        )

    def test_promote_patches_baseline_audits_and_emits_event(
        self, api_client, auth, fx
    ):
        resp = api_client.post(
            f"/v1/scenarios/{fx.scenario_id}/promote",
            json={"actor": "ngoineau", "actor_kind": "human", "reason": "Q3 plan"},
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["scenario_id"] == fx.scenario_id
        assert data["promoted_by"] == "ngoineau"
        assert data["override_count"] == 1
        assert data["patched_nodes"] == 1
        assert data["conflict_checked"] is True

        # Baseline node carries the promoted value
        assert fx.baseline_quantity() == Decimal("150")
        # Promoted scenario archived
        assert fx.scenario_status() == "archived"

        # Audit row (migration 052)
        promos = fx.promotion_rows()
        assert len(promos) == 1
        assert promos[0]["promotion_id"] == UUID(data["promotion_id"])
        assert promos[0]["promoted_by"] == "ngoineau"
        assert promos[0]["reason"] == "Q3 plan"
        assert promos[0]["override_count"] == 1
        assert promos[0]["conflict_checked"] is True

        # scenario_merge event, attributed to the actor
        events = fx.merge_events()
        assert len(events) == 1
        assert str(events[0]["event_id"]) == data["event_id"]
        assert events[0]["user_ref"] == "ngoineau"
        assert events[0]["new_text"] == "promoted"

    def test_promote_reports_sibling_invalidation(self, api_client, auth, fx):
        sibling_id = fx.add_sibling()
        fx._extra.append(sibling_id)

        resp = api_client.post(
            f"/v1/scenarios/{fx.scenario_id}/promote",
            json={"actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        # >= 1, not == 1: the sibling query counts EVERY active child of the
        # same parent (baseline), so scenarios leaked by other modules of the
        # same session would inflate the count — the contract here is "OUR
        # sibling was counted", not "we are alone in the database".
        assert resp.json()["siblings_invalidated"] >= 1
        promos = fx.promotion_rows()
        assert promos[0]["siblings_invalidated"] >= 1
        assert promos[0]["siblings_invalidated"] == resp.json()["siblings_invalidated"]


# ---------------------------------------------------------------------------
# POST /v1/scenarios/{id}/promote — conflict path (nothing written)
# ---------------------------------------------------------------------------


class TestPromoteConflict:
    def test_diverged_baseline_is_409_with_typed_conflicts(
        self, api_client, auth, fx
    ):
        fx.diverge_baseline("999")  # baseline moved since the override captured '100'

        resp = api_client.post(
            f"/v1/scenarios/{fx.scenario_id}/promote",
            json={"actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 409, resp.text
        detail = resp.json()["detail"]
        assert "diverged" in detail["message"]
        conflicts = detail["conflicts"]
        assert len(conflicts) == 1
        assert conflicts[0]["node_id"] == fx.baseline_node_id
        assert conflicts[0]["field_name"] == "quantity"
        assert conflicts[0]["expected"] == "100"
        assert conflicts[0]["actual"] == "999"

        # NOTHING was written
        assert fx.baseline_quantity() == Decimal("999")  # untouched
        assert fx.scenario_status() == "active"          # not archived
        assert fx.promotion_rows() == []                 # no audit row
        assert fx.merge_events() == []                   # no event


# ---------------------------------------------------------------------------
# POST /v1/scenarios/{id}/promote — guards
# ---------------------------------------------------------------------------


class TestPromoteGuards:
    def test_agent_cannot_promote(self, api_client, auth, fx):
        """Decision Ladder guard: promote is L3+ → human-only (403 for agents)."""
        resp = api_client.post(
            f"/v1/scenarios/{fx.scenario_id}/promote",
            json={"actor": "shortage_watcher", "actor_kind": "agent"},
            headers=auth,
        )
        assert resp.status_code == 403, resp.text
        assert "human" in resp.json()["detail"].lower()
        # Nothing written
        assert fx.scenario_status() == "active"
        assert fx.promotion_rows() == []

    def test_promote_baseline_is_400(self, api_client, auth):
        resp = api_client.post(
            f"/v1/scenarios/{BASELINE}/promote",
            json={"actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 400

    def test_promote_unknown_scenario_is_404(self, api_client, auth):
        resp = api_client.post(
            f"/v1/scenarios/{uuid4()}/promote",
            json={"actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 404

    def test_promote_archived_scenario_is_409(self, api_client, auth, fx):
        with _db_conn(fx.dsn) as conn:
            conn.execute(
                "UPDATE scenarios SET status = 'archived' WHERE scenario_id = %s",
                (fx.scenario_id,),
            )
        resp = api_client.post(
            f"/v1/scenarios/{fx.scenario_id}/promote",
            json={"actor": "ngoineau", "actor_kind": "human"},
            headers=auth,
        )
        assert resp.status_code == 409
        assert "active" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GET /v1/scenarios/{id}/diff
# ---------------------------------------------------------------------------


class TestDiffEndpoint:
    def test_diff_exposes_scenario_diff_entries(self, api_client, auth, fx, migrated_db):
        # Make a computed field differ (diff compares _DIFF_FIELDS, not quantity)
        with _db_conn(migrated_db) as conn:
            conn.execute(
                "UPDATE nodes SET closing_stock = 10 WHERE node_id = %s",
                (fx.baseline_node_id,),
            )
            conn.execute(
                "UPDATE nodes SET closing_stock = 42 WHERE node_id = %s",
                (fx.scenario_node_id,),
            )
            _insert_calc_run(conn, BASELINE)
            _insert_calc_run(conn, fx.scenario_id)

        resp = api_client.get(
            f"/v1/scenarios/{fx.scenario_id}/diff", headers=auth
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["scenario_id"] == fx.scenario_id
        assert data["baseline_id"] == BASELINE
        assert data["total"] == len(data["diffs"]) >= 1

        closing = [d for d in data["diffs"] if d["field_name"] == "closing_stock"]
        assert len(closing) == 1
        assert Decimal(closing[0]["baseline_value"]) == Decimal("10")
        assert Decimal(closing[0]["scenario_value"]) == Decimal("42")

        # Entries are persisted in scenario_diffs (upsert per calc_run pair)
        with _db_conn(migrated_db) as conn:
            rows = conn.execute(
                "SELECT field_name FROM scenario_diffs WHERE scenario_id = %s",
                (fx.scenario_id,),
            ).fetchall()
        assert "closing_stock" in {r["field_name"] for r in rows}

    def test_diff_without_calc_run_is_409(self, api_client, auth, fx):
        resp = api_client.get(
            f"/v1/scenarios/{fx.scenario_id}/diff", headers=auth
        )
        assert resp.status_code == 409, resp.text
        assert "calc_run" in resp.json()["detail"]

    def test_diff_unknown_scenario_is_404(self, api_client, auth):
        resp = api_client.get(f"/v1/scenarios/{uuid4()}/diff", headers=auth)
        assert resp.status_code == 404
