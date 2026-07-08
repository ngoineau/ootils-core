"""
tests/integration/test_prod_qw_integration.py — PROD-QW package, DB-backed
(real PostgreSQL, no mocks). Three capability areas the pure unit tests
(tests/test_l3_webhook.py, tests/test_router_audit.py) cannot reach:

  A. GET /v1/audit end to end — real authenticated requests write api_request_log
     via the app middleware (audit_client has NO get_db override, so
     _should_audit_request() is True and the middleware actually runs); the
     admin-scoped router then reads them back, with the actor_kind / path_prefix
     / status / from-to filters, the literal-% (starts_with, not LIKE) guard,
     newest-first ordering, and the admin-scope floor.

  B. The hardened pool session guards — a connection borrowed from the OotilsDB
     pool carries statement_timeout=900s (env-overridable, 0 = disabled), while a
     bare psycopg.connect() (the path scripts/watchers use) inherits the server
     default (0/unlimited) — proving the guard is POOL-only.

  C. Reschedule watcher × L3 webhook — a genuinely-new CANCEL (the first
     watcher-emitted L3) fires notify_l3_pending exactly once; a re-run on the
     unchanged plan (ON CONFLICT DO NOTHING → zero new rows) fires nothing; an
     L2-only plan fires no L3 webhook. notify_l3_pending is monkeypatched to a
     recording spy, so nothing leaves the process.

Determinism: every date anchors on the DB-side CURRENT_DATE, never Python now();
the api_request_log assertions filter on entities THIS module creates (a
uuid4-suffixed agent token, a distinct path) so the ever-growing log never makes
them flaky.
"""
from __future__ import annotations

import datetime as _dt
import os
import sys
from pathlib import Path

import pytest

from .conftest import requires_db

pytestmark = requires_db

BASELINE = "00000000-0000-0000-0000-000000000001"
LEGACY_TOKEN = "integration-test-token"


# ---------------------------------------------------------------------------
# Import seam: mrp_core + the watcher live under scripts/ (outside the package).
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ===========================================================================
# A. GET /v1/audit — real authenticated traffic → api_request_log → router
# ===========================================================================


@pytest.fixture(scope="module")
def audit_client(migrated_db):
    """TestClient WITHOUT a get_db override, so the api_request_log middleware
    (api/app.py:_log_api_request) actually runs — mirrors
    test_agent_floor_integration.py::audit_client. Binds to the real DB via
    DATABASE_URL exactly as the app resolves its pool in production."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = LEGACY_TOKEN

    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app

    app = create_app()
    with TestClient(app) as client:
        yield client


@pytest.fixture(autouse=True)
def _clear_token_cache():
    """The minted-token lookup + rate counter are in-process singletons shared
    by the app; clear both around every test so a seed/revoke never leaks a
    cached decision (and revocation is observable without a TTL sleep)."""
    import ootils_core.api.auth as auth

    auth._token_cache.clear()
    auth._rate_counter.clear()
    yield
    auth._token_cache.clear()
    auth._rate_counter.clear()


def _mint_token(dsn, *, actor_kind: str, scopes: list[str]) -> tuple[str, str]:
    """Insert one api_tokens row; return (cleartext, token_id). The cleartext
    exists only here — the DB stores hash_token(clear), like the auth path."""
    from uuid import uuid4

    from ootils_core.api.auth import hash_token, token_prefix

    clear = f"ootk_{actor_kind}_{uuid4().hex}"
    token_id = uuid4()
    with _db_conn(dsn) as conn:
        conn.execute(
            "INSERT INTO api_tokens (token_id, name, actor_kind, token_hash, "
            "token_prefix, scopes) VALUES (%s, %s, %s, %s, %s, %s)",
            (token_id, f"prodqw-{token_id}", actor_kind, hash_token(clear),
             token_prefix(clear), scopes),
        )
    return clear, str(token_id)


@pytest.fixture
def token_tracker(migrated_db):
    """Mint api_tokens rows and scrub them (and their audit rows) at teardown."""
    created: list[str] = []

    def _make(*, actor_kind: str, scopes: list[str]) -> str:
        clear, token_id = _mint_token(migrated_db, actor_kind=actor_kind, scopes=scopes)
        created.append(token_id)
        return clear

    yield _make

    with _db_conn(migrated_db) as conn:
        if created:
            conn.execute(
                "DELETE FROM api_request_log WHERE token_id = ANY(%s::uuid[])", (created,)
            )
            conn.execute(
                "DELETE FROM api_tokens WHERE token_id = ANY(%s::uuid[])", (created,)
            )


class TestAuditReadPath:
    def test_authenticated_requests_populate_the_paginated_audit_trail(
        self, audit_client
    ):
        """A handful of real authenticated calls write api_request_log rows the
        admin-scoped GET /v1/audit then pages back, newest-first."""
        for _ in range(3):
            r = audit_client.get("/v1/recommendations", headers=_bearer(LEGACY_TOKEN))
            assert r.status_code == 200, r.text

        resp = audit_client.get(
            "/v1/audit?path_prefix=/v1/recommendations&limit=50",
            headers=_bearer(LEGACY_TOKEN),
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["total"] >= 3
        assert len(body["entries"]) >= 3
        for e in body["entries"]:
            assert e["path"].startswith("/v1/recommendations")
            assert e["method"] == "GET"
            # Never a raw token / hash — only the non-secret correlation fields.
            assert "token" not in e and "token_hash" not in e
            assert "token_prefix" in e and "token_id" in e

    def test_newest_first_ordering(self, audit_client):
        for _ in range(3):
            audit_client.get("/v1/recommendations", headers=_bearer(LEGACY_TOKEN))
        body = audit_client.get(
            "/v1/audit?path_prefix=/v1/recommendations&limit=10",
            headers=_bearer(LEGACY_TOKEN),
        ).json()
        stamps = [e["created_at"] for e in body["entries"]]
        assert stamps == sorted(stamps, reverse=True), "ORDER BY created_at DESC"

    def test_pagination_limit_is_honoured(self, audit_client):
        for _ in range(3):
            audit_client.get("/v1/recommendations", headers=_bearer(LEGACY_TOKEN))
        page = audit_client.get(
            "/v1/audit?path_prefix=/v1/recommendations&limit=2",
            headers=_bearer(LEGACY_TOKEN),
        ).json()
        assert page["limit"] == 2
        assert len(page["entries"]) <= 2
        assert page["total"] >= 3  # total counts beyond the page

    def test_actor_kind_filter_is_applied(self, audit_client, token_tracker):
        # An agent token makes a call → an agent-kind audit row exists.
        agent_clear = token_tracker(actor_kind="agent", scopes=["read"])
        assert (
            audit_client.get("/v1/recommendations", headers=_bearer(agent_clear)).status_code
            == 200
        )
        # A legacy (human) call too.
        audit_client.get("/v1/recommendations", headers=_bearer(LEGACY_TOKEN))

        agents = audit_client.get(
            "/v1/audit?actor_kind=agent&limit=100", headers=_bearer(LEGACY_TOKEN)
        ).json()
        assert agents["total"] >= 1
        assert all(e["actor_kind"] == "agent" for e in agents["entries"])

        humans = audit_client.get(
            "/v1/audit?actor_kind=human&limit=100", headers=_bearer(LEGACY_TOKEN)
        ).json()
        assert all(e["actor_kind"] == "human" for e in humans["entries"])

    def test_status_code_filter_is_applied(self, audit_client):
        # An unauthenticated /v1/* call is still audited, with status 401.
        bad = audit_client.get("/v1/recommendations", headers=_bearer("wrong-token"))
        assert bad.status_code == 401
        rows = audit_client.get(
            "/v1/audit?status_code=401&limit=100", headers=_bearer(LEGACY_TOKEN)
        ).json()
        assert rows["total"] >= 1
        assert all(e["status_code"] == 401 for e in rows["entries"])

    def test_path_prefix_treats_percent_literally_not_as_wildcard(self, audit_client):
        """starts_with(), not LIKE: a '%' in path_prefix is a literal char, so a
        prefix no real path starts with returns ZERO rows (a LIKE wildcard would
        have matched everything)."""
        audit_client.get("/v1/recommendations", headers=_bearer(LEGACY_TOKEN))
        # '%25' is the URL-encoding of a literal '%'.
        resp = audit_client.get(
            "/v1/audit?path_prefix=/v1/recommendations%25", headers=_bearer(LEGACY_TOKEN)
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["total"] == 0

    def test_from_to_window_bounds(self, audit_client):
        audit_client.get("/v1/recommendations", headers=_bearer(LEGACY_TOKEN))
        # A far-past upper bound excludes everything.
        past = audit_client.get(
            "/v1/audit?to=2000-01-01T00:00:00Z&limit=10", headers=_bearer(LEGACY_TOKEN)
        ).json()
        assert past["total"] == 0
        # An epoch lower bound includes the recent rows.
        recent = audit_client.get(
            "/v1/audit?from=1970-01-01T00:00:00Z&limit=10", headers=_bearer(LEGACY_TOKEN)
        ).json()
        assert recent["total"] >= 1

    def test_admin_scope_required(self, audit_client, token_tracker):
        # No auth → 401.
        assert audit_client.get("/v1/audit").status_code == 401
        # A non-admin (read-only agent) token → 403 with the scope named.
        reader = token_tracker(actor_kind="agent", scopes=["read"])
        denied = audit_client.get("/v1/audit", headers=_bearer(reader))
        assert denied.status_code == 403, denied.text
        assert denied.json()["detail"] == "missing scope 'admin'"
        # Legacy admin token → 200.
        assert audit_client.get("/v1/audit", headers=_bearer(LEGACY_TOKEN)).status_code == 200


# ===========================================================================
# B. Pool session guards — statement_timeout is POOL-only, env-overridable.
# ===========================================================================


class TestPoolSessionGuards:
    def _current(self, conn, setting: str) -> str:
        return conn.execute(
            "SELECT current_setting(%s) AS v", (setting,)
        ).fetchone()["v"]

    def _skip_if_no_pool(self, db):
        if db._get_pool() is None:
            pytest.skip("psycopg_pool unavailable — no pool path to exercise")

    def test_pool_connection_carries_the_default_statement_timeout(self, migrated_db):
        from ootils_core.db.connection import OotilsDB

        db = OotilsDB(migrated_db)
        try:
            self._skip_if_no_pool(db)
            with db.conn() as conn:
                # 900_000 ms → Postgres renders it "15min"; idle guard 60_000 → "1min".
                assert self._current(conn, "statement_timeout") == "15min"
                assert (
                    self._current(conn, "idle_in_transaction_session_timeout") == "1min"
                )
        finally:
            db.close()

    def test_statement_timeout_is_env_overridable(self, migrated_db, monkeypatch):
        from ootils_core.db.connection import OotilsDB

        monkeypatch.setenv("OOTILS_DB_STATEMENT_TIMEOUT_MS", "300000")  # 5 min
        db = OotilsDB(migrated_db)
        try:
            self._skip_if_no_pool(db)
            with db.conn() as conn:
                assert self._current(conn, "statement_timeout") == "5min"
        finally:
            db.close()

    def test_statement_timeout_zero_disables_the_guard(self, migrated_db, monkeypatch):
        from ootils_core.db.connection import OotilsDB

        monkeypatch.setenv("OOTILS_DB_STATEMENT_TIMEOUT_MS", "0")  # Postgres: disabled
        db = OotilsDB(migrated_db)
        try:
            self._skip_if_no_pool(db)
            with db.conn() as conn:
                assert self._current(conn, "statement_timeout") == "0"
        finally:
            db.close()

    def test_direct_connect_does_not_inherit_the_guard(self, migrated_db):
        """A bare psycopg.connect() (scripts/watchers path) gets the SERVER
        default — 0/unlimited — never the pool's session guard."""
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
            value = conn.execute(
                "SELECT current_setting('statement_timeout') AS v"
            ).fetchone()["v"]
        assert value == "0", (
            "a direct connection must inherit the server default, not the pool guard"
        )


# ===========================================================================
# C. Reschedule watcher × L3 webhook — CANCEL fires once, idempotent, L2 silent.
# ===========================================================================

import agent_reschedule_watcher  # noqa: E402
import mrp_core as core  # noqa: E402
import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from ootils_core.notifications.l3_webhook import is_l3_or_higher  # noqa: E402

_WEEK = 7
_RESCHED_BASELINE = core.BASELINE


class _WebhookSpy:
    """Records every notify_l3_pending invocation. The reschedule watcher calls
    notify_l3_pending for EVERY genuinely-new insert and the L3 gate lives
    INSIDE notify_l3_pending (which we replace here) — so "an L3 webhook fired"
    is precisely the subset of calls whose decision_level is L3+. The spy
    returns the real gate result so the watcher's own `notified` counter/log
    stay honest."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def __call__(self, **kwargs) -> bool:
        self.calls.append(kwargs)
        return is_l3_or_higher(kwargs["decision_level"])

    @property
    def l3_fired(self) -> list[dict]:
        return [c for c in self.calls if is_l3_or_higher(c["decision_level"])]


def _run_watcher(dsn, scenario=None) -> int:
    argv = ["--dsn", dsn, "--allow-dev"]
    if scenario is not None:
        argv += ["--scenario", str(scenario)]
    return agent_reschedule_watcher.main(argv)


def _reset_graph(conn) -> None:
    conn.execute(
        "TRUNCATE nodes, edges, recommendations, agent_runs, "
        "item_planning_params, supplier_items, items, suppliers, locations, "
        "scenario_planning_overrides RESTART IDENTITY CASCADE"
    )
    conn.execute("TRUNCATE shortages RESTART IDENTITY CASCADE")


def _seed_common(conn) -> tuple:
    loc_id = conn.execute(
        "INSERT INTO locations (name, location_type, external_id) "
        "VALUES (%s, %s, %s) RETURNING location_id",
        ("PRODQW Plant", "plant", "LOC-PQW"),
    ).fetchone()["location_id"]
    sup_id = conn.execute(
        "INSERT INTO suppliers (external_id, name, reliability_score, status) "
        "VALUES (%s, %s, %s, %s) RETURNING supplier_id",
        ("SUP-PQW", "PRODQW Supplier", 0.95, "active"),
    ).fetchone()["supplier_id"]
    item_id = conn.execute(
        "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING item_id",
        ("ITM-PQW", "PRODQW Item", "component", 40.0, "EUR"),
    ).fetchone()["item_id"]
    conn.execute(
        "INSERT INTO item_planning_params "
        "(item_id, location_id, is_make, lead_time_sourcing_days, "
        " lead_time_manufacturing_days, lead_time_transit_days, safety_stock_qty, "
        " lot_size_rule, frozen_time_fence_days, slashed_time_fence_days, "
        " forecast_consumption_strategy) "
        "VALUES (%s,%s,FALSE,14,0,0,0,%s,0,1,%s)",
        (item_id, loc_id, "LOTFORLOT", "max_only"),
    )
    conn.execute(
        "INSERT INTO supplier_items "
        "(supplier_id, item_id, lead_time_days, unit_cost, currency, is_preferred) "
        "VALUES (%s,%s,14,4.0,%s,TRUE)",
        (sup_id, item_id, "EUR"),
    )
    return loc_id, item_id


def _node(conn, ntype, item_id, loc_id, today, days_out, qty, is_firm=False) -> None:
    conn.execute(
        "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
        " time_grain, time_ref, active, is_firm) "
        "VALUES (%s, %s, %s, %s, %s, 'exact_date', %s, TRUE, %s)",
        (ntype, _RESCHED_BASELINE, item_id, loc_id, qty,
         today + _dt.timedelta(days=days_out), is_firm),
    )


def _onhand(conn, item_id, loc_id, today, qty) -> None:
    _node(conn, "OnHandSupply", item_id, loc_id, today, 0, qty)


def _demand(conn, item_id, loc_id, today, weeks_out, qty) -> None:
    _node(conn, "CustomerOrderDemand", item_id, loc_id, today, weeks_out * _WEEK, qty)


def _receipt(conn, item_id, loc_id, today, weeks_out, qty) -> None:
    _node(conn, "PurchaseOrderSupply", item_id, loc_id, today,
          weeks_out * _WEEK, qty, is_firm=True)


class TestRescheduleWebhook:
    def test_new_cancel_fires_the_l3_webhook_exactly_once(self, migrated_db, monkeypatch):
        """A surplus firm receipt (no demand) → CANCEL at L3 → notify_l3_pending
        fires exactly once for a genuinely-new L3 row."""
        dsn = migrated_db
        with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
            today = conn.execute("SELECT CURRENT_DATE").fetchone()["current_date"]
            _reset_graph(conn)
            loc_id, item_id = _seed_common(conn)
            _onhand(conn, item_id, loc_id, today, 0)
            _receipt(conn, item_id, loc_id, today, 10, 50)  # no demand → CANCEL

        spy = _WebhookSpy()
        monkeypatch.setattr(agent_reschedule_watcher, "notify_l3_pending", spy)

        assert _run_watcher(dsn) == 0
        assert len(spy.l3_fired) == 1, "exactly one L3 webhook for the new CANCEL"
        fired = spy.l3_fired[0]
        assert fired["action"] == "CANCEL"
        assert fired["decision_level"] == "L3"

    def test_rerun_on_unchanged_plan_fires_nothing(self, migrated_db, monkeypatch):
        """ON CONFLICT DO NOTHING → the re-run inserts zero new rows → the
        webhook (fired only for genuinely-new inserts) fires nothing."""
        dsn = migrated_db
        with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
            today = conn.execute("SELECT CURRENT_DATE").fetchone()["current_date"]
            _reset_graph(conn)
            loc_id, item_id = _seed_common(conn)
            _onhand(conn, item_id, loc_id, today, 0)
            _receipt(conn, item_id, loc_id, today, 10, 50)

        spy = _WebhookSpy()
        monkeypatch.setattr(agent_reschedule_watcher, "notify_l3_pending", spy)

        assert _run_watcher(dsn) == 0
        assert len(spy.l3_fired) == 1  # run 1 fired the CANCEL once
        first_run_calls = len(spy.calls)

        assert _run_watcher(dsn) == 0  # identical plan
        assert len(spy.calls) == first_run_calls, "re-run must not invoke the webhook"
        assert len(spy.l3_fired) == 1  # still just the one from run 1

    def test_l2_only_plan_fires_no_l3_webhook(self, migrated_db, monkeypatch):
        """A mis-dated receipt → RESCHEDULE_OUT (L2). The watcher calls
        notify_l3_pending for the L2 insert, but the L3 gate (inside
        notify_l3_pending) means NO L3 webhook fires — assert on the L3 subset."""
        dsn = migrated_db
        with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
            today = conn.execute("SELECT CURRENT_DATE").fetchone()["current_date"]
            _reset_graph(conn)
            loc_id, item_id = _seed_common(conn)
            _onhand(conn, item_id, loc_id, today, 0)
            _demand(conn, item_id, loc_id, today, 20, 100)  # need at week 20
            _receipt(conn, item_id, loc_id, today, 4, 100)   # far too early → RESCHEDULE_OUT

        spy = _WebhookSpy()
        monkeypatch.setattr(agent_reschedule_watcher, "notify_l3_pending", spy)

        assert _run_watcher(dsn) == 0
        assert spy.l3_fired == [], "an L2 plan must fire no L3 webhook"
        # The watcher DID hand the L2 insert to notify_l3_pending (gate is inside it).
        assert spy.calls, "sanity: the run emitted at least one recommendation"
        assert all(c["decision_level"] == "L2" for c in spy.calls)
