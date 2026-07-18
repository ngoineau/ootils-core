"""
tests/integration/test_daily_report_integration.py — the daily-report read
surface (ADR-042 PR-4c) against a real PostgreSQL — no mocks (CLAUDE.md).
The pure renderer + the CLI's dry-run/apply emission split live in
tests/test_daily_report.py.

Two halves:

  1. ``GET /v1/daily-runs`` (api/routers/daily_runs.py) through the real app
     (api_client pattern of test_daily_orchestrator_integration.py):
     200 with every persisted ``daily_runs`` row + the governed decision read
     back from its ``daily_run_completed`` event after a REAL seeded
     ``apply_daily_run`` (never recomputed — the event IS the durable
     record); an omitted ``date`` defaults to today (UTC); a same-day
     re-evaluation appends and reads back newest-first per feed (migration
     078's append-only "current verdict" rule); the
     ``OOTILS_DAILY_RUN_REPORT_ENABLED`` kill switch (default ON, falsy →
     503, checked AFTER auth so an unauthenticated caller can never probe
     the switch state); scope enforcement (no/garbage token → 401, a token
     without ``read`` → 403, a bare ``read`` token → 200).

  2. ``build_shortages_summary`` (engine/reporting/daily_report.py) on a
     minimal canonical ``shortages`` seed: honest ``[]`` when the baseline
     has no completed calc_run (never an error), and — with rows — the
     latest-completed-calc-run scoping (an older run's actives are
     invisible), ``status='active'`` filter, $-severity DESC ordering with
     the (shortage_date ASC, shortage_id ASC) tie-break, the ``limit``, and
     the external_id → name → "(article inconnu)"/"(site inconnu)" fallback
     chain. Seeded INSIDE the ``conn`` fixture's transaction and rolled
     back — SELECT-only by contract, zero committed residue.

Isolation (pattern of test_daily_orchestrator_integration.py): referential/
governance seeds under a unique PREFIX, contracts registered through the
REAL loader path (the 3 seed config/feed-contracts/*.yaml, volume guards
adapted to tiny files, delta silenced) and neutralized by DEACTIVATION in a
module finalizer — never a DELETE cascade. daily_runs/events stay append-only
audit rows on far-past run_dates (April 2026, disjoint from the orchestrator
module's March dates); the shared governance tables get one pre-clean
TRUNCATE before the module's contracts are registered, same convention as
test_daily_runs_integration.py.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.engine.ingest.apply import RunDecisionStatus
from ootils_core.engine.ingest.daily_orchestrator import apply_daily_run
from ootils_core.engine.reporting import build_shortages_summary
from ootils_core.interfaces.contracts import parse_contract_file, upsert_contract

from .conftest import requires_db

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}

PREFIX = f"DRP-{uuid4().hex[:8]}"

_REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = _REPO_ROOT / "config" / "feed-contracts"
SEED_FEED_KEYS = ("on-hand", "open-purchase-orders", "open-work-orders")

# One run_date per scenario — all clocks pinned, disjoint from the
# orchestrator module's March dates so daily_runs/events never cross-talk.
RD1 = date(2026, 4, 6)   # seeded apply → 200 with rows + decision
RD2 = date(2026, 4, 13)  # same-day re-evaluation appends

# Deadlines from the REAL seed cadences: on-hand 06:00+90' = 07:30 UTC,
# open-purchase-orders 05:30+120' = 07:30 UTC, open-work-orders (advisory)
# 07:00+180' = 10:00 UTC.
GREEN_ON_HAND = (6, 10)
GREEN_PO = (5, 40)


def _utc(d: date, hour: int, minute: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=timezone.utc)


def _touch(path: Path, arrived: datetime) -> None:
    ts = arrived.timestamp()
    os.utime(path, (ts, ts))


def _write_tsv(path: Path, header: list[str], rows: list[list[str]], arrived: datetime) -> Path:
    lines = ["\t".join(header)] + ["\t".join(r) for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _touch(path, arrived)
    return path


def _green_inbox(tmp_path: Path, run_date: date) -> Path:
    """A green drop for both blocking feeds (3-row on-hand, 1-row PO); the
    advisory WO feed stays absent (NOT_EVALUATED before its 10:00 deadline
    at now=08:00). Never loaded here — apply_daily_run only observes/decides,
    so the TSV row contents never reach the ingest API."""
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    _write_tsv(
        inbox / f"on-hand_{run_date:%Y%m%d}.tsv",
        ["item_external_id", "location_external_id", "quantity", "uom", "as_of_date"],
        [[f"{PREFIX}-IT-{i}", f"{PREFIX}-LOC-1", "5", "EA", run_date.isoformat()] for i in (1, 2, 3)],
        _utc(run_date, *GREEN_ON_HAND),
    )
    _write_tsv(
        inbox / f"open-purchase-orders_{run_date:%Y%m%d}.tsv",
        ["external_id", "item_external_id", "location_external_id",
         "supplier_external_id", "quantity", "uom", "expected_delivery_date", "status"],
        [[f"{PREFIX}-PO-1", f"{PREFIX}-IT-1", f"{PREFIX}-LOC-1", f"{PREFIX}-SUP-1",
          "12", "EA", "2026-05-01", "confirmed"]],
        _utc(run_date, *GREEN_PO),
    )
    return inbox


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB (same
    pattern as test_daily_orchestrator_integration.py)."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = TOKEN

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
def seed(api_client, request, migrated_db):
    """The 3 REAL seed contracts through the real loader path, volume guards
    adapted to tiny files (delta silenced so run_dates never contaminate each
    other through the previous-day baseline). Neutralized by DEACTIVATION —
    never a DELETE cascade."""
    with psycopg.connect(migrated_db, autocommit=True) as c:
        # Child listed with its parent — 078's FK forbids truncating
        # feed_contracts alone (convention of test_daily_runs_integration.py).
        c.execute("TRUNCATE daily_runs, feed_contracts")

    guard_overrides = {
        "on-hand": {"volume_guard_min_rows": 2, "volume_guard_max_pct_delta": None},
        "open-purchase-orders": {"volume_guard_min_rows": 1, "volume_guard_max_pct_delta": None},
        "open-work-orders": {},  # already None/None in the seed YAML
    }
    with psycopg.connect(migrated_db) as conn:
        for feed_key in SEED_FEED_KEYS:
            spec = parse_contract_file(CONTRACTS_DIR / f"{feed_key}.yaml")
            assert spec.feed_key == feed_key
            if guard_overrides[feed_key]:
                spec = spec.model_copy(update=guard_overrides[feed_key])
            upsert_contract(conn, spec)
        conn.commit()

    def _neutralize():
        with psycopg.connect(migrated_db, autocommit=True) as conn:
            conn.execute(
                "UPDATE feed_contracts SET active = FALSE WHERE feed_key = ANY(%s)",
                (list(SEED_FEED_KEYS),),
            )

    request.addfinalizer(_neutralize)
    return SEED_FEED_KEYS


@pytest.fixture
def conn2(migrated_db):
    """Function-scoped dict_row connection driving apply_daily_run (the
    CLI's role: it owns — and here commits — the transaction)."""
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        yield c
        c.rollback()


# ---------------------------------------------------------------------------
# 1. GET /v1/daily-runs
# ---------------------------------------------------------------------------


class TestDailyRunsEndpoint:
    def test_200_rows_and_decision_after_seeded_apply(self, seed, conn2, api_client, tmp_path):
        inbox = _green_inbox(tmp_path, RD1)
        evaluation = apply_daily_run(conn2, inbox, RD1, now=_utc(RD1, 8))
        conn2.commit()
        assert evaluation.decision is not None
        assert evaluation.decision.status is RunDecisionStatus.DEGRADED

        resp = api_client.get(f"/v1/daily-runs?date={RD1.isoformat()}", headers=AUTH)
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["run_date"] == RD1.isoformat()
        assert body["total_feeds"] == 3
        feeds = body["feeds"]
        assert [f["feed_key"] for f in feeds] == [
            "on-hand", "open-purchase-orders", "open-work-orders",
        ]
        assert [(f["feed_key"], f["overall_status"], f["row_count"]) for f in feeds] == [
            ("on-hand", "ok", 3),
            ("open-purchase-orders", "ok", 1),
            ("open-work-orders", "ok", None),  # absent before its deadline — None-honest
        ]
        assert [f["criticality"] for f in feeds] == ["blocking", "blocking", "advisory"]
        by_key = {f["feed_key"]: f for f in feeds}
        assert by_key["on-hand"]["arrival_status"] == "ok"
        assert by_key["open-work-orders"]["arrival_status"] == "not_evaluated"
        assert by_key["open-work-orders"]["file_arrived_at"] is None
        assert by_key["on-hand"]["file_arrived_at"] is not None
        # The rows read back are the very rows apply_daily_run persisted.
        assert {f["daily_run_id"] for f in feeds} == {
            str(fe.daily_run_id) for fe in evaluation.feed_evaluations
        }

        # The governed decision, read back from its daily_run_completed
        # event (migration 079) — never recomputed.
        decision = body["decision"]
        assert decision is not None
        assert decision["status"] == "degraded"
        assert decision["feeds_evaluated"] == 3
        # DQ is unwired in V1: every feed's combined verdict is
        # NOT_EVALUATED, so all 3 are honest culprits of the DEGRADED.
        assert sorted(decision["culprit_feed_keys"]) == [
            "on-hand", "open-purchase-orders", "open-work-orders",
        ]
        UUID(decision["event_id"])  # a real event row's id
        assert decision["decided_at"] is not None

    def test_default_date_is_today_utc_and_empty(self, seed, api_client):
        today_before = datetime.now(timezone.utc).date()
        resp = api_client.get("/v1/daily-runs", headers=AUTH)
        today_after = datetime.now(timezone.utc).date()
        assert resp.status_code == 200, resp.text
        body = resp.json()
        # Tolerate a midnight rollover between the two clock reads.
        assert body["run_date"] in {today_before.isoformat(), today_after.isoformat()}
        assert body["feeds"] == []
        assert body["total_feeds"] == 0
        assert body["decision"] is None

    def test_bad_date_is_422(self, seed, api_client):
        resp = api_client.get("/v1/daily-runs?date=not-a-date", headers=AUTH)
        assert resp.status_code == 422, resp.text

    def test_same_day_reevaluation_appends_and_reads_newest_first(
        self, seed, conn2, api_client, tmp_path
    ):
        inbox = _green_inbox(tmp_path, RD2)
        apply_daily_run(conn2, inbox, RD2, now=_utc(RD2, 8))
        apply_daily_run(conn2, inbox, RD2, now=_utc(RD2, 9))  # re-evaluated intra-day
        conn2.commit()

        resp = api_client.get(f"/v1/daily-runs?date={RD2.isoformat()}", headers=AUTH)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        # Append-only: BOTH attempts read back, 2 rows per feed.
        assert body["total_feeds"] == 6
        assert [f["feed_key"] for f in body["feeds"]] == [
            "on-hand", "on-hand",
            "open-purchase-orders", "open-purchase-orders",
            "open-work-orders", "open-work-orders",
        ]
        # Newest first within each feed (migration 078's "current verdict").
        for i in (0, 2, 4):
            newer = datetime.fromisoformat(body["feeds"][i]["observed_at"])
            older = datetime.fromisoformat(body["feeds"][i + 1]["observed_at"])
            assert newer > older
        assert body["decision"] is not None
        assert body["decision"]["status"] == "degraded"

    def test_kill_switch_503_default_on_and_auth_first(self, seed, api_client, monkeypatch):
        monkeypatch.setenv("OOTILS_DAILY_RUN_REPORT_ENABLED", "0")
        resp = api_client.get("/v1/daily-runs", headers=AUTH)
        assert resp.status_code == 503, resp.text
        assert "OOTILS_DAILY_RUN_REPORT_ENABLED" in resp.json()["detail"]

        monkeypatch.setenv("OOTILS_DAILY_RUN_REPORT_ENABLED", "false")
        assert api_client.get("/v1/daily-runs", headers=AUTH).status_code == 503

        # Auth is checked BEFORE the switch: an unauthenticated caller gets
        # 401, never a 503 that would leak the switch state.
        assert api_client.get("/v1/daily-runs").status_code == 401

        # Default ON: removing the variable restores the surface.
        monkeypatch.delenv("OOTILS_DAILY_RUN_REPORT_ENABLED")
        assert api_client.get("/v1/daily-runs", headers=AUTH).status_code == 200

    def test_read_scope_required_401_and_403(self, seed, api_client):
        # No token / garbage token → 401.
        assert api_client.get("/v1/daily-runs").status_code == 401
        assert (
            api_client.get(
                "/v1/daily-runs", headers={"Authorization": "Bearer not-a-real-token"}
            ).status_code
            == 401
        )

        # A scoped token WITHOUT 'read' → 403; WITH 'read' → 200 (ADR-032).
        minted: list[str] = []
        try:
            for scopes, expected in ((["ingest"], 403), (["read"], 200)):
                resp = api_client.post(
                    "/v1/tokens",
                    headers=AUTH,
                    json={
                        "name": f"daily-report-{PREFIX}-{'-'.join(scopes)}",
                        "actor_kind": "agent",
                        "scopes": scopes,
                    },
                )
                assert resp.status_code == 201, resp.text
                data = resp.json()
                minted.append(data["token_id"])
                got = api_client.get(
                    "/v1/daily-runs",
                    headers={"Authorization": f"Bearer {data['token']}"},
                )
                assert got.status_code == expected, got.text
        finally:
            # Soft-revoke (never a DELETE on the audit rows) + cache purge.
            for token_id in minted:
                api_client.delete(f"/v1/tokens/{token_id}", headers=AUTH)


# ---------------------------------------------------------------------------
# 2. build_shortages_summary — minimal canonical shortages seed
# ---------------------------------------------------------------------------


def _seed_item(conn, *, external_id: str | None, name: str) -> UUID:
    return conn.execute(
        "INSERT INTO items (item_id, external_id, name) VALUES (%s, %s, %s) RETURNING item_id",
        (uuid4(), external_id, name),
    ).fetchone()["item_id"]


def _seed_location(conn, *, external_id: str | None, name: str) -> UUID:
    return conn.execute(
        "INSERT INTO locations (location_id, external_id, name) VALUES (%s, %s, %s) "
        "RETURNING location_id",
        (uuid4(), external_id, name),
    ).fetchone()["location_id"]


def _seed_calc_run(conn, *, completed_at: datetime) -> UUID:
    return conn.execute(
        "INSERT INTO calc_runs (calc_run_id, scenario_id, status, completed_at) "
        "VALUES (%s, %s, 'completed', %s) RETURNING calc_run_id",
        (uuid4(), BASELINE_SCENARIO_ID, completed_at),
    ).fetchone()["calc_run_id"]


def _seed_shortage(
    conn,
    *,
    calc_run_id: UUID,
    item_id: UUID | None,
    location_id: UUID | None,
    anchor_item_id: UUID,
    anchor_location_id: UUID,
    severity: float,
    shortage_qty: float,
    shortage_date: date,
    status: str = "active",
) -> UUID:
    """One canonical `shortages` row (migration 005) on its own PI node.
    ``item_id``/``location_id`` may be None (the fallback-chain axis) —
    the PI node itself always anchors to real ids."""
    pi_node = conn.execute(
        """
        INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id,
                           time_grain, time_ref, active)
        VALUES (%s, 'ProjectedInventory', %s, %s, %s, 'exact_date', CURRENT_DATE, TRUE)
        RETURNING node_id
        """,
        (uuid4(), BASELINE_SCENARIO_ID, anchor_item_id, anchor_location_id),
    ).fetchone()["node_id"]
    return conn.execute(
        """
        INSERT INTO shortages (shortage_id, scenario_id, pi_node_id, item_id,
            location_id, shortage_date, shortage_qty, severity_score, calc_run_id, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING shortage_id
        """,
        (
            uuid4(), BASELINE_SCENARIO_ID, pi_node, item_id, location_id,
            shortage_date, shortage_qty, severity, calc_run_id, status,
        ),
    ).fetchone()["shortage_id"]


class TestBuildShortagesSummary:
    def test_empty_when_baseline_has_no_completed_calc_run(self, conn):
        # A fresh install: never raises, honest "nothing to report".
        assert build_shortages_summary(conn) == []

    def test_scoping_ordering_fallbacks_and_limit(self, conn):
        """Seeded inside the conn fixture's transaction (rolled back — the
        function is SELECT-only by contract, so it sees the uncommitted seed
        on the same connection)."""
        now = datetime.now(timezone.utc)
        old_run = _seed_calc_run(conn, completed_at=now - timedelta(hours=2))
        latest_run = _seed_calc_run(conn, completed_at=now - timedelta(hours=1))

        item_a = _seed_item(conn, external_id=f"{PREFIX}-ITEM-A", name="Rapport item A")
        item_b = _seed_item(conn, external_id=None, name="Rapport item B")
        loc_a = _seed_location(conn, external_id=f"{PREFIX}-LOC-A", name="Rapport site A")
        loc_b = _seed_location(conn, external_id=None, name="Rapport site B")
        anchor = {"anchor_item_id": item_a, "anchor_location_id": loc_a}

        # On the LATEST completed run:
        _seed_shortage(conn, calc_run_id=latest_run, item_id=item_a, location_id=loc_a,
                       severity=900, shortage_qty=5, shortage_date=date(2026, 4, 20), **anchor)
        _seed_shortage(conn, calc_run_id=latest_run, item_id=item_b, location_id=loc_b,
                       severity=500, shortage_qty=3, shortage_date=date(2026, 4, 21), **anchor)
        # Severity tie at 100 → shortage_date ASC breaks it.
        _seed_shortage(conn, calc_run_id=latest_run, item_id=item_a, location_id=loc_b,
                       severity=100, shortage_qty=2, shortage_date=date(2026, 4, 19), **anchor)
        _seed_shortage(conn, calc_run_id=latest_run, item_id=None, location_id=None,
                       severity=100, shortage_qty=1, shortage_date=date(2026, 4, 25), **anchor)
        # Excluded: resolved (even at the highest severity of all).
        _seed_shortage(conn, calc_run_id=latest_run, item_id=item_a, location_id=loc_a,
                       severity=9999, shortage_qty=9, shortage_date=date(2026, 4, 18),
                       status="resolved", **anchor)
        # Excluded: active but on a SUPERSEDED calc_run (ADR-021 scoping —
        # an unscoped active scan would straddle two historical runs).
        _seed_shortage(conn, calc_run_id=old_run, item_id=item_a, location_id=loc_a,
                       severity=950, shortage_qty=7, shortage_date=date(2026, 4, 17), **anchor)

        rows = build_shortages_summary(conn)
        assert [r["severity"] for r in rows] == [900.0, 500.0, 100.0, 100.0]
        assert [r["item"] for r in rows] == [
            f"{PREFIX}-ITEM-A",      # external_id preferred
            "Rapport item B",        # name fallback (no external_id)
            f"{PREFIX}-ITEM-A",
            "(article inconnu)",     # no item at all — honest, never invented
        ]
        assert [r["location"] for r in rows] == [
            f"{PREFIX}-LOC-A",
            "Rapport site B",
            "Rapport site B",
            "(site inconnu)",
        ]
        # The 100-severity tie: earlier shortage_date first.
        assert [r["shortage_date"] for r in rows[2:]] == [date(2026, 4, 19), date(2026, 4, 25)]
        assert all(isinstance(r["severity"], float) for r in rows)
        assert all(isinstance(r["shortage_qty"], float) for r in rows)
        assert all(isinstance(r["shortage_date"], date) for r in rows)

        # The limit is honoured (top-N by $ severity).
        top2 = build_shortages_summary(conn, limit=2)
        assert [r["severity"] for r in top2] == [900.0, 500.0]
