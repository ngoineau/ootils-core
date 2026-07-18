"""
tests/integration/test_daily_orchestrator_integration.py — the governed daily
run end to end (ADR-042 PR-4b) against a real PostgreSQL — no mocks
(CLAUDE.md). The pure scan/resolution/load-phase logic lives in
tests/test_daily_orchestrator.py.

The three canonical scenarios of the PR-4b contract, each on its own
run_date so they stay order-independent:

  1. TOUT VERT (R1) — every guard green: one ``daily_runs`` row is persisted
     per active contract, the governed decision is DEGRADED — NEVER
     AUTO_APPROVED, because DQ status has no DB wiring in V1 and
     NOT_EVALUATED is never promoted to green (apply.py, "DQ STATUS — V1 IS
     HONEST, NOT WIRED") — the green feeds ACTUALLY load through the real
     ``/v1/ingest/*`` endpoints (kebab feed_key → snake entity_type
     translation included, on-hand → on_hand, open-purchase-orders →
     purchase_orders as a grouped .partNN drop), and EXACTLY ONE
     ``daily_run_completed`` event is emitted for the run.
  2. GARDE ROUGE BLOCKING (R2) — a blocking feed (on-hand) fails its volume
     floor: the decision is ESCALATED, NOTHING loads (every candidate gets a
     RUN_ESCALATED outcome, the green PO drop included), the inbox is left
     untouched, and the run adds exactly ONE event total (the escalated
     ``daily_run_completed`` — zero load-side events).
  3. ADVISORY ROUGE (R3) — the advisory feed (open-work-orders) arrives
     after its deadline: the run is DEGRADED (never escalated by an
     advisory), the red feed is EXCLUDED from the load (GUARD_FAILED, its
     file stays in the inbox), and the other feeds load normally.

Plus the write-free preview: ``plan_daily_run`` (R4) persists no
``daily_runs`` row, emits no event, and its evaluation is REFUSED by
``load_eligible_feeds`` (a dry-run must never drive a real load).

Determinism: every file's arrival time is pinned with ``os.utime`` and every
``now`` is caller-supplied — no wall-clock dependence (guards.py timezone
contract). Contract seeding goes through the REAL loader path
(``parse_contract_file`` on the 3 seed ``config/feed-contracts/*.yaml`` +
``upsert_contract``), with only the volume guards adapted to minuscule test
files (min_rows 100 → 2/1, delta → None so scenarios can't contaminate each
other through the previous-day baseline).

Isolation (pattern of test_bom_obsolete_integration.py): referential seeds
under a unique PREFIX via the real API, neutralized by DEACTIVATION in a
module finalizer (items obsoleted, suppliers inactivated, nodes/contracts
deactivated) — never a DELETE cascade (leçon #461). daily_runs/events stay
append-only audit rows on far-past run_dates; the shared governance tables
get one pre-clean TRUNCATE (child first, one statement — 078's FK) before
the module's contracts are registered, same convention as
test_daily_runs_integration.py.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.ingest.apply import RunDecisionStatus
from ootils_core.engine.ingest.daily_orchestrator import (
    FeedLoadStatus,
    apply_daily_run,
    load_eligible_feeds,
    plan_daily_run,
)
from ootils_core.interfaces.contracts import parse_contract_file, upsert_contract
from ootils_core.interfaces.guards import GuardStatus

from .conftest import requires_db

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}

PREFIX = f"DOR-{uuid4().hex[:8]}"

_REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = _REPO_ROOT / "config" / "feed-contracts"
SEED_FEED_KEYS = ("on-hand", "open-purchase-orders", "open-work-orders")

# One run_date per scenario — far in the past relative to nothing (all clocks
# are pinned), distinct so daily_runs/decision queries never cross-talk.
R1 = date(2026, 3, 2)   # tout vert
R2 = date(2026, 3, 9)   # garde rouge sur flux blocking
R3 = date(2026, 3, 16)  # advisory rouge
R4 = date(2026, 3, 23)  # dry-run preview
R5 = date(2026, 3, 30)  # flux blocking present mais corrompu (revue PR-4b, finding 2)

# Deadlines from the REAL seed cadences: on-hand 06:00+90' = 07:30 UTC,
# open-purchase-orders 05:30+120' = 07:30 UTC, open-work-orders (advisory)
# 07:00+180' = 10:00 UTC.
GREEN_ON_HAND = (6, 10)
GREEN_PO = (5, 40)
LATE = (23, 30)


def _utc(d: date, hour: int, minute: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=timezone.utc)


def _touch(path: Path, arrived: datetime) -> None:
    ts = arrived.timestamp()
    os.utime(path, (ts, ts))


def _ext(base: str) -> str:
    return f"{PREFIX}-{base}"


def _write_tsv(path: Path, header: list[str], rows: list[list[str]], arrived: datetime) -> Path:
    lines = ["\t".join(header)] + ["\t".join(r) for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _touch(path, arrived)
    return path


def _on_hand_file(inbox: Path, run_date: date, rows: list[tuple[str, str]], arrived: datetime) -> Path:
    return _write_tsv(
        inbox / f"on-hand_{run_date:%Y%m%d}.tsv",
        ["item_external_id", "location_external_id", "quantity", "uom", "as_of_date"],
        [[item, _ext("LOC-1"), qty, "EA", run_date.isoformat()] for item, qty in rows],
        arrived,
    )


_PO_HEADER = [
    "external_id", "item_external_id", "location_external_id",
    "supplier_external_id", "quantity", "uom", "expected_delivery_date", "status",
]


def _po_row(po_ext: str, item: str) -> list[str]:
    return [po_ext, item, _ext("LOC-1"), _ext("SUP-1"), "12", "EA", "2026-04-01", "confirmed"]


def _events_for(conn, run_date: date) -> list[dict]:
    return conn.execute(
        "SELECT field_changed, new_date, new_quantity, old_text, source "
        "FROM events WHERE event_type = 'daily_run_completed' AND new_date = %s "
        "ORDER BY created_at, stream_seq",
        (run_date,),
    ).fetchall()


def _count_all_events(conn) -> int:
    return conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]


def _daily_rows(conn, run_date: date) -> list[dict]:
    return conn.execute(
        "SELECT feed_key, overall_status, row_count, file_arrived_at "
        "FROM daily_runs WHERE run_date = %s ORDER BY feed_key",
        (run_date,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB (same
    pattern as test_bom_obsolete_integration.py). Also pins the environment
    the orchestrator's load phase (ingest_exec.call_api → the module-level
    app) will read: DATABASE_URL + OOTILS_API_TOKEN."""
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
    """The 3 REAL seed contracts (config/feed-contracts/*.yaml) through the
    real loader path, volume guards adapted to tiny files; referential data
    (items / location / supplier) under PREFIX via the real ingest API.
    Neutralized by DEACTIVATION — never a DELETE cascade."""
    # Pre-clean the shared governance tables (child listed with its parent —
    # 078's FK forbids truncating feed_contracts alone), same convention as
    # test_daily_runs_integration.py.
    with psycopg.connect(migrated_db, autocommit=True) as c:
        c.execute("TRUNCATE daily_runs, feed_contracts")

    guard_overrides = {
        # Real file: min 100 rows. Test drop: 3 rows. Delta guard silenced so
        # each scenario's row count never becomes the next one's red baseline.
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

    items = [
        {"external_id": _ext(f"ITEM-{i}"), "name": f"Daily orchestrator item {i}",
         "item_type": "component", "uom": "EA", "status": "active"}
        for i in (1, 2, 3)
    ]
    resp = api_client.post("/v1/ingest/items", json={"items": items}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    resp = api_client.post(
        "/v1/ingest/locations",
        json={"locations": [{"external_id": _ext("LOC-1"), "name": "Daily orchestrator DC"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    resp = api_client.post(
        "/v1/ingest/suppliers",
        json={"suppliers": [{"external_id": _ext("SUP-1"), "name": "Daily orchestrator supplier"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    def _neutralize():
        with psycopg.connect(migrated_db, autocommit=True) as conn:
            conn.execute(
                "UPDATE feed_contracts SET active = FALSE WHERE feed_key = ANY(%s)",
                (list(SEED_FEED_KEYS),),
            )
            conn.execute(
                """
                UPDATE nodes SET active = FALSE
                WHERE item_id IN (SELECT item_id FROM items WHERE external_id LIKE %s)
                """,
                (PREFIX + "%",),
            )
            conn.execute(
                "UPDATE items SET status = 'obsolete' WHERE external_id LIKE %s",
                (PREFIX + "%",),
            )
            conn.execute(
                "UPDATE suppliers SET status = 'inactive' WHERE external_id LIKE %s",
                (PREFIX + "%",),
            )

    request.addfinalizer(_neutralize)
    return {
        "item1": _ext("ITEM-1"),
        "item2": _ext("ITEM-2"),
        "item3": _ext("ITEM-3"),
        "loc": _ext("LOC-1"),
        "sup": _ext("SUP-1"),
    }


@pytest.fixture
def conn2(migrated_db):
    """Function-scoped dict_row connection the tests drive the orchestrator
    with (the CLI's role: it owns — and here commits — the transaction)."""
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        yield c
        c.rollback()


# ---------------------------------------------------------------------------
# Scenario 1 — tout vert
# ---------------------------------------------------------------------------


class TestScenario1AllGreen:
    def test_green_run_persists_decides_degraded_loads_and_emits_one_event(
        self, seed, migrated_db, conn2, tmp_path
    ):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        _on_hand_file(
            inbox, R1,
            [(seed["item1"], "25"), (seed["item2"], "40"), (seed["item3"], "10")],
            _utc(R1, *GREEN_ON_HAND),
        )
        # The PO drop arrives as a grouped .partNN pair — the PR-4a grammar
        # through the whole governed pipeline.
        _write_tsv(
            inbox / f"open-purchase-orders.part01_{R1:%Y%m%d}.tsv",
            _PO_HEADER, [_po_row(_ext("PO-1"), seed["item1"])], _utc(R1, *GREEN_PO),
        )
        _write_tsv(
            inbox / f"open-purchase-orders.part02_{R1:%Y%m%d}.tsv",
            _PO_HEADER, [_po_row(_ext("PO-2"), seed["item2"])], _utc(R1, *GREEN_PO),
        )
        # open-work-orders: no file, and now (08:00) is BEFORE its 10:00
        # deadline — NOT_EVALUATED, which must not block anything.

        evaluation = apply_daily_run(conn2, inbox, R1, now=_utc(R1, 8))
        conn2.commit()

        # Decision: DEGRADED — and NEVER AUTO_APPROVED (DQ unwired in V1).
        assert evaluation.decision is not None
        assert evaluation.decision.status is RunDecisionStatus.DEGRADED
        assert evaluation.decision.status is not RunDecisionStatus.AUTO_APPROVED

        # One PERSISTED daily_runs row per active contract.
        assert all(fe.daily_run_id is not None for fe in evaluation.feed_evaluations)
        rows = _daily_rows(conn2, R1)
        assert [(r["feed_key"], r["overall_status"], r["row_count"]) for r in rows] == [
            ("on-hand", "ok", 3),
            ("open-purchase-orders", "ok", 2),
            ("open-work-orders", "ok", None),  # NOT_EVALUATED guards never fail a feed
        ]

        # Load: green feeds through the REAL endpoints, kebab → snake.
        outcomes = load_eligible_feeds(evaluation, token=TOKEN, inbox_dir=inbox)
        by = {o.feed_key: o for o in outcomes}
        assert by["on-hand"].status is FeedLoadStatus.LOADED
        assert by["on-hand"].canonical == "on_hand.tsv"
        assert by["on-hand"].http_status == 200
        assert by["open-purchase-orders"].status is FeedLoadStatus.LOADED
        assert by["open-purchase-orders"].canonical == "purchase_orders.tsv"
        assert by["open-work-orders"].status is FeedLoadStatus.NO_FILE

        # Chargement EFFECTIF: canonical rows really landed.
        qty = conn2.execute(
            """
            SELECT n.quantity FROM nodes n
            JOIN items i ON i.item_id = n.item_id
            WHERE i.external_id = %s AND n.node_type = 'OnHandSupply' AND n.active
            """,
            (seed["item1"],),
        ).fetchone()
        assert qty is not None and float(qty["quantity"]) == 25.0
        n_pos = conn2.execute(
            "SELECT COUNT(*) AS n FROM external_references "
            "WHERE entity_type = 'purchase_order' AND external_id = ANY(%s)",
            ([_ext("PO-1"), _ext("PO-2")],),
        ).fetchone()["n"]
        assert n_pos == 2

        # EXACTLY ONE daily_run_completed event for this run.
        events = _events_for(conn2, R1)
        assert len(events) == 1
        assert events[0]["field_changed"] == "degraded"
        assert events[0]["new_quantity"] == 3  # the 3 governed feeds
        assert events[0]["source"] == "ingestion"

        # Inbox drained, drops archived to processed/ with their reports.
        assert list(inbox.iterdir()) == []
        processed = sorted(p.name for p in (tmp_path / "processed").iterdir())
        assert sum(1 for n in processed if n.endswith(".tsv")) == 3
        assert any(n.endswith(".report.json") for n in processed)
        assert not (tmp_path / "rejected").exists()


# ---------------------------------------------------------------------------
# Scenario 2 — garde rouge sur flux blocking
# ---------------------------------------------------------------------------


class TestScenario2BlockingRed:
    def test_blocking_red_escalates_loads_nothing_one_event_only(
        self, seed, migrated_db, conn2, tmp_path
    ):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        # on-hand: ONE row — below the (test-adapted) volume floor of 2 on a
        # BLOCKING feed. Arrival itself is green: the floor is the red guard.
        _on_hand_file(inbox, R2, [(seed["item1"], "99")], _utc(R2, *GREEN_ON_HAND))
        # open-purchase-orders: perfectly green — must STILL not load.
        _write_tsv(
            inbox / f"open-purchase-orders_{R2:%Y%m%d}.tsv",
            _PO_HEADER, [_po_row(_ext("PO-9"), seed["item1"])], _utc(R2, *GREEN_PO),
        )
        inbox_before = sorted(p.name for p in inbox.iterdir())

        total_before = _count_all_events(conn2)
        evaluation = apply_daily_run(conn2, inbox, R2, now=_utc(R2, 8))
        conn2.commit()

        assert evaluation.decision is not None
        assert evaluation.decision.status is RunDecisionStatus.ESCALATED
        on_hand_eval = {fe.feed_key: fe for fe in evaluation.feed_evaluations}["on-hand"]
        assert on_hand_eval.evaluation.overall_status is GuardStatus.FAILED
        assert on_hand_eval.evaluation.by_name("volume_floor").status is GuardStatus.FAILED

        outcomes = load_eligible_feeds(evaluation, token=TOKEN, inbox_dir=inbox)

        # ZERO chargement: every candidate blocked, green PO included.
        assert sorted(o.feed_key for o in outcomes) == [
            "on-hand", "open-purchase-orders", "open-work-orders",
        ]
        assert all(o.status is FeedLoadStatus.RUN_ESCALATED for o in outcomes)

        # Nothing landed in the canonical model.
        assert conn2.execute(
            "SELECT 1 FROM external_references "
            "WHERE entity_type = 'purchase_order' AND external_id = %s",
            (_ext("PO-9"),),
        ).fetchone() is None

        # Exactly ONE event total for the whole apply+load — the escalated
        # daily_run_completed; zero load-side events (nothing loaded).
        assert _count_all_events(conn2) - total_before == 1
        events = _events_for(conn2, R2)
        assert len(events) == 1
        assert events[0]["field_changed"] == "escalated"
        assert "on-hand" in (events[0]["old_text"] or "")

        # Inbox untouched — no archive dir ever appears.
        assert sorted(p.name for p in inbox.iterdir()) == inbox_before
        assert not (tmp_path / "processed").exists()
        assert not (tmp_path / "rejected").exists()

        # The daily_runs audit trail still recorded every feed honestly.
        rows = _daily_rows(conn2, R2)
        assert [(r["feed_key"], r["overall_status"]) for r in rows] == [
            ("on-hand", "failed"),
            ("open-purchase-orders", "ok"),
            ("open-work-orders", "ok"),
        ]


# ---------------------------------------------------------------------------
# Scenario 2b — blocking feed PRESENT but CORRUPTED (revue PR-4b, finding 2)
# ---------------------------------------------------------------------------


class TestScenario2bBlockingCorrupted:
    def test_corrupted_blocking_file_escalates_and_loads_nothing(
        self, seed, migrated_db, conn2, tmp_path
    ):
        """A file EXISTS for the blocking feed (on-hand) but fails TSV
        parsing (bad column count) — distinct from Scenario 2's "too few
        rows" axis. Before finding 2's fix, an unparseable-but-present file
        yielded ``row_count=None`` (NOT_EVALUATED), which never fails a
        guard and let the run settle on DEGRADED with the corrupted feed
        simply skipped — silently loading the OTHER green feeds against a
        torn picture. The fix measures 0 exploitable rows instead, which
        the volume floor guard treats as an honest FAILED, correctly
        escalating the whole run and blocking every candidate feed."""
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        # Malformed on-hand drop: 2 cells vs the header's 5 columns.
        bad = inbox / f"on-hand_{R5:%Y%m%d}.tsv"
        bad.write_text(
            "item_external_id\tlocation_external_id\tquantity\tuom\tas_of_date\n"
            "IT-1\tLOC-1\n",
            encoding="utf-8",
        )
        _touch(bad, _utc(R5, *GREEN_ON_HAND))
        # open-purchase-orders: perfectly green — must STILL not load.
        _write_tsv(
            inbox / f"open-purchase-orders_{R5:%Y%m%d}.tsv",
            _PO_HEADER, [_po_row(_ext("PO-5"), seed["item1"])], _utc(R5, *GREEN_PO),
        )
        inbox_before = sorted(p.name for p in inbox.iterdir())

        total_before = _count_all_events(conn2)
        evaluation = apply_daily_run(conn2, inbox, R5, now=_utc(R5, 8))
        conn2.commit()

        assert "on-hand" in evaluation.scan.issues
        on_hand_eval = {fe.feed_key: fe for fe in evaluation.feed_evaluations}["on-hand"]
        assert on_hand_eval.observation.row_count == 0  # honest zero, never None
        assert on_hand_eval.evaluation.overall_status is GuardStatus.FAILED
        assert on_hand_eval.evaluation.by_name("volume_floor").status is GuardStatus.FAILED

        assert evaluation.decision is not None
        assert evaluation.decision.status is RunDecisionStatus.ESCALATED

        outcomes = load_eligible_feeds(evaluation, token=TOKEN, inbox_dir=inbox)

        # ZERO chargement: every candidate blocked, green PO included.
        assert sorted(o.feed_key for o in outcomes) == [
            "on-hand", "open-purchase-orders", "open-work-orders",
        ]
        assert all(o.status is FeedLoadStatus.RUN_ESCALATED for o in outcomes)

        # Nothing landed in the canonical model.
        assert conn2.execute(
            "SELECT 1 FROM external_references "
            "WHERE entity_type = 'purchase_order' AND external_id = %s",
            (_ext("PO-5"),),
        ).fetchone() is None

        # Exactly ONE event total for the whole apply+load.
        assert _count_all_events(conn2) - total_before == 1
        events = _events_for(conn2, R5)
        assert len(events) == 1
        assert events[0]["field_changed"] == "escalated"
        assert "on-hand" in (events[0]["old_text"] or "")

        # Inbox untouched — no archive dir ever appears (apply_daily_run
        # records the guard verdict but load_eligible_feeds never runs the
        # archive step for an escalated run; the malformed file itself was
        # never archived by scan_inbox either — only load-phase writes move
        # files).
        assert sorted(p.name for p in inbox.iterdir()) == inbox_before
        assert not (tmp_path / "processed").exists()
        assert not (tmp_path / "rejected").exists()


# ---------------------------------------------------------------------------
# Scenario 3 — advisory rouge
# ---------------------------------------------------------------------------


class TestScenario3AdvisoryRed:
    def test_advisory_red_is_excluded_and_the_others_load(
        self, seed, migrated_db, conn2, tmp_path
    ):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        _on_hand_file(
            inbox, R3,
            [(seed["item1"], "60"), (seed["item2"], "5"), (seed["item3"], "7")],
            _utc(R3, *GREEN_ON_HAND),
        )
        _write_tsv(
            inbox / f"open-purchase-orders_{R3:%Y%m%d}.tsv",
            _PO_HEADER, [_po_row(_ext("PO-3"), seed["item3"])], _utc(R3, *GREEN_PO),
        )
        # open-work-orders (ADVISORY): arrives well after its 10:00 deadline.
        wo_file = _write_tsv(
            inbox / f"open-work-orders_{R3:%Y%m%d}.tsv",
            ["external_id", "item_external_id", "location_external_id", "quantity", "status"],
            [[_ext("WO-1"), seed["item1"], seed["loc"], "4", "released"]],
            _utc(R3, *LATE),
        )

        evaluation = apply_daily_run(conn2, inbox, R3, now=_utc(R3, 23, 45))
        conn2.commit()

        # An advisory red DEGRADES, it never escalates.
        assert evaluation.decision is not None
        assert evaluation.decision.status is RunDecisionStatus.DEGRADED
        wo_eval = {fe.feed_key: fe for fe in evaluation.feed_evaluations}["open-work-orders"]
        assert wo_eval.evaluation.overall_status is GuardStatus.FAILED
        assert wo_eval.evaluation.by_name("arrival_window").status is GuardStatus.FAILED

        outcomes = load_eligible_feeds(evaluation, token=TOKEN, inbox_dir=inbox)
        by = {o.feed_key: o for o in outcomes}

        # Flux exclu: the advisory-red feed never loads — suspect data stays
        # out whatever the criticality — and its drop stays in the inbox.
        assert by["open-work-orders"].status is FeedLoadStatus.GUARD_FAILED
        assert "deadline" in by["open-work-orders"].detail
        assert wo_file.exists()

        # Les autres chargent.
        assert by["on-hand"].status is FeedLoadStatus.LOADED
        assert by["open-purchase-orders"].status is FeedLoadStatus.LOADED
        qty = conn2.execute(
            """
            SELECT n.quantity FROM nodes n
            JOIN items i ON i.item_id = n.item_id
            WHERE i.external_id = %s AND n.node_type = 'OnHandSupply' AND n.active
            """,
            (seed["item1"],),
        ).fetchone()
        assert qty is not None and float(qty["quantity"]) == 60.0
        assert conn2.execute(
            "SELECT 1 FROM external_references "
            "WHERE entity_type = 'purchase_order' AND external_id = %s",
            (_ext("PO-3"),),
        ).fetchone() is not None
        # The excluded feed left no canonical trace.
        assert conn2.execute(
            """
            SELECT 1 FROM nodes n JOIN items i ON i.item_id = n.item_id
            WHERE i.external_id LIKE %s AND n.node_type = 'WorkOrderSupply'
            """,
            (PREFIX + "%",),
        ).fetchone() is None

        # Still exactly ONE daily_run_completed for this run.
        events = _events_for(conn2, R3)
        assert len(events) == 1
        assert events[0]["field_changed"] == "degraded"

        # Green drops archived; only the red one remains in the inbox.
        assert [p.name for p in inbox.iterdir()] == [f"open-work-orders_{R3:%Y%m%d}.tsv"]
        processed = [p.name for p in (tmp_path / "processed").iterdir() if p.suffix == ".tsv"]
        assert len(processed) == 2


# ---------------------------------------------------------------------------
# Dry-run preview — write-free by construction
# ---------------------------------------------------------------------------


class TestDryRunPreview:
    def test_plan_daily_run_writes_nothing_and_cannot_drive_a_load(
        self, seed, migrated_db, conn2, tmp_path
    ):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        drop = _on_hand_file(
            inbox, R4, [(seed["item1"], "1"), (seed["item2"], "2")], _utc(R4, *GREEN_ON_HAND)
        )

        total_before = _count_all_events(conn2)
        evaluation = plan_daily_run(conn2, inbox, R4, now=_utc(R4, 8))

        assert evaluation.is_applied is False
        assert all(fe.daily_run_id is None for fe in evaluation.feed_evaluations)
        assert evaluation.decision is not None
        assert evaluation.decision.status is RunDecisionStatus.DEGRADED  # in memory only

        # SELECT-only: zero daily_runs rows, zero events — visible from a
        # FRESH connection (nothing was even left uncommitted).
        assert _daily_rows(conn2, R4) == []
        assert _events_for(conn2, R4) == []
        assert _count_all_events(conn2) == total_before
        with psycopg.connect(migrated_db, row_factory=dict_row) as fresh:
            assert _daily_rows(fresh, R4) == []

        # A preview can never drive a real load.
        with pytest.raises(ValueError, match="apply_daily_run"):
            load_eligible_feeds(evaluation, token=TOKEN, inbox_dir=inbox)
        assert drop.exists()
