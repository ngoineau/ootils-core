"""
tests/integration/test_daily_run_decision_integration.py — the governed
daily-run DECISION engine (ADR-042 PR-3, absorbing ADR-037's INT-1 PR3 §0
option (a)) against a real PostgreSQL — no DB mocks (CLAUDE.md). The pure
decision matrix lives in tests/test_daily_run_decision.py.

Covers the DB half of src/ootils_core/engine/ingest/apply.py + migration 079:

  1. The full decide+record cycle on REAL daily_runs rows (seeded through the
     REAL PR-2 path — upsert_contract + record_daily_run, helpers imported
     from tests/integration/test_daily_runs_integration.py, never hand-rolled
     INSERTs on the happy path): all green -> AUTO_APPROVED; a blocking
     feed's red guard OR rejected DQ -> ESCALATED; an advisory red ->
     DEGRADED; an absent DQ observation -> DEGRADED (never a fabricated
     auto-approve); the decision input per feed is the LATEST daily_runs row
     by observed_at (migration 078's "current verdict" rule); a run_date
     nothing evaluated raises DailyRunDecisionError; record never commits.
  2. The ``daily_run_completed`` event (migration 079): emitted exactly ONE
     per record_daily_run_decision call (RUN granularity, ADR-027 — never one
     per feed), visible in ``events`` with the right type and the full
     typed-column contract (field_changed = decision, new_date = run_date,
     new_quantity = feed count, old_text = comma-joined culprits or NULL —
     None-honest, never ''), scoped to the baseline scenario, source
     'engine'. Plus the DB CHECK itself: 'daily_run_completed' INSERTs
     cleanly, an unknown type is still rejected, and re-executing migration
     079 verbatim (twice) is a clean no-op (defensive-idempotence contract,
     migration 063 header).
  3. The L3 webhook escalation on a blocking red (ADR-037 §0): driven through
     the REAL notify_daily_run_escalation/_post_payload transport with ONLY
     ``httpx.post`` monkeypatched to a recording MagicMock (the exact seam
     tests/test_l3_webhook.py already uses — nothing leaves the process):
     one POST per FAILED blocking feed, the DailyRunEscalationPayload body
     (event='daily_run_escalated', run_date, feed_key, criticality, reason,
     message — and no credential-shaped key), ZERO network on AUTO_APPROVED/
     DEGRADED, best-effort (a webhook exception never breaks the decision
     nor the event), and silent no-op when no URL is configured.
  4. Re-decision lifecycle: NOT deduplicated by design (apply.py docstring
     "NOT DEDUPLICATED ACROSS CALLS") — the documented escalated-then-
     re-approved-once-the-late-file-lands flow yields two honest decisions
     and two events in stream order; an identical re-decision is
     deterministic (same verdict) but still appends its own event.

ISOLATION (the committed-seed lesson, cf. test_daily_runs_integration.py):
NO test in this module commits business rows — every seed and every decision
rides the rollback-teardown ``conn`` fixture. The only committed writes are
(a) the autouse pre-test sweep (TRUNCATE daily_runs+feed_contracts in ONE
statement — 078's FK forbids truncating feed_contracts alone — plus a
surgical DELETE of any committed 'daily_run_completed' leftovers a crashed
prior run of another module could have left in events), and (b) the
migration-079 re-execution, which is pure DDL ending in the exact same
schema state and creates zero rows — nothing to finalize. Belt and braces:
the module teardown (migrated_db) drops all public tables. The autouse
fixture also delenv's OOTILS_WEBHOOK_L3_URL so an operator's ambient webhook
config can never make an ESCALATED test leak a real POST.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import psycopg
import pytest
from psycopg import errors

import ootils_core.notifications.l3_webhook as l3
from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.engine.ingest.apply import (
    DailyRunDecisionError,
    RunDecisionStatus,
    plan_daily_run_decision,
    record_daily_run_decision,
)
from ootils_core.interfaces.daily_run import DailyRunObservation, record_daily_run
from ootils_core.interfaces.guards import GuardStatus
from ootils_core.notifications.l3_webhook import WEBHOOK_URL_ENV

from .conftest import requires_db

# Reuse the PR-2 seed helpers/timeline verbatim (never a parallel seed path):
# _register drives the REAL upsert_contract/get_active_contract loader;
# cadence "0 6 * * *" + 90 min window -> 07:30 UTC deadline on each run date.
from .test_daily_runs_integration import D1, D2, _register, _utc

pytestmark = requires_db

_REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_079 = (
    _REPO_ROOT / "src" / "ootils_core" / "db" / "migrations"
    / "079_daily_run_completed_event.sql"
)

HOOK_URL = "https://hook.example/daily-run"


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_slate(migrated_db, monkeypatch):
    """Pre-test sweep on its own autocommit connection (calqued on the PR-2
    sibling's _clean_daily_runs — see the module docstring's ISOLATION note)
    + guarantee no ambient webhook URL can leak a real POST."""
    monkeypatch.delenv(WEBHOOK_URL_ENV, raising=False)
    with psycopg.connect(migrated_db, autocommit=True) as c:
        c.execute("TRUNCATE daily_runs, feed_contracts")
        c.execute("DELETE FROM events WHERE event_type = 'daily_run_completed'")
    yield


def _record_green(conn, feed_key: str, run_date=D1, *, now=None) -> None:
    """One green PR-2 evaluation: file on time, floor cleared (first-ever
    evaluation -> delta/deletion honestly NOT_EVALUATED, overall 'ok')."""
    record_daily_run(
        conn, feed_key, run_date,
        DailyRunObservation(file_arrived_at=_utc(run_date, 6, 30), row_count=150),
        now=now if now is not None else _utc(run_date, 8),
    )


def _record_floor_breach(conn, feed_key: str, run_date=D1, *, now=None) -> None:
    """One red PR-2 evaluation: file on time but 40 rows under the 100-row
    floor -> overall 'failed' (the 'extraction partielle silencieuse' case)."""
    record_daily_run(
        conn, feed_key, run_date,
        DailyRunObservation(file_arrived_at=_utc(run_date, 6, 30), row_count=40),
        now=now if now is not None else _utc(run_date, 8),
    )


def _record_missing_file(conn, feed_key: str, run_date=D1, *, now=None) -> None:
    """One red PR-2 evaluation: no file by the 07:30 deadline -> arrival
    FAILED, overall 'failed' (the 'flux totalement absent' case)."""
    record_daily_run(
        conn, feed_key, run_date,
        DailyRunObservation(file_arrived_at=None, row_count=None),
        now=now if now is not None else _utc(run_date, 9),
    )


def _decision_events(conn) -> list[dict]:
    """Every daily_run_completed event visible to this transaction, in
    stream order — the exact rows GET /v1/stream would serve."""
    return conn.execute(
        "SELECT event_type, scenario_id, field_changed, new_date, new_quantity, "
        "       old_text, new_text, source, processed "
        "FROM events WHERE event_type = 'daily_run_completed' "
        "ORDER BY stream_seq",
    ).fetchall()


@pytest.fixture
def fake_post(monkeypatch):
    """The exact seam tests/test_l3_webhook.py uses: httpx.post replaced by a
    recording MagicMock (+ _HTTPX_AVAILABLE forced True), so the REAL
    notify_daily_run_escalation/_post_payload transport runs end to end and
    nothing leaves the process."""
    fake = MagicMock(return_value=MagicMock(status_code=200))
    monkeypatch.setattr(l3.httpx, "post", fake)
    monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)
    return fake


# ---------------------------------------------------------------------------
# 1. The full decide+record cycle on real daily_runs rows
# ---------------------------------------------------------------------------


class TestDecideRecordCycle:
    def test_all_green_run_auto_approved_end_to_end(self, conn):
        """Blocking + advisory feed both evaluated green through the REAL
        PR-2 path, DQ validated for both -> ONE AUTO_APPROVED decision with
        zero reasons, and its event on the stream."""
        _register(conn, feed_key="drd-onhand")  # criticality='blocking'
        _register(conn, feed_key="drd-hints", criticality="advisory")
        _record_green(conn, "drd-onhand")
        _record_green(conn, "drd-hints")

        decision = record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated", "drd-hints": "validated"},
            now=_utc(D1, 10),
        )

        assert decision.status == RunDecisionStatus.AUTO_APPROVED
        assert decision.run_date == D1
        assert decision.evaluated_at == _utc(D1, 10)
        assert decision.reasons == ()
        assert {f.feed_key for f in decision.feeds} == {"drd-onhand", "drd-hints"}
        assert all(f.combined_status == GuardStatus.OK for f in decision.feeds)
        assert len(_decision_events(conn)) == 1

    def test_blocking_guard_failure_escalates_and_names_culprit(self, conn):
        _register(conn, feed_key="drd-onhand")
        _register(conn, feed_key="drd-pos")
        _record_floor_breach(conn, "drd-onhand")
        _record_green(conn, "drd-pos")

        decision = record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated", "drd-pos": "validated"},
            now=_utc(D1, 10),
        )

        assert decision.status == RunDecisionStatus.ESCALATED
        assert len(decision.reasons) == 1
        assert "drd-onhand" in decision.reasons[0]
        by_key = {f.feed_key: f for f in decision.feeds}
        assert by_key["drd-onhand"].guard_status == GuardStatus.FAILED
        assert by_key["drd-pos"].combined_status == GuardStatus.OK

    def test_dq_rejected_on_blocking_feed_escalates_despite_green_guards(self, conn):
        """The decision combines the persisted guard verdict WITH the DQ
        status: green guards + a rejected batch still block the run."""
        _register(conn, feed_key="drd-onhand")
        _record_green(conn, "drd-onhand")

        decision = record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "rejected"},
            now=_utc(D1, 10),
        )

        assert decision.status == RunDecisionStatus.ESCALATED
        assert decision.feeds[0].guard_status == GuardStatus.OK
        assert decision.feeds[0].combined_status == GuardStatus.FAILED

    def test_advisory_failure_degrades_run_without_blocking(self, conn):
        """ADR-037 §0: an advisory red degrades confidence, never blocks —
        approved-degraded, with the culprit traced in reasons."""
        _register(conn, feed_key="drd-onhand")
        _register(conn, feed_key="drd-hints", criticality="advisory")
        _record_green(conn, "drd-onhand")
        _record_floor_breach(conn, "drd-hints")

        decision = record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated", "drd-hints": "validated"},
            now=_utc(D1, 10),
        )

        assert decision.status == RunDecisionStatus.DEGRADED
        assert len(decision.reasons) == 1
        assert "drd-hints" in decision.reasons[0]

    def test_missing_dq_observation_degrades_never_auto_approves(self, conn):
        """The cadrage's tranché on SKIP, end to end: green guards but NO DQ
        observation supplied at all -> DEGRADED (None-honest — 'not yet
        confirmed green' is never promoted to AUTO_APPROVED), and NOT
        escalated (a missing verdict is not a blocking failure)."""
        _register(conn, feed_key="drd-onhand")
        _record_green(conn, "drd-onhand")

        decision = record_daily_run_decision(conn, D1, now=_utc(D1, 10))

        assert decision.status == RunDecisionStatus.DEGRADED
        assert decision.feeds[0].combined_status == GuardStatus.NOT_EVALUATED
        assert "drd-onhand" in decision.reasons[0]

    def test_latest_attempt_per_feed_is_the_decision_input(self, conn):
        """Migration 078's 'current verdict' rule read back through
        plan_daily_run_decision: a feed red at 08:00 and re-evaluated green
        at 12:00 enters the decision as GREEN (most recent row by
        observed_at) — the documented re-evaluated-once-the-file-lands
        lifecycle."""
        _register(conn, feed_key="drd-onhand")
        _record_missing_file(conn, "drd-onhand", now=_utc(D1, 9))
        _record_green(conn, "drd-onhand", now=_utc(D1, 12))

        plan = plan_daily_run_decision(conn, D1)
        assert len(plan.feed_inputs) == 1
        assert plan.feed_inputs[0].guard_status == GuardStatus.OK

        decision = record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated"},
            now=_utc(D1, 13),
        )
        assert decision.status == RunDecisionStatus.AUTO_APPROVED

    def test_run_date_nothing_evaluated_raises_never_vacuous(self, conn):
        """A run_date with zero daily_runs rows has nothing to decide — both
        the plan and the recorder refuse (never a vacuous AUTO_APPROVED),
        and refusal emits NO event. A feed evaluated on D1 does not leak
        into D2's decision."""
        _register(conn, feed_key="drd-onhand")
        _record_green(conn, "drd-onhand", run_date=D1)

        with pytest.raises(DailyRunDecisionError, match=str(D2)):
            plan_daily_run_decision(conn, D2)
        with pytest.raises(DailyRunDecisionError, match=str(D2)):
            record_daily_run_decision(conn, D2, now=_utc(D2, 10))
        assert _decision_events(conn) == []

    def test_record_never_commits_caller_owns_transaction(self, conn):
        """Same convention as record_daily_run/upsert_contract: rollback
        after an un-committed decision leaves no event behind."""
        _register(conn, feed_key="drd-onhand")
        _record_green(conn, "drd-onhand")
        record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated"},
            now=_utc(D1, 10),
        )
        assert len(_decision_events(conn)) == 1
        conn.rollback()
        assert _decision_events(conn) == []


# ---------------------------------------------------------------------------
# 2. The daily_run_completed event — migration 079
# ---------------------------------------------------------------------------


class TestDailyRunCompletedEvent:
    def test_exactly_one_event_per_decision_call(self, conn):
        """RUN granularity (ADR-027): one decision over THREE feeds emits
        exactly ONE event — never one per feed; a second decision call
        appends exactly one more (its own honest attempt)."""
        _register(conn, feed_key="drd-a")
        _register(conn, feed_key="drd-b")
        _register(conn, feed_key="drd-c", criticality="advisory")
        for key in ("drd-a", "drd-b", "drd-c"):
            _record_green(conn, key)
        dq = {k: "validated" for k in ("drd-a", "drd-b", "drd-c")}

        record_daily_run_decision(conn, D1, dq_status_by_feed=dq, now=_utc(D1, 10))
        assert len(_decision_events(conn)) == 1

        record_daily_run_decision(conn, D1, dq_status_by_feed=dq, now=_utc(D1, 11))
        assert len(_decision_events(conn)) == 2

    def test_event_typed_columns_carry_the_full_contract(self, conn):
        """emit.py's typed-column contract for daily_run_completed
        (migration 079 header): field_changed = the decision discriminant,
        new_date = run_date, new_quantity = feeds included, old_text = the
        comma-joined culprits, new_text unused, baseline-scoped, source
        'engine', processed TRUE (announce-only, nothing to propagate)."""
        _register(conn, feed_key="drd-onhand")
        _register(conn, feed_key="drd-hints", criticality="advisory")
        _record_floor_breach(conn, "drd-onhand")
        _record_green(conn, "drd-hints")

        record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated"},  # drd-hints: no DQ
            now=_utc(D1, 10),
        )

        events = _decision_events(conn)
        assert len(events) == 1
        ev = events[0]
        assert ev["event_type"] == "daily_run_completed"
        assert ev["scenario_id"] == BASELINE_SCENARIO_ID
        assert ev["field_changed"] == "escalated"
        assert ev["new_date"] == D1
        assert int(ev["new_quantity"]) == 2
        # Both non-green feeds are culprits: the blocking FAILED and the
        # advisory NOT_EVALUATED (no DQ supplied).
        assert set(ev["old_text"].split(",")) == {"drd-onhand", "drd-hints"}
        assert ev["new_text"] is None
        assert ev["source"] == "engine"
        assert ev["processed"] is True

    def test_culprits_null_when_all_green_none_honest(self, conn):
        """An AUTO_APPROVED run has no culprits: old_text is NULL — never an
        empty string (None-honest, migration 079 header)."""
        _register(conn, feed_key="drd-onhand")
        _record_green(conn, "drd-onhand")
        record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated"},
            now=_utc(D1, 10),
        )
        ev = _decision_events(conn)[0]
        assert ev["field_changed"] == "auto_approved"
        assert ev["old_text"] is None

    def test_db_check_accepts_new_type_and_still_rejects_unknown(self, conn):
        """The migration-079 CHECK, probed directly (bypassing the Python
        emitter's own FLEET_EVENT_TYPES whitelist): 'daily_run_completed'
        INSERTs cleanly; an unknown type is still a CheckViolation."""
        conn.execute(
            "INSERT INTO events (event_type, scenario_id) VALUES (%s, %s)",
            ("daily_run_completed", BASELINE_SCENARIO_ID),
        )
        assert len(_decision_events(conn)) == 1
        with pytest.raises(errors.CheckViolation):
            conn.execute(
                "INSERT INTO events (event_type, scenario_id) VALUES (%s, %s)",
                ("daily_run_definitely_unknown", BASELINE_SCENARIO_ID),
            )
        conn.rollback()

    def test_migration_079_reexecution_idempotent(self, migrated_db, conn):
        """Defensive-idempotence contract (migration 063 header, the runner
        does NOT swallow 'already exists'): re-running 079 verbatim — twice —
        neither raises nor changes the constraint's effect. The file carries
        its own BEGIN/COMMIT, so it runs on a fresh autocommit connection
        (mirrors the PR-2 sibling's 078 rerun); it is pure DDL ending in the
        same schema state, so there is no residue to finalize."""
        sql_text = MIGRATION_079.read_text(encoding="utf-8")
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            raw.execute(sql_text)  # 2nd application overall
            raw.execute(sql_text)  # and a 3rd — still a clean no-op

        # Exactly one CHECK survives, still accepting the new type and still
        # rejecting garbage.
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM pg_constraint "
            "WHERE conname = 'events_event_type_check' "
            "AND conrelid = 'events'::regclass"
        ).fetchone()["n"]
        assert n == 1
        conn.execute(
            "INSERT INTO events (event_type, scenario_id) VALUES (%s, %s)",
            ("daily_run_completed", BASELINE_SCENARIO_ID),
        )
        with pytest.raises(errors.CheckViolation):
            conn.execute(
                "INSERT INTO events (event_type, scenario_id) VALUES (%s, %s)",
                ("daily_run_definitely_unknown", BASELINE_SCENARIO_ID),
            )
        conn.rollback()


# ---------------------------------------------------------------------------
# 3. The L3 webhook escalation on a blocking red
# ---------------------------------------------------------------------------


class TestL3WebhookEscalation:
    def test_blocking_red_posts_payload_through_real_transport(self, conn, fake_post):
        """ESCALATED -> exactly one POST through the REAL
        notify_daily_run_escalation/_post_payload path, carrying the
        DailyRunEscalationPayload body — and no credential-shaped key
        (the transport's no-secret contract)."""
        _register(conn, feed_key="drd-onhand")
        _record_missing_file(conn, "drd-onhand")

        decision = record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated"},
            now=_utc(D1, 10),
            webhook_url=HOOK_URL,
        )

        assert decision.status == RunDecisionStatus.ESCALATED
        assert fake_post.call_count == 1
        args, kwargs = fake_post.call_args
        assert args[0] == HOOK_URL
        body = kwargs["json"]
        assert body["event"] == "daily_run_escalated"
        assert body["run_date"] == D1.isoformat()
        assert body["feed_key"] == "drd-onhand"
        assert body["criticality"] == "blocking"
        assert "guard_status=failed" in body["reason"]
        assert D1.isoformat() in body["message"]
        lowered = {k.lower() for k in body}
        for forbidden in ("token", "secret", "authorization", "password", "api_key"):
            assert forbidden not in lowered

    def test_one_post_per_failed_blocking_feed_advisory_never_posts(
        self, conn, fake_post
    ):
        """Two blocking reds + one advisory red: the run escalates ONCE (one
        event) but a human is pinged once PER failed blocking feed — the
        advisory culprit never reaches the webhook (it degrades, ADR-037 §0)."""
        _register(conn, feed_key="drd-onhand")
        _register(conn, feed_key="drd-pos")
        _register(conn, feed_key="drd-hints", criticality="advisory")
        _record_missing_file(conn, "drd-onhand")
        _record_floor_breach(conn, "drd-pos")
        _record_floor_breach(conn, "drd-hints")

        decision = record_daily_run_decision(
            conn, D1, now=_utc(D1, 10), webhook_url=HOOK_URL
        )

        assert decision.status == RunDecisionStatus.ESCALATED
        assert fake_post.call_count == 2
        posted_feeds = {
            call.kwargs["json"]["feed_key"] for call in fake_post.call_args_list
        }
        assert posted_feeds == {"drd-onhand", "drd-pos"}
        assert len(_decision_events(conn)) == 1  # still ONE event per run

    def test_auto_approved_and_degraded_never_touch_network(self, conn, fake_post):
        """The webhook is escalation-only: a green run AND an
        advisory-degraded run both stay off the wire, even with a URL
        configured."""
        _register(conn, feed_key="drd-onhand")
        _register(conn, feed_key="drd-hints", criticality="advisory")
        _record_green(conn, "drd-onhand")
        _record_floor_breach(conn, "drd-hints")

        degraded = record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated", "drd-hints": "validated"},
            now=_utc(D1, 10),
            webhook_url=HOOK_URL,
        )
        assert degraded.status == RunDecisionStatus.DEGRADED

        _record_green(conn, "drd-hints", now=_utc(D1, 12))
        approved = record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated", "drd-hints": "validated"},
            now=_utc(D1, 13),
            webhook_url=HOOK_URL,
        )
        assert approved.status == RunDecisionStatus.AUTO_APPROVED
        fake_post.assert_not_called()

    def test_webhook_failure_never_breaks_decision_nor_event(self, conn, monkeypatch):
        """Best-effort transport: an httpx-level exception is swallowed by
        _post_payload — the decision returns normally and its event is
        already on the stream (the webhook never owns the transaction)."""
        fake = MagicMock(side_effect=RuntimeError("connection refused"))
        monkeypatch.setattr(l3.httpx, "post", fake)
        monkeypatch.setattr(l3, "_HTTPX_AVAILABLE", True)
        _register(conn, feed_key="drd-onhand")
        _record_missing_file(conn, "drd-onhand")

        decision = record_daily_run_decision(
            conn, D1, now=_utc(D1, 10), webhook_url=HOOK_URL
        )

        assert decision.status == RunDecisionStatus.ESCALATED
        assert fake.call_count == 1
        events = _decision_events(conn)
        assert len(events) == 1
        assert events[0]["field_changed"] == "escalated"

    def test_no_url_configured_is_silent_noop(self, conn, fake_post):
        """No webhook_url param + no env (autouse delenv): an ESCALATED run
        still decides and emits its event; the transport opts out before
        touching the network."""
        _register(conn, feed_key="drd-onhand")
        _record_missing_file(conn, "drd-onhand")

        decision = record_daily_run_decision(conn, D1, now=_utc(D1, 10))

        assert decision.status == RunDecisionStatus.ESCALATED
        fake_post.assert_not_called()
        assert len(_decision_events(conn)) == 1


# ---------------------------------------------------------------------------
# 4. Re-decision lifecycle — append-only, deterministic
# ---------------------------------------------------------------------------


class TestRedecisionLifecycle:
    def test_escalated_then_auto_approved_after_late_file(self, conn):
        """The documented intra-day flow (apply.py 'NOT DEDUPLICATED ACROSS
        CALLS'): missing blocking feed at the deadline -> ESCALATED; the file
        lands, the feed is re-evaluated green, DQ validates -> a SECOND
        decision AUTO_APPROVED. Two honest events, in stream order."""
        _register(conn, feed_key="drd-onhand")
        _record_missing_file(conn, "drd-onhand", now=_utc(D1, 9))

        first = record_daily_run_decision(conn, D1, now=_utc(D1, 9, 30))
        assert first.status == RunDecisionStatus.ESCALATED

        _record_green(conn, "drd-onhand", now=_utc(D1, 12))
        second = record_daily_run_decision(
            conn, D1,
            dq_status_by_feed={"drd-onhand": "validated"},
            now=_utc(D1, 13),
        )
        assert second.status == RunDecisionStatus.AUTO_APPROVED

        events = _decision_events(conn)
        assert [e["field_changed"] for e in events] == ["escalated", "auto_approved"]
        assert events[0]["old_text"] == "drd-onhand"
        assert events[1]["old_text"] is None

    def test_identical_redecision_is_deterministic_but_appends_event(self, conn):
        """Same persisted verdicts, same DQ observation, same stamp -> the
        SAME decision (deterministic core), while each call still appends
        its own event (attempt semantics — an event is an announcement, not
        an upsert)."""
        _register(conn, feed_key="drd-onhand")
        _record_green(conn, "drd-onhand")
        dq = {"drd-onhand": "validated"}

        a = record_daily_run_decision(conn, D1, dq_status_by_feed=dq, now=_utc(D1, 10))
        b = record_daily_run_decision(conn, D1, dq_status_by_feed=dq, now=_utc(D1, 10))

        assert a == b  # frozen dataclasses compare by value
        assert a.status == b.status == RunDecisionStatus.AUTO_APPROVED
        assert len(_decision_events(conn)) == 2
