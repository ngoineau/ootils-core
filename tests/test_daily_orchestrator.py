"""
tests/test_daily_orchestrator.py — Pure unit tests (no DB) for
engine/ingest/daily_orchestrator.py (ADR-042 PR-4b) and the
scripts/run_daily_ingest.py kill switch.

Five axes:

1. `scan_inbox` — the pure filesystem scan: one run_date's dated drops only
   (canonical/dateless, other-date, non-TSV names all land in `ignored`,
   never an error), `.partNN` siblings grouped ascending with `__line__`
   re-prefixed per source file, duplicate part numbers and malformed TSVs
   collected as `ScanIssue`, same-feed_key ambiguity flagged and BOTH groups
   evicted from `feeds` (revue PR-4b, finding 1 — "refusing both" actually
   honoured), missing inbox a hard FileNotFoundError. `_observation_for`
   distinguishes a present-but-unusable file (row_count=0, honest measured
   zero — revue PR-4b, finding 2) from a genuinely absent one (row_count
   None, nothing measured at all).

2. Resolution — the feed_key/entity_type mismatch (module docstring of
   daily_orchestrator.py): a governed kebab-case feed_key ('on-hand')
   translates through its contract's snake_case `entity_type` to the
   DISPATCH/PAYLOAD_BUILDERS key ('on_hand.tsv'); an ungoverned feed_key is
   used as the stem directly. Plus `_resolve_ungoverned` (warning logged,
   sorted output, restricted to feed_keys the registry has NEVER heard of),
   `_quarantine_deactivated_contracts` (a feed_key the registry KNOWS but
   has no active version for becomes an explicit ScanIssue, never
   ungoverned — revue PR-4b, finding 3), `_observation_for` (scanned /
   issue / absent), and `_load_order_key` (LOAD_ORDER respected, unknown
   canonicals sort last — and every registered builder has an explicit
   position).

3. The load gate — `load_eligible_feeds` refuses a dry-run evaluation
   (ValueError), and an ESCALATED (or decision-less) run loads NOTHING:
   every candidate feed_key (scanned or governed-but-absent) gets a
   RUN_ESCALATED outcome and no file moves.

4. The load phase with `call_api` monkeypatched (the API/DB boundary —
   integration covers the real call): guard-failed feeds excluded (file
   left in the inbox), green feeds loaded in LOAD_ORDER through the
   translated canonical endpoint, non-2xx → REJECTED + archived to
   rejected/, a raising call_api → API_CRASH, builderless entities →
   UNSUPPORTED_ENTITY, builder ValueError → SCAN_ERROR, scan issues
   archived to rejected/, governed feeds without a file → NO_FILE.

5. The CLI kill switch (scripts/run_daily_ingest.py): --apply without
   OOTILS_DAILY_RUN_ENABLED (or with a falsy value) is refused with exit
   code 1 BEFORE any DB connection; --apply with the switch on but no
   OOTILS_API_TOKEN likewise; dry-run is deliberately NOT gated by either;
   missing DSN / bad --date exit 2.

No DB required — this file must stay collectible and green without
DATABASE_URL.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from ootils_core.engine.ingest import daily_orchestrator
from ootils_core.engine.ingest.apply import (
    DailyRunDecision,
    FeedDecisionInput,
    RunDecisionStatus,
    decide_daily_run,
)
from ootils_core.engine.ingest.daily_orchestrator import (
    LOAD_ORDER,
    DailyRunEvaluation,
    FeedLoadStatus,
    FeedRunEvaluation,
    InboxScan,
    _canonical_dispatch_name,
    _load_order_key,
    _observation_for,
    _quarantine_deactivated_contracts,
    _resolve_ungoverned,
    load_eligible_feeds,
    scan_inbox,
)
from ootils_core.interfaces.contracts import FeedContract
from ootils_core.interfaces.guards import FeedGuardEvaluation, GuardStatus, evaluate_feed_guards
from ootils_core.interfaces.ingest_exec import DISPATCH, PAYLOAD_BUILDERS

# scripts/ are not a package — same import pattern as tests/test_ingest_file_naming.py.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import run_daily_ingest  # noqa: E402

RUN_DATE = date(2026, 3, 2)
DATE_STR = "20260302"  # RUN_DATE as AAAAMMJJ


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
_HEADER = "item_external_id\tlocation_external_id\tquantity\tuom\tas_of_date"


def _utc(hour: int, minute: int = 0, run_date: date = RUN_DATE) -> datetime:
    return datetime(run_date.year, run_date.month, run_date.day, hour, minute, tzinfo=timezone.utc)


def _write_tsv(path: Path, rows: list[str], header: str = _HEADER) -> Path:
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path


def _row(item: str = "IT-1", qty: str = "5") -> str:
    return f"{item}\tLOC-1\t{qty}\tEA\t2026-03-02"


def _contract(feed_key: str, entity_type: str, criticality: str = "blocking") -> FeedContract:
    """A hand-built FeedContract row (pure dataclass — no DB involved)."""
    now = datetime.now(timezone.utc)
    return FeedContract(
        feed_contract_id=uuid4(),
        feed_key=feed_key,
        version=1,
        entity_type=entity_type,
        source_system="UNIT",
        format="tsv",
        key_columns=["item_external_id"],
        mandatory_columns=["item_external_id"],
        load_mode="full",
        cadence="0 6 * * *",
        arrival_window_minutes=90,
        owner="unit-tests",
        criticality=criticality,
        volume_guard_min_rows=None,
        volume_guard_max_pct_delta=None,
        depends_on=[],
        active=True,
        created_at=now,
        updated_at=now,
    )


def _guard_eval(feed_key: str, criticality: str, *, failed: bool = False) -> FeedGuardEvaluation:
    """A real FeedGuardEvaluation via the pure evaluator: green = file within
    the 07:30 deadline (cadence 06:00 + 90 min), red = file after it."""
    arrived = _utc(23, 0) if failed else _utc(6, 30)
    return evaluate_feed_guards(
        feed_key=feed_key,
        criticality=criticality,
        cadence="0 6 * * *",
        arrival_window_minutes=90,
        volume_guard_min_rows=None,
        volume_guard_max_pct_delta=None,
        run_date=RUN_DATE,
        file_arrived_at=arrived,
        row_count=3,
        previous_row_count=None,
        deleted_count=None,
        previous_active_count=None,
        now=_utc(8),
    )


def _feed_eval(
    feed_key: str, entity_type: str, scan: InboxScan, *,
    criticality: str = "blocking", failed: bool = False,
) -> FeedRunEvaluation:
    return FeedRunEvaluation(
        feed_key=feed_key,
        contract=_contract(feed_key, entity_type, criticality),
        observation=_observation_for(feed_key, scan),
        evaluation=_guard_eval(feed_key, criticality, failed=failed),
        daily_run_id=uuid4(),
    )


def _decision(*inputs: FeedDecisionInput) -> DailyRunDecision:
    return decide_daily_run(list(inputs), RUN_DATE, evaluated_at=_utc(8))


def _input(feed_key: str, criticality: str, status: GuardStatus) -> FeedDecisionInput:
    return FeedDecisionInput(
        feed_key=feed_key, criticality=criticality, guard_status=status, dq_status=None
    )


def _evaluation(
    scan: InboxScan,
    feed_evals: list[FeedRunEvaluation],
    decision: DailyRunDecision | None,
    *,
    is_applied: bool = True,
    ungoverned: tuple[str, ...] = (),
) -> DailyRunEvaluation:
    return DailyRunEvaluation(
        run_date=RUN_DATE,
        is_applied=is_applied,
        scan=scan,
        feed_evaluations=tuple(feed_evals),
        ungoverned_feed_keys=ungoverned,
        decision=decision,
    )


# ─────────────────────────────────────────────────────────────
# 1. scan_inbox
# ─────────────────────────────────────────────────────────────
class TestScanInbox:
    def test_missing_inbox_is_a_hard_error(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="inbox directory does not exist"):
            scan_inbox(tmp_path / "nope", RUN_DATE)
        # A FILE at the inbox path is just as much a config error as nothing.
        not_a_dir = tmp_path / "inbox"
        not_a_dir.write_text("oops", encoding="utf-8")
        with pytest.raises(FileNotFoundError):
            scan_inbox(not_a_dir, RUN_DATE)

    def test_dated_file_scanned_with_rows_and_utc_mtime(self, tmp_path):
        _write_tsv(tmp_path / f"on-hand_{DATE_STR}.tsv", [_row("IT-1"), _row("IT-2")])
        scan = scan_inbox(tmp_path, RUN_DATE)

        assert set(scan.feeds) == {"on-hand"}
        assert scan.issues == {}
        assert scan.ignored == ()
        scanned = scan.feeds["on-hand"]
        assert scanned.row_count == 2  # header excluded
        assert scanned.headers == tuple(_HEADER.split("\t"))
        assert scanned.rows[0]["item_external_id"] == "IT-1"
        assert scanned.rows[0]["__line__"] == "2"  # 1-based, header is line 1
        assert [p.name for p in scanned.paths] == [f"on-hand_{DATE_STR}.tsv"]
        # mtime is returned timezone-AWARE UTC (the guards' hard contract).
        assert scanned.file_arrived_at.tzinfo is not None
        assert scanned.file_arrived_at.utcoffset().total_seconds() == 0

    def test_not_todays_business_is_ignored_not_an_error(self, tmp_path):
        _write_tsv(tmp_path / "on_hand.tsv", [_row()])              # canonical (dateless)
        _write_tsv(tmp_path / "on-hand_20260301.tsv", [_row()])     # another day's drop
        _write_tsv(tmp_path / "notes.txt", [_row()])                # not a .tsv name
        _write_tsv(tmp_path / f"on-hand_{DATE_STR}.TSV", [_row()])  # extension is case-sensitive
        (tmp_path / "subdir").mkdir()                               # directories skipped silently

        scan = scan_inbox(tmp_path, RUN_DATE)
        assert scan.feeds == {}
        assert scan.issues == {}
        assert sorted(p.name for p in scan.ignored) == [
            "notes.txt", "on-hand_20260301.tsv", f"on-hand_{DATE_STR}.TSV", "on_hand.tsv",
        ]

    def test_part_group_grouped_ascending_and_lines_traceable(self, tmp_path):
        # Written part02-first to prove the ordering comes from the part
        # number, not from creation/collation order.
        _write_tsv(tmp_path / f"forecasts.part02_{DATE_STR}.tsv", [_row("IT-3")])
        _write_tsv(tmp_path / f"forecasts.part01_{DATE_STR}.tsv", [_row("IT-1"), _row("IT-2")])

        scan = scan_inbox(tmp_path, RUN_DATE)
        assert set(scan.feeds) == {"forecasts"}
        scanned = scan.feeds["forecasts"]
        assert [p.name for p in scanned.paths] == [
            f"forecasts.part01_{DATE_STR}.tsv", f"forecasts.part02_{DATE_STR}.tsv",
        ]
        assert scanned.row_count == 3
        assert [r["item_external_id"] for r in scanned.rows] == ["IT-1", "IT-2", "IT-3"]
        # __line__ re-prefixed with the source part for cross-part traceability.
        assert scanned.rows[0]["__line__"] == f"forecasts.part01_{DATE_STR}.tsv:L2"
        assert scanned.rows[2]["__line__"] == f"forecasts.part02_{DATE_STR}.tsv:L2"

    def test_part_group_ignores_other_dates_and_other_entities(self, tmp_path):
        _write_tsv(tmp_path / f"forecasts.part01_{DATE_STR}.tsv", [_row("IT-1")])
        _write_tsv(tmp_path / "forecasts.part02_20260301.tsv", [_row("OLD")])   # other date
        _write_tsv(tmp_path / f"transfers.part01_{DATE_STR}.tsv", [_row("TR")])  # other entity

        scan = scan_inbox(tmp_path, RUN_DATE)
        assert [p.name for p in scan.feeds["forecasts"].paths] == [
            f"forecasts.part01_{DATE_STR}.tsv"
        ]
        assert scan.feeds["forecasts"].row_count == 1
        assert set(scan.feeds) == {"forecasts", "transfers"}
        assert [p.name for p in scan.ignored] == ["forecasts.part02_20260301.tsv"]

    def test_duplicate_part_number_is_a_scan_issue(self, tmp_path):
        # part001 and part01 both parse to part=1 — ambiguous, cannot pick one.
        _write_tsv(tmp_path / f"on-hand.part001_{DATE_STR}.tsv", [_row()])
        _write_tsv(tmp_path / f"on-hand.part01_{DATE_STR}.tsv", [_row()])

        scan = scan_inbox(tmp_path, RUN_DATE)
        assert scan.feeds == {}
        assert set(scan.issues) == {"on-hand"}
        # The refusal survives (as the ambiguity flag once the second file is
        # reached); the feed never becomes loadable.
        assert "on-hand" in scan.issues["on-hand"].error

    def test_malformed_tsv_is_a_scan_issue_with_no_fabricated_row_count(self, tmp_path):
        bad = tmp_path / f"on-hand_{DATE_STR}.tsv"
        bad.write_text(_HEADER + "\nIT-1\tLOC-1\n", encoding="utf-8")  # 2 cells vs 5 headers

        scan = scan_inbox(tmp_path, RUN_DATE)
        assert scan.feeds == {}
        issue = scan.issues["on-hand"]
        assert "column count" in issue.error
        assert issue.file_arrived_at.tzinfo is not None
        obs = _observation_for("on-hand", scan)
        assert obs.file_arrived_at is not None
        # A present-but-unusable file measures 0 exploitable rows — honest,
        # never a fabricated non-zero count, and distinct from the
        # absent-file case (row_count=None, tested below).
        assert obs.row_count == 0

    def test_empty_file_is_a_scan_issue(self, tmp_path):
        (tmp_path / f"on-hand_{DATE_STR}.tsv").write_bytes(b"")
        scan = scan_inbox(tmp_path, RUN_DATE)
        assert scan.feeds == {}
        assert "empty" in scan.issues["on-hand"].error

    def test_same_feed_key_from_two_file_groups_is_flagged(self, tmp_path):
        # A part group AND a plain dated file for the same (feed_key, date):
        # BOTH groups are refused — the group already accepted into `feeds`
        # is EVICTED once the second group is discovered (revue PR-4b,
        # finding 1: "refusing both" is now actually honoured, not just the
        # second-seen group).
        part01 = _write_tsv(tmp_path / f"on-hand.part01_{DATE_STR}.tsv", [_row("IT-1")])
        part02 = _write_tsv(tmp_path / f"on-hand.part02_{DATE_STR}.tsv", [_row("IT-2")])
        plain = _write_tsv(tmp_path / f"on-hand_{DATE_STR}.tsv", [_row("IT-9")])

        scan = scan_inbox(tmp_path, RUN_DATE)
        assert "on-hand" not in scan.feeds
        assert "on-hand" in scan.issues
        issue = scan.issues["on-hand"]
        assert "more than one file group" in issue.error
        # The issue's paths cover BOTH groups — the 2-file part group AND the
        # plain single-file group — not just the one seen second.
        assert set(issue.paths) == {part01, part02, plain}


# ─────────────────────────────────────────────────────────────
# 2. Resolution — feed_key kebab vs entity_type snake, ungoverned, ordering
# ─────────────────────────────────────────────────────────────
class TestResolution:
    def test_governed_feed_key_translates_through_contract_entity_type(self):
        # THE mismatch: registry feed_keys are kebab-case, DISPATCH keys are
        # snake_case — the active contract's entity_type is the only bridge.
        assert _canonical_dispatch_name("on-hand", _contract("on-hand", "on_hand")) == "on_hand.tsv"
        assert (
            _canonical_dispatch_name(
                "open-purchase-orders", _contract("open-purchase-orders", "purchase_orders")
            )
            == "purchase_orders.tsv"
        )
        assert (
            _canonical_dispatch_name(
                "open-work-orders", _contract("open-work-orders", "work_orders", "advisory")
            )
            == "work_orders.tsv"
        )

    def test_ungoverned_feed_key_is_used_as_the_stem_directly(self):
        assert _canonical_dispatch_name("items", None) == "items.tsv"
        assert _canonical_dispatch_name("bom_header", None) == "bom_header.tsv"
        # A kebab feed_key with no contract has no translation — honest miss,
        # refused later at the DISPATCH lookup (UNSUPPORTED_ENTITY).
        assert _canonical_dispatch_name("on-hand", None) == "on-hand.tsv"

    def test_resolve_ungoverned_warns_and_sorts(self, tmp_path, caplog):
        _write_tsv(tmp_path / f"items_{DATE_STR}.tsv", [_row()])
        _write_tsv(tmp_path / f"on-hand_{DATE_STR}.tsv", [_row()])
        gizmos = tmp_path / f"gizmos_{DATE_STR}.tsv"
        gizmos.write_text(_HEADER + "\nIT-1\tLOC-1\n", encoding="utf-8")  # scan ISSUE, still scanned
        scan = scan_inbox(tmp_path, RUN_DATE)

        # known_feed_keys covers every feed_key ever registered (active or
        # not) — "on-hand" is both active and known here, so it is excluded
        # from `ungoverned` regardless.
        with caplog.at_level(logging.WARNING, logger="ootils_core.engine.ingest.daily_orchestrator"):
            ungoverned = _resolve_ungoverned(scan, known_feed_keys={"on-hand"})

        assert ungoverned == ("gizmos", "items")  # sorted; issues included; governed excluded
        assert "no_contract" in caplog.text
        assert "gizmos" in caplog.text and "items" in caplog.text
        assert "on-hand" not in [k for k in ungoverned]

    def test_quarantine_deactivated_contract_moves_feed_to_scan_issue(self, tmp_path, caplog):
        # "on-hand" is KNOWN to the registry (a prior version was uploaded)
        # but currently has no ACTIVE contract — must never be treated as an
        # ungoverned/undeclared feed (revue PR-4b, finding 3).
        on_hand = _write_tsv(tmp_path / f"on-hand_{DATE_STR}.tsv", [_row(), _row("IT-2")])
        scan = scan_inbox(tmp_path, RUN_DATE)
        assert "on-hand" in scan.feeds

        with caplog.at_level(logging.ERROR, logger="ootils_core.engine.ingest.daily_orchestrator"):
            quarantined = _quarantine_deactivated_contracts(
                scan, governed_keys=set(), known_feed_keys={"on-hand"}
            )

        assert "on-hand" not in quarantined.feeds
        issue = quarantined.issues["on-hand"]
        assert "contract deactivated" in issue.error
        assert "refusing feed" in issue.error
        assert issue.paths == (on_hand,)
        assert "contract_deactivated" in caplog.text

        # Never surfaced as ungoverned once quarantined.
        ungoverned = _resolve_ungoverned(quarantined, known_feed_keys={"on-hand"})
        assert ungoverned == ()

    def test_quarantine_is_a_no_op_for_active_or_unknown_feeds(self, tmp_path):
        _write_tsv(tmp_path / f"on-hand_{DATE_STR}.tsv", [_row()])
        _write_tsv(tmp_path / f"gizmos_{DATE_STR}.tsv", [_row()])
        scan = scan_inbox(tmp_path, RUN_DATE)

        # "on-hand" is active (governed) and "gizmos" is truly unknown to the
        # registry — neither should be moved into `issues`.
        quarantined = _quarantine_deactivated_contracts(
            scan, governed_keys={"on-hand"}, known_feed_keys={"on-hand"}
        )
        assert quarantined is scan  # unchanged scan returned as-is
        assert set(quarantined.feeds) == {"on-hand", "gizmos"}
        assert quarantined.issues == {}

    def test_quarantine_leaves_an_existing_scan_issue_untouched(self, tmp_path):
        # A malformed file for a deactivated feed_key was ALREADY a scan
        # issue for its own reason — quarantine must not clobber it, and it
        # still never appears in `ungoverned`.
        bad = tmp_path / f"on-hand_{DATE_STR}.tsv"
        bad.write_text(_HEADER + "\nIT-1\tLOC-1\n", encoding="utf-8")  # bad column count
        scan = scan_inbox(tmp_path, RUN_DATE)
        original_error = scan.issues["on-hand"].error

        quarantined = _quarantine_deactivated_contracts(
            scan, governed_keys=set(), known_feed_keys={"on-hand"}
        )
        assert quarantined.issues["on-hand"].error == original_error
        assert _resolve_ungoverned(quarantined, known_feed_keys={"on-hand"}) == ()

    def test_observation_for_three_branches(self, tmp_path):
        _write_tsv(tmp_path / f"on-hand_{DATE_STR}.tsv", [_row(), _row("IT-2")])
        bad = tmp_path / f"transfers_{DATE_STR}.tsv"
        bad.write_text(_HEADER + "\nIT-1\n", encoding="utf-8")
        scan = scan_inbox(tmp_path, RUN_DATE)

        scanned = _observation_for("on-hand", scan)
        assert scanned.row_count == 2 and scanned.file_arrived_at is not None

        # Present-but-unusable content measures 0 exploitable rows — honest,
        # never None (that stays reserved for "no file at all").
        issue = _observation_for("transfers", scan)
        assert issue.row_count == 0 and issue.file_arrived_at is not None

        absent = _observation_for("open-work-orders", scan)
        assert absent.row_count is None and absent.file_arrived_at is None

    def test_load_order_key_respects_manifest_and_unknowns_sort_last(self):
        assert _load_order_key("items.tsv") < _load_order_key("locations.tsv")
        assert _load_order_key("suppliers.tsv") < _load_order_key("on_hand.tsv")
        assert _load_order_key("on_hand.tsv") < _load_order_key("purchase_orders.tsv")
        # Unknown canonicals sort AFTER every known one, tie-broken by name —
        # never refused outright.
        assert _load_order_key("zzz_custom.tsv")[0] == len(LOAD_ORDER)
        assert _load_order_key("purchase_orders.tsv") < _load_order_key("aaa_custom.tsv")
        assert _load_order_key("aaa_custom.tsv") < _load_order_key("zzz_custom.tsv")

    def test_every_registered_builder_has_an_explicit_load_order_slot(self):
        # A new PAYLOAD_BUILDERS entry that forgets LOAD_ORDER would silently
        # load LAST — fail here instead, loudly.
        assert set(PAYLOAD_BUILDERS) <= set(LOAD_ORDER)
        assert set(PAYLOAD_BUILDERS) <= set(DISPATCH)


# ─────────────────────────────────────────────────────────────
# 3. The load gate
# ─────────────────────────────────────────────────────────────
class TestLoadGate:
    def test_refuses_a_dry_run_evaluation(self, tmp_path):
        (tmp_path / "inbox").mkdir()
        scan = scan_inbox(tmp_path / "inbox", RUN_DATE)
        evaluation = _evaluation(scan, [], None, is_applied=False)
        with pytest.raises(ValueError, match="apply_daily_run"):
            load_eligible_feeds(evaluation, token="t", inbox_dir=tmp_path / "inbox")

    def test_escalated_run_loads_nothing_and_touches_no_file(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        on_hand = _write_tsv(inbox / f"on-hand_{DATE_STR}.tsv", [_row()])
        scan = scan_inbox(inbox, RUN_DATE)

        decision = _decision(
            _input("on-hand", "blocking", GuardStatus.FAILED),
            _input("open-work-orders", "advisory", GuardStatus.OK),
        )
        assert decision.status is RunDecisionStatus.ESCALATED
        evaluation = _evaluation(
            scan,
            [
                _feed_eval("on-hand", "on_hand", scan, failed=True),
                _feed_eval("open-work-orders", "work_orders", scan, criticality="advisory"),
            ],
            decision,
        )

        outcomes = load_eligible_feeds(evaluation, token="t", inbox_dir=inbox)

        # Every candidate — scanned OR governed-but-absent — gets an outcome.
        assert sorted(o.feed_key for o in outcomes) == ["on-hand", "open-work-orders"]
        assert all(o.status is FeedLoadStatus.RUN_ESCALATED for o in outcomes)
        assert all(o.http_status is None and o.canonical is None for o in outcomes)
        # Zero side effects: the drop stays in place, no archive dir appears.
        assert on_hand.exists()
        assert not (tmp_path / "processed").exists()
        assert not (tmp_path / "rejected").exists()

    def test_no_computable_decision_also_blocks_everything(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        gizmos = _write_tsv(inbox / f"gizmos_{DATE_STR}.tsv", [_row()])
        scan = scan_inbox(inbox, RUN_DATE)
        evaluation = _evaluation(scan, [], None, ungoverned=("gizmos",))

        outcomes = load_eligible_feeds(evaluation, token="t", inbox_dir=inbox)
        assert [o.feed_key for o in outcomes] == ["gizmos"]
        assert outcomes[0].status is FeedLoadStatus.RUN_ESCALATED
        assert "no governed decision" in outcomes[0].detail
        assert gizmos.exists()


# ─────────────────────────────────────────────────────────────
# 4. The load phase (call_api monkeypatched — integration covers the real API)
# ─────────────────────────────────────────────────────────────
class TestLoadPhase:
    @pytest.fixture
    def api_calls(self, monkeypatch):
        """Record every call_api invocation; respond 200 unless the endpoint
        was primed otherwise via .responses."""
        calls: list[tuple[str, dict, str]] = []
        responses: dict[str, tuple[int, dict]] = {}

        def fake_call_api(endpoint, payload, token):
            calls.append((endpoint, payload, token))
            return responses.get(endpoint, (200, {"summary": {"inserted": len(next(iter(payload.values())))}}))

        fake_call_api.responses = responses  # type: ignore[attr-defined]
        monkeypatch.setattr(daily_orchestrator, "call_api", fake_call_api)
        fake_call_api.calls = calls  # type: ignore[attr-defined]
        return fake_call_api

    def _degraded_evaluation(self, scan: InboxScan, feed_evals, ungoverned=()):
        inputs = [
            _input(fe.feed_key, fe.contract.criticality, fe.evaluation.overall_status)
            for fe in feed_evals
        ] or [_input("placeholder", "advisory", GuardStatus.OK)]
        decision = _decision(*inputs)
        assert decision.status is not RunDecisionStatus.ESCALATED
        return _evaluation(scan, list(feed_evals), decision, ungoverned=tuple(ungoverned))

    def test_guard_failed_feed_excluded_green_feed_loads_through_translation(
        self, tmp_path, api_calls
    ):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        _write_tsv(inbox / f"on-hand_{DATE_STR}.tsv", [_row("IT-1", "5"), _row("IT-2", "7")])
        wo_file = _write_tsv(
            inbox / f"open-work-orders_{DATE_STR}.tsv", [_row("WO-1")],
        )
        scan = scan_inbox(inbox, RUN_DATE)
        evaluation = self._degraded_evaluation(
            scan,
            [
                _feed_eval("on-hand", "on_hand", scan),
                _feed_eval(
                    "open-work-orders", "work_orders", scan,
                    criticality="advisory", failed=True,
                ),
            ],
        )

        outcomes = load_eligible_feeds(evaluation, token="tok", inbox_dir=inbox)
        by = {o.feed_key: o for o in outcomes}

        # kebab feed_key -> snake entity_type -> canonical endpoint, end to end.
        assert by["on-hand"].status is FeedLoadStatus.LOADED
        assert by["on-hand"].canonical == "on_hand.tsv"
        assert by["on-hand"].http_status == 200
        assert [c[0] for c in api_calls.calls] == ["/v1/ingest/on-hand"]
        endpoint, payload, token = api_calls.calls[0]
        assert token == "tok"
        assert payload["dry_run"] is False
        assert [r["item_external_id"] for r in payload["on_hand"]] == ["IT-1", "IT-2"]

        # The advisory-red feed is EXCLUDED (never sent, file left in place).
        assert by["open-work-orders"].status is FeedLoadStatus.GUARD_FAILED
        assert "deadline" in by["open-work-orders"].detail
        assert wo_file.exists()

        # The green feed's drop was archived to processed/ with its report.
        assert not (inbox / f"on-hand_{DATE_STR}.tsv").exists()
        processed = sorted(p.name for p in (tmp_path / "processed").iterdir())
        assert any(n.startswith(f"on-hand_{DATE_STR}_") and n.endswith(".tsv") for n in processed)
        assert any(n.endswith(".report.json") for n in processed)

    def test_ungoverned_referential_feed_loads_in_load_order(self, tmp_path, api_calls):
        # items (referential, no contract) must be POSTed BEFORE on_hand
        # (LOAD_ORDER is the FK manifest), whatever the scan order was.
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        _write_tsv(
            inbox / f"items_{DATE_STR}.tsv", ["IT-1\tItem one"],
            header="external_id\tname",
        )
        _write_tsv(inbox / f"on-hand_{DATE_STR}.tsv", [_row("IT-1")])
        scan = scan_inbox(inbox, RUN_DATE)
        evaluation = self._degraded_evaluation(
            scan, [_feed_eval("on-hand", "on_hand", scan)], ungoverned=("items",)
        )

        outcomes = load_eligible_feeds(evaluation, token="tok", inbox_dir=inbox)
        assert [c[0] for c in api_calls.calls] == ["/v1/ingest/items", "/v1/ingest/on-hand"]
        assert {o.status for o in outcomes} == {FeedLoadStatus.LOADED}
        assert list(inbox.iterdir()) == []

    def test_rejected_non_2xx_is_archived_to_rejected(self, tmp_path, api_calls):
        api_calls.responses["/v1/ingest/on-hand"] = (422, {"detail": [{"errors": ["nope"]}]})
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        _write_tsv(inbox / f"on-hand_{DATE_STR}.tsv", [_row()])
        scan = scan_inbox(inbox, RUN_DATE)
        evaluation = self._degraded_evaluation(scan, [_feed_eval("on-hand", "on_hand", scan)])

        outcomes = load_eligible_feeds(evaluation, token="tok", inbox_dir=inbox)
        assert outcomes[0].status is FeedLoadStatus.REJECTED
        assert outcomes[0].http_status == 422
        assert not (tmp_path / "processed").exists()
        rejected = sorted(p.name for p in (tmp_path / "rejected").iterdir())
        assert any(n.endswith(".tsv") for n in rejected)
        assert any(n.endswith(".report.json") for n in rejected)

    def test_api_crash_is_reported_and_archived_to_rejected(self, tmp_path, monkeypatch):
        def boom(endpoint, payload, token):
            raise RuntimeError("connection lost")

        monkeypatch.setattr(daily_orchestrator, "call_api", boom)
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        _write_tsv(inbox / f"on-hand_{DATE_STR}.tsv", [_row()])
        scan = scan_inbox(inbox, RUN_DATE)
        evaluation = self._degraded_evaluation(scan, [_feed_eval("on-hand", "on_hand", scan)])

        outcomes = load_eligible_feeds(evaluation, token="tok", inbox_dir=inbox)
        assert outcomes[0].status is FeedLoadStatus.API_CRASH
        assert outcomes[0].http_status is None
        assert any(p.suffix == ".tsv" for p in (tmp_path / "rejected").iterdir())

    def test_builderless_entities_are_unsupported_never_bypassed(self, tmp_path, api_calls):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        # Governed feed whose entity has no PAYLOAD_BUILDERS entry (work_orders)
        _write_tsv(inbox / f"open-work-orders_{DATE_STR}.tsv", [_row("WO-1")])
        # BOM bundle sentinel: in DISPATCH but deliberately NOT in builders
        _write_tsv(inbox / f"bom_header_{DATE_STR}.tsv", ["P-1\t1.0"], header="parent_external_id\tbom_version")
        scan = scan_inbox(inbox, RUN_DATE)
        evaluation = self._degraded_evaluation(
            scan,
            [_feed_eval("open-work-orders", "work_orders", scan, criticality="advisory")],
            ungoverned=("bom_header",),
        )

        outcomes = load_eligible_feeds(evaluation, token="tok", inbox_dir=inbox)
        by = {o.feed_key: o for o in outcomes}
        assert by["open-work-orders"].status is FeedLoadStatus.UNSUPPORTED_ENTITY
        assert by["open-work-orders"].canonical == "work_orders.tsv"
        assert by["bom_header"].status is FeedLoadStatus.UNSUPPORTED_ENTITY
        assert api_calls.calls == []  # nothing ever reached the API
        assert list(inbox.iterdir()) == []  # both archived to rejected/
        assert len([p for p in (tmp_path / "rejected").iterdir() if p.suffix == ".tsv"]) == 2

    def test_builder_value_error_is_a_scan_error_not_a_crash(self, tmp_path, api_calls):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        # quantity blank -> build_on_hand_payload raises ValueError
        _write_tsv(inbox / f"on-hand_{DATE_STR}.tsv", ["IT-1\tLOC-1\t\tEA\t2026-03-02"])
        scan = scan_inbox(inbox, RUN_DATE)
        evaluation = self._degraded_evaluation(scan, [_feed_eval("on-hand", "on_hand", scan)])

        outcomes = load_eligible_feeds(evaluation, token="tok", inbox_dir=inbox)
        assert outcomes[0].status is FeedLoadStatus.SCAN_ERROR
        assert "payload build error" in outcomes[0].detail
        assert api_calls.calls == []
        assert any(p.suffix == ".tsv" for p in (tmp_path / "rejected").iterdir())

    def test_scan_issues_archived_and_absent_governed_feed_reported(self, tmp_path, api_calls):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        bad = inbox / f"on-hand_{DATE_STR}.tsv"
        bad.write_text(_HEADER + "\nIT-1\tLOC-1\n", encoding="utf-8")  # malformed
        scan = scan_inbox(inbox, RUN_DATE)
        assert "on-hand" in scan.issues
        evaluation = self._degraded_evaluation(
            scan,
            [
                _feed_eval("on-hand", "on_hand", scan),
                _feed_eval("open-purchase-orders", "purchase_orders", scan),  # no file today
            ],
        )

        outcomes = load_eligible_feeds(evaluation, token="tok", inbox_dir=inbox)
        by = {o.feed_key: o for o in outcomes}
        assert by["on-hand"].status is FeedLoadStatus.SCAN_ERROR
        assert "column count" in by["on-hand"].detail
        assert by["open-purchase-orders"].status is FeedLoadStatus.NO_FILE
        assert api_calls.calls == []
        assert not bad.exists()  # broken drop archived so tomorrow never re-sees it
        assert any(p.suffix == ".tsv" for p in (tmp_path / "rejected").iterdir())

    def test_deactivated_contract_feed_is_never_loaded(self, tmp_path, api_calls):
        # A feed_key KNOWN to the registry but with no active version today
        # (revue PR-4b, finding 3): quarantined into a ScanIssue upstream —
        # this proves the load phase then treats it exactly like any other
        # unloadable scan issue, never reaching the API.
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        _write_tsv(inbox / f"on-hand_{DATE_STR}.tsv", [_row("IT-1", "5")])
        scan = scan_inbox(inbox, RUN_DATE)
        assert "on-hand" in scan.feeds

        quarantined = _quarantine_deactivated_contracts(
            scan, governed_keys=set(), known_feed_keys={"on-hand"}
        )
        assert "on-hand" not in quarantined.feeds
        evaluation = self._degraded_evaluation(quarantined, [])

        outcomes = load_eligible_feeds(evaluation, token="tok", inbox_dir=inbox)
        assert outcomes[0].feed_key == "on-hand"
        assert outcomes[0].status is FeedLoadStatus.SCAN_ERROR
        assert "contract deactivated" in outcomes[0].detail
        assert api_calls.calls == []
        assert any(p.suffix == ".tsv" for p in (tmp_path / "rejected").iterdir())


# ─────────────────────────────────────────────────────────────
# 5. CLI kill switch — scripts/run_daily_ingest.py
# ─────────────────────────────────────────────────────────────
class _Boom(Exception):
    """Sentinel raised by the patched psycopg.connect."""


def _no_db(monkeypatch):
    def connect(*args, **kwargs):
        raise _Boom("psycopg.connect must not be reached")

    monkeypatch.setattr(run_daily_ingest.psycopg, "connect", connect)


class TestCliKillSwitch:
    DSN = "postgresql://localhost/ootils_unit"

    def test_apply_refused_when_kill_switch_unset(self, monkeypatch, caplog):
        monkeypatch.delenv("OOTILS_DAILY_RUN_ENABLED", raising=False)
        monkeypatch.setenv("OOTILS_API_TOKEN", "t")
        _no_db(monkeypatch)
        with caplog.at_level(logging.ERROR, logger="run_daily_ingest"):
            rc = run_daily_ingest.main(["--dsn", self.DSN, "--apply"])
        assert rc == 1
        assert "OOTILS_DAILY_RUN_ENABLED" in caplog.text

    @pytest.mark.parametrize("value", ["0", "false", "no", "off", "", "2", "enabled"])
    def test_apply_refused_on_falsy_or_unknown_values(self, monkeypatch, value):
        monkeypatch.setenv("OOTILS_DAILY_RUN_ENABLED", value)
        monkeypatch.setenv("OOTILS_API_TOKEN", "t")
        _no_db(monkeypatch)
        assert run_daily_ingest.main(["--dsn", self.DSN, "--apply"]) == 1

    @pytest.mark.parametrize("value", ["1", "true", "yes", "on", "TRUE", "  On  "])
    def test_truthy_values_pass_the_switch_then_hit_the_token_gate(
        self, monkeypatch, caplog, value
    ):
        monkeypatch.setenv("OOTILS_DAILY_RUN_ENABLED", value)
        monkeypatch.delenv("OOTILS_API_TOKEN", raising=False)
        _no_db(monkeypatch)
        with caplog.at_level(logging.ERROR, logger="run_daily_ingest"):
            rc = run_daily_ingest.main(["--dsn", self.DSN, "--apply"])
        assert rc == 1
        assert "OOTILS_API_TOKEN" in caplog.text
        assert "OOTILS_DAILY_RUN_ENABLED" not in caplog.text  # the switch itself passed

    def test_dry_run_is_not_gated_by_switch_or_token(self, monkeypatch):
        # Neither the kill switch nor the token guards a preview — main
        # proceeds all the way to the DB connection (our sentinel).
        monkeypatch.delenv("OOTILS_DAILY_RUN_ENABLED", raising=False)
        monkeypatch.delenv("OOTILS_API_TOKEN", raising=False)
        _no_db(monkeypatch)
        with pytest.raises(_Boom):
            run_daily_ingest.main(["--dsn", self.DSN])

    def test_missing_dsn_exits_2(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        _no_db(monkeypatch)
        assert run_daily_ingest.main(["--apply"]) == 2

    def test_bad_date_exits_2(self, monkeypatch):
        _no_db(monkeypatch)
        assert run_daily_ingest.main(["--dsn", self.DSN, "--date", "18/07/2026"]) == 2

    def test_daily_run_enabled_truth_table(self, monkeypatch):
        for value, expected in [
            ("1", True), ("true", True), ("yes", True), ("on", True),
            ("TRUE", True), ("  on  ", True),
            ("0", False), ("", False), ("off", False), ("no", False),
            ("2", False), ("enabled", False),
        ]:
            monkeypatch.setenv("OOTILS_DAILY_RUN_ENABLED", value)
            assert run_daily_ingest._daily_run_enabled() is expected, value
        monkeypatch.delenv("OOTILS_DAILY_RUN_ENABLED")
        assert run_daily_ingest._daily_run_enabled() is False
