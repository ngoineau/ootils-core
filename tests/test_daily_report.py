"""
tests/test_daily_report.py — Pure unit tests (no DB) for the daily-report
renderer (engine/reporting/daily_report.py, ADR-042 PR-4c) and the CLI's
report-emission split (scripts/run_daily_ingest.py:_emit_daily_report).

Five axes:

1. DETERMINISM, OCTET-EXACT — the renderer's hard contract: the SAME
   evaluation/outcomes/shortages/generated_at yield the byte-identical
   Markdown, whether the same objects are rendered twice or the whole input
   graph is rebuilt from scratch (fixed UUIDs); a naive ``generated_at`` is
   treated as UTC (never the system timezone — machine-independent); exactly
   one trailing newline.

2. The 3 governed decision statuses, each rendered with its full honest
   French phrase (AUTO_APPROVED / DEGRADED / ESCALATED), plus the
   no-decision case (N/A + its own explanation) — asserted as LITERAL
   sentences so accidental wording drift fails the build.

3. The per-feed table, complete: every FeedLoadStatus has a French label
   (mapping totality — no real outcome can ever KeyError), the canonical
   vocabulary (chargé / exclu / bloqué / rejeté au scan) is pinned, rows are
   sorted by feed_key, guard cells carry the French guard labels, the
   deadline column names a concrete instant (or an honest "non calculable"
   for an unsupported cadence), a feed with no load trace reads its honest
   not-attempted phrase (applied vs dry-run variants), the empty table shows
   its placeholder row, and '|' in free-text cells is escaped.

4. The pénuries section, None-honest at two levels: ``None`` (caller made no
   query — section says so, never a fabricated zero), ``[]`` (a genuine
   computed zero — "Aucune pénurie active détectée."), and rows (formatted
   table, em-dash for missing values, '|' escaped).

5. ``_emit_daily_report`` (scripts/run_daily_ingest.py): dry-run prints the
   report to STDOUT ONLY and never creates/writes the outbox; --apply writes
   ``daily_report_<AAAAMMJJ>.md`` byte-identically and prints nothing; both
   modes produce the identical body for identical inputs (clock frozen).
   ``build_shortages_summary`` is monkeypatched (the DB half is integration
   territory) and its pass-through (conn + top-N limit) is asserted.

No DB required — this file must stay collectible and green without
DATABASE_URL.
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

from ootils_core.engine.ingest.apply import (
    FeedDecisionInput,
    RunDecisionStatus,
    decide_daily_run,
)
from ootils_core.engine.ingest.daily_orchestrator import (
    DailyRunEvaluation,
    FeedLoadOutcome,
    FeedLoadStatus,
    FeedRunEvaluation,
    InboxScan,
    ScanIssue,
    ScannedFeedFile,
)
from ootils_core.engine.reporting import render_daily_report
from ootils_core.engine.reporting.daily_report import _LOAD_STATUS_LABELS
from ootils_core.interfaces.contracts import FeedContract
from ootils_core.interfaces.daily_run import DailyRunObservation
from ootils_core.interfaces.guards import GuardStatus, evaluate_feed_guards

# scripts/ are not a package — same import pattern as tests/test_daily_orchestrator.py.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import run_daily_ingest  # noqa: E402

RUN_DATE = date(2026, 3, 2)
GEN_AT = datetime(2026, 3, 2, 8, 15, tzinfo=timezone.utc)
FIXED_TS = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 3, 2, 8, 0, tzinfo=timezone.utc)


# ─────────────────────────────────────────────────────────────
# Deterministic input builders — fixed UUIDs, fixed clocks, so the SAME
# graph can be rebuilt from scratch and must render byte-identically.
# ─────────────────────────────────────────────────────────────
def _uuid(n: int) -> UUID:
    return UUID(int=n)


def _utc(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 3, 2, hour, minute, tzinfo=timezone.utc)


def _contract(
    feed_key: str,
    entity_type: str,
    criticality: str = "blocking",
    *,
    cadence: str = "0 6 * * *",
    n: int = 0,
) -> FeedContract:
    return FeedContract(
        feed_contract_id=_uuid(0x1000 + n),
        feed_key=feed_key,
        version=1,
        entity_type=entity_type,
        source_system="UNIT",
        format="tsv",
        key_columns=["k"],
        mandatory_columns=["k"],
        load_mode="full",
        cadence=cadence,
        arrival_window_minutes=90,
        owner="unit-tests",
        criticality=criticality,
        volume_guard_min_rows=None,
        volume_guard_max_pct_delta=None,
        depends_on=[],
        active=True,
        created_at=FIXED_TS,
        updated_at=FIXED_TS,
    )


def _scanned(feed_key: str, n_rows: int, arrived: datetime) -> ScannedFeedFile:
    return ScannedFeedFile(
        feed_key=feed_key,
        paths=(Path(f"{feed_key}_20260302.tsv"),),
        file_arrived_at=arrived,
        headers=("k",),
        rows=tuple({"k": str(i)} for i in range(n_rows)),
    )


def _guard_eval(
    feed_key: str,
    criticality: str,
    *,
    arrived: datetime | None,
    row_count: int | None,
    min_rows: int | None = None,
    cadence: str = "0 6 * * *",
):
    """A real FeedGuardEvaluation via the pure evaluator (cadence 06:00 +
    90 min → 07:30 UTC deadline; ``now`` pinned at 08:00)."""
    return evaluate_feed_guards(
        feed_key=feed_key,
        criticality=criticality,
        cadence=cadence,
        arrival_window_minutes=90,
        volume_guard_min_rows=min_rows,
        volume_guard_max_pct_delta=None,
        run_date=RUN_DATE,
        file_arrived_at=arrived,
        row_count=row_count,
        previous_row_count=None,
        deleted_count=None,
        previous_active_count=None,
        now=NOW,
    )


def _feed_eval(
    feed_key: str,
    entity_type: str,
    *,
    criticality: str = "blocking",
    arrived: datetime | None,
    row_count: int | None,
    min_rows: int | None = None,
    guard_cadence: str = "0 6 * * *",
    contract_cadence: str = "0 6 * * *",
    n: int,
) -> FeedRunEvaluation:
    return FeedRunEvaluation(
        feed_key=feed_key,
        contract=_contract(feed_key, entity_type, criticality, cadence=contract_cadence, n=n),
        observation=DailyRunObservation(file_arrived_at=arrived, row_count=row_count),
        evaluation=_guard_eval(
            feed_key, criticality, arrived=arrived, row_count=row_count,
            min_rows=min_rows, cadence=guard_cadence,
        ),
        daily_run_id=_uuid(n),
    )


def _evaluation(
    scan: InboxScan,
    feed_evals: list[FeedRunEvaluation],
    decision,
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


def _empty_scan() -> InboxScan:
    return InboxScan(run_date=RUN_DATE, feeds={}, issues={}, ignored=())


def _input(feed_key: str, criticality: str, status: GuardStatus, dq: str | None = None) -> FeedDecisionInput:
    return FeedDecisionInput(feed_key=feed_key, criticality=criticality, guard_status=status, dq_status=dq)


def _degraded():
    return decide_daily_run([_input("on-hand", "blocking", GuardStatus.OK)], RUN_DATE, evaluated_at=NOW)


def _escalated():
    return decide_daily_run([_input("on-hand", "blocking", GuardStatus.FAILED)], RUN_DATE, evaluated_at=NOW)


def _auto_approved():
    return decide_daily_run(
        [_input("on-hand", "blocking", GuardStatus.OK, dq="validated")], RUN_DATE, evaluated_at=NOW
    )


def _full_inputs():
    """The complete deterministic input graph — every renderer section is
    exercised: governed feeds in every load state, an ungoverned feed, a
    scan issue with a '|' to escape, volumes, shortages (incl. missing
    values), footer audit ids. Rebuilt from scratch on every call: two
    calls MUST render byte-identically (fixed UUIDs, fixed clocks)."""
    scan = InboxScan(
        run_date=RUN_DATE,
        feeds={
            # Deliberately unsorted insertion order — the renderer must sort.
            "orders-x": _scanned("orders-x", 4, _utc(5, 40)),
            "on-hand": _scanned("on-hand", 3, _utc(6, 10)),
            "late-feed": _scanned("late-feed", 2, _utc(23, 30)),
            "gizmos": _scanned("gizmos", 1, _utc(6, 0)),  # ungoverned
        },
        issues={
            "broken-feed": ScanIssue(
                feed_key="broken-feed",
                paths=(Path("broken-feed_20260302.tsv"),),
                file_arrived_at=_utc(6, 5),
                error="bad | column count",
            ),
        },
        ignored=(),
    )
    feed_evals = [
        # Green + loaded (blocking).
        _feed_eval("on-hand", "on_hand", arrived=_utc(6, 10), row_count=3, n=1),
        # Advisory red on arrival — excluded from the load.
        _feed_eval("late-feed", "transfers", criticality="advisory",
                   arrived=_utc(23, 30), row_count=2, n=2),
        # Green + loaded (blocking).
        _feed_eval("orders-x", "customer_orders", arrived=_utc(5, 40), row_count=4, n=3),
        # Governed feed, no file at all (advisory so the run stays DEGRADED).
        _feed_eval("absent-feed", "forecasts", criticality="advisory",
                   arrived=None, row_count=None, n=4),
        # Present-but-unusable file: honest 0 rows, volume floor FAILED.
        _feed_eval("broken-feed", "purchase_orders", criticality="advisory",
                   arrived=_utc(6, 5), row_count=0, min_rows=2, n=5),
        # Contract cadence outside the V1 'M H * * *' shape: the deadline
        # column must read "non calculable", never a silent guess. (The
        # guard eval itself uses a supported cadence whose deadline has not
        # elapsed at 08:00 — arrival "non évalué".) No load outcome either:
        # the applied not-attempted phrase.
        _feed_eval("odd-cadence", "forecasts", criticality="advisory",
                   arrived=None, row_count=None,
                   guard_cadence="0 20 * * *", contract_cadence="0 6 * * 1", n=6),
    ]
    decision = decide_daily_run(
        [
            _input("on-hand", "blocking", GuardStatus.OK),
            _input("late-feed", "advisory", GuardStatus.FAILED),
            _input("orders-x", "blocking", GuardStatus.OK),
            _input("absent-feed", "advisory", GuardStatus.FAILED),
            _input("broken-feed", "advisory", GuardStatus.FAILED),
            _input("odd-cadence", "advisory", GuardStatus.OK),
        ],
        RUN_DATE,
        evaluated_at=NOW,
    )
    assert decision.status is RunDecisionStatus.DEGRADED  # input-graph invariant
    evaluation = _evaluation(scan, feed_evals, decision, ungoverned=("gizmos",))
    outcomes = (
        FeedLoadOutcome("broken-feed", None, FeedLoadStatus.SCAN_ERROR, None, "scan issue: bad | column count"),
        FeedLoadOutcome("late-feed", None, FeedLoadStatus.GUARD_FAILED, None, "arrived after deadline"),
        FeedLoadOutcome("absent-feed", None, FeedLoadStatus.NO_FILE, None, "no file found"),
        FeedLoadOutcome("on-hand", "on_hand.tsv", FeedLoadStatus.LOADED, 200, "HTTP 200"),
        FeedLoadOutcome("orders-x", "customer_orders.tsv", FeedLoadStatus.LOADED, 200, "HTTP 200"),
        FeedLoadOutcome("gizmos", "gizmos.tsv", FeedLoadStatus.UNSUPPORTED_ENTITY, None, "no builder"),
        # NOTE: no outcome at all for "odd-cadence" — the applied
        # not-attempted phrase must appear for it.
    )
    shortages = [
        {"item": "A|B", "location": "LOC-1", "severity": 1234.5, "shortage_qty": 5.0,
         "shortage_date": date(2026, 3, 9)},
        {"item": "ITEM-2", "location": "LOC-2", "severity": None, "shortage_qty": None,
         "shortage_date": None},
    ]
    return evaluation, outcomes, shortages


def _table_rows(report: str, header_start: str) -> list[list[str]]:
    """The data rows of the Markdown table whose header line starts with
    ``header_start`` — each row split into stripped cells."""
    lines = report.splitlines()
    idx = next(i for i, line in enumerate(lines) if line.startswith(header_start))
    rows = []
    for line in lines[idx + 2:]:  # skip header + separator
        if not line.startswith("|"):
            break
        rows.append([c.strip() for c in line.strip("|").split(" | ")])
    return rows


# ─────────────────────────────────────────────────────────────
# 1. Determinism — octet-exact
# ─────────────────────────────────────────────────────────────
class TestDeterminism:
    def test_same_objects_rendered_twice_are_byte_identical(self):
        evaluation, outcomes, shortages = _full_inputs()
        r1 = render_daily_report(evaluation, outcomes, shortages_summary=shortages, generated_at=GEN_AT)
        r2 = render_daily_report(evaluation, outcomes, shortages_summary=shortages, generated_at=GEN_AT)
        assert r1 == r2
        assert r1.encode("utf-8") == r2.encode("utf-8")

    def test_rebuilt_input_graph_renders_byte_identical(self):
        # Not the same objects — the whole graph rebuilt from scratch. Byte
        # equality proves no id()/insertion-order/uuid4 dependence anywhere.
        e1, o1, s1 = _full_inputs()
        e2, o2, s2 = _full_inputs()
        assert e1 is not e2
        r1 = render_daily_report(e1, o1, shortages_summary=s1, generated_at=GEN_AT)
        r2 = render_daily_report(e2, o2, shortages_summary=s2, generated_at=GEN_AT)
        assert r1.encode("utf-8") == r2.encode("utf-8")

    def test_naive_generated_at_is_treated_as_utc_never_system_tz(self):
        evaluation, outcomes, shortages = _full_inputs()
        aware = render_daily_report(evaluation, outcomes, shortages_summary=shortages, generated_at=GEN_AT)
        naive = render_daily_report(
            evaluation, outcomes, shortages_summary=shortages,
            generated_at=GEN_AT.replace(tzinfo=None),
        )
        assert aware.encode("utf-8") == naive.encode("utf-8")
        assert "_Généré le : 2026-03-02 08:15 UTC_" in aware

    def test_exactly_one_trailing_newline(self):
        evaluation, outcomes, shortages = _full_inputs()
        report = render_daily_report(evaluation, outcomes, shortages_summary=shortages, generated_at=GEN_AT)
        assert report.endswith("\n")
        assert not report.endswith("\n\n")


# ─────────────────────────────────────────────────────────────
# 2. The 3 decision statuses — honest phrases, literal
# ─────────────────────────────────────────────────────────────
class TestDecisionStatuses:
    def _render(self, decision) -> str:
        return render_daily_report(
            _evaluation(_empty_scan(), [], decision), (), generated_at=GEN_AT
        )

    def test_auto_approved_phrase(self):
        report = self._render(_auto_approved())
        assert "**Statut de la décision : AUTO_APPROVED**" in report
        assert (
            "Tous les flux attendus ont passé leurs contrôles avec succès — le "
            "run est approuvé automatiquement, aucune intervention n'est requise."
        ) in report

    def test_degraded_phrase(self):
        report = self._render(_degraded())
        assert "**Statut de la décision : DEGRADED**" in report
        assert (
            "Contrôles qualité non encore câblés, confiance bornée — les flux à "
            "gardes vertes ont bien été chargés."
        ) in report

    def test_escalated_phrase(self):
        report = self._render(_escalated())
        assert "**Statut de la décision : ESCALATED**" in report
        assert (
            "Au moins un flux critique (blocking) a échoué son contrôle — le run "
            "est bloqué, rien n'a été chargé pour ce run, et une alerte a été "
            "envoyée à un humain (webhook L3)."
        ) in report

    def test_no_decision_is_na_with_its_own_explanation(self):
        report = self._render(None)
        assert "**Statut de la décision : N/A**" in report
        assert (
            "Aucune décision gouvernée n'a pu être calculée pour cette date — aucun "
            "flux actif n'a été évalué (aucun contrat de flux actif, ou aucun fichier "
            "traité)."
        ) in report

    def test_mode_line_applied_vs_dry_run(self):
        applied = self._render(_degraded())
        assert "_Mode : RUN APPLIQUÉ_" in applied
        dry = render_daily_report(
            _evaluation(_empty_scan(), [], _degraded(), is_applied=False), (), generated_at=GEN_AT
        )
        assert "_Mode : APERÇU (dry-run — rien n'a été chargé ni persisté)_" in dry


# ─────────────────────────────────────────────────────────────
# 3. The per-feed table — complete
# ─────────────────────────────────────────────────────────────
class TestFeedTable:
    def test_load_status_label_mapping_is_total_and_vocabulary_pinned(self):
        # Totality: no real FeedLoadStatus can ever fall back to a raw enum
        # value in the report.
        assert set(_LOAD_STATUS_LABELS) == set(FeedLoadStatus)
        # The architect's 4 canonical words, pinned.
        assert _LOAD_STATUS_LABELS[FeedLoadStatus.LOADED] == "chargé"
        assert _LOAD_STATUS_LABELS[FeedLoadStatus.GUARD_FAILED] == "exclu (garde échouée)"
        assert _LOAD_STATUS_LABELS[FeedLoadStatus.NO_FILE] == "exclu (flux absent)"
        assert _LOAD_STATUS_LABELS[FeedLoadStatus.UNSUPPORTED_ENTITY] == "bloqué (hors périmètre V1)"
        assert _LOAD_STATUS_LABELS[FeedLoadStatus.RUN_ESCALATED] == "bloqué (run escaladé)"
        assert _LOAD_STATUS_LABELS[FeedLoadStatus.SCAN_ERROR] == "rejeté au scan"
        assert _LOAD_STATUS_LABELS[FeedLoadStatus.REJECTED] == "rejeté (réponse API)"
        assert _LOAD_STATUS_LABELS[FeedLoadStatus.API_CRASH] == "rejeté (erreur technique API)"

    def test_rows_sorted_by_feed_key_and_every_state_rendered(self):
        evaluation, outcomes, shortages = _full_inputs()
        report = render_daily_report(evaluation, outcomes, shortages_summary=shortages, generated_at=GEN_AT)
        rows = _table_rows(report, "| Flux | Criticité |")
        assert [r[0] for r in rows] == [
            "absent-feed", "broken-feed", "late-feed", "odd-cadence", "on-hand", "orders-x",
        ]
        by_feed = {r[0]: r for r in rows}

        # Green, loaded, deadline recomputed to a concrete instant.
        assert by_feed["on-hand"] == [
            "on-hand", "blocking", "2026-03-02 07:30 UTC", "2026-03-02 06:10 UTC",
            "3", "à l'heure", "non évalué", "non évalué", "non évalué", "chargé",
        ]
        # Advisory red arrival — excluded.
        assert by_feed["late-feed"][5] == "en retard"
        assert by_feed["late-feed"][9] == "exclu (garde échouée)"
        # Governed, no file: nothing measured (— cells), honest exclusion.
        assert by_feed["absent-feed"][3] == "—"   # Reçu
        assert by_feed["absent-feed"][4] == "—"   # Lignes
        assert by_feed["absent-feed"][9] == "exclu (flux absent)"
        # Present-but-unusable: honest 0 rows, floor red, rejected at scan.
        assert by_feed["broken-feed"][4] == "0"
        assert by_feed["broken-feed"][6] == "sous le plancher"
        assert by_feed["broken-feed"][9] == "rejeté au scan"
        # Unsupported cadence: deadline honestly non-computable, and a feed
        # with no load trace in an APPLIED run says so.
        assert by_feed["odd-cadence"][2] == "non calculable (cadence='0 6 * * 1')"
        assert by_feed["odd-cadence"][9] == "statut de chargement inconnu (aucune trace pour ce flux)"

    def test_escalated_run_rows_read_bloque(self):
        scan = InboxScan(
            run_date=RUN_DATE,
            feeds={"on-hand": _scanned("on-hand", 1, _utc(6, 10))},
            issues={}, ignored=(),
        )
        feed_evals = [_feed_eval("on-hand", "on_hand", arrived=_utc(6, 10), row_count=1, min_rows=5, n=1)]
        evaluation = _evaluation(scan, feed_evals, _escalated())
        outcomes = (
            FeedLoadOutcome("on-hand", None, FeedLoadStatus.RUN_ESCALATED, None, "run ESCALATED"),
        )
        report = render_daily_report(evaluation, outcomes, generated_at=GEN_AT)
        rows = _table_rows(report, "| Flux | Criticité |")
        assert rows[0][9] == "bloqué (run escaladé)"
        # And the volumes section is an honest zero, not an invented table.
        assert "Aucun flux chargé pour ce run." in report

    def test_dry_run_cells_read_apercu_everywhere(self):
        evaluation, _, _ = _full_inputs()
        dry = DailyRunEvaluation(
            run_date=evaluation.run_date, is_applied=False, scan=evaluation.scan,
            feed_evaluations=evaluation.feed_evaluations,
            ungoverned_feed_keys=evaluation.ungoverned_feed_keys,
            decision=evaluation.decision,
        )
        report = render_daily_report(dry, (), generated_at=GEN_AT)
        rows = _table_rows(report, "| Flux | Criticité |")
        assert all(r[9] == "aperçu seul (dry-run — rien n'a été chargé)" for r in rows)
        # Ungoverned section too, and the volumes + footer variants.
        ungoverned = _table_rows(report, "| Flux | Décision |")
        assert ["gizmos", "aperçu seul (dry-run — rien n'a été chargé)"] in ungoverned
        assert "aucun (aperçu dry-run, rien n'a été persisté)" in report

    def test_empty_feed_table_placeholder(self):
        report = render_daily_report(
            _evaluation(_empty_scan(), [], None), (), generated_at=GEN_AT
        )
        assert "| _aucun flux actif évalué pour cette date_ | | | | | | | | | |" in report

    def test_ungoverned_and_scan_issue_sections_with_pipe_escaped(self):
        evaluation, outcomes, shortages = _full_inputs()
        report = render_daily_report(evaluation, outcomes, shortages_summary=shortages, generated_at=GEN_AT)
        assert "## Flux non gouvernés détectés" in report
        assert "| gizmos | bloqué (hors périmètre V1) |" in report
        assert "## Anomalies de lecture (fichiers non exploitables)" in report
        # The '|' inside the issue's error text is escaped — the table never breaks.
        assert "| broken-feed | bad \\| column count |" in report

    def test_volumes_total_counts_only_loaded_feeds(self):
        evaluation, outcomes, shortages = _full_inputs()
        report = render_daily_report(evaluation, outcomes, shortages_summary=shortages, generated_at=GEN_AT)
        assert "**Total lignes chargées : 7**" in report  # on-hand 3 + orders-x 4
        rows = _table_rows(report, "| Flux | Lignes chargées |")
        assert rows == [["on-hand", "3"], ["orders-x", "4"]]

    def test_footer_audit_ids_and_daily_runs_pointer(self):
        evaluation, outcomes, shortages = _full_inputs()
        report = render_daily_report(evaluation, outcomes, shortages_summary=shortages, generated_at=GEN_AT)
        assert f"on-hand={_uuid(1)}" in report
        assert "`GET /v1/daily-runs?date=2026-03-02`" in report


# ─────────────────────────────────────────────────────────────
# 4. Pénuries — None-honest at two levels
# ─────────────────────────────────────────────────────────────
class TestShortagesSection:
    def _render(self, shortages_summary) -> str:
        return render_daily_report(
            _evaluation(_empty_scan(), [], None), (),
            shortages_summary=shortages_summary, generated_at=GEN_AT,
        )

    def test_none_means_not_computed_never_a_fabricated_zero(self):
        report = self._render(None)
        assert "## Pénuries actives (les plus sévères)" in report
        assert (
            "Non fourni par l'appelant pour ce rapport — section ignorée "
            "(aucune requête n'a été faite)."
        ) in report
        # Neither the good-news zero nor a table — nothing was computed.
        assert "Aucune pénurie active détectée." not in report
        assert "| Article | Site |" not in report

    def test_empty_list_is_a_genuine_computed_zero(self):
        report = self._render([])
        assert "Aucune pénurie active détectée." in report
        assert "Non fourni par l'appelant" not in report
        assert "| Article | Site |" not in report

    def test_rows_render_formatted_with_missing_values_honest(self):
        _, _, shortages = _full_inputs()
        report = self._render(shortages)
        assert "| Article | Site | Sévérité ($) | Quantité | Date |" in report
        # Money/qty/date formatting + '|' escaped in the item name.
        assert "| A\\|B | LOC-1 | 1,234.50 $ | 5.00 | 2026-03-09 |" in report
        # Missing severity/qty/date: em-dash, never an invented 0.
        assert "| ITEM-2 | LOC-2 | — | — | — |" in report
        assert "Aucune pénurie active détectée." not in report


# ─────────────────────────────────────────────────────────────
# 5. _emit_daily_report — dry-run stdout only, apply writes the file
# ─────────────────────────────────────────────────────────────
_SUMMARY_ROW = {
    "item": "EMIT-ITEM", "location": "EMIT-LOC", "severity": 42.0,
    "shortage_qty": 2.0, "shortage_date": date(2026, 3, 9),
}


class _FrozenDatetime(datetime):
    """Freeze run_daily_ingest's clock so dry-run stdout and the applied
    file can be compared byte-for-byte."""

    @classmethod
    def now(cls, tz=None):
        return GEN_AT if tz is not None else GEN_AT.replace(tzinfo=None)


class TestEmitDailyReport:
    def _patch(self, monkeypatch):
        calls: list[tuple] = []
        sentinel = object()

        def fake_summary(conn, limit=10):
            calls.append((conn, limit))
            return [dict(_SUMMARY_ROW)]

        monkeypatch.setattr(run_daily_ingest, "build_shortages_summary", fake_summary)
        monkeypatch.setattr(run_daily_ingest, "datetime", _FrozenDatetime)
        return sentinel, calls

    def test_dry_run_prints_to_stdout_and_never_touches_the_outbox(
        self, monkeypatch, capsys, tmp_path
    ):
        sentinel, calls = self._patch(monkeypatch)
        evaluation = _evaluation(_empty_scan(), [], _degraded(), is_applied=False)
        outbox = tmp_path / "outbox"  # never created beforehand

        run_daily_ingest._emit_daily_report(
            evaluation, (), sentinel, apply=False, outbox_dir=outbox
        )

        expected = render_daily_report(
            evaluation, (), shortages_summary=[dict(_SUMMARY_ROW)], generated_at=GEN_AT
        )
        out = capsys.readouterr().out
        assert out == expected + "\n"  # the report body + print()'s newline
        assert "| EMIT-ITEM | EMIT-LOC |" in out  # summary passed through
        # STDOUT ONLY: the outbox directory was never even created.
        assert not outbox.exists()
        # The summary was gathered from the caller's conn with the top-N limit.
        assert calls == [(sentinel, run_daily_ingest._SHORTAGES_TOP_N)]

    def test_apply_writes_the_dated_file_byte_identically_and_prints_nothing(
        self, monkeypatch, capsys, tmp_path
    ):
        sentinel, _ = self._patch(monkeypatch)
        evaluation, outcomes, _ = _full_inputs()
        outbox = tmp_path / "outbox"

        run_daily_ingest._emit_daily_report(
            evaluation, outcomes, sentinel, apply=True, outbox_dir=outbox
        )

        out_path = outbox / "daily_report_20260302.md"
        assert out_path.exists()
        expected = render_daily_report(
            evaluation, outcomes, shortages_summary=[dict(_SUMMARY_ROW)], generated_at=GEN_AT
        )
        # NOTE: Path.write_text(newline=None) applies the PLATFORM's newline
        # translation — LF on the Linux deployment target (/home/debian),
        # CRLF on a Windows dev box. The renderer's byte-exactness contract
        # is at the string level; normalize the platform translation before
        # comparing raw bytes so this test is honest on both.
        raw = out_path.read_bytes()
        assert raw.replace(b"\r\n", b"\n") == expected.encode("utf-8")
        assert out_path.read_text(encoding="utf-8") == expected
        assert capsys.readouterr().out == ""  # the report body never hits stdout

    def test_dry_run_preview_equals_applied_deposit_for_identical_inputs(
        self, monkeypatch, capsys, tmp_path
    ):
        # The docstring's promise: an IDENTICAL preview vs deposit, no hidden
        # second computation in between (clock frozen, same inputs).
        sentinel, _ = self._patch(monkeypatch)
        evaluation, outcomes, _ = _full_inputs()
        outbox = tmp_path / "outbox"

        run_daily_ingest._emit_daily_report(
            evaluation, outcomes, sentinel, apply=False, outbox_dir=outbox
        )
        printed = capsys.readouterr().out
        run_daily_ingest._emit_daily_report(
            evaluation, outcomes, sentinel, apply=True, outbox_dir=outbox
        )
        written = (outbox / "daily_report_20260302.md").read_text(encoding="utf-8")
        assert printed == written + "\n"
