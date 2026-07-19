"""
run_daily_ingest.py — the daily governed-run CLI (ADR-042 decision 3, PR-4b).

Runs ``engine.ingest.daily_orchestrator`` end to end for one ``run_date``:
scan the inbox for today's dated TSV drops, resolve each feed_key's active
contract, evaluate PR-2's runtime guards, compute PR-3's governed decision,
and — gated all-or-nothing on that decision — load every feed whose OWN
guard is green, reusing the exact same parse/build/call/archive primitives
``scripts/ingest_file.py`` uses for a manual single-file drop.

DRY-RUN BY DEFAULT: without ``--apply`` this only calls ``plan_daily_run``
(SELECT-only preview — zero ``daily_runs`` INSERTs, zero
``daily_run_completed`` event, zero L3 webhook call) and reports the scan,
each feed's guard verdict, and the decision that WOULD be taken — nothing is
loaded, nothing moves in the inbox. ``--apply`` is additionally gated by the
``OOTILS_DAILY_RUN_ENABLED`` kill switch (must be exactly one of
'1'/'true'/'yes'/'on'; unset or any other value refuses ``--apply`` before a
DB connection is even opened) — the same double-guard shape as
``scripts/purge_maintenance.py``'s ``OOTILS_PURGE_ENABLED``.

DAILY REPORT (ADR-042 PR-4c, "daily update via la Dropbox"): every
invocation that reaches the DB — dry-run preview or applied run alike —
renders the deterministic Markdown compte-rendu
(``engine.reporting.render_daily_report``) for the team reading the
Dropbox, e.g. ERP. In ``--apply`` mode the report is written under
``--outbox`` (default ``/home/debian/outbox``) as
``daily_report_<AAAAMMJJ>.md`` — the actual Dropbox deposit is a SEPARATE,
deliberately dumb step (``scripts/deposit_outbox.sh``, an ``rclone copy``
cron job over that same directory, mirroring the existing backup pattern —
this script never talks to Dropbox itself). In dry-run mode the report goes
to STDOUT ONLY — nothing is ever written to ``--outbox`` without ``--apply``
(so a preview run never leaves a stray file an operator might mistake for a
real daily deposit). The report is generated even for an ESCALATED run
(nothing loaded) — that is precisely the situation the ERP team most needs
explained.

NO AUTOMATIC RECOMPUTE (deliberate, V1 scope). Loading the green feeds is
this script's entire job. Propagation / shortage detection is a SEPARATE,
deliberate call an operator (or a future PR) makes afterwards, e.g.:
    OOTILS_API_TOKEN=... DATABASE_URL=... python scripts/... (calc:run path)
Coupling the load and the recompute here would silently widen this PR's
blast radius (the API endpoints this script calls already trigger their own
per-entity DQ/graph writes — see ``engine.ingest.daily_orchestrator``'s
module docstring — but nothing here re-runs propagation across the whole
graph). A future PR may wire this explicitly; today it is a conscious
omission, not an oversight.

RECONCILIATION (ADR-042 decision 4, PR-5b): after the feeds are loaded (in
``--apply`` mode — today's inbound POs are now in the DB) and BEFORE the daily
report (so its counts feed the compte-rendu), a best-effort call to
``engine.reconciliation.matcher.run_reconciliation`` heuristically pairs each
inbound ERP PO with an already-exported recommendation and stamps
``fulfilled_at`` for the unambiguous matches (an OBSERVATION — never a status
change). It is gated by ``OOTILS_RECONCILIATION_ENABLED`` (default ON — an
observation, not a destructive write, unlike the OFF-by-default write
switches) and is strictly best-effort: any failure is caught, rolled back, and
reported as "n/a" in the daily report — a reconciliation failure NEVER fails
the daily run. In dry-run mode nothing was loaded, so reconciliation does not
run (the report's section reads "n/a").

OUTBOUND EXPORT (ADR-042 decision 4, PR-5): a THIRD phase, run AFTER the
daily report, calls ``engine.reporting.outbound_export.execute_export`` —
idempotent (``WHERE status IN ('APPROVED','APPLIED') AND exported_at IS
NULL``), one TSV per outbound family (``po_drafts``/``reschedule_messages``/
``transfers``) written under ``--outbox``, stamped, and announced by ONE
``export_executed`` stream event per run. This phase runs on EVERY
invocation (dry-run or ``--apply``) but only ever WRITES when BOTH
``--apply`` AND the export-specific kill switch
``OOTILS_OUTBOUND_EXPORT_ENABLED`` are set (default OFF) — a SEPARATE,
narrower double-guard than ``OOTILS_DAILY_RUN_ENABLED`` (mirrors
``OOTILS_DAILY_RUN_REPORT_ENABLED`` vs ``OOTILS_DAILY_RUN_ENABLED``,
``api/routers/daily_runs.py``), so a plain ``--apply`` run that only means
"load today's feeds" never starts pushing files to the real ERP outbox
until export is explicitly turned on separately. In dry-run mode (global,
no ``--apply``) the export phase is ALWAYS a preview, printed to STDOUT —
zero file write, zero DB write, same discipline as the daily report.

AUTH: reads ``OOTILS_API_TOKEN`` from the environment, exactly like
``scripts/ingest_file.py`` — the SAME in-process bearer token, passed
through to every ``/v1/ingest/<entity>`` call the load phase makes (scope
``ingest``, per ``api/routers/ingest.py``'s ``require_scope("ingest")`` on
every route this script can reach). Not read/required in dry-run mode (the
preview never calls the API). Automatic recompute is explicitly OUT of
scope (see above) so a ``calc:run``-scoped token is never needed here.

Usage:
    DATABASE_URL=postgresql://... OOTILS_API_TOKEN=... \\
    OOTILS_DAILY_RUN_ENABLED=1 OOTILS_OUTBOUND_EXPORT_ENABLED=1 \\
    python scripts/run_daily_ingest.py \\
        [--inbox /home/debian/inbox] [--outbox /home/debian/outbox] \\
        [--date 2026-07-18] [--apply] [--allow-dev]

Exit codes: 0 the orchestrator ran to completion (including an ESCALATED
run that loaded nothing by design — check the printed decision / logs, the
L3 webhook is the real escalation channel, not this exit code); 1 --apply
refused by the kill switch or a missing OOTILS_API_TOKEN; 2 missing
DATABASE_URL, a bad --date, a bad --inbox path (does not exist), or bad
CLI args.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

import mrp_core as core

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.ingest.apply import RunDecisionStatus
from ootils_core.engine.ingest.daily_orchestrator import (
    DailyRunEvaluation,
    FeedLoadOutcome,
    apply_daily_run,
    load_eligible_feeds,
    plan_daily_run,
)
from ootils_core.engine.reconciliation.matcher import (
    ReconciliationRunResult,
    run_reconciliation,
)
from ootils_core.engine.reporting import build_shortages_summary, render_daily_report
from ootils_core.engine.reporting.outbound_export import execute_export

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("run_daily_ingest")

_DEFAULT_INBOX = "/home/debian/inbox"
_DEFAULT_OUTBOX = "/home/debian/outbox"
_SHORTAGES_TOP_N = 10


def _daily_run_enabled() -> bool:
    """Kill switch, default OFF — same truthy-set + double-guard shape as
    ``purge_maintenance.py``'s ``OOTILS_PURGE_ENABLED``."""
    return os.environ.get("OOTILS_DAILY_RUN_ENABLED", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _outbound_export_enabled() -> bool:
    """Kill switch, default OFF — same truthy-set shape as
    ``_daily_run_enabled``. A SEPARATE, narrower guard than
    ``OOTILS_DAILY_RUN_ENABLED`` (see module docstring's "OUTBOUND EXPORT"
    section): gates only whether the export phase actually WRITES, not
    whether the daily run itself applies."""
    return os.environ.get("OOTILS_OUTBOUND_EXPORT_ENABLED", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _reconciliation_enabled() -> bool:
    """Kill switch, default ON (an observation, not a destructive write —
    mirrors ``api/routers/outcomes.py``, unlike the OFF-by-default write
    switches ``OOTILS_DAILY_RUN_ENABLED``/``OOTILS_OUTBOUND_EXPORT_ENABLED``).
    Same truthy-set shape as every other kill switch in the repo."""
    return os.environ.get("OOTILS_RECONCILIATION_ENABLED", "1").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _run_reconciliation(
    conn: DictRowConnection,
    run_date: date,
    *,
    apply: bool,
) -> ReconciliationRunResult | None:
    """ADR-042 decision 4 (PR-5b) — best-effort outbound reconciliation, run
    AFTER the feeds are loaded (today's inbound POs are now in the DB). A
    failure here NEVER fails the daily run (the reconciliation is an
    observation, not part of the load contract): it is caught, rolled back so
    the shared connection stays usable for the report/export phases, logged,
    and reported as ``None`` (the daily report's section then reads "n/a").

    Only runs under ``--apply`` (in dry-run nothing was loaded, so there is
    nothing new to reconcile) and only when ``OOTILS_RECONCILIATION_ENABLED``
    is not disabled. Commits its OWN writes on success (independent of the
    export phase that follows) — the stamp + ``reconciliation_runs`` row + the
    ``reconciliation_completed`` event are one atomic observation.
    """
    if not apply:
        return None
    if not _reconciliation_enabled():
        logger.warning(
            "reconciliation.skipped reason=OOTILS_RECONCILIATION_ENABLED_disabled"
        )
        return None
    try:
        result = run_reconciliation(conn, run_date)
        conn.commit()
        logger.info(
            "reconciliation.done run_date=%s candidates=%d matched=%d ambiguous=%d "
            "unmatched=%d run_id=%s",
            run_date, result.candidates, result.matched, result.ambiguous,
            result.unmatched, result.run_id,
        )
        return result
    except Exception:  # noqa: BLE001 — best-effort observation, never fatal to the run
        conn.rollback()
        logger.warning(
            "reconciliation.failed run_date=%s — best-effort observation skipped, "
            "daily run continues", run_date, exc_info=True,
        )
        return None


def _report_evaluation(evaluation: DailyRunEvaluation) -> None:
    scan = evaluation.scan
    logger.info(
        "SCAN inbox feeds_found=%d issues=%d ignored=%d",
        len(scan.feeds), len(scan.issues), len(scan.ignored),
    )
    for feed_key, scanned in sorted(scan.feeds.items()):
        logger.info(
            "  feed_key=%-24s rows=%-6d parts=%d arrived_at=%s",
            feed_key, scanned.row_count, len(scanned.paths), scanned.file_arrived_at.isoformat(),
        )
    for feed_key, issue in sorted(scan.issues.items()):
        logger.warning("  feed_key=%-24s SCAN ISSUE: %s", feed_key, issue.error)

    if evaluation.ungoverned_feed_keys:
        logger.warning(
            "UNGOVERNED feed_keys present in inbox (no active feed_contracts row): %s",
            ", ".join(evaluation.ungoverned_feed_keys),
        )

    logger.info("GUARD VERDICTS (%d governed feed(s))", len(evaluation.feed_evaluations))
    for fe in evaluation.feed_evaluations:
        logger.info(
            "  feed_key=%-24s criticality=%-9s overall_status=%s",
            fe.feed_key, fe.contract.criticality, fe.evaluation.overall_status.value,
        )
        for result in fe.evaluation.results:
            if result.status.value != "ok":
                logger.info("    %-16s %-13s %s", result.guard_name, result.status.value, result.detail)

    if evaluation.decision is None:
        logger.error("DECISION: none computable (no active feed_contracts row evaluated)")
    else:
        logger.info(
            "DECISION status=%s feeds=%d",
            evaluation.decision.status.value, len(evaluation.decision.feeds),
        )
        for reason in evaluation.decision.reasons:
            logger.info("  %s", reason)


def _report_load_outcomes(outcomes: tuple[FeedLoadOutcome, ...]) -> None:
    by_status: dict[str, int] = {}
    for o in outcomes:
        by_status[o.status.value] = by_status.get(o.status.value, 0) + 1
    logger.info("LOAD OUTCOMES total=%d %s", len(outcomes), by_status)
    for o in outcomes:
        logger.info(
            "  feed_key=%-24s canonical=%-28s status=%-18s %s",
            o.feed_key, o.canonical or "-", o.status.value, o.detail,
        )


def _emit_daily_report(
    evaluation: DailyRunEvaluation,
    outcomes: tuple[FeedLoadOutcome, ...],
    conn: DictRowConnection,
    *,
    apply: bool,
    outbox_dir: Path,
    reconciliation: ReconciliationRunResult | None = None,
) -> None:
    """Render the deterministic daily report and either print it (dry-run —
    STDOUT ONLY, nothing written) or write it to ``--outbox`` (``--apply``).

    ``print()`` here is deliberate (not ``logger``): the report body must
    reach STDOUT byte-for-byte, with no logging prefix/timestamp interleaved
    — the CLI's own progress messages already go through ``logger``
    everywhere else in this file. ``build_shortages_summary`` is SELECT-only
    and safe to call in both modes (returns ``[]``, never raises, when the
    baseline has no completed calc_run yet). ``reconciliation`` is the
    best-effort outbound-reconciliation result (``None`` in dry-run or on
    failure — the report's section then reads "n/a").
    """
    shortages_summary = build_shortages_summary(conn, limit=_SHORTAGES_TOP_N)
    report = render_daily_report(
        evaluation,
        outcomes,
        shortages_summary=shortages_summary,
        reconciliation=reconciliation,
        generated_at=datetime.now(timezone.utc),
    )

    if not apply:
        print(report)  # noqa: T201 — deliberate: the report body, not a log line
        return

    outbox_dir.mkdir(parents=True, exist_ok=True)
    out_path = outbox_dir / f"daily_report_{evaluation.run_date:%Y%m%d}.md"
    out_path.write_text(report, encoding="utf-8")
    logger.info(
        "daily_report.written path=%s run_date=%s bytes=%d",
        out_path, evaluation.run_date, len(report.encode("utf-8")),
    )


def _run_outbound_export(
    conn: DictRowConnection,
    *,
    apply: bool,
    outbox_dir: Path,
) -> None:
    """ADR-042 decision 4 (PR-5) — the EXPORT phase, run AFTER the daily
    report. See the module docstring's "OUTBOUND EXPORT" section for the
    double-guard rationale: this phase only ever WRITES when both ``apply``
    (the CLI's ``--apply``) and ``OOTILS_OUTBOUND_EXPORT_ENABLED`` are set;
    otherwise it renders the SAME preview a real run would produce and prints
    it to STDOUT (``print()`` deliberate here too — the TSV body must reach
    STDOUT byte-for-byte, mirroring ``_emit_daily_report``).
    """
    export_apply = apply and _outbound_export_enabled()
    if apply and not export_apply:
        logger.warning(
            "outbound_export.preview_only reason=OOTILS_OUTBOUND_EXPORT_ENABLED_not_set"
        )

    result = execute_export(
        conn, outbox_dir, now=datetime.now(timezone.utc), dry_run=not export_apply
    )

    if not export_apply:
        if not result.render.files:
            logger.info("outbound_export.preview run_date=%s nothing_pending", result.run_date)
            return
        for f in result.render.files:
            print(f"--- {f.filename} ---")  # noqa: T201 — deliberate: preview body, not a log line
            print(f.content)  # noqa: T201
        return

    logger.info(
        "outbound_export.applied run_date=%s files=%s recommendations=%d event_id=%s",
        result.run_date, result.files_written, len(result.recommendation_ids_exported),
        result.event_id,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Daily governed-run orchestrator (ADR-042 PR-4b) — scans "
        "an inbox, evaluates PR-2 guards + PR-3 decision, loads the green "
        "feeds. Dry-run by default."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--inbox", default=_DEFAULT_INBOX, help=f"inbox directory (default: {_DEFAULT_INBOX})")
    p.add_argument(
        "--outbox", default=_DEFAULT_OUTBOX,
        help=f"daily-report outbox directory, --apply only (default: {_DEFAULT_OUTBOX})",
    )
    p.add_argument("--date", default=None, help="run_date as YYYY-MM-DD (default: today UTC)")
    p.add_argument("--apply", action="store_true", help="actually persist + load (default: dry-run / preview only)")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)

    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2

    if args.date is None:
        run_date = datetime.now(timezone.utc).date()
    else:
        try:
            run_date = date.fromisoformat(args.date)
        except ValueError:
            logger.error("--date %r is not a valid YYYY-MM-DD date", args.date)
            return 2

    inbox_dir = Path(args.inbox)
    outbox_dir = Path(args.outbox)

    if args.apply and not _daily_run_enabled():
        logger.error(
            "REFUSED: --apply requires OOTILS_DAILY_RUN_ENABLED=1 (got %r)",
            os.environ.get("OOTILS_DAILY_RUN_ENABLED"),
        )
        return 1

    token: str | None = None
    if args.apply:
        token = os.environ.get("OOTILS_API_TOKEN")
        if not token:
            logger.error("REFUSED: --apply requires OOTILS_API_TOKEN to be set")
            return 1

    db = core.guard_db(args.dsn, args.allow_dev)
    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(
        "Daily Ingest Orchestrator (ADR-042 PR-4b) running on DB=%s inbox=%s run_date=%s mode=%s",
        db, inbox_dir, run_date, mode,
    )

    try:
        with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
            if not args.apply:
                evaluation = plan_daily_run(conn, inbox_dir, run_date)
                _report_evaluation(evaluation)
                _emit_daily_report(evaluation, (), conn, apply=False, outbox_dir=outbox_dir)
                _run_outbound_export(conn, apply=False, outbox_dir=outbox_dir)
                logger.info("DRY-RUN — nothing persisted, nothing loaded.")
                return 0

            evaluation = apply_daily_run(conn, inbox_dir, run_date)
            conn.commit()
            _report_evaluation(evaluation)

            assert token is not None  # guarded above
            outcomes = load_eligible_feeds(evaluation, token=token, inbox_dir=inbox_dir)
            _report_load_outcomes(outcomes)
            # Reconciliation runs AFTER the load (today's inbound POs are now in
            # the DB) and BEFORE the report (so its counts feed the compte-rendu)
            # — best-effort, never fatal to the run.
            reconciliation = _run_reconciliation(conn, run_date, apply=True)
            _emit_daily_report(
                evaluation, outcomes, conn, apply=True, outbox_dir=outbox_dir,
                reconciliation=reconciliation,
            )
            _run_outbound_export(conn, apply=True, outbox_dir=outbox_dir)
    except FileNotFoundError as exc:
        logger.error("REFUSED: %s", exc)
        return 2

    if evaluation.decision is not None and evaluation.decision.status == RunDecisionStatus.ESCALATED:
        logger.error(
            "RUN ESCALATED for run_date=%s — nothing loaded, L3 webhook already notified.", run_date,
        )
    logger.info("=" * 92)
    logger.info("DAILY INGEST ORCHESTRATOR — COMPLETED (mode=%s, run_date=%s)", mode, run_date)
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
