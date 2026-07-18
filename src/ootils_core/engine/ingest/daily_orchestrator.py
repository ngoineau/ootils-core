"""
daily_orchestrator.py — the daily-run conductor (ADR-042 decision 3, "la
séquence entrante d'un run quotidien"; PR-4b of the doctrine's plan).

Wires together, end to end, everything PR-2/PR-3 already built:

  1. **Scan** an inbox directory for TODAY's dated TSV drops
     (``<feed_key>_<AAAAMMJJ>.tsv``, plus grouped ``.partNN`` siblings —
     ``interfaces.ingest_exec.parse_ingest_filename``/``find_sibling_parts``/
     ``parse_tsv_parts``, PR-4a's filename grammar).
  2. **Resolve** each scanned feed_key's active contract
     (``interfaces.contracts.list_active_contracts``/``get_active_contract``)
     — see "THE feed_key / entity_type MISMATCH" below.
  3. **Observe**: file arrival time (mtime) + row count feed the runtime
     guards (``interfaces.daily_run.record_daily_run``, PR-2).
  4. **Decide**: combine every feed's guard verdict into ONE governed
     run-level decision (``engine.ingest.apply.record_daily_run_decision``,
     PR-3) — with ``dq_status_by_feed`` deliberately EMPTY (see "DQ STATUS —
     V1 IS HONEST, NOT WIRED" below).
  5. **Gate, then load**: an ESCALATED run loads NOTHING (the L3 webhook is
     already fired by step 4's own call into
     ``notifications.l3_webhook.notify_daily_run_escalation`` — this module
     never emits a second escalation or a second ``daily_run_completed``
     event, ADR-027's one-event-per-run convention); otherwise, every feed
     whose OWN guard evaluation is green gets loaded, in FK-dependency
     order, by re-using — never duplicating — ``interfaces.ingest_exec``'s
     canonical parse/build/call/archive primitives (the same ones
     ``scripts/ingest_file.py`` uses for a manual single-file drop).

THE feed_key / entity_type MISMATCH (read before touching ``_canonical_
dispatch_name``). A governed feed's inbox filename is named after its
``feed_contracts`` row's ``feed_key`` — kebab-case by the registry's own
convention (``config/feed-contracts/on-hand.yaml``: ``feed_key: on-hand``,
so the file is ``on-hand_20260718.tsv``). ``interfaces.ingest_exec.DISPATCH``
(inherited from PR-4a) is keyed by ``entity_type`` in snake_case
(``on_hand.tsv``) — the two vocabularies do NOT coincide. The active
contract's own ``entity_type`` column is the ONLY authority for this
translation (config/feed-contracts/*.yaml wins, never a guess derived from
the filename) — ``_canonical_dispatch_name`` performs exactly this lookup.
A feed_key with NO active contract (a referential/on-demand entity that was
never given a ``feed_contracts`` row by design — ADR-042 decision 1's table
marks items/locations/suppliers/supplier_items/item_planning_params/BOM
"jamais dans le run bloquant quotidien") has no ``entity_type`` to translate
through; its raw feed_key is used AS the canonical stem directly, which is
already correct for every entity in that referential set (none of them was
ever given a kebab alias distinct from its DISPATCH key).

A DEACTIVATED CONTRACT IS NOT AN UNGOVERNED FEED. A feed_key that HAS a
``feed_contracts`` row but no currently ACTIVE version (an operator retired
it — ``interfaces.contracts``' "ACTIVE SEMANTICS") must never fall through
to the ungoverned "loaded without governance" path, which is reserved for
feed_keys the registry has genuinely never heard of.
``interfaces.contracts.list_known_feed_keys`` (every feed_key ever
registered, active or not) lets ``_quarantine_deactivated_contracts`` catch
this case explicitly: any file scanned for such a feed_key becomes a
``ScanIssue`` ("contract deactivated — refusing feed") before resolution
even runs, so it is rejected by the SAME scan-issue machinery that already
handles a malformed TSV, never loaded, and never counted in
``ungoverned_feed_keys``.

DQ STATUS — V1 IS HONEST, NOT WIRED. ``engine.ingest.apply.
record_daily_run_decision`` accepts a caller-supplied ``dq_status_by_feed``
mapping; this orchestrator always passes ``None`` (empty). Per that module's
own None-honest vocabulary, an absent DQ status is NOT_EVALUATED — which
means the run-level decision can reach ``AUTO_APPROVED`` for exactly zero
real V1 runs (NOT_EVALUATED is never silently promoted to green) and will
sit at best on ``DEGRADED`` (or ``ESCALATED`` on an actual blocking-feed
guard failure). This is a DELIBERATE, ACCEPTED consequence, not a bug to
route around — see ``engine.ingest.apply``'s own docstring, "DQ STATUS HAS
NO DB WIRING YET". Crucially, the per-feed LOAD gate below does NOT use this
combined (guard+DQ) status — it uses the feed's raw GUARD verdict
(``FeedGuardEvaluation.overall_status``, PR-2, "only ever OK or FAILED,
never NOT_EVALUATED at that column"). This is what makes DEGRADED (the
permanent V1 steady state) still a productive outcome: a run that is
DEGRADED because DQ is unwired still loads every feed whose OWN runtime
guard passed; DEGRADED only means "a human should not treat this run's
confidence as fully proven", never "nothing loaded".

BOM IS OUT OF SCOPE IN V1. ``bom_header.tsv``/``bom_components.tsv`` have no
entry in ``interfaces.ingest_exec.PAYLOAD_BUILDERS`` (only in ``DISPATCH``,
as a sentinel — the 2-file, N-payload bundle needs
``scripts.ingest_file.handle_bom_bundle``'s bespoke orchestration). Since
BOM is referential/on-demand by ADR-042 doctrine ("à la demande, jamais dans
le run bloquant quotidien"), a scanned ``bom_header`` feed is reported as
``FeedLoadStatus.UNSUPPORTED_ENTITY`` and archived to ``rejected/`` with a
clear reason — load it manually via ``scripts/ingest_file.py`` instead. The
same applies to any other entity with no registered builder (e.g.
``work_orders`` — ``open-work-orders.yaml`` declares the contract, but
``ingest_exec.PAYLOAD_BUILDERS`` has no ``work_orders.tsv`` entry yet,
despite ``POST /v1/ingest/work-orders`` existing — a capability gap this
module surfaces loudly, never bypasses).

DEPENDS_ON IS NOT TOPO-SORTED IN V1. Load ordering follows ``LOAD_ORDER``
only (the FK-respecting order mirrored from ``scripts/bulk_ingest.py``'s
``ORDERED_FILES`` — the "MANIFEST V1"), NOT each contract's own
``depends_on`` field. All 3 seed contracts declare an empty ``depends_on``,
so this is a no-op today; a contract that later declares a real dependency
needs a topological sort added here (not yet written).

SCOPE — WHAT THIS MODULE DOES NOT DO. No automatic recompute: loading data
is this module's whole job; triggering propagation/shortage detection is a
DELIBERATE, SEPARATE call the operator (or a future PR) makes afterwards —
see ``scripts/run_daily_ingest.py``'s docstring. No commit/rollback: like
every other ``interfaces``/``engine.ingest`` module in this repo, functions
here never call ``conn.commit()``/``conn.rollback()`` — the caller (the CLI)
owns the transaction. Baseline-only: every write this module drives — the
``daily_runs`` rows, the governed decision, the canonical loads via
``/v1/ingest/<entity>`` — is baseline by construction (ADR-030's rationale:
an ERP feed is an OBSERVED fact, not a fork's counter-factual); no
``scenario_id`` parameter is exposed for the load path, and
``apply_daily_run``'s own ``scenario_id`` defaults to
``BASELINE_SCENARIO_ID`` and exists only as the same test-seam
``engine.ingest.apply.record_daily_run_decision`` already carries.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.ingest.apply import (
    DailyRunDecision,
    FeedDecisionInput,
    RunDecisionStatus,
    decide_daily_run,
    record_daily_run_decision,
)
from ootils_core.interfaces.contracts import (
    FeedContract,
    list_active_contracts,
    list_known_feed_keys,
)
from ootils_core.interfaces.daily_run import (
    DailyRunObservation,
    plan_daily_run_guard_check,
    record_daily_run,
)
from ootils_core.interfaces.guards import FeedGuardEvaluation, GuardStatus, evaluate_feed_guards
from ootils_core.interfaces.ingest_exec import (
    DISPATCH,
    PAYLOAD_BUILDERS,
    ParsedFilename,
    archive,
    archive_group,
    call_api,
    find_sibling_parts,
    parse_ingest_filename,
    parse_tsv,
    parse_tsv_parts,
)

logger = logging.getLogger(__name__)

# FK-respecting load order — mirrors scripts/bulk_ingest.py's ORDERED_FILES
# (the "MANIFEST V1"). Kept in lockstep by CONVENTION, not by import:
# scripts/ is outside the installed package boundary (same reason
# ingest_exec.py exists at all — see module docstring), so this list is a
# deliberate, documented duplicate of a short constant, not a second
# canonical WRITER (nothing here re-implements bulk_ingest.py's loaders).
# A canonical filename absent from this tuple sorts LAST (see
# `_load_order_key`), never refused outright — but
# `test_every_registered_builder_has_an_explicit_load_order_slot`
# (tests/test_daily_orchestrator.py) still requires every
# `interfaces.ingest_exec.PAYLOAD_BUILDERS` key to have an EXPLICIT slot
# here, so a new entity is never silently pushed to "last" by omission.
#
# `distribution_links.tsv` (DESC-1 PR-D) is a DELIBERATE, DOCUMENTED
# divergence from scripts/bulk_ingest.py's ORDERED_FILES — that script has
# no `load_distribution_links` loader yet (a separate, DB-direct-write
# implementation, out of this chantier's scope), so mirroring it exactly
# is not possible today. Positioned right after the other referential/
# topology entries (items/locations/suppliers/supplier_items/
# item_planning_params) since its own FKs are items (optional) + locations
# only.
LOAD_ORDER: tuple[str, ...] = (
    "items.tsv",
    "locations.tsv",
    "suppliers.tsv",
    "supplier_items.tsv",
    "item_planning_params.tsv",
    "distribution_links.tsv",
    "on_hand.tsv",
    "purchase_orders.tsv",
    "customer_orders.tsv",
    "transfers.tsv",
    "forecasts.tsv",
    "bom_header.tsv",
)


def _mtime_utc(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


# ─────────────────────────────────────────────────────────────
# 1. Inbox scan — pure filesystem + parsing, zero DB
# ─────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ScannedFeedFile:
    """One feed_key's file(s) found in the inbox for one ``run_date`` — a
    single file, or N ``.partNN`` siblings already grouped ascending by
    ``find_sibling_parts``, parsed once (headers + rows kept in memory so
    the later load phase never re-reads the file from disk)."""

    feed_key: str
    paths: tuple[Path, ...]
    file_arrived_at: datetime  # max mtime across paths, UTC-aware
    headers: tuple[str, ...]
    rows: tuple[dict[str, str], ...]

    @property
    def row_count(self) -> int:
        return len(self.rows)


@dataclass(frozen=True)
class ScanIssue:
    """A feed_key's file(s) that could not be turned into a
    ``ScannedFeedFile`` — a malformed part grouping (duplicate part number)
    or a TSV parse error (bad column count, empty file, ...). The file(s)
    still exist and their arrival time is still knowable — only their
    CONTENT is unusable, so ``row_count`` stays ``None`` (never a fabricated
    0) for the guard evaluation, and the load phase always rejects them."""

    feed_key: str
    paths: tuple[Path, ...]
    file_arrived_at: datetime
    error: str


@dataclass(frozen=True)
class InboxScan:
    """The result of one ``scan_inbox`` call."""

    run_date: date
    feeds: dict[str, ScannedFeedFile]
    issues: dict[str, ScanIssue]
    ignored: tuple[Path, ...]  # not dated for run_date, or not a '.tsv' name


def scan_inbox(inbox_dir: Path, run_date: date) -> InboxScan:
    """Scan ``inbox_dir`` (non-recursive) for every file dated ``run_date``
    under the daily-drop grammar (``<feed_key>_<AAAAMMJJ>.tsv``, plus
    ``.partNN`` groups sharing that date) and parse each into a
    ``ScannedFeedFile``. Pure filesystem + parsing — no DB, no network,
    never mutates the directory (files are archived only by the later load
    phase, and only under ``--apply``).

    A canonical (dateless) file, a file for a different date, or a name that
    doesn't parse as ``<entity>[...].tsv`` at all is collected in
    ``InboxScan.ignored`` rather than raising — this scan targets exactly
    ONE day's drop; anything else is simply not today's business (a
    canonical file is the ``scripts/ingest_file.py`` manual/on-demand path).

    Raises ``FileNotFoundError`` if ``inbox_dir`` does not exist or is not a
    directory — a missing inbox is a hard configuration error, never an
    "empty scan".
    """
    if not inbox_dir.is_dir():
        raise FileNotFoundError(f"scan_inbox: inbox directory does not exist: {inbox_dir}")

    date_str = run_date.strftime("%Y%m%d")
    feeds: dict[str, ScannedFeedFile] = {}
    issues: dict[str, ScanIssue] = {}
    ignored: list[Path] = []
    handled: set[Path] = set()

    candidates = sorted(p for p in inbox_dir.iterdir() if p.is_file())
    for path in candidates:
        if path in handled:
            continue
        try:
            parsed: ParsedFilename = parse_ingest_filename(path.name)
        except ValueError:
            ignored.append(path)
            continue
        if parsed.date != date_str:
            ignored.append(path)
            continue

        feed_key = parsed.entity
        if feed_key in feeds or feed_key in issues:
            # Two distinct file groups both resolving to the same feed_key
            # for the same run_date — ambiguous, fail loudly and refuse BOTH:
            # the group already accepted is EVICTED from `feeds` (revue PR-4b:
            # le laisser chargeable contredisait « refusing both »).
            prior = feeds.pop(feed_key, None)
            prior_paths = prior.paths if prior is not None else ()
            existing_issue = issues.get(feed_key)
            if existing_issue is not None:
                prior_paths = (*existing_issue.paths, *prior_paths)
            issues[feed_key] = ScanIssue(
                feed_key=feed_key,
                paths=(*prior_paths, path),
                file_arrived_at=_mtime_utc(path),
                error=(
                    f"feed_key {feed_key!r} matched more than one file group "
                    f"for run_date={run_date} — ambiguous, refusing both"
                ),
            )
            handled.add(path)
            continue

        if parsed.part is not None:
            try:
                group = find_sibling_parts(path)
            except (FileNotFoundError, ValueError) as exc:
                issues[feed_key] = ScanIssue(
                    feed_key=feed_key,
                    paths=(path,),
                    file_arrived_at=_mtime_utc(path),
                    error=str(exc),
                )
                handled.add(path)
                continue
        else:
            group = [path]

        for p in group:
            handled.add(p)

        arrived_at = max(_mtime_utc(p) for p in group)
        try:
            if len(group) == 1:
                headers, rows = parse_tsv(group[0])
            else:
                headers, rows = parse_tsv_parts(group)
        except (FileNotFoundError, ValueError) as exc:
            issues[feed_key] = ScanIssue(
                feed_key=feed_key, paths=tuple(group), file_arrived_at=arrived_at, error=str(exc),
            )
            continue

        feeds[feed_key] = ScannedFeedFile(
            feed_key=feed_key,
            paths=tuple(group),
            file_arrived_at=arrived_at,
            headers=tuple(headers),
            rows=tuple(rows),
        )

    logger.info(
        "daily_orchestrator.scan_complete inbox=%s run_date=%s feeds=%d issues=%d ignored=%d",
        inbox_dir, run_date, len(feeds), len(issues), len(ignored),
    )
    return InboxScan(run_date=run_date, feeds=feeds, issues=issues, ignored=tuple(ignored))


# ─────────────────────────────────────────────────────────────
# 2-4. Resolve + observe + decide
# ─────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class FeedRunEvaluation:
    """One GOVERNED feed's guard evaluation for this run — either a REAL
    persisted ``daily_runs`` row (``apply_daily_run``, ``daily_run_id``
    set) or a PURE, zero-write preview (``plan_daily_run``,
    ``daily_run_id`` is ``None``). Only feeds with an active
    ``feed_contracts`` row get one of these — see ``DailyRunEvaluation.
    ungoverned_feed_keys`` for everything else found in the inbox."""

    feed_key: str
    contract: FeedContract
    observation: DailyRunObservation
    evaluation: FeedGuardEvaluation
    daily_run_id: UUID | None


@dataclass(frozen=True)
class DailyRunEvaluation:
    """The full guard + governed-decision picture for one ``run_date`` —
    the return value of both ``plan_daily_run`` (preview) and
    ``apply_daily_run`` (the sole writer); ``is_applied`` tells them apart.
    """

    run_date: date
    is_applied: bool
    scan: InboxScan
    feed_evaluations: tuple[FeedRunEvaluation, ...]
    ungoverned_feed_keys: tuple[str, ...]
    decision: DailyRunDecision | None  # None iff zero feeds were evaluated at all


def _observation_for(feed_key: str, scan: InboxScan) -> DailyRunObservation:
    """The caller-supplied observation `record_daily_run`/`evaluate_feed_
    guards` need for one feed_key: file arrival time + row count when a
    scan produced usable rows, arrival time only (row_count unknown, never
    fabricated) when the scan hit a parse issue, or nothing at all when no
    file was found for this feed_key on this run_date (the "flux totalement
    absent" case the arrival-window guard exists to catch).
    """
    scanned = scan.feeds.get(feed_key)
    if scanned is not None:
        return DailyRunObservation(file_arrived_at=scanned.file_arrived_at, row_count=scanned.row_count)
    issue = scan.issues.get(feed_key)
    if issue is not None:
        # Present-but-unusable file (unparseable TSV, ambiguous groups, bad
        # parts). row_count=0 is the HONEST measure here — the file arrived
        # and carries ZERO exploitable rows (distinct from the absent-file
        # case below, where nothing was measured → None). Consequence by the
        # EXISTING pure guard machinery: volume floor FAILED → a blocking
        # feed escalates the whole run (ADR-042: charger les flux verts
        # contre un blocking corrompu = image déchirée), an advisory feed is
        # excluded. Revue PR-4b : sans ce 0, un blocking corrompu tombait en
        # NOT_EVALUATED → DEGRADED et laissait charger les autres flux.
        return DailyRunObservation(file_arrived_at=issue.file_arrived_at, row_count=0)
    return DailyRunObservation(file_arrived_at=None, row_count=None)


def _quarantine_deactivated_contracts(
    scan: InboxScan, governed_keys: set[str], known_feed_keys: set[str],
) -> InboxScan:
    """A feed_key REGISTERED in ``feed_contracts`` (it has at least one
    version, historical or current) but with NO currently active version is
    a DEACTIVATED contract — distinct from a feed_key the registry has
    never heard of. It must never fall back to the ungoverned "loaded
    without governance" path (revue PR-4b, finding 3): any file scanned for
    such a feed_key — whether it parsed cleanly into ``scan.feeds`` or was
    already unusable in ``scan.issues`` — is turned into (or kept as) an
    explicit ``ScanIssue`` so the load phase's existing scan-issue handling
    rejects it uniformly, and it never appears in ``ungoverned_feed_keys``.
    """
    deactivated = (set(scan.feeds) | set(scan.issues)) & (known_feed_keys - governed_keys)
    if not deactivated:
        return scan

    feeds = dict(scan.feeds)
    issues = dict(scan.issues)
    for feed_key in deactivated:
        scanned = feeds.pop(feed_key, None)
        if scanned is None:
            # Already a ScanIssue for another reason (e.g. malformed TSV) —
            # stays as-is: already excluded from load, already excluded from
            # `ungoverned` by the known_feed_keys subtraction below.
            continue
        logger.error(
            "daily_orchestrator.contract_deactivated feed_key=%s run_date=%s — "
            "feed_contracts row exists but has no active version, refusing feed",
            feed_key, scan.run_date,
        )
        issues[feed_key] = ScanIssue(
            feed_key=feed_key,
            paths=scanned.paths,
            file_arrived_at=scanned.file_arrived_at,
            error=(
                f"feed_key {feed_key!r} — contract deactivated — refusing feed "
                "(registered in feed_contracts but no active version)"
            ),
        )

    return InboxScan(run_date=scan.run_date, feeds=feeds, issues=issues, ignored=scan.ignored)


def _resolve_ungoverned(scan: InboxScan, known_feed_keys: set[str]) -> tuple[str, ...]:
    """Feed_keys found in the inbox that the registry has NEVER heard of at
    all (``known_feed_keys`` already covers active AND deactivated
    versions, so subtracting it also excludes a deactivated contract's
    feed_key — that one is surfaced separately by
    ``_quarantine_deactivated_contracts``, never here)."""
    scanned_keys = set(scan.feeds) | set(scan.issues)
    ungoverned = tuple(sorted(scanned_keys - known_feed_keys))
    for feed_key in ungoverned:
        logger.warning(
            "daily_orchestrator.no_contract feed_key=%s run_date=%s — file present "
            "in inbox but no feed_contracts row at all (referential/on-demand "
            "entity, or an undeclared feed — loaded without governance if the "
            "run is not escalated)",
            feed_key, scan.run_date,
        )
    return ungoverned


def plan_daily_run(
    conn: DictRowConnection,
    inbox_dir: Path,
    run_date: date,
    *,
    now: datetime | None = None,
) -> DailyRunEvaluation:
    """Read-only PREVIEW of one run_date's guard verdicts + governed
    decision: ZERO writes, ZERO events, ZERO L3 webhook calls — safe to call
    from a dry-run CLI invocation or a future read-only endpoint. Reads the
    active contracts + each feed's previous ``daily_runs`` row (SELECT
    only, via ``plan_daily_run_guard_check``) but never INSERTs.
    """
    evaluated_at = now if now is not None else datetime.now(timezone.utc)
    scan = scan_inbox(inbox_dir, run_date)
    contracts = list_active_contracts(conn)
    known_feed_keys = list_known_feed_keys(conn)
    governed_keys = {c.feed_key for c in contracts}
    scan = _quarantine_deactivated_contracts(scan, governed_keys, known_feed_keys)
    ungoverned = _resolve_ungoverned(scan, known_feed_keys)

    feed_evaluations: list[FeedRunEvaluation] = []
    feed_inputs: list[FeedDecisionInput] = []
    for contract in contracts:
        observation = _observation_for(contract.feed_key, scan)
        guard_plan = plan_daily_run_guard_check(conn, contract.feed_key, run_date)
        evaluation = evaluate_feed_guards(
            feed_key=contract.feed_key,
            criticality=contract.criticality,
            cadence=contract.cadence,
            arrival_window_minutes=contract.arrival_window_minutes,
            volume_guard_min_rows=contract.volume_guard_min_rows,
            volume_guard_max_pct_delta=contract.volume_guard_max_pct_delta,
            run_date=run_date,
            file_arrived_at=observation.file_arrived_at,
            row_count=observation.row_count,
            previous_row_count=guard_plan.previous_row_count,
            deleted_count=observation.deleted_count,
            previous_active_count=observation.previous_active_count,
            now=evaluated_at,
        )
        feed_evaluations.append(
            FeedRunEvaluation(
                feed_key=contract.feed_key, contract=contract, observation=observation,
                evaluation=evaluation, daily_run_id=None,
            )
        )
        feed_inputs.append(
            FeedDecisionInput(
                feed_key=contract.feed_key, criticality=contract.criticality,
                guard_status=evaluation.overall_status, dq_status=None,
            )
        )

    decision = decide_daily_run(feed_inputs, run_date, evaluated_at=evaluated_at) if feed_inputs else None
    return DailyRunEvaluation(
        run_date=run_date, is_applied=False, scan=scan,
        feed_evaluations=tuple(feed_evaluations), ungoverned_feed_keys=ungoverned, decision=decision,
    )


def apply_daily_run(
    conn: DictRowConnection,
    inbox_dir: Path,
    run_date: date,
    *,
    now: datetime | None = None,
    scenario_id: UUID = BASELINE_SCENARIO_ID,
    source: str = "ingestion",
    webhook_url: str | None = None,
) -> DailyRunEvaluation:
    """The SOLE writer of the guard+decision phase: persists one
    ``daily_runs`` row per active contract (``interfaces.daily_run.
    record_daily_run``, PR-2) then the governed run-level decision
    (``engine.ingest.apply.record_daily_run_decision``, PR-3 — emits the
    ONE ``daily_run_completed`` event and, on ESCALATED, the L3 webhook).

    ``source`` feeds ``events.source`` (migration 002 CHECK: 'api' |
    'ingestion' | 'engine' | 'user' | 'test') — defaults to ``'ingestion'``,
    the accurate classification for this pipeline (NOT a free-text CLI
    attribution string; passing anything outside that vocabulary raises
    ``ValueError`` inside ``emit_stream_event``).

    ``dq_status_by_feed`` is always ``None`` here — see module docstring
    "DQ STATUS — V1 IS HONEST, NOT WIRED".

    Does NOT commit — the caller (the CLI) owns the transaction. Does NOT
    load anything — call ``load_eligible_feeds`` with this function's return
    value next.
    """
    evaluated_at = now if now is not None else datetime.now(timezone.utc)
    scan = scan_inbox(inbox_dir, run_date)
    contracts = list_active_contracts(conn)
    known_feed_keys = list_known_feed_keys(conn)
    governed_keys = {c.feed_key for c in contracts}
    scan = _quarantine_deactivated_contracts(scan, governed_keys, known_feed_keys)
    ungoverned = _resolve_ungoverned(scan, known_feed_keys)

    feed_evaluations: list[FeedRunEvaluation] = []
    for contract in contracts:
        observation = _observation_for(contract.feed_key, scan)
        record = record_daily_run(conn, contract.feed_key, run_date, observation, now=evaluated_at)
        feed_evaluations.append(
            FeedRunEvaluation(
                feed_key=contract.feed_key, contract=contract, observation=observation,
                evaluation=record.evaluation, daily_run_id=record.daily_run_id,
            )
        )

    decision: DailyRunDecision | None
    if feed_evaluations:
        decision = record_daily_run_decision(
            conn, run_date, dq_status_by_feed=None, now=evaluated_at,
            scenario_id=scenario_id, source=source, webhook_url=webhook_url,
        )
    else:
        logger.error(
            "daily_orchestrator.no_active_contracts run_date=%s — nothing to "
            "evaluate, no governed decision possible", run_date,
        )
        decision = None

    return DailyRunEvaluation(
        run_date=run_date, is_applied=True, scan=scan,
        feed_evaluations=tuple(feed_evaluations), ungoverned_feed_keys=ungoverned, decision=decision,
    )


# ─────────────────────────────────────────────────────────────
# 5. Gate, then load
# ─────────────────────────────────────────────────────────────
class FeedLoadStatus(str, Enum):
    """One feed's outcome for the load phase. Explicit vocabulary — never a
    bare string — so a caller/report can branch on it without guessing."""

    LOADED = "loaded"
    REJECTED = "rejected"                    # API returned a non-2xx status
    API_CRASH = "api_crash"                  # call_api raised
    SCAN_ERROR = "scan_error"                # parse/grouping failure — archived to rejected
    GUARD_FAILED = "guard_failed"            # this feed's own guard evaluation FAILED
    NO_FILE = "no_file"                      # governed feed, no file found today
    UNSUPPORTED_ENTITY = "unsupported_entity"  # no ingest_exec builder registered
    RUN_ESCALATED = "run_escalated"          # top-level gate: whole run blocked


@dataclass(frozen=True)
class FeedLoadOutcome:
    """One feed's outcome for the load phase — always emitted, even for a
    feed that was never attempted (``NO_FILE``, ``RUN_ESCALATED``, ...), so
    a daily report can list every expected feed's fate, not just the
    successful ones."""

    feed_key: str
    canonical: str | None
    status: FeedLoadStatus
    http_status: int | None
    detail: str


def _canonical_dispatch_name(feed_key: str, contract: FeedContract | None) -> str:
    """The ``ingest_exec.DISPATCH``/``PAYLOAD_BUILDERS`` lookup key for one
    feed — see module docstring "THE feed_key / entity_type MISMATCH"."""
    if contract is not None:
        return f"{contract.entity_type}.tsv"
    return f"{feed_key}.tsv"


def _load_order_key(canonical: str) -> tuple[int, str]:
    try:
        idx = LOAD_ORDER.index(canonical)
    except ValueError:
        idx = len(LOAD_ORDER)
    return (idx, canonical)


def _archive_one_or_group(paths: tuple[Path, ...], dest_dir: Path, report: dict[str, Any]) -> None:
    if len(paths) == 1:
        archive(paths[0], dest_dir, report)
    else:
        archive_group(list(paths), dest_dir, report)


def load_eligible_feeds(
    evaluation: DailyRunEvaluation,
    *,
    token: str,
    inbox_dir: Path,
    processed_dir: Path | None = None,
    rejected_dir: Path | None = None,
) -> tuple[FeedLoadOutcome, ...]:
    """Load every "green" feed found by ``apply_daily_run`` — reusing
    ``interfaces.ingest_exec``'s parse/build/call/archive primitives, the
    SAME ones ``scripts/ingest_file.py`` uses for a manual drop (zero
    second canonical writer).

    GATING (ADR-042 decision 3 step 7, "tout-ou-rien"): if the governed
    decision could not be computed at all, or is ESCALATED, NOTHING is
    attempted — every candidate feed_key gets a ``RUN_ESCALATED`` outcome
    for visibility, and the L3 webhook has ALREADY fired inside
    ``apply_daily_run`` (this function never notifies). Otherwise, a feed is
    loaded iff (a) its scan produced usable rows AND (b) it is either
    UNGOVERNED (no active contract — see module docstring) or its OWN
    guard evaluation is ``GuardStatus.OK`` — a FAILED guard always excludes
    THAT feed from THIS run's load, blocking or advisory alike (advisory
    only changes whether the run-level decision escalates, never whether
    this feed's suspect data gets loaded). Feeds are loaded in
    ``LOAD_ORDER`` (FK dependency order).

    ``processed_dir``/``rejected_dir`` default to ``inbox_dir.parent /
    "processed"``/``"rejected"`` — siblings of the inbox, mirroring
    ``scripts/ingest_file.py``'s own ``data/{inbox,processed,rejected}``
    convention, generalized to whatever ``--inbox`` path is given.

    Raises ``ValueError`` if ``evaluation.is_applied`` is ``False`` — a
    dry-run preview (``plan_daily_run``) must never be used to drive a real
    load.
    """
    if not evaluation.is_applied:
        raise ValueError(
            "load_eligible_feeds: refusing to load from a dry-run "
            "(plan_daily_run) evaluation — call apply_daily_run first"
        )

    processed_dir = processed_dir if processed_dir is not None else inbox_dir.parent / "processed"
    rejected_dir = rejected_dir if rejected_dir is not None else inbox_dir.parent / "rejected"

    outcomes: list[FeedLoadOutcome] = []
    scan = evaluation.scan

    if evaluation.decision is None or evaluation.decision.status == RunDecisionStatus.ESCALATED:
        reason = (
            "no governed decision could be computed (no active feed_contracts row)"
            if evaluation.decision is None
            else f"run ESCALATED: {'; '.join(evaluation.decision.reasons) or 'blocking feed guard failure'}"
        )
        logger.error(
            "daily_orchestrator.run_blocked run_date=%s reason=%s", evaluation.run_date, reason,
        )
        # Every candidate feed_key gets a RUN_ESCALATED outcome, including a
        # governed feed absent from today's inbox — a complete picture of
        # "nothing was loaded", not just the ones that had a file.
        all_feed_keys = (
            set(scan.feeds) | set(scan.issues) | {fe.feed_key for fe in evaluation.feed_evaluations}
        )
        for feed_key in sorted(all_feed_keys):
            outcomes.append(
                FeedLoadOutcome(feed_key=feed_key, canonical=None, status=FeedLoadStatus.RUN_ESCALATED,
                                 http_status=None, detail=reason)
            )
        return tuple(outcomes)

    guard_by_feed = {fe.feed_key: fe for fe in evaluation.feed_evaluations}

    # Scan issues are structurally unloadable regardless of governance —
    # always rejected, archived so tomorrow's scan (a different run_date)
    # never re-discovers the same broken drop.
    for feed_key, issue in scan.issues.items():
        # The issue.error already names its own cause (parse failure,
        # ambiguous groups, deactivated contract…) — no cause-prefix here,
        # it would lie for the non-parse cases.
        detail = f"scan issue: {issue.error}"
        logger.error(
            "daily_orchestrator.scan_error feed_key=%s run_date=%s detail=%s",
            feed_key, evaluation.run_date, detail,
        )
        report: dict[str, Any] = {
            "feed_key": feed_key, "run_date": evaluation.run_date.isoformat(),
            "outcome": "scan_error", "error": issue.error,
            "source_files": [p.name for p in issue.paths],
        }
        _archive_one_or_group(issue.paths, rejected_dir, report)
        outcomes.append(
            FeedLoadOutcome(feed_key=feed_key, canonical=None, status=FeedLoadStatus.SCAN_ERROR,
                             http_status=None, detail=detail)
        )

    candidates: list[tuple[str, str]] = []  # (feed_key, canonical)
    for feed_key in scan.feeds:
        fe = guard_by_feed.get(feed_key)
        contract = fe.contract if fe is not None else None
        if fe is not None and fe.evaluation.overall_status != GuardStatus.OK:
            reasons = "; ".join(r.detail for r in fe.evaluation.results if r.status == GuardStatus.FAILED)
            logger.warning(
                "daily_orchestrator.guard_failed feed_key=%s run_date=%s reasons=%s",
                feed_key, evaluation.run_date, reasons,
            )
            outcomes.append(
                FeedLoadOutcome(feed_key=feed_key, canonical=None, status=FeedLoadStatus.GUARD_FAILED,
                                 http_status=None, detail=reasons)
            )
            continue
        candidates.append((feed_key, _canonical_dispatch_name(feed_key, contract)))

    # Governed feeds expected (active contract) but absent from the scan.
    scanned_feed_keys = set(scan.feeds) | set(scan.issues)
    for fe in evaluation.feed_evaluations:
        if fe.feed_key in scanned_feed_keys:
            continue
        logger.info(
            "daily_orchestrator.no_file feed_key=%s run_date=%s", fe.feed_key, evaluation.run_date,
        )
        outcomes.append(
            FeedLoadOutcome(feed_key=fe.feed_key, canonical=None, status=FeedLoadStatus.NO_FILE,
                             http_status=None, detail="no file found in inbox for this run_date")
        )

    candidates.sort(key=lambda pair: _load_order_key(pair[1]))

    for feed_key, canonical in candidates:
        scanned = scan.feeds[feed_key]
        if canonical not in PAYLOAD_BUILDERS or canonical not in DISPATCH:
            detail = (
                f"no ingest builder registered for canonical={canonical!r} "
                f"(feed_key={feed_key!r}) — see module docstring 'BOM IS OUT OF SCOPE IN V1'"
            )
            logger.error(
                "daily_orchestrator.unsupported_entity feed_key=%s run_date=%s detail=%s",
                feed_key, evaluation.run_date, detail,
            )
            report = {
                "feed_key": feed_key, "canonical": canonical, "run_date": evaluation.run_date.isoformat(),
                "outcome": "unsupported_entity", "error": detail,
            }
            _archive_one_or_group(scanned.paths, rejected_dir, report)
            outcomes.append(
                FeedLoadOutcome(feed_key=feed_key, canonical=canonical, status=FeedLoadStatus.UNSUPPORTED_ENTITY,
                                 http_status=None, detail=detail)
            )
            continue

        cfg = DISPATCH[canonical]
        builder = PAYLOAD_BUILDERS[canonical]
        try:
            payload = builder(list(scanned.rows), False)
        except ValueError as exc:
            detail = f"payload build error: {exc}"
            logger.error(
                "daily_orchestrator.build_error feed_key=%s run_date=%s detail=%s",
                feed_key, evaluation.run_date, detail,
            )
            report = {
                "feed_key": feed_key, "canonical": canonical, "run_date": evaluation.run_date.isoformat(),
                "outcome": "build_error", "error": detail,
            }
            _archive_one_or_group(scanned.paths, rejected_dir, report)
            outcomes.append(
                FeedLoadOutcome(feed_key=feed_key, canonical=canonical, status=FeedLoadStatus.SCAN_ERROR,
                                 http_status=None, detail=detail)
            )
            continue

        try:
            status_code, body = call_api(cfg["endpoint"], payload, token)
        except Exception:  # noqa: BLE001 — mirrors ingest_file.py's own api-crash carve-out
            logger.exception(
                "daily_orchestrator.api_crash feed_key=%s run_date=%s endpoint=%s",
                feed_key, evaluation.run_date, cfg["endpoint"],
            )
            report = {
                "feed_key": feed_key, "canonical": canonical, "endpoint": cfg["endpoint"],
                "run_date": evaluation.run_date.isoformat(), "outcome": "api_crash",
            }
            _archive_one_or_group(scanned.paths, rejected_dir, report)
            outcomes.append(
                FeedLoadOutcome(feed_key=feed_key, canonical=canonical, status=FeedLoadStatus.API_CRASH,
                                 http_status=None, detail="call_api raised — see logs")
            )
            continue

        accepted = 200 <= status_code < 300
        summary = body.get("summary", {}) if isinstance(body, dict) else {}
        report = {
            "feed_key": feed_key, "canonical": canonical, "endpoint": cfg["endpoint"],
            "run_date": evaluation.run_date.isoformat(), "outcome": "ok" if accepted else "rejected",
            "http_status": status_code, "rows_parsed": len(scanned.rows), "api_summary": summary,
            "api_response": body,
        }
        dest = processed_dir if accepted else rejected_dir
        _archive_one_or_group(scanned.paths, dest, report)

        status = FeedLoadStatus.LOADED if accepted else FeedLoadStatus.REJECTED
        detail = f"HTTP {status_code}" if accepted else f"HTTP {status_code}, summary={summary}"
        logger.info(
            "daily_orchestrator.load feed_key=%s run_date=%s canonical=%s status=%s http_status=%d",
            feed_key, evaluation.run_date, canonical, status.value, status_code,
        )
        outcomes.append(
            FeedLoadOutcome(feed_key=feed_key, canonical=canonical, status=status,
                             http_status=status_code, detail=detail)
        )

    return tuple(outcomes)
