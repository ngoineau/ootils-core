"""
daily_report.py — the daily-run compte-rendu (ADR-042 decision 3 §5, PR-4c of
the pilot-decided delivery order; this is the "daily update via la Dropbox"
the pilot explicitly asked for on 2026-07-17 — see the ERP-canal-Dropbox
memory note).

Two halves, split the same way every other DB-boundary module in this repo
is split (``engine/maintenance/purge.py``'s ``plan_*``/``apply_*``,
``engine/snapshot/capture.py``'s capture/persist):

  * ``render_daily_report`` — the renderer. DETERMINISTIC and DB-FREE: given
    the SAME ``DailyRunEvaluation``/``load_outcomes``/``shortages_summary``/
    ``generated_at``, it returns byte-identical Markdown, every time, on
    every machine. No wall-clock read (``generated_at`` is always
    caller-supplied — same discipline as ``interfaces/guards.py``'s ``now``
    parameter), no DB call, no locale-dependent formatting (no ``locale``
    module, no system-timezone-dependent ``astimezone()`` on a naive
    datetime — see ``_fmt_dt``). Safe to call from a read-only endpoint or a
    dry-run CLI preview alike.
  * ``build_shortages_summary`` — the ONE DB-touching helper in this module.
    SELECT-only, never commits/rolls back (the caller owns the transaction,
    same convention as every other engine module here). Produces the plain
    ``list[dict]`` the renderer's ``shortages_summary`` parameter expects —
    the renderer itself never queries the DB, so this split keeps
    "computing the top-N pénuries" and "laying out Markdown" independently
    testable (a golden-Markdown test needs no DB fixture at all).

WHY THE RENDERER TAKES NO CONNECTION: the whole point of a daily report an
ERP team can trust is that re-running it against the SAME inputs produces
the SAME file — useful for diffing two runs, and for the CLI to print an
IDENTICAL preview to stdout (dry-run) vs write to the outbox (--apply)
without a hidden second DB round-trip in between that could observe a
different DB state (a race the pilot's "coupe-circuit" doctrine explicitly
distrusts). Every fact the report needs is gathered by the caller BEFORE
calling ``render_daily_report`` — ``evaluation`` (``plan_daily_run``/
``apply_daily_run``, ``engine/ingest/daily_orchestrator.py``),
``load_outcomes`` (``load_eligible_feeds`` — an EMPTY tuple for a dry-run
preview, since nothing is loaded there by construction), and
``shortages_summary`` (this module's own ``build_shortages_summary``, or
``None`` when the caller chooses not to compute it).

CONTENT AUDIENCE: the ERP team, not an Ootils engineer. French, professional,
plain — no internal jargon (guard names, enum values) surfacing unexplained;
every status is spelled out in clear French with an honest one-line
explanation, including the DEGRADED steady-state the pilot specifically
asked to be explained honestly rather than alarmingly (ADR-042/ADR-037: DQ
wiring is a KNOWN, ACCEPTED V1 gap, not a fault).

NO JSONB, NO EVENT EMISSION: this module reads/renders already-computed
facts and writes nothing to Postgres — it does not touch ``events`` at all.
The ``daily_run_completed`` event this report describes was already emitted
by ``engine/ingest/apply.py:record_daily_run_decision`` (migration 079) —
this module only REFERENCES that fact in its footer (see
``_render_footer``), it never emits a second one (ADR-027's one-event-per-run
convention would be violated by a duplicate). The report's own ``event_id``
is not available here since the renderer takes no connection and
``record_daily_run_decision`` does not return one to
``apply_daily_run``/``plan_daily_run`` — the footer points the reader at
``GET /v1/daily-runs`` (``api/routers/daily_runs.py``) instead of fabricating
a UUID it does not have.
"""
from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any, Optional

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.ingest.apply import DailyRunDecision, RunDecisionStatus
from ootils_core.engine.ingest.daily_orchestrator import (
    DailyRunEvaluation,
    FeedLoadOutcome,
    FeedLoadStatus,
    FeedRunEvaluation,
)
from ootils_core.engine.scenario.manager import ScenarioManager
from ootils_core.interfaces.guards import compute_expected_arrival_deadline

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# French vocabulary — one place, so the report's wording never drifts
# accidentally across functions.
# ---------------------------------------------------------------------------

_DECISION_LABELS: dict[str, str] = {
    RunDecisionStatus.AUTO_APPROVED.value: "AUTO_APPROVED",
    RunDecisionStatus.DEGRADED.value: "DEGRADED",
    RunDecisionStatus.ESCALATED.value: "ESCALATED",
}

_DECISION_EXPLANATIONS: dict[str, str] = {
    RunDecisionStatus.AUTO_APPROVED.value: (
        "Tous les flux attendus ont passé leurs contrôles avec succès — le "
        "run est approuvé automatiquement, aucune intervention n'est requise."
    ),
    RunDecisionStatus.DEGRADED.value: (
        "Contrôles qualité non encore câblés, confiance bornée — les flux à "
        "gardes vertes ont bien été chargés."
    ),
    RunDecisionStatus.ESCALATED.value: (
        "Au moins un flux critique (blocking) a échoué son contrôle — le run "
        "est bloqué, rien n'a été chargé pour ce run, et une alerte a été "
        "envoyée à un humain (webhook L3)."
    ),
}

_NO_DECISION_EXPLANATION = (
    "Aucune décision gouvernée n'a pu être calculée pour cette date — aucun "
    "flux actif n'a été évalué (aucun contrat de flux actif, ou aucun fichier "
    "traité)."
)

# GuardStatus.value -> French label, per guard_name (interfaces/guards.py's
# four runtime guards). Kept as raw strings (not the GuardStatus enum) so this
# module has zero import coupling to interfaces/guards.py beyond
# compute_expected_arrival_deadline — the vocabulary is a lockstep mirror of
# GuardStatus's three values ('ok' | 'failed' | 'not_evaluated').
_GUARD_LABELS: dict[str, dict[str, str]] = {
    "arrival_window": {
        "ok": "à l'heure",
        "failed": "en retard",
        "not_evaluated": "non évalué",
    },
    "volume_floor": {
        "ok": "volume OK",
        "failed": "sous le plancher",
        "not_evaluated": "non évalué",
    },
    "volume_delta": {
        "ok": "delta OK",
        "failed": "delta anormal",
        "not_evaluated": "non évalué",
    },
    "deletion_ratio": {
        "ok": "suppressions OK",
        "failed": "suppressions anormales",
        "not_evaluated": "non évalué",
    },
}

# FeedLoadStatus -> French decision label. Rooted in the 4 canonical words the
# architect specified (charge / exclu / bloque / rejete scan), qualified for
# every FeedLoadStatus variant so the mapping is total (never a KeyError on a
# real outcome) and each row still names its precise cause.
_LOAD_STATUS_LABELS: dict[FeedLoadStatus, str] = {
    FeedLoadStatus.LOADED: "chargé",
    FeedLoadStatus.REJECTED: "rejeté (réponse API)",
    FeedLoadStatus.API_CRASH: "rejeté (erreur technique API)",
    FeedLoadStatus.SCAN_ERROR: "rejeté au scan",
    FeedLoadStatus.GUARD_FAILED: "exclu (garde échouée)",
    FeedLoadStatus.NO_FILE: "exclu (flux absent)",
    FeedLoadStatus.UNSUPPORTED_ENTITY: "bloqué (hors périmètre V1)",
    FeedLoadStatus.RUN_ESCALATED: "bloqué (run escaladé)",
}

_NOT_ATTEMPTED_APPLIED = "statut de chargement inconnu (aucune trace pour ce flux)"
_NOT_ATTEMPTED_DRYRUN = "aperçu seul (dry-run — rien n'a été chargé)"

_NA = "—"


def _esc(text: str) -> str:
    """Escape a Markdown table cell so an embedded '|' never breaks the
    table layout — the only Markdown-structural character this report's
    free-text fields (feed_key, item/location names, error strings) can
    plausibly contain."""
    return text.replace("|", "\\|")


def _fmt_dt(value: Optional[datetime]) -> str:
    """Deterministic, locale-free, machine-independent datetime rendering.

    Never calls ``.astimezone()`` on a NAIVE datetime (Python would silently
    assume the SYSTEM's local timezone, which is the opposite of
    deterministic across machines) — a naive value is treated as already UTC
    (the TIMEZONE CONTRACT every upstream module in this pipeline already
    guarantees, interfaces/guards.py's own docstring), an aware value is
    normalized to UTC explicitly."""
    if value is None:
        return _NA
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _fmt_date(value: Optional[date]) -> str:
    if value is None:
        return _NA
    return value.isoformat()


def _fmt_int(value: Optional[int]) -> str:
    return _NA if value is None else str(value)


def _fmt_money(value: Optional[float]) -> str:
    if value is None:
        return _NA
    return f"{value:,.2f} $"


def _fmt_qty(value: Optional[float]) -> str:
    if value is None:
        return _NA
    return f"{value:,.2f}"


def _expected_deadline_label(fe: FeedRunEvaluation, run_date: date) -> str:
    """The arrival-window guard's own deadline, recomputed here (pure, no DB,
    no clock — mirrors interfaces/guards.py:compute_expected_arrival_deadline
    exactly) so the "Attendu" column names a concrete instant rather than a
    bare "oui". A contract whose cadence is outside guards.py's V1-supported
    'M H * * *' shape (see that module's docstring) cannot be resolved to a
    deadline — reported honestly, never silently guessed."""
    try:
        deadline = compute_expected_arrival_deadline(
            fe.contract.cadence, fe.contract.arrival_window_minutes, run_date
        )
    except ValueError:
        return f"non calculable (cadence={fe.contract.cadence!r})"
    return _fmt_dt(deadline)


def _decision_status_value(decision: Optional[DailyRunDecision]) -> Optional[str]:
    return decision.status.value if decision is not None else None


def _render_header(evaluation: DailyRunEvaluation, generated_at: datetime) -> list[str]:
    decision = evaluation.decision
    status_key = _decision_status_value(decision)
    label = _DECISION_LABELS.get(status_key, "N/A") if status_key else "N/A"
    explanation = (
        _DECISION_EXPLANATIONS.get(status_key, "")
        if status_key
        else _NO_DECISION_EXPLANATION
    )
    mode_label = "RUN APPLIQUÉ" if evaluation.is_applied else "APERÇU (dry-run — rien n'a été chargé ni persisté)"

    lines = [
        f"# Compte-rendu quotidien Ootils — {evaluation.run_date.isoformat()}",
        "",
        f"**Statut de la décision : {label}**",
        "",
        explanation,
        "",
        f"_Mode : {mode_label}_",
        f"_Généré le : {_fmt_dt(generated_at)}_",
        "",
    ]
    return lines


def _render_feed_table(evaluation: DailyRunEvaluation, outcomes_by_feed: dict[str, FeedLoadOutcome]) -> list[str]:
    lines = [
        "## Flux gouvernés",
        "",
        "| Flux | Criticité | Attendu (au plus tard) | Reçu | Lignes | Arrivée | Volume plancher | Delta jour | Suppressions | Décision |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    if not evaluation.feed_evaluations:
        lines.append("| _aucun flux actif évalué pour cette date_ | | | | | | | | | |")
        lines.append("")
        return lines

    for fe in sorted(evaluation.feed_evaluations, key=lambda x: x.feed_key):
        guard_cells = []
        for guard_name in ("arrival_window", "volume_floor", "volume_delta", "deletion_ratio"):
            result = fe.evaluation.by_name(guard_name)
            label = _GUARD_LABELS.get(guard_name, {}).get(result.status.value, result.status.value)
            guard_cells.append(label)

        outcome = outcomes_by_feed.get(fe.feed_key)
        if outcome is not None:
            decision_label = _LOAD_STATUS_LABELS.get(outcome.status, outcome.status.value)
        elif evaluation.is_applied:
            decision_label = _NOT_ATTEMPTED_APPLIED
        else:
            decision_label = _NOT_ATTEMPTED_DRYRUN

        row = [
            _esc(fe.feed_key),
            fe.contract.criticality,
            _expected_deadline_label(fe, evaluation.run_date),
            _fmt_dt(fe.observation.file_arrived_at),
            _fmt_int(fe.observation.row_count),
            guard_cells[0],
            guard_cells[1],
            guard_cells[2],
            guard_cells[3],
            _esc(decision_label),
        ]
        lines.append("| " + " | ".join(row) + " |")

    lines.append("")
    return lines


def _render_ungoverned(evaluation: DailyRunEvaluation, outcomes_by_feed: dict[str, FeedLoadOutcome]) -> list[str]:
    if not evaluation.ungoverned_feed_keys:
        return []
    lines = [
        "## Flux non gouvernés détectés",
        "",
        "Fichiers présents dans l'inbox mais sans contrat de flux actif "
        "(`feed_contracts`) — aucune garde n'a été évaluée pour ces flux. "
        "Chargés sans gouvernance si le run n'est pas escaladé.",
        "",
        "| Flux | Décision |",
        "|---|---|",
    ]
    for feed_key in sorted(evaluation.ungoverned_feed_keys):
        outcome = outcomes_by_feed.get(feed_key)
        if outcome is not None:
            decision_label = _LOAD_STATUS_LABELS.get(outcome.status, outcome.status.value)
        elif evaluation.is_applied:
            decision_label = _NOT_ATTEMPTED_APPLIED
        else:
            decision_label = _NOT_ATTEMPTED_DRYRUN
        lines.append(f"| {_esc(feed_key)} | {_esc(decision_label)} |")
    lines.append("")
    return lines


def _render_scan_issues(evaluation: DailyRunEvaluation) -> list[str]:
    if not evaluation.scan.issues:
        return []
    lines = [
        "## Anomalies de lecture (fichiers non exploitables)",
        "",
        "Détail technique pour l'équipe ERP : ces fichiers ont été détectés "
        "dans l'inbox mais n'ont pas pu être exploités (format, doublons de "
        "parties, ...).",
        "",
        "| Flux | Détail |",
        "|---|---|",
    ]
    for feed_key, issue in sorted(evaluation.scan.issues.items()):
        lines.append(f"| {_esc(feed_key)} | {_esc(issue.error)} |")
    lines.append("")
    return lines


def _render_volumes(evaluation: DailyRunEvaluation, load_outcomes: Sequence[FeedLoadOutcome]) -> list[str]:
    lines = ["## Volumes chargés", ""]
    if not evaluation.is_applied:
        lines.append(_NOT_ATTEMPTED_DRYRUN)
        lines.append("")
        return lines

    loaded = sorted(
        (o for o in load_outcomes if o.status == FeedLoadStatus.LOADED),
        key=lambda o: o.feed_key,
    )
    if not loaded:
        lines.append("Aucun flux chargé pour ce run.")
        lines.append("")
        return lines

    total = 0
    rows = ["| Flux | Lignes chargées |", "|---|---|"]
    for outcome in loaded:
        scanned = evaluation.scan.feeds.get(outcome.feed_key)
        row_count = scanned.row_count if scanned is not None else None
        if row_count is not None:
            total += row_count
        rows.append(f"| {_esc(outcome.feed_key)} | {_fmt_int(row_count)} |")

    lines.append(f"**Total lignes chargées : {total}**")
    lines.append("")
    lines.extend(rows)
    lines.append("")
    return lines


def _render_shortages(shortages_summary: Optional[Sequence[Mapping[str, Any]]]) -> list[str]:
    lines = ["## Pénuries actives (les plus sévères)", ""]
    if shortages_summary is None:
        lines.append(
            "Non fourni par l'appelant pour ce rapport — section ignorée "
            "(aucune requête n'a été faite)."
        )
        lines.append("")
        return lines
    if not shortages_summary:
        lines.append("Aucune pénurie active détectée.")
        lines.append("")
        return lines

    lines.append("| Article | Site | Sévérité ($) | Quantité | Date |")
    lines.append("|---|---|---|---|---|")
    for row in shortages_summary:
        item = str(row.get("item", _NA))
        location = str(row.get("location", _NA))
        severity = row.get("severity")
        qty = row.get("shortage_qty")
        shortage_date = row.get("shortage_date")
        lines.append(
            "| "
            + " | ".join(
                [
                    _esc(item),
                    _esc(location),
                    _fmt_money(float(severity) if severity is not None else None),
                    _fmt_qty(float(qty) if qty is not None else None),
                    _fmt_date(shortage_date) if isinstance(shortage_date, date) else str(shortage_date or _NA),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def _render_footer(evaluation: DailyRunEvaluation) -> list[str]:
    if evaluation.is_applied:
        ids = sorted(
            (fe.feed_key, fe.daily_run_id)
            for fe in evaluation.feed_evaluations
            if fe.daily_run_id is not None
        )
        run_ids = ", ".join(f"{key}={rid}" for key, rid in ids) if ids else "aucun"
    else:
        run_ids = "aucun (aperçu dry-run, rien n'a été persisté)"

    return [
        "---",
        "",
        "_Généré automatiquement par Ootils — ne pas modifier ce fichier._",
        "",
        f"- Run date : {evaluation.run_date.isoformat()}",
        f"- Identifiants d'audit (`daily_run_id` par flux) : {run_ids}",
        "- Référence événement : `daily_run_completed` (table `events`, "
        "voir `GET /v1/stream`) — émis par le moteur de décision gouvernée "
        "lors d'un run appliqué avec au moins un flux évalué. Ce rapport "
        "RÉFÉRENCE cet événement, il ne l'émet pas et n'expose pas son "
        "identifiant technique ici — voir `GET /v1/daily-runs?date="
        f"{evaluation.run_date.isoformat()}` pour le retrouver.",
        "",
    ]


def render_daily_report(
    evaluation: DailyRunEvaluation,
    load_outcomes: Sequence[FeedLoadOutcome],
    *,
    shortages_summary: Optional[Sequence[Mapping[str, Any]]] = None,
    generated_at: datetime,
) -> str:
    """Render one run_date's Markdown compte-rendu — DETERMINISTIC, DB-free.

    Same ``evaluation``/``load_outcomes``/``shortages_summary``/
    ``generated_at`` always yields the byte-identical string: no internal
    clock read, no DB call, no locale/system-timezone-dependent formatting
    (see ``_fmt_dt``), no unsorted dict/set iteration (every collection this
    function walks is explicitly sorted first).

    ``load_outcomes`` is the EMPTY tuple for a dry-run preview
    (``evaluation.is_applied is False`` — ``load_eligible_feeds`` is never
    called in that mode by construction, see
    ``engine/ingest/daily_orchestrator.py``) — every per-feed "Décision" cell
    then honestly reads "aperçu seul (dry-run — rien n'a été chargé)" rather
    than a fabricated status.

    ``shortages_summary`` is None-honest at TWO levels: ``None`` means the
    caller chose not to compute it (section says so, no query was made on
    this call); an empty sequence means the caller DID compute it and found
    zero active shortages (a genuine, good-news zero). See
    ``build_shortages_summary`` for the DB-touching counterpart that builds
    this structure; the renderer trusts the caller's ordering/limit (no
    re-sort, no re-limit — "le rendu ne fait aucune requête").
    """
    lines: list[str] = []
    lines.extend(_render_header(evaluation, generated_at))

    outcomes_by_feed = {o.feed_key: o for o in load_outcomes}

    lines.extend(_render_feed_table(evaluation, outcomes_by_feed))
    lines.extend(_render_ungoverned(evaluation, outcomes_by_feed))
    lines.extend(_render_scan_issues(evaluation))
    lines.extend(_render_volumes(evaluation, load_outcomes))
    lines.extend(_render_shortages(shortages_summary))
    lines.extend(_render_footer(evaluation))

    return "\n".join(lines).rstrip("\n") + "\n"


# ---------------------------------------------------------------------------
# build_shortages_summary — the ONE DB-touching function in this module.
# ---------------------------------------------------------------------------


def build_shortages_summary(conn: DictRowConnection, limit: int = 10) -> list[dict[str, Any]]:
    """Top-``limit`` active shortages by $ severity, for the baseline
    scenario's LATEST completed calc_run — SELECT-only, never commits/rolls
    back (caller owns the transaction, same convention as every other engine
    module here).

    Scoping mirrors ``engine/scenario/compare.py``'s documented rationale
    (reused via ``ScenarioManager._latest_calc_run``, not reimplemented):
    ``shortages`` is append-only per ``(pi_node_id, calc_run_id)`` — an
    unscoped scan over ``status = 'active'`` alone can straddle more than one
    historical run until ``ShortageDetector.resolve_stale`` has caught up, so
    the ``calc_run_id`` filter is not optional for a report a human will read
    as "today's picture" (ADR-021's canonical persistence axis).

    Returns ``[]`` (never raises) when the baseline has no completed
    calc_run yet — an honest "nothing to report" for a fresh install or a
    scenario that has never been propagated, not an error the daily-report
    generation should abort on.

    Each row: ``{"item": str, "location": str, "severity": float,
    "shortage_qty": float | None, "shortage_date": date | None}``. "item"/
    "location" prefer the business ``external_id``, falling back to the
    internal ``name`` and finally the raw UUID as text, so the report is
    readable by the ERP team without needing to resolve UUIDs by hand.
    """
    manager = ScenarioManager()
    try:
        calc_run_id = manager._latest_calc_run(BASELINE_SCENARIO_ID, conn)  # noqa: SLF001
    except ValueError:
        logger.info(
            "daily_report.shortages_summary_empty scenario=baseline reason=no_completed_calc_run"
        )
        return []

    rows = conn.execute(
        """
        SELECT
            COALESCE(i.external_id, i.name, s.item_id::text, '(article inconnu)') AS item,
            COALESCE(l.external_id, l.name, s.location_id::text, '(site inconnu)') AS location,
            s.severity_score AS severity,
            s.shortage_qty AS shortage_qty,
            s.shortage_date AS shortage_date
        FROM shortages s
        LEFT JOIN items i ON i.item_id = s.item_id
        LEFT JOIN locations l ON l.location_id = s.location_id
        WHERE s.scenario_id = %s
          AND s.status = 'active'
          AND s.calc_run_id = %s
        ORDER BY s.severity_score DESC, s.shortage_date ASC, s.shortage_id ASC
        LIMIT %s
        """,
        (BASELINE_SCENARIO_ID, calc_run_id, limit),
    ).fetchall()

    return [
        {
            "item": row["item"],
            "location": row["location"],
            "severity": float(row["severity"]) if row["severity"] is not None else 0.0,
            "shortage_qty": float(row["shortage_qty"]) if row["shortage_qty"] is not None else None,
            "shortage_date": row["shortage_date"],
        }
        for row in rows
    ]
