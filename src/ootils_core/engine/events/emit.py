"""
emit.py — the single fleet-emission helper (chantier AN-1, #401).

North Star "Streamable": every state-changing capability MUST write a typed
``events`` row so the fleet subscribes to deltas via ``GET /v1/stream`` (keyset
cursor on ``events.stream_seq``, migration 063) instead of polling. Until AN-1
five fleet-relevant capabilities emitted nothing and were invisible to the
stream; ``emit_stream_event`` is the one place they now write their run-level
event.

ONE INSERT, ONE TRANSACTION, ONE CONTRACT. The helper takes the caller's own
connection and inserts on it — so the event is part of the SAME transaction as
the business write it announces. Atomicity is the contract: if the business
write rolls back, its event rolls back with it (no phantom stream row for a
change that did not happen); if it commits, the event is durably visible to the
stream. The helper NEVER commits, NEVER rolls back, NEVER opens its own
connection — the caller owns the transaction (get_db in a router, governed_run
in a watcher, the CLI's own connection in a batch).

GRANULARITY = RUN, NEVER PER-ITEM (ADR-027, migration 071 header). One event per
governed run / calc run / capture batch / evaluation batch — the per-run count
travels in ``new_quantity`` so a subscriber sees "how many" without a join.

TYPED-COLUMN CONTRACT per event_type (no JSONB — migration 002 columns, kept in
sync with the migration 071 header block):

  recommendation_created:
      trigger_node_id = target node of the reco (nullable if none)
      field_changed   = the reco action (e.g. 'EXPEDITE','ORDER_NOW',
                        'RESCHEDULE_IN','TRANSFER') — the discriminant
      new_text        = recommendation_id (UUID as text) OR the agent_run_id
                        when a run emits an aggregate for several recos
      old_text        = source/agent ref (e.g. 'shortage_watcher')
      new_quantity    = count of recos created in the run
  shortage_detected:
      field_changed   = 'shortage_detected' (discriminant)
      new_quantity    = count of shortages persisted in the run
      new_text        = calc_run_id / detector run ref (UUID as text)
  calc_run_finished:
      field_changed   = terminal status ('completed'|'completed_stale'|'failed')
      new_text        = calc_run_id (UUID as text)
      new_quantity    = count of nodes (re)computed in the run
  snapshot_captured:
      field_changed   = 'snapshot_captured' (discriminant)
      new_date        = as_of_date of the capture
      new_quantity    = count of snapshot rows persisted
      new_text        = capture run ref (UUID as text)
  outcome_evaluated:
      field_changed   = 'outcome_evaluated' (discriminant)
      new_quantity    = count of recommendations classified in the run
      new_text        = evaluation run ref (UUID as text)
  purge_executed:
      field_changed   = the purge kind ('fork_purge' | 'shortage_retention') —
                        the discriminant (migration 076, PURGE-1)
      old_text        = executed_by (who/what triggered the run)
      new_text        = maintenance_purge_runs.run_id (UUID as text)
      new_quantity    = rows_deleted_total for this scenario's run
  daily_run_completed:
      field_changed   = the governed decision ('auto_approved' | 'escalated' |
                        'degraded') — the discriminant (migration 079,
                        ADR-042 PR-3 / ADR-037 INT-1 PR3)
      new_date        = run_date the decision covers
      new_quantity    = count of feeds included in the decision
      old_text        = comma-joined feed_keys whose combined guard/DQ
                        verdict was NOT green (the culprits), NULL when
                        every feed was green (never an empty string —
                        None-honest)
      new_text        = not used (no companion audit table — the decision is
                        derived on read from daily_runs, migration 078; this
                        event row IS the durable record of the decision)
  demand_descended:
      field_changed   = 'demand_descended' (discriminant, constant — unlike
                        purge_executed there is only one kind of descent run
                        in V1, migration 084, ADR-043)
      new_text        = descent_run_id (calc_runs.calc_run_id as text) — the
                        demand_descent_lines.descent_run_id FK target
                        (migration 083), same "run ref in new_text" idiom as
                        calc_run_finished/shortage_detected/outcome_evaluated
      new_quantity    = count of demand_descent_lines rows persisted by the
                        run (per-DC lines written, RUN granularity — never
                        per line)
      old_text        = comma-joined item_ids whose national source node(s)
                        had zero eligible DC and stayed national (fail-loudly
                        per ADR-043 §2, "zéro centre éligible -> la demande
                        reste nationale"), NULL when every item split cleanly
                        (never an empty string — None-honest, same idiom as
                        daily_run_completed's old_text)
  export_executed:
      field_changed   = 'export_executed' (discriminant, constant — like
                        demand_descended there is only one kind of export
                        run in V1, migration 085, ADR-042 decision 4)
      new_date        = the export's run_date (engine/reporting/
                        outbound_export.py's execute_export, run_date =
                        now.date() — same idiom as daily_run_completed)
      new_quantity    = count of recommendations stamped exported_at by the
                        run (RUN granularity — never per recommendation row)
      new_text        = comma-joined list of file names written to the
                        outbox this run (e.g. 'po_drafts_20260718.tsv,
                        reschedule_messages_20260718.tsv') — NOT a run id:
                        unlike calc_run_finished/shortage_detected/
                        outcome_evaluated/demand_descended there is no
                        export_runs companion table to reference (same "no
                        companion audit table" posture as
                        daily_run_completed, migration 079), so new_text
                        carries the artifact a fleet subscriber can act on
                        directly instead of an id with nowhere to join
      old_text        = not used in V1 (no skipped/ineligible-recommendation
                        concept yet for export — every exported_at-eligible
                        row is written every run). NOT emitted at all for a
                        genuinely empty run (zero pending rows) — same
                        "nothing to announce" posture as
                        emit_recommendation_created_for_run below, so the
                        events table does not accumulate a zero-content row
                        on the (common) days nothing was approved

``scenario_id`` (NOT NULL, migration 002) scopes the event to the fork/baseline.
snapshot_captured / outcome_evaluated are baseline-only by nature (ADR-030) but
the column contract is identical. purge_executed is emitted once per scenario
touched by a purge run (engine/maintenance/purge.py) — for fork_purge that is
the purged scenario itself; for shortage_retention it is each scenario whose
resolved shortages were swept, so a single retention run can emit several
events, one per affected scenario (still RUN granularity — one per scenario's
own delete, never per shortage row). demand_descended is forkable (ADR-043
§1): a fork's own descent run emits its own event scoped to that fork's
scenario_id, never replayed onto baseline by promote() (L0, simulation-only,
same doctrine as the ADR-025 overlay). export_executed is baseline-only in
V1 (the outbound export reads APPROVED recommendations off baseline — ADR-042
decision 4, PR-5), like snapshot_captured/outcome_evaluated.

Keep FLEET_EVENT_TYPES in sync with the events.event_type CHECK constraint
(migrations 071 + 076 + 079 + 084 + 085) and with VALID_EVENT_TYPES in
api/routers/events.py.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional, Union
from uuid import UUID, uuid4

import psycopg

# The fleet-emission types added by migration 071 (#401 AN-1) + migration 076
# (PURGE-1) + migration 079 (ADR-042 PR-3) + migration 084 (ADR-043, DESC-1
# PR-B) + migration 085 (ADR-042 decision 4, PR-5). Validated locally so a
# typo fails loudly in Python (ValueError) rather than as an opaque psycopg
# CHECK violation at INSERT time. This set is the subset emit_stream_event is
# meant to write; the DB CHECK (migrations 071/076/079/084/085) is the full
# authoritative list.
FLEET_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "recommendation_created",
        "shortage_detected",
        "calc_run_finished",
        "snapshot_captured",
        "outcome_evaluated",
        "purge_executed",
        "daily_run_completed",
        "demand_descended",
        "export_executed",
    }
)

# events.source CHECK (migration 002): 'api' | 'ingestion' | 'engine' | 'user'
# | 'test'. Fleet emissions default to 'engine' (a deterministic-core write);
# a router on the API request path passes source='api'.
_VALID_SOURCES: frozenset[str] = frozenset(
    {"api", "ingestion", "engine", "user", "test"}
)


def emit_stream_event(
    conn: psycopg.Connection[Any],
    event_type: str,
    scenario_id: Union[UUID, str],
    *,
    trigger_node_id: Optional[Union[UUID, str]] = None,
    field_changed: Optional[str] = None,
    new_quantity: Optional[Union[int, Decimal]] = None,
    old_text: Optional[str] = None,
    new_text: Optional[str] = None,
    new_date: Optional[date] = None,
    source: str = "engine",
) -> UUID:
    """Insert ONE typed fleet event on the caller's connection (same transaction).

    Returns the generated event_id. Does NOT commit — the caller owns the
    transaction so the event is atomic with the business write it announces.

    Raises ValueError on an unknown ``event_type`` (not in FLEET_EVENT_TYPES) or
    an invalid ``source`` — fail-loudly, so a typo never becomes a silent
    CHECK-violation at INSERT time or a mis-sourced row.

    The parameter set maps 1:1 to the typed-column contract documented at the top
    of this module; a caller passes only the columns its event_type populates and
    leaves the rest at their None default.
    """
    if event_type not in FLEET_EVENT_TYPES:
        raise ValueError(
            f"emit_stream_event: unknown fleet event_type {event_type!r} — "
            f"valid: {sorted(FLEET_EVENT_TYPES)}"
        )
    if source not in _VALID_SOURCES:
        raise ValueError(
            f"emit_stream_event: invalid source {source!r} — "
            f"valid: {sorted(_VALID_SOURCES)}"
        )

    event_id = uuid4()
    now = datetime.now(timezone.utc)

    conn.execute(
        """
        INSERT INTO events (
            event_id, event_type, scenario_id, trigger_node_id,
            field_changed, new_date, new_quantity, old_text, new_text,
            processed, source, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s)
        """,
        (
            event_id,
            event_type,
            scenario_id,
            trigger_node_id,
            field_changed,
            new_date,
            new_quantity,
            old_text,
            new_text,
            source,
            now,
        ),
    )
    return event_id


# The governed-RECOMMENDATION artifact tables — the ones whose rows are
# "recommendation_created" in the migration-071 sense (a governed DRAFT action a
# planner reviews): the procurement/reschedule/transfer queue (`recommendations`),
# the planning-parameter proposals (`parameter_recommendations`, the
# scenario-backed lot_policy_watcher, ADR-025), and the demand-accuracy verdicts
# (`forecast_drift_recommendations`, the DEM-1 forecast watcher, migration 072).
# All three carry agent_run_id (migrations 039/041/072). DELIBERATELY EXCLUDES
# dq_findings and eando_recommendations: those are data-quality findings /
# disposition changes, not governed action recommendations (baseline-only by
# nature, out of the #340/#347 scenario-backed scope per CLAUDE.md) — counting
# them here would mislabel a DQ scan as a recommendation run.
_RECO_TABLES: tuple[str, ...] = (
    "recommendations",
    "parameter_recommendations",
    "forecast_drift_recommendations",
)


def emit_recommendation_created_for_run(
    conn: psycopg.Connection[Any],
    agent_run_id: Union[UUID, str],
    scenario_id: Union[UUID, str],
    agent_name: str,
    *,
    source: str = "engine",
) -> Optional[UUID]:
    """Emit ONE ``recommendation_created`` event for a governed run, iff the run
    created >=1 governed recommendation row (#401 AN-1).

    The count is read from the authoritative governed-recommendation tables
    (_RECO_TABLES) by agent_run_id — the ONE run-level source of truth that is
    robust across the fleet's divergent metric-key conventions (the shortage
    watcher stores ``recommendations``, the transfer/reschedule watchers
    ``recommendations_inserted``, the lot-policy watcher ``proposals``; a COUNT by
    agent_run_id needs no such convention) AND across insertion paths (run.insert
    for the supersede-reinsert watchers, the deterministic-uuid ON CONFLICT DO
    NOTHING _upsert for transfer/reschedule). Idempotent no-op re-emissions keep
    the ORIGINAL run's agent_run_id, so a re-run that inserts nothing new counts 0
    and emits nothing — exactly the "unchanged plan re-run emits zero new rows"
    contract (#346/#395).

    Same connection/transaction as the reco writes (atomic). Returns the event_id
    when an event was emitted, or None when the run created no recommendation
    (nothing to announce). ``agent_name`` is carried in old_text; the run id in
    new_text; the total count in new_quantity (RUN granularity, ADR-027).
    """
    count = 0
    for table in _RECO_TABLES:
        # Table name is a hardcoded literal from the module-level whitelist, never
        # caller data — safe to interpolate. Values are bound positionally.
        row = conn.execute(
            f"SELECT COUNT(*) AS n FROM {table} WHERE agent_run_id = %s",  # noqa: S608
            (agent_run_id,),
        ).fetchone()
        count += _count_from_row(row)
    if count <= 0:
        return None
    return emit_stream_event(
        conn,
        "recommendation_created",
        scenario_id,
        old_text=agent_name,
        new_text=str(agent_run_id),
        new_quantity=count,
        source=source,
    )


def _count_from_row(row: Any) -> int:
    """Extract a COUNT(*) result from either a dict_row or a tuple_row.

    The fleet mixes row factories: the app pool is dict_row (get_db), a watcher's
    psycopg.connect() default is tuple_row. Reading the count works for both so
    emit_recommendation_created_for_run is callable from every path."""
    if row is None:
        return 0
    if isinstance(row, dict):
        return int(row.get("n", 0) or 0)
    return int(row[0] or 0)
