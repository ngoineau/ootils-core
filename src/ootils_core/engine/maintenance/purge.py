"""
purge.py — scenario archive/purge + shortage-retention lifecycle (PURGE-1,
migration 076).

Ootils soft-deletes scenarios (``status='archived'``, migration 002/015/032)
and every FK pointing at ``scenarios(scenario_id)`` is ``ON DELETE RESTRICT``
(migration 032) — a scenario ROW is never hard-deleted by application code.
Left unchecked this leaves an unbounded number of archived forks (and their
deep-copied node/edge/shortage/explanation/override rows) accumulating with
no retention path. PURGE-1 introduces the retention lifecycle: a TTL-driven
sweep that deletes an archived fork's CHILD data (never the ``scenarios`` row
itself, which survives forever with ``purged_at`` stamped) plus a bounded
retention sweep over long-resolved ``shortages`` rows.

Split, mirroring the engine's DB-boundary convention used across the repo
(``engine/outcome/evaluator.py``, ``engine/snapshot/capture.py``): every
``plan_*`` function is SELECT-only (safe to call from a read-only preview
endpoint, never writes) and every ``apply_*`` function is the sole writer for
its lifecycle, re-verifying the absolute guards on FRESH data (defense in
depth against a stale/hand-built plan or a concurrent write) before touching
anything. Neither function commits — the caller owns the transaction
(``get_db`` for the API, the CLI's own connection).

THE FK-SAFE WHITELIST (``PURGE_WHITELIST``) is the one thing a scenario-fork
purge must get exactly right: deleting a table before something that still
references it raises ``ForeignKeyViolation``. The order below was derived by
reading every migration that declares a FK either directly at ``scenarios``
or transitively through ``nodes``/``calc_runs``/``explanations`` (see
``tests/test_purge_whitelist_guard.py``, which re-derives the same schema
scan and fails the build if a future migration adds a scenario-scoped table
that is neither whitelisted here nor exempted in ``PURGE_EXEMPT_TABLES``):

  1. ``causal_steps``      — FK to explanations(explanation_id), no cascade.
  2. ``shortages``         — FK to nodes/calc_runs/explanations, no cascade.
  3. ``dirty_nodes``       — FK to calc_runs/nodes/scenarios, no cascade.
  4. ``scenario_diffs``    — FK to calc_runs (x2) /nodes/scenarios, no cascade.
  5. ``scenario_overrides``— FK to nodes/scenarios, no cascade.
  6. ``explanations``      — FK to calc_runs/nodes, no cascade. Must follow
     causal_steps and shortages (both reference IT), and precede calc_runs
     and nodes (it references THEM).
  7. ``edges``              — FK to nodes (x2) /scenarios, no cascade.
  8. ``ghost_nodes``        — FK to scenarios/resources/nodes, no cascade.
     ``ghost_members`` is NOT separately deleted: its FK to
     ``ghost_nodes(ghost_id)`` is ``ON DELETE CASCADE`` (migration 011), so
     deleting ``ghost_nodes`` removes its members automatically — the plan
     still COUNTS ``ghost_members`` (for an honest preview) via a pre-count,
     since the cascade makes it un-observable as a separate DELETE rowcount.
  9. ``events``             — FK to nodes (trigger_node_id)/scenarios, no
     cascade. This is the ADR-005-amended carve-out: events are normally
     insert-only on their payload, but a scenario's OWN events lose their
     reason to exist once the scenario is purged — the ONE row that survives
     is the ``purge_executed`` confirmation event, emitted AFTER this step
     (see ``_apply_one``), so it is never wiped by its own purge pass.
  10. ``nodes``             — FK to scenarios; parent of edges/dirty_nodes/
     scenario_diffs/scenario_overrides/explanations/ghost_nodes/events/
     shortages (all deleted above) and of projection_series/calc_runs
     (deferred FKs, deleted below). ``nodes.parent_node_id`` (migration 024,
     MRP pegging) is SELF-referencing with NO ``ON DELETE`` clause (default
     RESTRICT, NOT deferrable) — bulk-deleting a scenario's nodes in one
     statement while some rows still reference sibling rows in the SAME
     statement risks a spurious RI failure depending on internal row
     processing order, so ``_apply_one`` nulls every ``parent_node_id`` in
     the scenario FIRST (a plain UPDATE, not itself a whitelist entry: it
     changes no row count, just breaks the self-reference before the delete).
  11. ``projection_series`` — child of items/locations/scenarios; parent of
     nodes via a DEFERRABLE INITIALLY DEFERRED FK (migration 002) — deleted
     after nodes so the deferred check never fires against a surviving node.
  12. ``calc_runs``         — child of scenarios; parent of nodes (deferred
     FK, migration 002) and of dirty_nodes/scenario_diffs/explanations/
     shortages (all deleted above) — deleted last among the calc_run-linked
     tables.
  13. ``scenario_planning_overrides`` — child of scenarios/items/locations
     only (migration 060); independent of the node/calc_run subgraph, so its
     position is not order-sensitive, kept last for readability.

``zone_transition_runs`` (mentioned in earlier drafts of this plan) was
VERIFIED and excluded: migration 003 dropped and recreated it WITHOUT a
``scenario_id`` column (it is a global job-tracking table for weekly/monthly
zone-boundary transitions across every series, not scenario-scoped data) — it
is therefore not scenario-purgeable at all and does not appear in either
``PURGE_WHITELIST`` or ``PURGE_EXEMPT_TABLES``.

DELIBERATE, DOCUMENTED SIDE EFFECTS on tables OUTSIDE the whitelist (both
pre-engineered by their own migrations, not something this module's code
needs to handle):
  * ``recommendations.target_node_id`` is ``ON DELETE SET NULL`` (migration
    061) — purging a scenario's nodes silently nulls out any reschedule
    message's target on a surviving (exempted) recommendation row; the reco
    itself survives as an audit record, per its own migration's rationale.
  * ``pyramide_snapshot_demand_nodes.demand_node_id`` is ``ON DELETE CASCADE``
    (migration 038) — purging nodes cascade-deletes the (exempted)
    ``pyramide_snapshots``' demand-node mapping rows, but the owning
    ``pyramide_snapshots``/``pyramide_runs`` rows themselves are untouched.

SHORTAGE RETENTION is a SEPARATE, narrower sweep — not scenario-scoped, and
NOT bound to the archive/purge lifecycle: it deletes long-``resolved``
``shortages`` rows across every scenario (archived or not) that are older
than ``retention_days`` AND do not belong to their scenario's latest
``completed`` calc_run (the currently-authoritative shortage picture is never
touched, whatever its age or status). ``status='active'`` rows are NEVER
eligible by construction (the predicate hardcodes ``status = 'resolved'``).
"""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.events.emit import emit_stream_event

logger = logging.getLogger(__name__)


class PurgeGuardError(Exception):
    """Raised when a purge target fails an ABSOLUTE safety guard (is_baseline,
    status != 'archived', archived_at NULL or inside the TTL window). Never
    bypassed. NOT raised for an already-purged scenario (``purged_at`` set) —
    that is treated as an idempotent no-op by the caller, not a guard
    violation (see ``_apply_one``)."""


# ---------------------------------------------------------------------------
# The FK-safe whitelist — see the module docstring for the full ordering
# rationale. ORDER MATTERS: this tuple IS the deletion order.
# ---------------------------------------------------------------------------
PURGE_WHITELIST: tuple[str, ...] = (
    "causal_steps",
    "shortages",
    "dirty_nodes",
    "scenario_diffs",
    "scenario_overrides",
    "explanations",
    "edges",
    "ghost_nodes",
    "events",
    "nodes",
    "projection_series",
    "calc_runs",
    "scenario_planning_overrides",
)

# Tables carrying scenario_id (directly, or — for causal_steps/explanations —
# indirectly via calc_runs/explanations) that are DELIBERATELY excluded from
# PURGE_WHITELIST, each with an explicit justification.
# tests/test_purge_whitelist_guard.py re-derives the full set of
# scenario-scoped tables straight from the migrations and asserts every one
# of them is EITHER in PURGE_WHITELIST OR in this dict — a future migration
# that adds a new scenario_id column must update one of the two, or the
# guard fails the build.
PURGE_EXEMPT_TABLES: dict[str, str] = {
    "scenarios": (
        "The scenario registry row itself — never deleted by PURGE-1. "
        "purged_at is stamped on it; the row survives forever (ADR-011: "
        "every FK onto scenarios is RESTRICT, hard-delete is not a supported "
        "path)."
    ),
    "maintenance_purge_runs": (
        "The purge audit trail this very module writes — purging its own "
        "audit rows would erase the record of the purge that just ran."
    ),
    "agent_runs": (
        "Governed-fleet work ledger (migration 039). scenario_id carries NO "
        "FK to scenarios at all (verified: migration 032's dynamic FK-fixup "
        "loop only touches EXISTING FKs + explicitly adds one for mrp_runs; "
        "agent_runs was never wired to the RESTRICT policy). Retrospective "
        "audit of every agent execution, not forkable working state — "
        "PURGE-1 V1 deliberately excludes the whole governed-recommendation "
        "audit family (see recommendations below)."
    ),
    "recommendations": (
        "Governed L1-L4 recommendation queue (migration 039), Decision "
        "Ladder audit trail feeding the ADR-030 proof machine "
        "(recommendation_outcomes -> recommendations). Same no-FK-to-"
        "scenarios finding as agent_runs. Purging it on scenario archive "
        "would erase the accountability record the proof-of-value KPIs "
        "depend on. Out of PURGE-1 V1 scope by design — a future PURGE-2 "
        "could add a SEPARATE stale-DRAFT/EXPIRED retention policy, distinct "
        "from scenario purge."
    ),
    "parameter_recommendations": (
        "Same governed-audit family as recommendations (migration 041, "
        "lot_policy_watcher). No FK to scenarios. Excluded for the same "
        "accountability reason."
    ),
    "dq_findings": (
        "Same governed-audit family (migration 044, DQ watcher fleet). No FK "
        "to scenarios. Excluded for the same accountability reason."
    ),
    "eando_recommendations": (
        "Same governed-audit family (migration 045, E&O watcher). No FK to "
        "scenarios. Excluded for the same accountability reason."
    ),
    "forecast_drift_recommendations": (
        "Same governed-audit family (migration 072, DEM-1 forecast "
        "watcher) — DOES carry a real FK to scenarios (ON DELETE RESTRICT, "
        "unlike its siblings above), but the exclusion rationale is "
        "identical: a governed decision-ladder audit trail, not forkable "
        "working state."
    ),
    "scenario_promotions": (
        "Audit trail of scenario -> baseline promotions (migration 052): who "
        "promoted, when, how many overrides replayed. An archived scenario's "
        "promotion history is exactly the governance record that must "
        "survive purge, same class as recommendations."
    ),
    "inventory_snapshots": (
        "Baseline-only by nature (ADR-030, migration 067's header: 'V1 "
        "CAPTURES BASELINE ONLY'). A purge candidate is by construction "
        "is_baseline=FALSE, so no eligible scenario ever holds rows here — "
        "genuinely inapplicable, not merely deferred."
    ),
    "mrp_runs": (
        "MRP run-history metadata (migration 021/032). A read/audit trail of "
        "past MRP computations, not the live graph working-state PURGE-1 "
        "targets (nodes/edges/shortages/explanations/dirty-flags/overrides). "
        "Out of PURGE-1 V1 scope — the whitelist targets the core "
        "propagation-engine substrate; MRP/forecast/Pyramide run history is "
        "a separate, still-evolving subsystem (see forecasts/pyramide_* "
        "below)."
    ),
    "forecasts": (
        "Statistical forecast headers (migration 026). Same run-history "
        "rationale as mrp_runs — out of PURGE-1 V1 scope."
    ),
    "pyramide_runs": (
        "Pyramide forecast run metadata (migration 038). Same run-history "
        "rationale as mrp_runs — out of PURGE-1 V1 scope. Its child "
        "pyramide_snapshots/pyramide_snapshot_demand_nodes follow the same "
        "exclusion transitively."
    ),
    "pyramide_snapshots": (
        "Immutable Pyramide forecast snapshot headers (migration 038), child "
        "of pyramide_runs. Same run-history rationale — out of PURGE-1 V1 "
        "scope. NOTE: pyramide_snapshot_demand_nodes (its junction table) has "
        "NO direct scenario_id column and is not separately listed here, but "
        "IS transitively affected: its demand_node_id FK is ON DELETE "
        "CASCADE from nodes (migration 038), so purging a scenario's nodes "
        "silently removes the matching junction rows even though "
        "pyramide_snapshots itself is exempted (see the module docstring's "
        "'documented side effects' section)."
    ),
    "mps_nodes": (
        "MPS (Master Production Schedule) relational table (migration 027) — "
        "a PARALLEL relational representation alongside the graph, used by "
        "the MPS router. Out of PURGE-1 V1 scope (the whitelist targets the "
        "graph substrate itself: nodes/edges/propagation state). Extending "
        "purge to the MPS/CRP/ATP relational subsystem is a follow-up once "
        "its own scenario-lifecycle semantics are confirmed."
    ),
    "mps_planned_for_edges": (
        "Child of mps_nodes (migration 027). Same MPS-subsystem exclusion."
    ),
    "mps_supplies_edges": (
        "Child of mps_nodes (migration 027). Same MPS-subsystem exclusion."
    ),
    "routing_requires_capacity_edges": (
        "CRP (Capacity Requirements Planning) relational table (migration "
        "028) — same parallel-subsystem rationale as the MPS tables above."
    ),
    "planned_supply": (
        "Phase-1 E2E/ATP relational table (migration 030) — same "
        "parallel-subsystem rationale as the MPS/CRP tables above."
    ),
    "customer_order_demand": (
        "Phase-1 E2E/ATP relational table (migration 030) — same "
        "parallel-subsystem rationale as the MPS/CRP tables above."
    ),
}


# ---------------------------------------------------------------------------
# Per-table SQL — COUNT (plan, read-only) and DELETE (apply), one pair per
# PURGE_WHITELIST entry, each parameterized by scenario_id exactly once.
# Table names are hardcoded literals from the module-level whitelist above,
# never caller data — safe to interpolate (same precedent as
# engine/events/emit.py's _RECO_TABLES loop).
# ---------------------------------------------------------------------------

_CAUSAL_STEPS_SCOPE = (
    "explanation_id IN ("
    "SELECT e.explanation_id FROM explanations e "
    "JOIN calc_runs cr ON cr.calc_run_id = e.calc_run_id "
    "WHERE cr.scenario_id = %s)"
)
_EXPLANATIONS_SCOPE = (
    "calc_run_id IN (SELECT calc_run_id FROM calc_runs WHERE scenario_id = %s)"
)

def _build_table_queries() -> dict[str, tuple[str, str]]:
    queries: dict[str, tuple[str, str]] = {}
    for table in PURGE_WHITELIST:
        if table == "causal_steps":
            scope = _CAUSAL_STEPS_SCOPE
        elif table == "explanations":
            scope = _EXPLANATIONS_SCOPE
        else:
            scope = "scenario_id = %s"
        queries[table] = (
            f"SELECT COUNT(*) AS n FROM {table} WHERE {scope}",  # noqa: S608
            f"DELETE FROM {table} WHERE {scope}",  # noqa: S608
        )
    return queries


_TABLE_QUERIES: dict[str, tuple[str, str]] = _build_table_queries()

# ghost_members is NOT in PURGE_WHITELIST (it cascades from ghost_nodes) but
# the plan still reports an honest pre-count for operator visibility.
_GHOST_MEMBERS_COUNT_SQL = (
    "SELECT COUNT(*) AS n FROM ghost_members "
    "WHERE ghost_id IN (SELECT ghost_id FROM ghost_nodes WHERE scenario_id = %s)"
)


# ---------------------------------------------------------------------------
# Fork purge — plan (SELECT-only) / apply (the sole writer)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PurgeCandidate:
    """One archived scenario eligible for purge, with the FK-safe per-table
    row counts a purge run would delete (``ghost_members`` included in the
    breakdown for visibility even though it is cascade-deleted, not directly
    targeted — see ``PURGE_WHITELIST``'s docstring)."""

    scenario_id: UUID
    name: str
    archived_at: _dt.datetime
    per_table_counts: dict[str, int]

    @property
    def rows_total(self) -> int:
        return sum(self.per_table_counts.values())


@dataclass(frozen=True)
class PurgePlan:
    """The output of ``plan_fork_purge`` — a pure preview, nothing written."""

    ttl_days: int
    generated_at: _dt.datetime
    candidates: tuple[PurgeCandidate, ...]

    @property
    def rows_total(self) -> int:
        return sum(c.rows_total for c in self.candidates)


@dataclass(frozen=True)
class PurgeRunResult:
    """The outcome of one ``apply_fork_purge`` / ``apply_shortage_retention``
    action against one scenario. ``run_id`` is None when ``skipped`` is True
    (an idempotent no-op — see ``_apply_one`` / ``apply_shortage_retention``:
    already-purged or nothing eligible)."""

    scenario_id: UUID
    run_id: Optional[UUID]
    skipped: bool
    per_table_counts: dict[str, int]
    rows_deleted_total: int


def _db_now(conn: DictRowConnection) -> _dt.datetime:
    cur = conn.cursor(row_factory=dict_row)
    row = cur.execute("SELECT now() AS n").fetchone()
    if row is None:  # pragma: no cover — now() always returns a row
        raise RuntimeError("purge: SELECT now() yielded no row")
    return row["n"]


def _coerce_uuid(value: object) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _count_from_row(row: Optional[dict]) -> int:
    if row is None:  # pragma: no cover — COUNT(*) always returns one row
        return 0
    return int(row.get("n", 0) or 0)


def _count_whitelist_for_scenario(
    conn: DictRowConnection, scenario_id: UUID
) -> dict[str, int]:
    """Read-only per-table row counts for one scenario, in PURGE_WHITELIST
    order, plus the cascade-only ghost_members visibility count."""
    cur = conn.cursor(row_factory=dict_row)
    counts: dict[str, int] = {}
    for table in PURGE_WHITELIST:
        count_sql, _ = _TABLE_QUERIES[table]
        counts[table] = _count_from_row(cur.execute(count_sql, (scenario_id,)).fetchone())
    counts["ghost_members"] = _count_from_row(
        cur.execute(_GHOST_MEMBERS_COUNT_SQL, (scenario_id,)).fetchone()
    )
    return counts


def plan_fork_purge(conn: DictRowConnection, ttl_days: int = 7) -> PurgePlan:
    """SELECT-only: the archived scenarios eligible for purge and the
    per-table row counts a purge would delete. Writes NOTHING.

    Eligibility (the planner's own guard — a scenario failing any of these is
    simply never a candidate, never surfaced):
      * ``status = 'archived'``
      * ``is_baseline = FALSE``
      * ``purged_at IS NULL`` (not already purged)
      * ``archived_at IS NOT NULL`` (an unknown archive time can never clear
        a TTL honestly — never treated as "eligible by default")
      * ``archived_at`` older than ``ttl_days`` (computed against the DB's
        own clock, not the caller's, so a plan built by a client with a
        skewed clock cannot game the window)
    """
    if ttl_days < 0:
        raise ValueError(f"ttl_days must be >= 0, got {ttl_days}")

    now = _db_now(conn)
    cutoff = now - _dt.timedelta(days=ttl_days)

    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute(
        """
        SELECT scenario_id, name, archived_at
        FROM scenarios
        WHERE status = 'archived'
          AND is_baseline = FALSE
          AND purged_at IS NULL
          AND archived_at IS NOT NULL
          AND archived_at < %s
        ORDER BY archived_at
        """,
        (cutoff,),
    ).fetchall()

    candidates: list[PurgeCandidate] = []
    for row in rows:
        scenario_id = _coerce_uuid(row["scenario_id"])
        candidates.append(
            PurgeCandidate(
                scenario_id=scenario_id,
                name=row["name"],
                archived_at=row["archived_at"],
                per_table_counts=_count_whitelist_for_scenario(conn, scenario_id),
            )
        )
    return PurgePlan(ttl_days=ttl_days, generated_at=now, candidates=tuple(candidates))


def _verify_purge_guards(
    *,
    scenario_id: UUID,
    status: str,
    is_baseline: bool,
    archived_at: Optional[_dt.datetime],
    ttl_days: int,
    now: _dt.datetime,
) -> None:
    """Pure guard check — raises PurgeGuardError on ANY violation. Called by
    ``_apply_one`` with FRESHLY re-read values (defense in depth against a
    stale/hand-built PurgePlan or a concurrent write racing the purge).
    ``purged_at`` is intentionally NOT a parameter here: an already-purged
    scenario is the ONE case the caller treats as an idempotent no-op
    BEFORE reaching this function, never a guard violation."""
    if is_baseline:
        raise PurgeGuardError(
            f"scenario {scenario_id}: refusing to purge the baseline scenario"
        )
    if status != "archived":
        raise PurgeGuardError(
            f"scenario {scenario_id}: status={status!r}, expected 'archived'"
        )
    if archived_at is None:
        raise PurgeGuardError(
            f"scenario {scenario_id}: archived_at is NULL, cannot evaluate TTL"
        )
    cutoff = now - _dt.timedelta(days=ttl_days)
    if archived_at >= cutoff:
        raise PurgeGuardError(
            f"scenario {scenario_id}: archived_at={archived_at.isoformat()} is "
            f"inside the {ttl_days}-day TTL window (cutoff={cutoff.isoformat()})"
        )


def _nullify_self_referencing_parent(conn: DictRowConnection, scenario_id: UUID) -> None:
    """Break nodes.parent_node_id (migration 024, MRP pegging) self-references
    for this scenario BEFORE the nodes DELETE. See the module docstring
    (whitelist entry 10) for why a bulk self-referencing DELETE is unsafe
    without this step first."""
    conn.execute(
        "UPDATE nodes SET parent_node_id = NULL "
        "WHERE scenario_id = %s AND parent_node_id IS NOT NULL",
        (scenario_id,),
    )


def _delete_whitelist_for_scenario(
    conn: DictRowConnection, scenario_id: UUID
) -> dict[str, int]:
    """The sole writer of the FK-safe whitelist deletes, in PURGE_WHITELIST
    order. Returns ACTUAL deleted-row counts (cursor.rowcount), never a
    stale pre-count — a plan built earlier may be out of date by the time
    apply runs."""
    counts: dict[str, int] = {}
    for table in PURGE_WHITELIST:
        _, delete_sql = _TABLE_QUERIES[table]
        cur = conn.execute(delete_sql, (scenario_id,))
        counts[table] = cur.rowcount if cur.rowcount is not None else 0
    return counts


def _analyze_tables(conn: DictRowConnection, tables: set[str]) -> None:
    """ANALYZE every touched table after a bulk DELETE (convention #455: a
    bulk write that a same-request planner reads back must be followed by an
    explicit ANALYZE, or stale stats can collapse a later query plan)."""
    for table in sorted(tables):
        conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(table)))


def _apply_one(
    conn: DictRowConnection,
    scenario_id: UUID,
    ttl_days: int,
    executed_by: str,
) -> PurgeRunResult:
    cur = conn.cursor(row_factory=dict_row)
    # FOR UPDATE: lock the scenario row against a concurrent purge attempt
    # racing this one (two apply_fork_purge calls targeting the same fork).
    row = cur.execute(
        "SELECT status, is_baseline, archived_at, purged_at "
        "FROM scenarios WHERE scenario_id = %s FOR UPDATE",
        (scenario_id,),
    ).fetchone()
    if row is None:
        raise PurgeGuardError(f"scenario {scenario_id}: not found")

    if row["purged_at"] is not None:
        logger.info("purge.fork.skip scenario_id=%s reason=already_purged", scenario_id)
        return PurgeRunResult(
            scenario_id=scenario_id,
            run_id=None,
            skipped=True,
            per_table_counts={},
            rows_deleted_total=0,
        )

    now = _db_now(conn)
    _verify_purge_guards(
        scenario_id=scenario_id,
        status=row["status"],
        is_baseline=bool(row["is_baseline"]),
        archived_at=row["archived_at"],
        ttl_days=ttl_days,
        now=now,
    )

    _nullify_self_referencing_parent(conn, scenario_id)
    per_table_counts = _delete_whitelist_for_scenario(conn, scenario_id)
    rows_deleted_total = sum(per_table_counts.values())

    conn.execute(
        "UPDATE scenarios SET purged_at = %s WHERE scenario_id = %s",
        (now, scenario_id),
    )

    # cursor pinned to dict_row explicitly: the connection handed in may be a
    # bare psycopg.connect() (tuple_row default, the CLI's own connection) —
    # this module never assumes dict-style access without pinning it itself
    # (same defensive pattern as engine/outcome/evaluator.py).
    insert_cur = conn.cursor(row_factory=dict_row)
    run_id = insert_cur.execute(
        """
        INSERT INTO maintenance_purge_runs
            (scenario_id, mode, ttl_days, per_table_counts, rows_deleted_total, executed_by)
        VALUES (%s, 'apply', %s, %s, %s, %s)
        RETURNING run_id
        """,
        (scenario_id, ttl_days, _to_jsonb(per_table_counts), rows_deleted_total, executed_by),
    ).fetchone()
    if run_id is None:  # INSERT..RETURNING yields exactly one row — fail loudly
        raise RuntimeError("maintenance_purge_runs INSERT returned no row")
    run_id = _coerce_uuid(run_id["run_id"])

    # Emitted AFTER the events-table delete above (whitelist position 9), so
    # this confirmation event survives its own scenario's purge pass — the
    # ADR-005-amended carve-out documented in the module docstring.
    emit_stream_event(
        conn,
        "purge_executed",
        scenario_id,
        field_changed="fork_purge",
        old_text=executed_by,
        new_text=str(run_id),
        new_quantity=rows_deleted_total,
        source="engine",
    )

    logger.info(
        "purge.fork.applied scenario_id=%s run_id=%s rows_deleted_total=%d executed_by=%s",
        scenario_id, run_id, rows_deleted_total, executed_by,
    )
    return PurgeRunResult(
        scenario_id=scenario_id,
        run_id=run_id,
        skipped=False,
        per_table_counts=per_table_counts,
        rows_deleted_total=rows_deleted_total,
    )


def apply_fork_purge(
    conn: DictRowConnection,
    plan: PurgePlan,
    executed_by: str,
) -> tuple[PurgeRunResult, ...]:
    """The sole writer of the fork-purge lifecycle. For every candidate in
    ``plan``: re-verifies the absolute guards on FRESH data (never trusts the
    plan blindly — it may be stale), deletes the FK-safe whitelist in order,
    stamps ``scenarios.purged_at``, writes one ``maintenance_purge_runs`` row,
    emits ONE ``purge_executed`` stream event, and finally ANALYZEs every
    table touched across the whole call (convention #455 — once, in bulk,
    after all candidates, not per-candidate).

    IDEMPOTENT: a candidate whose ``purged_at`` is already set (re-purge, or
    a race with a concurrent apply) is a clean no-op — logged, not raised.
    A genuine guard violation (baseline, wrong status, TTL not yet elapsed)
    IS raised (``PurgeGuardError``) and aborts the WHOLE call — the caller
    (CLI/API) owns whether to retry the remaining candidates in a fresh call.

    Does NOT commit — the caller owns the transaction.
    """
    if not executed_by or not executed_by.strip():
        raise ValueError("executed_by is required (audit attribution)")

    results: list[PurgeRunResult] = []
    touched_tables: set[str] = set()
    for candidate in plan.candidates:
        result = _apply_one(conn, candidate.scenario_id, plan.ttl_days, executed_by)
        results.append(result)
        touched_tables.update(t for t, n in result.per_table_counts.items() if n > 0)

    if touched_tables:
        _analyze_tables(conn, touched_tables)

    return tuple(results)


def _to_jsonb(counts: dict[str, int]) -> Jsonb:
    """Wrap a per-table count dict for a jsonb column. psycopg3 does NOT
    auto-adapt a plain dict to JSONB — every JSONB write site in this repo
    wraps explicitly with ``psycopg.types.json.Jsonb`` (engine/recommendation/
    transfer.py, pyramide/repository.py, api/routers/drp.py); this is that
    same idiom, named so the INSERT call sites read clearly."""
    return Jsonb(counts)


# ---------------------------------------------------------------------------
# Shortage retention — bounded sweep over long-resolved shortages, NOT
# scoped to the archive/purge lifecycle (runs across every scenario).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ShortageRetentionCandidate:
    """One scenario with >=1 resolved shortage row old enough to purge."""

    scenario_id: UUID
    rows_to_delete: int


@dataclass(frozen=True)
class ShortageRetentionPlan:
    """The output of ``plan_shortage_retention`` — a pure preview."""

    retention_days: int
    generated_at: _dt.datetime
    candidates: tuple[ShortageRetentionCandidate, ...]

    @property
    def rows_total(self) -> int:
        return sum(c.rows_to_delete for c in self.candidates)


# The eligibility predicate, shared verbatim by plan (COUNT) and apply
# (DELETE): status='resolved' (status='active' is NEVER reachable — hardcoded
# literal, not a parameter), old enough, and NOT the scenario's own latest
# completed calc_run (COALESCEd against an impossible all-zero sentinel UUID
# so a scenario with NO completed run at all does not accidentally exclude
# every one of its resolved shortages — see the module docstring).
_NEVER_A_REAL_CALC_RUN_ID = UUID("00000000-0000-0000-0000-000000000000")

_SHORTAGE_RETENTION_SCOPE = """
    status = 'resolved'
    AND updated_at < %s
    AND calc_run_id <> COALESCE(
        (SELECT cr.calc_run_id FROM calc_runs cr
         WHERE cr.scenario_id = shortages.scenario_id AND cr.status = 'completed'
         ORDER BY cr.completed_at DESC NULLS LAST, cr.created_at DESC
         LIMIT 1),
        %s
    )
"""


def plan_shortage_retention(
    conn: DictRowConnection, retention_days: int = 30
) -> ShortageRetentionPlan:
    """SELECT-only: per-scenario counts of resolved shortages old enough to
    purge, excluding each scenario's own latest completed calc_run. Writes
    NOTHING."""
    if retention_days < 0:
        raise ValueError(f"retention_days must be >= 0, got {retention_days}")

    now = _db_now(conn)
    cutoff = now - _dt.timedelta(days=retention_days)

    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute(
        f"""
        SELECT scenario_id, COUNT(*) AS n
        FROM shortages
        WHERE {_SHORTAGE_RETENTION_SCOPE}
        GROUP BY scenario_id
        HAVING COUNT(*) > 0
        ORDER BY scenario_id
        """,  # noqa: S608 — static, no caller-controlled identifiers
        (cutoff, _NEVER_A_REAL_CALC_RUN_ID),
    ).fetchall()

    candidates = tuple(
        ShortageRetentionCandidate(
            scenario_id=_coerce_uuid(row["scenario_id"]), rows_to_delete=int(row["n"])
        )
        for row in rows
    )
    return ShortageRetentionPlan(
        retention_days=retention_days, generated_at=now, candidates=candidates
    )


def _apply_shortage_retention_one(
    conn: DictRowConnection,
    scenario_id: UUID,
    retention_days: int,
    cutoff: _dt.datetime,
    executed_by: str,
) -> PurgeRunResult:
    cur = conn.execute(
        f"""
        DELETE FROM shortages
        WHERE scenario_id = %s AND {_SHORTAGE_RETENTION_SCOPE}
        """,  # noqa: S608 — static, no caller-controlled identifiers
        (scenario_id, cutoff, _NEVER_A_REAL_CALC_RUN_ID),
    )
    deleted = cur.rowcount if cur.rowcount is not None else 0
    if deleted <= 0:
        # Re-derived fresh at DELETE time, not trusting the plan's pre-count —
        # nothing left to touch (already retained, or nothing ever matched).
        logger.info(
            "purge.shortage_retention.skip scenario_id=%s reason=nothing_eligible",
            scenario_id,
        )
        return PurgeRunResult(
            scenario_id=scenario_id,
            run_id=None,
            skipped=True,
            per_table_counts={},
            rows_deleted_total=0,
        )

    per_table_counts = {"shortages": deleted}
    # Pinned dict_row cursor — see the identical comment in _apply_one.
    insert_cur = conn.cursor(row_factory=dict_row)
    run_id = insert_cur.execute(
        """
        INSERT INTO maintenance_purge_runs
            (scenario_id, mode, ttl_days, per_table_counts, rows_deleted_total, executed_by)
        VALUES (%s, 'apply', %s, %s, %s, %s)
        RETURNING run_id
        """,
        (scenario_id, retention_days, _to_jsonb(per_table_counts), deleted, executed_by),
    ).fetchone()
    if run_id is None:  # INSERT..RETURNING yields exactly one row — fail loudly
        raise RuntimeError("maintenance_purge_runs INSERT returned no row")
    run_id = _coerce_uuid(run_id["run_id"])

    emit_stream_event(
        conn,
        "purge_executed",
        scenario_id,
        field_changed="shortage_retention",
        old_text=executed_by,
        new_text=str(run_id),
        new_quantity=deleted,
        source="engine",
    )

    logger.info(
        "purge.shortage_retention.applied scenario_id=%s run_id=%s rows_deleted=%d executed_by=%s",
        scenario_id, run_id, deleted, executed_by,
    )
    return PurgeRunResult(
        scenario_id=scenario_id,
        run_id=run_id,
        skipped=False,
        per_table_counts=per_table_counts,
        rows_deleted_total=deleted,
    )


def apply_shortage_retention(
    conn: DictRowConnection,
    plan: ShortageRetentionPlan,
    executed_by: str,
) -> tuple[PurgeRunResult, ...]:
    """The sole writer of the shortage-retention sweep. For every scenario in
    ``plan``: re-executes the SAME eligibility predicate at DELETE time
    (never trusts the plan's pre-count — a resolved shortage may have been
    re-activated or the scenario's completed calc_run may have changed since
    the plan was built), writes one ``maintenance_purge_runs`` row + one
    ``purge_executed`` event PER SCENARIO TOUCHED (skips silently when a
    scenario's predicate now matches zero rows — idempotent), and ANALYZEs
    ``shortages`` once at the end if anything was deleted.

    ``status = 'active'`` shortages and each scenario's own latest completed
    calc_run are NEVER eligible — hardcoded into the shared predicate, not a
    parameter a caller could override.

    Does NOT commit — the caller owns the transaction.
    """
    if not executed_by or not executed_by.strip():
        raise ValueError("executed_by is required (audit attribution)")

    now = _db_now(conn)
    cutoff = now - _dt.timedelta(days=plan.retention_days)

    results: list[PurgeRunResult] = []
    any_deleted = False
    for candidate in plan.candidates:
        result = _apply_shortage_retention_one(
            conn, candidate.scenario_id, plan.retention_days, cutoff, executed_by
        )
        results.append(result)
        any_deleted = any_deleted or result.rows_deleted_total > 0

    if any_deleted:
        _analyze_tables(conn, {"shortages"})

    return tuple(results)
