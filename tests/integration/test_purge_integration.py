"""
tests/integration/test_purge_integration.py — chantier PURGE-1 (migration 076).

DB-backed coverage of the scenario-fork purge + shortage-retention lifecycle
(``engine/maintenance/purge.py``) and its read-only HTTP preview
(``GET /v1/maintenance/purge-preview``), against a real Postgres — no mocks
(CLAUDE.md). The DB-free half (whitelist/exemption completeness derived from
the migrations) lives in ``tests/test_purge_whitelist_guard.py``; pure guard
logic in ``tests/test_purge_engine_pure.py``.

Locked contracts:

  1. FULL LIFECYCLE — a real fork (``ScenarioManager.create_scenario`` deep
     copy) propagated by the real SQL engine (shortage created by
     ``SHORTAGES_SQL``, not hand-inserted), payload seeded across EVERY
     whitelist table, archived with a backdated ``archived_at``:
       * dry-run (``plan_fork_purge``): per-table counts match independent
         SQL counts, ``ghost_members`` reported for visibility, and NOTHING
         is deleted by planning;
       * apply (``apply_fork_purge``): every whitelisted table is emptied for
         the fork, the ``scenarios`` row survives as a tombstone
         (``purged_at`` stamped, never deleted), ONE ``maintenance_purge_runs``
         audit row is written, ONE ``purge_executed`` event is emitted AFTER
         the events sweep (it survives its own purge — the ADR-005-amended
         carve-out), and the cascade-only ``ghost_members`` never appears in
         the DELETE counts (plan.rows_total = deleted + cascaded);
       * a second apply against the SAME (now stale) plan is an idempotent
         no-op: skipped result, no new audit row, no new event.

  2. ABSOLUTE GUARDS, re-verified on FRESH data even for a hand-built plan
     (defense in depth): baseline -> refused; non-archived fork -> refused;
     archived inside the TTL window -> refused; ``archived_at`` NULL ->
     refused; unknown scenario -> refused; already-purged -> idempotent
     no-op, never an exception. A refused apply deletes NOTHING. The planner
     itself never surfaces any of these as a candidate.

  3. INVARIANCE (the architect's required proof) — purging an archived fork
     changes NO baseline answer: ``ShortageDetector.get_active_shortages``
     on baseline, ``compare_scenarios`` over two LIVING scenarios
     (baseline + a live fork), and the ADR-030 evaluator path
     (``evaluate_and_persist`` re-run post-purge re-derives the byte-same
     verdict from its SELECTs over baseline shortages/snapshots). Full
     structural equality before/after, not just counts.

  4. SHORTAGE RETENTION — only ``resolved`` rows older than the retention
     window AND outside the scenario's latest completed calc_run are swept;
     ``active`` rows and the latest run's rows survive regardless of age;
     the COALESCE sentinel keeps a scenario with NO completed run eligible;
     one audit row + one ``purge_executed`` (``shortage_retention``
     discriminant) per scenario touched; idempotent second apply.

  5. HTTP PREVIEW — 401 without a token; 403 without the ``admin`` scope
     (checked BEFORE the kill switch: a scope-less caller can never probe
     the switch state); 503 when ``OOTILS_PURGE_ENABLED`` is unset/falsy
     (default OFF — destructive-adjacent capability, opt-in unlike the
     read-mostly switches); 200 with per-table counts when enabled, writing
     NOTHING (read purity: zero events / zero maintenance_purge_runs rows).
     There is deliberately NO apply endpoint to test — architect decision,
     the CLI is the only writer entry point.

Seeding style mirrors test_param_overlay_propagation_integration.py (direct
SQL seed + the real SqlPropagationEngine driven through the same low-level
``_propagate`` entry the seed calibrator uses) and the #392 minted-token /
TestClient pattern of test_snapshot_integration.py. Timestamps that must be
"old" are anchored on the DB-side now() via make_interval, never Python
now(). shortages.updated_at is backdated at INSERT time only — the
migration-016 trigger overwrites it on UPDATE, not on INSERT.
"""
from __future__ import annotations

import os
from dataclasses import asdict
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.kernel.calc.projection import ProjectionKernel
from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.engine.kernel.graph.traversal import GraphTraversal
from ootils_core.engine.kernel.shortage.detector import ShortageDetector
from ootils_core.engine.maintenance import (
    PURGE_WHITELIST,
    PurgeCandidate,
    PurgeGuardError,
    PurgePlan,
    ShortageRetentionPlan,
    apply_fork_purge,
    apply_shortage_retention,
    plan_fork_purge,
    plan_shortage_retention,
)
from ootils_core.engine.orchestration.calc_run import CalcRunManager
from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine
from ootils_core.engine.outcome import evaluate_and_persist
from ootils_core.engine.scenario.compare import compare_scenarios
from ootils_core.engine.scenario.manager import ScenarioManager
from ootils_core.engine.scenario.param_overlay import set_param_override

from .conftest import requires_db

pytestmark = requires_db

# Seeded by migration 002 (is_baseline=TRUE).
BASELINE = UUID("00000000-0000-0000-0000-000000000001")
LEGACY_TOKEN = "integration-test-token"

# Base safety stock: an isolated bucket-0 PI with no edges projects to
# closing_stock=0, i.e. a deterministic `below_safety_stock` shortage of
# qty=SS_BASE — the same clean signal test_param_overlay_propagation uses.
SS_BASE = Decimal("10")

# The auth layer reads OOTILS_API_TOKEN per request; create_app() refuses to
# start without it (same collection-time pattern as the sibling API tests).
os.environ.setdefault("OOTILS_API_TOKEN", LEGACY_TOKEN)


@pytest.fixture(autouse=True)
def _per_site_safety_scope(monkeypatch):
    """Pinned 2026-07-18 (ADR-021 safety_scope amendment, DESC-1 PR-C):
    OOTILS_SAFETY_SCOPE now defaults to 'national' (pilot arbitration,
    ADR-043), under which per-site `below_safety_stock` never fires — only
    a physical stockout does. This whole module's fixture (`SS_BASE` above,
    `_seed_pi_bucket`) deliberately seeds a closing_stock=0 bucket to
    produce a deterministic `below_safety_stock` row as PURGE-1's test
    payload (the row purged/retained, not the thing under test) — orthogonal
    to the safety_scope axis. Pinned explicitly to 'per_site' so this file
    keeps exercising that payload shape rather than silently losing its
    fixture data under the new default."""
    monkeypatch.setenv("OOTILS_SAFETY_SCOPE", "per_site")


# ---------------------------------------------------------------------------
# Seed helpers — graph side (mirrors test_param_overlay_propagation_integration)
# ---------------------------------------------------------------------------


def _seed_item(conn) -> UUID:
    return conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s) RETURNING item_id",
        (uuid4(), f"purge-item-{uuid4().hex[:8]}"),
    ).fetchone()["item_id"]


def _seed_location(conn) -> UUID:
    return conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s) RETURNING location_id",
        (uuid4(), f"purge-loc-{uuid4().hex[:8]}"),
    ).fetchone()["location_id"]


def _seed_planning_params(conn, item_id: UUID, location_id: UUID) -> None:
    """One CURRENT (effective_to NULL) item_planning_params row with
    safety_stock_qty=SS_BASE — what makes the zero-stock PI a shortage."""
    conn.execute(
        """
        INSERT INTO item_planning_params (
            item_id, location_id, effective_from, effective_to,
            lead_time_sourcing_days, safety_stock_qty, lot_size_rule
        ) VALUES (%s, %s, CURRENT_DATE, NULL, 30, %s, 'LOTFORLOT')
        """,
        (item_id, location_id, SS_BASE),
    )


def _cleanup_baseline_seed(conn, item_id: UUID, location_id: UUID) -> None:
    """Neutralize this test's COMMITTED baseline seed for the next run.

    The lifecycle test must commit its baseline seed (the deep-copy fork
    reads it from another statement), so the ``conn`` fixture's rollback
    teardown cannot undo it. Without this finalizer, a parametrized rerun
    ([fast-path] then [forced-fallback]) forks a baseline that still carries
    the PREVIOUS run's PI — the deep-copy copies every active baseline node,
    inflating the fork's node count and breaking the strict seed-sanity
    assertions (CI failure: ``assert 3 == 2``).

    DEACTIVATE, never DELETE: a delete-cascade here is unwinnable — on a
    mid-test failure the fork's un-purged payload still references the item
    (``nodes_item_id_fkey``), and the seeded ghost membership does too
    (``ghost_members_item_id_fkey``); one FK violation aborts the whole
    cleanup transaction and the pollution survives (both observed in CI).
    The deep-copy's pollution vector is exactly ``WHERE n.active = TRUE``,
    so flipping the baseline seed inactive closes it with a single UPDATE
    that can violate nothing. The inert seed rows (item/location/params)
    stay behind as uniquely-named test residue — same convention as the
    other committing tests in this file. Runs via ``request.addfinalizer``
    so a mid-test failure cleans up too."""
    _ = location_id  # kept in the signature for symmetry with the seed call
    conn.rollback()  # drop any aborted in-flight transaction first
    conn.execute(
        "UPDATE nodes SET active = FALSE, updated_at = NOW() "
        "WHERE scenario_id = %s AND item_id = %s",
        (BASELINE, item_id),
    )
    conn.commit()


def _seed_pi_bucket(conn, *, scenario_id, item_id, location_id) -> UUID:
    """A bucket-0 ProjectedInventory node in its own projection_series, no
    replenishes/consumes edges — projects to closing_stock=0, which is
    `below_safety_stock` for any safety_stock_qty > 0."""
    series_id = conn.execute(
        """
        INSERT INTO projection_series
            (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING series_id
        """,
        (uuid4(), item_id, location_id, scenario_id, date.today(), date.today()),
    ).fetchone()["series_id"]
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_span_start, time_span_end,
            projection_series_id, bucket_sequence,
            opening_stock, inflows, outflows, closing_stock,
            has_shortage, shortage_qty, active
        ) VALUES (
            %s, 'ProjectedInventory', %s, %s, %s,
            'day', %s, %s,
            %s, 0,
            0, 0, 0, 0,
            FALSE, 0, TRUE
        )
        """,
        (node_id, scenario_id, item_id, location_id,
         date.today(), date.today() + timedelta(days=7),
         series_id),
    )
    return node_id


def _build_sql_engine(conn) -> SqlPropagationEngine:
    store = GraphStore(conn)
    return SqlPropagationEngine(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        shortage_detector=ShortageDetector(),
    )


def _propagate_bucket(engine, conn, *, scenario_id, node_id) -> UUID:
    """Start a calc_run, mark `node_id` dirty, run the SQL engine's
    _propagate, resolve stale, complete the run. Returns the calc_run_id.
    Same shape as seed/projection/calibration.py:_run_propagation (and the
    sibling param-overlay test). COMMITS."""
    calc_mgr = CalcRunManager()
    calc_run = calc_mgr.start_calc_run(scenario_id=scenario_id, event_ids=[], db=conn)
    assert calc_run is not None, "could not acquire advisory lock for scenario"
    dirty = DirtyFlagManager()
    dirty.mark_dirty({node_id}, scenario_id, calc_run.calc_run_id, conn)
    dirty.flush_to_postgres(calc_run.calc_run_id, scenario_id, conn)

    engine._propagate(calc_run, {node_id}, conn)
    engine._shortage_detector.resolve_stale(
        scenario_id=scenario_id, calc_run_id=calc_run.calc_run_id, db=conn
    )
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE calc_run_id = %s",
        (calc_run.calc_run_id,),
    )
    conn.execute("SELECT pg_advisory_unlock(hashtext(%s)::bigint)", (str(scenario_id),))
    conn.commit()
    return calc_run.calc_run_id


def _fork(conn, name: str) -> UUID:
    """A REAL fork: ScenarioManager deep copy of every active baseline node +
    projection series (the temp mapping tables are ON COMMIT DROP, so each
    fork is committed immediately)."""
    scenario = ScenarioManager().create_scenario(f"{name}-{uuid4().hex[:8]}", BASELINE, conn)
    conn.commit()
    return scenario.scenario_id


def _fork_pi_node(conn, scenario_id: UUID, item_id: UUID) -> UUID:
    """The fork's deep-copied PI node for `item_id` (fresh node_id)."""
    row = conn.execute(
        "SELECT node_id FROM nodes WHERE scenario_id = %s AND item_id = %s "
        "AND node_type = 'ProjectedInventory'",
        (scenario_id, item_id),
    ).fetchall()
    assert len(row) == 1, f"expected exactly one copied PI node, got {len(row)}"
    return row[0]["node_id"]


def _archive(conn, scenario_id: UUID, *, days_ago: int):
    """Archive a fork with a DB-side backdated archived_at. COMMITS."""
    row = conn.execute(
        "UPDATE scenarios SET status = 'archived', "
        "archived_at = now() - make_interval(days => %s) "
        "WHERE scenario_id = %s RETURNING archived_at",
        (days_ago, scenario_id),
    ).fetchone()
    conn.commit()
    return row["archived_at"]


# ---------------------------------------------------------------------------
# Independent per-table counters (hand-written scoping — the test's own
# reading of "this scenario's rows", used to cross-check the planner's).
# ---------------------------------------------------------------------------

_CAUSAL_STEPS_COUNT = """
    SELECT COUNT(*) AS n FROM causal_steps
    WHERE explanation_id IN (
        SELECT e.explanation_id FROM explanations e
        JOIN calc_runs cr ON cr.calc_run_id = e.calc_run_id
        WHERE cr.scenario_id = %s)
"""
_EXPLANATIONS_COUNT = """
    SELECT COUNT(*) AS n FROM explanations
    WHERE calc_run_id IN (SELECT calc_run_id FROM calc_runs WHERE scenario_id = %s)
"""
_GHOST_MEMBERS_COUNT = """
    SELECT COUNT(*) AS n FROM ghost_members
    WHERE ghost_id IN (SELECT ghost_id FROM ghost_nodes WHERE scenario_id = %s)
"""


def _scoped_counts(conn, scenario_id: UUID) -> dict[str, int]:
    """Per-table row counts for one scenario, keyed like the planner's
    per_table_counts (13 whitelist tables + the cascade-only ghost_members)."""
    counts: dict[str, int] = {}
    for table in PURGE_WHITELIST:
        if table == "causal_steps":
            sql = _CAUSAL_STEPS_COUNT
        elif table == "explanations":
            sql = _EXPLANATIONS_COUNT
        else:
            sql = f"SELECT COUNT(*) AS n FROM {table} WHERE scenario_id = %s"
        counts[table] = conn.execute(sql, (scenario_id,)).fetchone()["n"]
    counts["ghost_members"] = conn.execute(
        _GHOST_MEMBERS_COUNT, (scenario_id,)
    ).fetchone()["n"]
    return counts


def _purge_runs_for(conn, scenario_id: UUID) -> list[dict]:
    return conn.execute(
        "SELECT * FROM maintenance_purge_runs WHERE scenario_id = %s "
        "ORDER BY executed_at",
        (scenario_id,),
    ).fetchall()


def _purge_events_for(conn, scenario_id: UUID) -> list[dict]:
    return conn.execute(
        "SELECT * FROM events WHERE scenario_id = %s "
        "AND event_type = 'purge_executed' ORDER BY created_at",
        (scenario_id,),
    ).fetchall()


def _scenario_row(conn, scenario_id: UUID) -> dict:
    return conn.execute(
        "SELECT status, is_baseline, archived_at, purged_at "
        "FROM scenarios WHERE scenario_id = %s",
        (scenario_id,),
    ).fetchone()


def _plan_for(plan: PurgePlan, scenario_id: UUID) -> PurgePlan:
    """Filter a real plan down to ONE candidate so tests stay independent of
    sibling candidates accumulated by earlier tests in this module."""
    candidates = tuple(c for c in plan.candidates if c.scenario_id == scenario_id)
    assert len(candidates) == 1, f"scenario {scenario_id} not among plan candidates"
    return PurgePlan(
        ttl_days=plan.ttl_days, generated_at=plan.generated_at, candidates=candidates
    )


def _hand_plan(scenario_id: UUID, *, ttl_days: int = 7, generated_at=None) -> PurgePlan:
    """A hand-built (attacker-shaped) plan bypassing the planner entirely —
    apply MUST re-verify every guard on fresh DB data, never trust this."""
    import datetime as _dt

    now = generated_at or _dt.datetime.now(_dt.timezone.utc)
    candidate = PurgeCandidate(
        scenario_id=scenario_id,
        name="hand-built",
        archived_at=now,  # irrelevant: apply re-reads the row
        per_table_counts={},
    )
    return PurgePlan(ttl_days=ttl_days, generated_at=now, candidates=(candidate,))


def _seed_fork_payload(conn, *, fork: UUID, fork_pi: UUID, calc_run_id: UUID,
                       item_id: UUID, location_id: UUID) -> None:
    """Populate every whitelisted table the propagation itself does not:
    a child node with a parent_node_id self-reference (the migration-024
    nullify path), an edge, an explanation + causal step, a node override, a
    planning-param overlay override, a dirty flag, a scenario diff, a fork
    event, and a ghost with one member (the ON DELETE CASCADE visibility
    case). COMMITS."""
    # Child node self-referencing the fork PI via parent_node_id (mig 024).
    child = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id,
                           time_grain, time_ref, parent_node_id, active)
        VALUES (%s, 'ProjectedInventory', %s, %s, %s, 'exact_date', CURRENT_DATE,
                %s, TRUE)
        """,
        (child, fork, item_id, location_id, fork_pi),
    )
    conn.execute(
        "INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id) "
        "VALUES ('depends_on', %s, %s, %s)",
        (child, fork_pi, fork),
    )
    explanation_id = uuid4()
    conn.execute(
        "INSERT INTO explanations (explanation_id, calc_run_id, target_node_id, "
        " target_type, summary) VALUES (%s, %s, %s, 'Shortage', 'purge-test seed')",
        (explanation_id, calc_run_id, fork_pi),
    )
    conn.execute(
        "INSERT INTO causal_steps (step_id, explanation_id, step, fact) "
        "VALUES (%s, %s, 1, 'purge-test causal step')",
        (uuid4(), explanation_id),
    )
    conn.execute(
        "INSERT INTO scenario_overrides (scenario_id, node_id, field_name, "
        " new_value, applied_by) VALUES (%s, %s, 'quantity', '42', 'purge-test')",
        (fork, fork_pi),
    )
    set_param_override(
        conn, fork, item_id, "safety_stock_qty", "25", "purge-test",
        location_id=location_id,
    )
    conn.execute(
        "INSERT INTO dirty_nodes (calc_run_id, node_id, scenario_id) "
        "VALUES (%s, %s, %s)",
        (calc_run_id, child, fork),
    )
    # scenario_diffs needs a baseline calc_run; antidate it so it never
    # becomes the baseline's LATEST run for any other test in this module.
    baseline_run = conn.execute(
        "INSERT INTO calc_runs (calc_run_id, scenario_id, status, started_at, completed_at) "
        "VALUES (%s, %s, 'completed', now() - make_interval(days => 10), "
        "        now() - make_interval(days => 10)) RETURNING calc_run_id",
        (uuid4(), BASELINE),
    ).fetchone()["calc_run_id"]
    conn.execute(
        "INSERT INTO scenario_diffs (scenario_id, baseline_calc_run_id, "
        " scenario_calc_run_id, node_id, field_name, baseline_value, scenario_value) "
        "VALUES (%s, %s, %s, %s, 'closing_stock', '0', '-10')",
        (fork, baseline_run, calc_run_id, fork_pi),
    )
    conn.execute(
        "INSERT INTO events (event_type, scenario_id, field_changed, new_text, "
        " processed, source) VALUES ('test_event', %s, 'purge-test', 'seed', TRUE, 'test')",
        (fork,),
    )
    ghost_id = conn.execute(
        "INSERT INTO ghost_nodes (name, ghost_type, scenario_id, node_id) "
        "VALUES (%s, 'phase_transition', %s, %s) RETURNING ghost_id",
        (f"purge-ghost-{uuid4().hex[:8]}", fork, fork_pi),
    ).fetchone()["ghost_id"]
    conn.execute(
        "INSERT INTO ghost_members (ghost_id, item_id, role) VALUES (%s, %s, 'member')",
        (ghost_id, item_id),
    )
    conn.commit()


# ===========================================================================
# 1. Full lifecycle — dry-run, apply, idempotent re-apply.
# ===========================================================================


@pytest.mark.parametrize(
    "force_fallback", [False, True], ids=["fast-path", "forced-fallback"]
)
def test_fork_purge_lifecycle_dry_run_apply_idempotent(
    conn, monkeypatch, force_fallback, request
):
    """The flagship path. Real fork, real propagation (the shortage row comes
    out of SHORTAGES_SQL, never hand-inserted), payload across every
    whitelist table, backdated archive — then the three phases locked by the
    module docstring's contract 1.

    Parametrized (ADR-040 extension, 2026-07-12) over both trigger-firing
    paths of the whitelist DELETE loop: ``fast-path`` lets
    ``_delete_whitelist_for_scenario`` engage the real
    ``session_replication_role='replica'`` derogation when the test role's
    privileges allow it (silently falling back on its own otherwise — same
    as production); ``forced-fallback`` monkeypatches
    ``engine.maintenance.purge.enable_replica_role`` with the exact
    observable effect of an ``InsufficientPrivilege`` denial (the same
    savepoint dance the real handler performs, then ``False``) — the
    ``test_fork_replica_parity_integration.py`` pattern, applied to the
    purge site. Every assertion below is identical across both: the
    lifecycle's OUTCOME must never depend on which path the DELETE loop
    took, only its cost does (ADR-040's core claim).
    """
    if force_fallback:
        from ootils_core.engine.maintenance import purge as purge_module

        def _denied(conn_, *, savepoint_name, log_event=None, fallback_description=None):
            conn_.execute(f"SAVEPOINT {savepoint_name}")
            conn_.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            conn_.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            return False

        monkeypatch.setattr(purge_module, "enable_replica_role", _denied)

    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    request.addfinalizer(lambda: _cleanup_baseline_seed(conn, item_id, location_id))
    _seed_planning_params(conn, item_id, location_id)
    _seed_pi_bucket(conn, scenario_id=BASELINE, item_id=item_id, location_id=location_id)
    conn.commit()

    fork = _fork(conn, "purge-lifecycle")
    fork_pi = _fork_pi_node(conn, fork, item_id)
    engine = _build_sql_engine(conn)
    calc_run_id = _propagate_bucket(engine, conn, scenario_id=fork, node_id=fork_pi)

    # Sanity: propagation itself created the fork's shortage.
    shortage = conn.execute(
        "SELECT shortage_qty, severity_class, status FROM shortages "
        "WHERE scenario_id = %s",
        (fork,),
    ).fetchone()
    assert shortage is not None, "propagation must have detected the fork shortage"
    assert Decimal(str(shortage["shortage_qty"])) == SS_BASE
    assert shortage["severity_class"] == "below_safety_stock"
    assert shortage["status"] == "active"

    _seed_fork_payload(
        conn, fork=fork, fork_pi=fork_pi, calc_run_id=calc_run_id,
        item_id=item_id, location_id=location_id,
    )
    archived_at = _archive(conn, fork, days_ago=30)

    before = _scoped_counts(conn, fork)
    # Every whitelisted table (and the cascade-only ghost_members) holds the
    # exact payload this test seeded — the purge sweep is never vacuous.
    assert before["nodes"] == 2, "deep-copied PI + the parent_node_id child"
    assert before["shortages"] == 1
    assert before["edges"] == 1
    assert before["explanations"] == 1
    assert before["causal_steps"] == 1
    assert before["scenario_overrides"] == 1
    assert before["scenario_planning_overrides"] == 1
    assert before["dirty_nodes"] == 1
    assert before["scenario_diffs"] == 1
    assert before["events"] == 1
    assert before["ghost_nodes"] == 1
    assert before["ghost_members"] == 1
    assert before["calc_runs"] == 1
    assert before["projection_series"] >= 1

    # --- Dry-run: counts correct, NOTHING deleted. --------------------------
    plan = plan_fork_purge(conn, ttl_days=7)
    candidate = next(c for c in plan.candidates if c.scenario_id == fork)
    assert candidate.per_table_counts == before, (
        "the planner's per-table counts must match an independent SQL count"
    )
    assert candidate.rows_total == sum(before.values())
    assert candidate.archived_at == archived_at

    assert _scoped_counts(conn, fork) == before, "planning must delete nothing"
    assert _purge_runs_for(conn, fork) == [], "a dry-run writes no audit row"
    assert _purge_events_for(conn, fork) == [], "a dry-run emits no event"
    assert _scenario_row(conn, fork)["purged_at"] is None

    # --- Apply: payload swept table by table, tombstone + audit + event. ----
    my_plan = _plan_for(plan, fork)
    results = apply_fork_purge(conn, my_plan, executed_by="test:purge-1")
    conn.commit()
    (result,) = results
    assert result.skipped is False
    assert result.run_id is not None

    # ghost_members is cascade-deleted, never a direct DELETE — it appears in
    # the PLAN counts (visibility) but not in the APPLY counts.
    expected_deleted = {k: v for k, v in before.items() if k != "ghost_members"}
    assert result.per_table_counts == expected_deleted
    assert result.rows_deleted_total == sum(expected_deleted.values())
    assert candidate.rows_total == result.rows_deleted_total + before["ghost_members"]

    after = _scoped_counts(conn, fork)
    for table, count in after.items():
        if table == "events":
            assert count == 1, (
                "exactly ONE fork event survives: the purge_executed "
                "confirmation, emitted AFTER the events sweep"
            )
        else:
            assert count == 0, f"{table} must be empty for the purged fork"

    # Tombstone: the scenarios row survives forever, purged_at stamped.
    row = _scenario_row(conn, fork)
    assert row is not None, "the scenarios row is NEVER deleted"
    assert row["status"] == "archived"
    assert row["purged_at"] is not None
    assert row["archived_at"] == archived_at

    # Audit row.
    runs = _purge_runs_for(conn, fork)
    assert len(runs) == 1
    run = runs[0]
    assert run["run_id"] == result.run_id
    assert run["mode"] == "apply"
    assert run["ttl_days"] == 7
    assert run["rows_deleted_total"] == result.rows_deleted_total
    assert run["per_table_counts"] == expected_deleted
    assert run["executed_by"] == "test:purge-1"

    # Stream event (typed-column contract, engine/events/emit.py header).
    events = _purge_events_for(conn, fork)
    assert len(events) == 1
    event = events[0]
    assert event["field_changed"] == "fork_purge"
    assert event["old_text"] == "test:purge-1"
    assert event["new_text"] == str(result.run_id)
    assert int(event["new_quantity"]) == result.rows_deleted_total
    assert event["source"] == "engine"
    assert event["processed"] is True

    # --- Second apply on the SAME stale plan: idempotent no-op. -------------
    results2 = apply_fork_purge(conn, my_plan, executed_by="test:purge-2")
    conn.commit()
    (result2,) = results2
    assert result2.skipped is True
    assert result2.run_id is None
    assert result2.rows_deleted_total == 0
    assert result2.per_table_counts == {}
    assert len(_purge_runs_for(conn, fork)) == 1, "no second audit row"
    assert len(_purge_events_for(conn, fork)) == 1, "no second event"

    # A fresh plan no longer surfaces the purged fork.
    fresh = plan_fork_purge(conn, ttl_days=7)
    assert fork not in {c.scenario_id for c in fresh.candidates}


# ===========================================================================
# 1b. Compensatory check (ADR-040 extension, 2026-07-12) — proves
#     _verify_whitelist_emptied actually fires and blocks a partial commit.
# ===========================================================================


def test_verify_whitelist_emptied_raises_on_residual_row(conn, monkeypatch):
    """A future regression that made the whitelist DELETE loop skip a table
    must never be allowed to commit a partial purge. Simulated by
    monkeypatching the `PURGE_WHITELIST` module global to drop 'shortages'
    from the DELETE loop's own iteration — `_delete_whitelist_for_scenario`
    and `plan_fork_purge` both read that SAME live global, so the fork's
    shortage row is never deleted — while `_VERIFY_WHITELIST_EMPTY_SQL`
    (built once at import time from the UN-patched, real 13-table
    whitelist, per its own module-level comment) still counts every real
    table, including the one just skipped: the residual shortage row is
    exactly what the check is designed to catch.

    Proves the fail-loudly contract end to end: `apply_fork_purge` raises
    RuntimeError, and after the caller's rollback NOTHING was committed —
    no purged_at stamp, no maintenance_purge_runs row, no purge_executed
    event, and the shortage row (the one the patched whitelist skipped) is
    still exactly there.
    """
    from ootils_core.engine.maintenance import purge as purge_module

    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id)
    _seed_pi_bucket(conn, scenario_id=BASELINE, item_id=item_id, location_id=location_id)
    conn.commit()

    fork = _fork(conn, "purge-residual-check")
    fork_pi = _fork_pi_node(conn, fork, item_id)
    engine = _build_sql_engine(conn)
    _propagate_bucket(engine, conn, scenario_id=fork, node_id=fork_pi)
    assert _scoped_counts(conn, fork)["shortages"] == 1, "fork must carry a real shortage"

    _archive(conn, fork, days_ago=30)

    truncated_whitelist = tuple(t for t in purge_module.PURGE_WHITELIST if t != "shortages")
    monkeypatch.setattr(purge_module, "PURGE_WHITELIST", truncated_whitelist)

    plan = plan_fork_purge(conn, ttl_days=7)
    my_plan = _plan_for(plan, fork)

    with pytest.raises(RuntimeError, match="residual row"):
        apply_fork_purge(conn, my_plan, executed_by="test:residual-check")
    conn.rollback()

    assert _scenario_row(conn, fork)["purged_at"] is None
    assert _purge_runs_for(conn, fork) == []
    assert _purge_events_for(conn, fork) == []
    assert _scoped_counts(conn, fork)["shortages"] == 1, (
        "the row the patched whitelist skipped must still be there — "
        "nothing partial was committed"
    )


# ===========================================================================
# 2. Absolute guards — planner eligibility + apply-time re-verification.
# ===========================================================================


class TestPurgeGuards:
    def test_planner_never_surfaces_ineligible_scenarios(self, conn):
        """Eligibility is the planner's own guard: baseline, an active fork,
        a freshly-archived fork, an archived-but-already-purged fork and a
        NULL-archived_at fork are all invisible; only the old-archived,
        never-purged fork is a candidate."""
        eligible = _fork(conn, "guard-eligible")
        _archive(conn, eligible, days_ago=30)

        active = _fork(conn, "guard-active")  # status stays 'active'

        recent = _fork(conn, "guard-recent")
        _archive(conn, recent, days_ago=0)

        purged = _fork(conn, "guard-purged")
        _archive(conn, purged, days_ago=30)
        conn.execute(
            "UPDATE scenarios SET purged_at = now() WHERE scenario_id = %s",
            (purged,),
        )

        null_archived = _fork(conn, "guard-null-archived")
        conn.execute(
            "UPDATE scenarios SET status = 'archived', archived_at = NULL "
            "WHERE scenario_id = %s",
            (null_archived,),
        )
        conn.commit()

        candidate_ids = {c.scenario_id for c in plan_fork_purge(conn, ttl_days=7).candidates}
        assert eligible in candidate_ids
        assert BASELINE not in candidate_ids
        assert active not in candidate_ids
        assert recent not in candidate_ids
        assert purged not in candidate_ids
        assert null_archived not in candidate_ids

    def test_apply_refuses_baseline_even_from_hand_built_plan(self, conn):
        nodes_before = conn.execute(
            "SELECT COUNT(*) AS n FROM nodes WHERE scenario_id = %s", (BASELINE,)
        ).fetchone()["n"]

        with pytest.raises(PurgeGuardError, match="baseline"):
            apply_fork_purge(conn, _hand_plan(BASELINE), executed_by="test:guard")
        conn.rollback()

        assert _scenario_row(conn, BASELINE)["purged_at"] is None
        nodes_after = conn.execute(
            "SELECT COUNT(*) AS n FROM nodes WHERE scenario_id = %s", (BASELINE,)
        ).fetchone()["n"]
        assert nodes_after == nodes_before, "a refused apply deletes NOTHING"
        assert _purge_runs_for(conn, BASELINE) == []

    def test_apply_refuses_non_archived_fork(self, conn):
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        _seed_pi_bucket(conn, scenario_id=BASELINE, item_id=item_id, location_id=location_id)
        conn.commit()
        fork = _fork(conn, "guard-not-archived")  # status 'active'

        with pytest.raises(PurgeGuardError, match="archived"):
            apply_fork_purge(conn, _hand_plan(fork), executed_by="test:guard")
        conn.rollback()

        assert _scenario_row(conn, fork)["purged_at"] is None
        assert _scoped_counts(conn, fork)["nodes"] >= 1, "fork data intact"
        assert _purge_runs_for(conn, fork) == []

    def test_apply_refuses_fork_archived_inside_ttl_window(self, conn):
        fork = _fork(conn, "guard-too-recent")
        _archive(conn, fork, days_ago=0)

        with pytest.raises(PurgeGuardError, match="TTL"):
            apply_fork_purge(conn, _hand_plan(fork, ttl_days=7), executed_by="test:guard")
        conn.rollback()

        assert _scenario_row(conn, fork)["purged_at"] is None
        assert _purge_runs_for(conn, fork) == []

    def test_apply_refuses_archived_at_null(self, conn):
        """An unknown archive time can never clear a TTL honestly — refused,
        never 'eligible by default'."""
        fork = _fork(conn, "guard-null-ts")
        conn.execute(
            "UPDATE scenarios SET status = 'archived', archived_at = NULL "
            "WHERE scenario_id = %s",
            (fork,),
        )
        conn.commit()

        with pytest.raises(PurgeGuardError, match="archived_at"):
            apply_fork_purge(conn, _hand_plan(fork), executed_by="test:guard")
        conn.rollback()
        assert _scenario_row(conn, fork)["purged_at"] is None

    def test_apply_refuses_unknown_scenario(self, conn):
        with pytest.raises(PurgeGuardError, match="not found"):
            apply_fork_purge(conn, _hand_plan(uuid4()), executed_by="test:guard")
        conn.rollback()

    def test_apply_on_already_purged_fork_is_noop_not_error(self, conn):
        """purged_at set is the ONE case treated as an idempotent no-op —
        never a guard violation (re-purge or a lost race)."""
        fork = _fork(conn, "guard-already-purged")
        _archive(conn, fork, days_ago=30)
        conn.execute(
            "UPDATE scenarios SET purged_at = now() WHERE scenario_id = %s",
            (fork,),
        )
        conn.commit()

        (result,) = apply_fork_purge(conn, _hand_plan(fork), executed_by="test:guard")
        conn.commit()
        assert result.skipped is True
        assert result.run_id is None
        assert result.rows_deleted_total == 0
        assert _purge_runs_for(conn, fork) == []
        assert _purge_events_for(conn, fork) == []

    def test_apply_requires_executed_by(self, conn):
        with pytest.raises(ValueError, match="executed_by"):
            apply_fork_purge(conn, _hand_plan(uuid4()), executed_by="   ")


# ===========================================================================
# 3. INVARIANCE — purging a fork changes NO baseline answer.
# ===========================================================================


def test_baseline_answers_invariant_across_fork_purge(conn):
    """The proof the architect required. Three canonical baseline read paths
    are captured BEFORE purging an archived fork and re-read AFTER: the
    ShortageDetector query surface, the SC-1 compare over two LIVING
    scenarios, and the ADR-030 outcome evaluator (whose classification is a
    pure function of its SELECTs over baseline shortages / snapshots /
    recommendations). Structural equality, not just counts."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id)
    baseline_pi = _seed_pi_bucket(
        conn, scenario_id=BASELINE, item_id=item_id, location_id=location_id
    )
    conn.commit()

    engine = _build_sql_engine(conn)
    _propagate_bucket(engine, conn, scenario_id=BASELINE, node_id=baseline_pi)

    # A LIVING fork (never purged) — the second compare_scenarios entry.
    living = _fork(conn, "invariance-living")
    living_pi = _fork_pi_node(conn, living, item_id)
    _propagate_bucket(engine, conn, scenario_id=living, node_id=living_pi)

    # The DOOMED fork: propagated (real shortages), archived past the TTL.
    doomed = _fork(conn, "invariance-doomed")
    doomed_pi = _fork_pi_node(conn, doomed, item_id)
    _propagate_bucket(engine, conn, scenario_id=doomed, node_id=doomed_pi)
    _archive(conn, doomed, days_ago=30)
    assert _scoped_counts(conn, doomed)["shortages"] == 1, "doomed fork has payload"

    # ADR-030 evaluator path: one APPROVED baseline reco predicting exactly
    # the shortage the propagation created (ratio 1.0 -> MATERIALIZED), plus
    # the observation snapshot that makes it classifiable.
    agent_run_id = conn.execute(
        "INSERT INTO agent_runs (agent_run_id, agent_name, scenario_id, status) "
        "VALUES (%s, 'shortage_watcher', %s, 'COMPLETED') RETURNING agent_run_id",
        (uuid4(), BASELINE),
    ).fetchone()["agent_run_id"]
    reco_id = conn.execute(
        """
        INSERT INTO recommendations (
            recommendation_id, agent_name, agent_run_id, scenario_id,
            item_id, item_external_id, shortage_date,
            deficit_qty, recommended_qty, estimated_cost, currency,
            action, status, confidence
        ) VALUES (%s, 'shortage_watcher', %s, %s, %s, %s, CURRENT_DATE,
                  %s, 20, 100, 'EUR', 'EXPEDITE', 'APPROVED', 'HIGH')
        RETURNING recommendation_id
        """,
        (uuid4(), agent_run_id, BASELINE, item_id, f"EXT-{uuid4().hex[:8]}", SS_BASE),
    ).fetchone()["recommendation_id"]
    conn.execute(
        "INSERT INTO inventory_snapshots (snapshot_id, scenario_id, item_id, "
        " location_id, as_of_date, on_hand_qty, source) "
        "VALUES (%s, %s, %s, %s, CURRENT_DATE, 0, 'cli')",
        (uuid4(), BASELINE, item_id, location_id),
    )
    conn.commit()

    def _outcome_row():
        row = conn.execute(
            "SELECT evaluation_status, predicted_shortage_date, predicted_deficit_qty, "
            " observed_deficit_qty, avoided_severity_usd, snapshot_id "
            "FROM recommendation_outcomes WHERE recommendation_id = %s",
            (reco_id,),
        ).fetchone()
        assert row is not None
        return row

    detector = ShortageDetector()

    # ---- BEFORE ------------------------------------------------------------
    evaluate_and_persist(conn, str(BASELINE))
    conn.commit()
    outcome_before = _outcome_row()
    assert outcome_before["evaluation_status"] == "MATERIALIZED", (
        "sanity: the evaluator really classified through the ratio branch"
    )
    shortages_before = detector.get_active_shortages(BASELINE, conn)
    assert len(shortages_before) >= 1, "baseline holds a real active shortage"
    compare_before = asdict(compare_scenarios(conn, [BASELINE, living]))

    # ---- PURGE the doomed fork ----------------------------------------------
    plan = plan_fork_purge(conn, ttl_days=7)
    (result,) = apply_fork_purge(conn, _plan_for(plan, doomed), executed_by="test:invariance")
    conn.commit()
    assert result.skipped is False and result.rows_deleted_total > 0
    # The purge is not vacuous: the doomed fork's payload is really gone.
    doomed_after = _scoped_counts(conn, doomed)
    assert doomed_after["shortages"] == 0 and doomed_after["nodes"] == 0

    # ---- AFTER: byte-same baseline answers ----------------------------------
    shortages_after = detector.get_active_shortages(BASELINE, conn)
    assert shortages_after == shortages_before, (
        "get_active_shortages(baseline) changed across a fork purge"
    )

    compare_after = asdict(compare_scenarios(conn, [BASELINE, living]))
    assert compare_after == compare_before, (
        "compare_scenarios over two living scenarios changed across a fork purge"
    )

    evaluate_and_persist(conn, str(BASELINE))
    conn.commit()
    outcome_after = _outcome_row()
    assert outcome_after == outcome_before, (
        "the evaluator re-derived a different verdict from post-purge baseline data"
    )


# ===========================================================================
# 4. Shortage retention — resolved + old + superseded ONLY.
# ===========================================================================


def _seed_retention_scenario(conn):
    """One fork with two completed calc_runs (old + latest) and four
    shortages spanning the eligibility matrix. Returns (scenario_id,
    {label: shortage_id}). updated_at is backdated at INSERT time — the
    migration-016 trigger only rewrites it on UPDATE."""
    scenario_id = conn.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status, parent_scenario_id) "
        "VALUES (%s, %s, FALSE, 'active', %s) RETURNING scenario_id",
        (uuid4(), f"retention-{uuid4().hex[:8]}", BASELINE),
    ).fetchone()["scenario_id"]
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)

    def _run(days_ago: int) -> UUID:
        return conn.execute(
            "INSERT INTO calc_runs (calc_run_id, scenario_id, status, started_at, completed_at) "
            "VALUES (%s, %s, 'completed', now() - make_interval(days => %s), "
            "        now() - make_interval(days => %s)) RETURNING calc_run_id",
            (uuid4(), scenario_id, days_ago, days_ago),
        ).fetchone()["calc_run_id"]

    old_run = _run(60)
    latest_run = _run(0)

    def _shortage(calc_run_id: UUID, *, status: str, updated_days_ago: int) -> UUID:
        pi_node = conn.execute(
            "INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id, "
            " time_grain, time_ref, active) "
            "VALUES (%s, 'ProjectedInventory', %s, %s, %s, 'exact_date', CURRENT_DATE, TRUE) "
            "RETURNING node_id",
            (uuid4(), scenario_id, item_id, location_id),
        ).fetchone()["node_id"]
        return conn.execute(
            "INSERT INTO shortages (shortage_id, scenario_id, pi_node_id, item_id, "
            " location_id, shortage_date, shortage_qty, severity_score, calc_run_id, "
            " status, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, 5, 50, %s, %s, "
            "        now() - make_interval(days => %s)) RETURNING shortage_id",
            (uuid4(), scenario_id, pi_node, item_id, location_id,
             calc_run_id, status, updated_days_ago),
        ).fetchone()["shortage_id"]

    ids = {
        # (a) resolved + old + superseded run -> THE ONLY eligible row.
        "eligible": _shortage(old_run, status="resolved", updated_days_ago=45),
        # (b) resolved + old but on the LATEST completed run -> protected.
        "latest_run": _shortage(latest_run, status="resolved", updated_days_ago=45),
        # (c) resolved + superseded run but too fresh -> protected.
        "too_fresh": _shortage(old_run, status="resolved", updated_days_ago=1),
        # (d) ACTIVE, old, superseded run -> NEVER eligible by construction.
        "active": _shortage(old_run, status="active", updated_days_ago=45),
    }
    conn.commit()
    return scenario_id, ids


def test_shortage_retention_deletes_only_superseded_old_resolved(conn):
    scenario_id, ids = _seed_retention_scenario(conn)

    # --- Plan: exactly one eligible row, nothing deleted. --------------------
    plan = plan_shortage_retention(conn, retention_days=30)
    candidate = next(c for c in plan.candidates if c.scenario_id == scenario_id)
    assert candidate.rows_to_delete == 1
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM shortages WHERE scenario_id = %s", (scenario_id,)
    ).fetchone()["n"]
    assert total == 4, "planning must delete nothing"

    # --- Apply: only (a) goes; actives + latest run + fresh rows survive. ----
    my_plan = ShortageRetentionPlan(
        retention_days=30, generated_at=plan.generated_at, candidates=(candidate,)
    )
    (result,) = apply_shortage_retention(conn, my_plan, executed_by="test:retention")
    conn.commit()
    assert result.skipped is False
    assert result.per_table_counts == {"shortages": 1}
    assert result.rows_deleted_total == 1

    surviving = {
        r["shortage_id"]: r["status"]
        for r in conn.execute(
            "SELECT shortage_id, status FROM shortages WHERE scenario_id = %s",
            (scenario_id,),
        ).fetchall()
    }
    assert ids["eligible"] not in surviving
    assert surviving[ids["latest_run"]] == "resolved", "latest completed run protected"
    assert surviving[ids["too_fresh"]] == "resolved", "inside the window protected"
    assert surviving[ids["active"]] == "active", "active NEVER eligible"

    # Audit row + typed event (shortage_retention discriminant).
    runs = _purge_runs_for(conn, scenario_id)
    assert len(runs) == 1
    assert runs[0]["mode"] == "apply"
    assert runs[0]["ttl_days"] == 30
    assert runs[0]["per_table_counts"] == {"shortages": 1}
    assert runs[0]["rows_deleted_total"] == 1
    assert runs[0]["executed_by"] == "test:retention"

    events = _purge_events_for(conn, scenario_id)
    assert len(events) == 1
    assert events[0]["field_changed"] == "shortage_retention"
    assert events[0]["new_text"] == str(result.run_id)
    assert int(events[0]["new_quantity"]) == 1

    # --- Idempotent second apply on the same (stale) plan. -------------------
    (result2,) = apply_shortage_retention(conn, my_plan, executed_by="test:retention")
    conn.commit()
    assert result2.skipped is True
    assert result2.run_id is None
    assert len(_purge_runs_for(conn, scenario_id)) == 1
    assert len(_purge_events_for(conn, scenario_id)) == 1

    # A fresh plan no longer surfaces the scenario.
    fresh = plan_shortage_retention(conn, retention_days=30)
    assert scenario_id not in {c.scenario_id for c in fresh.candidates}

    # The purged scenario is untouched as a SCENARIO: retention is not the
    # fork-purge lifecycle (no purged_at stamp).
    assert _scenario_row(conn, scenario_id)["purged_at"] is None


def test_shortage_retention_sentinel_scenario_without_completed_run(conn):
    """The COALESCE all-zero-sentinel branch: a scenario whose only calc_run
    never completed has NO latest completed run to protect — its old
    resolved rows stay eligible instead of being accidentally excluded."""
    scenario_id = conn.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
        "VALUES (%s, %s, FALSE, 'active') RETURNING scenario_id",
        (uuid4(), f"retention-sentinel-{uuid4().hex[:8]}",),
    ).fetchone()["scenario_id"]
    running_run = conn.execute(
        "INSERT INTO calc_runs (calc_run_id, scenario_id, status, started_at) "
        "VALUES (%s, %s, 'running', now() - make_interval(days => 60)) "
        "RETURNING calc_run_id",
        (uuid4(), scenario_id),
    ).fetchone()["calc_run_id"]
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    pi_node = conn.execute(
        "INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id, "
        " time_grain, time_ref, active) "
        "VALUES (%s, 'ProjectedInventory', %s, %s, %s, 'exact_date', CURRENT_DATE, TRUE) "
        "RETURNING node_id",
        (uuid4(), scenario_id, item_id, location_id),
    ).fetchone()["node_id"]
    conn.execute(
        "INSERT INTO shortages (shortage_id, scenario_id, pi_node_id, shortage_date, "
        " shortage_qty, severity_score, calc_run_id, status, updated_at) "
        "VALUES (%s, %s, %s, CURRENT_DATE, 5, 50, %s, 'resolved', "
        "        now() - make_interval(days => 45))",
        (uuid4(), scenario_id, pi_node, running_run),
    )
    conn.commit()

    plan = plan_shortage_retention(conn, retention_days=30)
    candidate = next(c for c in plan.candidates if c.scenario_id == scenario_id)
    assert candidate.rows_to_delete == 1

    my_plan = ShortageRetentionPlan(
        retention_days=30, generated_at=plan.generated_at, candidates=(candidate,)
    )
    (result,) = apply_shortage_retention(conn, my_plan, executed_by="test:retention")
    conn.commit()
    assert result.rows_deleted_total == 1
    remaining = conn.execute(
        "SELECT COUNT(*) AS n FROM shortages WHERE scenario_id = %s", (scenario_id,)
    ).fetchone()["n"]
    assert remaining == 0


# ===========================================================================
# 5. HTTP surface — GET /v1/maintenance/purge-preview.
# ===========================================================================


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """TestClient with get_db overridden onto the test DB (the #392 pattern of
    test_snapshot_integration.py). OotilsDB.conn() commits on success, so the
    read-purity assertions below are REAL — a preview that wrote anything
    would persist it."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = LEGACY_TOKEN

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


@pytest.fixture(autouse=True)
def _clear_token_cache():
    """Clear the in-process minted-token cache around every test so a token
    minted in one test never leaks a cached auth decision into another."""
    import ootils_core.api.auth as auth

    auth._token_cache.clear()
    yield
    auth._token_cache.clear()


def _db_conn(dsn):
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _mint_token(dsn, *, actor_kind: str, scopes: list[str]) -> str:
    """Insert one api_tokens row; return the cleartext (the DB stores its
    SHA-256 via the same hash_token the auth layer uses on lookup)."""
    from ootils_core.api.auth import hash_token, token_prefix

    clear = f"ootk_{actor_kind}_{uuid4().hex}"
    token_id = uuid4()
    with _db_conn(dsn) as c:
        c.execute(
            "INSERT INTO api_tokens (token_id, name, actor_kind, token_hash, "
            " token_prefix, scopes) VALUES (%s, %s, %s, %s, %s, %s)",
            (token_id, f"purge-{actor_kind}-{token_id}", actor_kind,
             hash_token(clear), token_prefix(clear), scopes),
        )
    return clear


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


PREVIEW = "/v1/maintenance/purge-preview"


class TestPurgePreviewEndpoint:
    def test_401_without_token(self, api_client, monkeypatch):
        monkeypatch.setenv("OOTILS_PURGE_ENABLED", "1")
        resp = api_client.get(PREVIEW)
        assert resp.status_code == 401, resp.text

    def test_403_without_admin_scope_even_when_switch_is_off(
        self, api_client, migrated_db, monkeypatch
    ):
        """Scope is checked BEFORE the kill switch (auth-first ordering in
        require_purge_enabled's docstring): a read-only caller gets 403, not
        503 — it can never probe the switch state."""
        monkeypatch.delenv("OOTILS_PURGE_ENABLED", raising=False)
        clear = _mint_token(migrated_db, actor_kind="agent", scopes=["read"])
        resp = api_client.get(PREVIEW, headers=_bearer(clear))
        assert resp.status_code == 403, resp.text
        assert "admin" in resp.json()["detail"].lower()

    def test_503_when_kill_switch_unset_default_off(self, api_client, monkeypatch):
        monkeypatch.delenv("OOTILS_PURGE_ENABLED", raising=False)
        resp = api_client.get(PREVIEW, headers=_bearer(LEGACY_TOKEN))
        assert resp.status_code == 503, resp.text
        assert "OOTILS_PURGE_ENABLED" in resp.json()["detail"]

    def test_503_when_kill_switch_falsy(self, api_client, monkeypatch):
        monkeypatch.setenv("OOTILS_PURGE_ENABLED", "0")
        resp = api_client.get(PREVIEW, headers=_bearer(LEGACY_TOKEN))
        assert resp.status_code == 503, resp.text

    def test_200_reports_both_sweeps_and_writes_nothing(
        self, api_client, migrated_db, monkeypatch
    ):
        """Kill switch ON + admin (legacy) token: both planners' output is
        reported verbatim, and the GET writes NOTHING (no events row, no
        maintenance_purge_runs row, no purged_at stamp, no deleted payload)."""
        monkeypatch.setenv("OOTILS_PURGE_ENABLED", "1")

        with _db_conn(migrated_db) as c:
            # One eligible archived fork carrying exactly one node.
            fork = c.execute(
                "INSERT INTO scenarios (scenario_id, name, is_baseline, status, "
                " archived_at, parent_scenario_id) "
                "VALUES (%s, %s, FALSE, 'archived', now() - make_interval(days => 30), %s) "
                "RETURNING scenario_id",
                (uuid4(), f"preview-fork-{uuid4().hex[:8]}", str(BASELINE)),
            ).fetchone()["scenario_id"]
            c.execute(
                "INSERT INTO nodes (node_id, node_type, scenario_id, time_grain, "
                " time_ref, active) "
                "VALUES (%s, 'ProjectedInventory', %s, 'exact_date', CURRENT_DATE, TRUE)",
                (uuid4(), fork),
            )
            events_before = c.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
            runs_before = c.execute(
                "SELECT COUNT(*) AS n FROM maintenance_purge_runs"
            ).fetchone()["n"]

        resp = api_client.get(PREVIEW, headers=_bearer(LEGACY_TOKEN))
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["ttl_days"] == 7, "default TTL echoed"
        assert body["retention_days"] == 30, "default retention echoed"
        assert body["generated_at"] is not None

        mine = next(
            c for c in body["fork_purge_candidates"] if c["scenario_id"] == str(fork)
        )
        assert mine["per_table_counts"]["nodes"] == 1
        assert "ghost_members" in mine["per_table_counts"], (
            "the cascade-only table is reported for operator visibility"
        )
        assert mine["rows_total"] == sum(mine["per_table_counts"].values())
        assert body["fork_purge_rows_total"] >= mine["rows_total"]
        assert body["shortage_retention_rows_total"] == sum(
            c["rows_to_delete"] for c in body["shortage_retention_candidates"]
        )

        # Read purity: the preview wrote NOTHING.
        with _db_conn(migrated_db) as c:
            assert c.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"] == events_before
            assert (
                c.execute("SELECT COUNT(*) AS n FROM maintenance_purge_runs").fetchone()["n"]
                == runs_before
            )
            row = c.execute(
                "SELECT purged_at FROM scenarios WHERE scenario_id = %s", (fork,)
            ).fetchone()
            assert row["purged_at"] is None
            n = c.execute(
                "SELECT COUNT(*) AS n FROM nodes WHERE scenario_id = %s", (fork,)
            ).fetchone()["n"]
            assert n == 1, "a preview must not delete the fork's payload"

    def test_custom_windows_echoed_and_validated(self, api_client, monkeypatch):
        monkeypatch.setenv("OOTILS_PURGE_ENABLED", "1")
        resp = api_client.get(
            PREVIEW, params={"ttl_days": 99, "retention_days": 120},
            headers=_bearer(LEGACY_TOKEN),
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["ttl_days"] == 99
        assert resp.json()["retention_days"] == 120

        resp = api_client.get(
            PREVIEW, params={"ttl_days": -1}, headers=_bearer(LEGACY_TOKEN)
        )
        assert resp.status_code == 422, "ge=0 validation on ttl_days"
