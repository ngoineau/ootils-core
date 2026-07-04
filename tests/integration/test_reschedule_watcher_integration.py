"""
tests/integration/test_reschedule_watcher_integration.py — chantier #346 PR-B.

DB-backed coverage of the GOVERNED reschedule emitter
(scripts/agent_reschedule_watcher.py) against a real Postgres, no mocks. The
watcher is a thin orchestrator over the pure core (mrp_core.reschedule_signals,
covered by tests/test_reschedule_signals.py) + the pure mapping
(engine.recommendation.reschedule, covered by
tests/test_reschedule_recommendation.py); this file asserts the pieces the pure
tests cannot: the real INSERT ... ON CONFLICT idempotence, the stale-DRAFT
expiration, scenario isolation through the #347 overlay, the ADR-021 no-write
into `shortages`, and the L3 CANCEL path — end to end on a seeded plan.

The seven invariants (one test each; STABILITY #2 is the headline):
  1. Emission        — a mis-dated firm receipt yields ONE governed DRAFT with
                       the right action/level/target/dates in `recommendations`
                       (never mrp_action_messages).
  2. Stability       — re-running on an UNCHANGED plan inserts ZERO new rows
                       (deterministic id + ON CONFLICT DO NOTHING). THE central
                       #346 invariant: a stable regenerative MRP never spams.
  3. Scenario iso    — a fork with a safety_stock_qty overlay (#347) shifts the
                       need date => a DIFFERENT message on the fork; baseline is
                       untouched.
  4. CANCEL => L3    — an entirely-surplus firm receipt (inside the horizon
                       edge) yields action=CANCEL at decision_level L3.
  5. ADR-021         — the watcher writes NOTHING into `shortages`
                       (count-before == count-after).
  6. Expiration      — a DRAFT whose mis-date is resolved between run 1 and run
                       2 flips to EXPIRED, and ONLY for this agent+scenario.

Determinism: every date is anchored on the DB-side CURRENT_DATE (never Python
now()), exactly like mrp_core.load_planning_data's horizon anchor and the
sibling watcher seeds (test_scenario_backed_watchers_integration.py). The seed
math is verified against the pure core in tests/test_reschedule_signals.py:
demand at bucket B with on-hand 0 gives a receipt whose need date is bucket B;
a receipt dated far from B (>> reschedule_min_days) fires a stable RESCHEDULE.
"""
from __future__ import annotations

import datetime as _dt
import sys
from pathlib import Path

import pytest

from .conftest import requires_db

# Import seam: mrp_core + watchers live under scripts/ (outside the package).
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_reschedule_watcher  # noqa: E402
import mrp_core as core  # noqa: E402
from agent_governance import decision_level  # noqa: E402

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from ootils_core.engine.scenario.param_overlay import set_param_override  # noqa: E402

pytestmark = [requires_db, pytest.mark.smoke]

BASELINE = core.BASELINE
AGENT = "reschedule_watcher"

# Bucket layout (weekly): a receipt whose need date lands N weeks out and whose
# current date is M weeks out fires a RESCHEDULE when |need - current| exceeds
# reschedule_min_days (default 3). We keep all movements ~months apart so the
# dampening threshold is never in play (the seed is about emission, not the
# dampening band — that band is unit-tested in test_reschedule_signals.py).
_WEEK = 7


def _run(dsn, scenario=None):
    """Drive the watcher in-process (main(argv) -> int), like the fleet smoke."""
    argv = ["--dsn", dsn, "--allow-dev"]
    if scenario is not None:
        argv += ["--scenario", str(scenario)]
    return agent_reschedule_watcher.main(argv)


def _drafts(dsn, scenario=BASELINE):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT * FROM recommendations WHERE agent_name=%s AND scenario_id=%s "
            "AND status='DRAFT'",
            (AGENT, str(scenario))).fetchall()


def _count_recos(dsn, scenario=BASELINE):
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM recommendations WHERE agent_name=%s AND scenario_id=%s",
            (AGENT, str(scenario))).fetchone()[0]


# ---------------------------------------------------------------------------
# Seed helpers. Each test gets a FUNCTION-scoped, freshly-truncated graph so the
# idempotence / expiration / isolation assertions never bleed across tests.
# ---------------------------------------------------------------------------


def _reset_graph(conn):
    """Wipe the per-test graph + agent artifacts. Keeps the migrated schema (and
    the migration-002 baseline scenario) intact; only clears rows this file
    seeds. TRUNCATE ... CASCADE so FK-linked recommendation rows go too."""
    conn.execute(
        "TRUNCATE nodes, edges, recommendations, agent_runs, "
        "item_planning_params, supplier_items, items, suppliers, locations, "
        "scenario_planning_overrides RESTART IDENTITY CASCADE"
    )
    # `shortages` may or may not have rows; truncate so the ADR-021 delta test
    # starts from a known baseline. Separate statement (not all schemas link it
    # into the graph FK web).
    conn.execute("TRUNCATE shortages RESTART IDENTITY CASCADE")


def _seed_common(conn, today):
    """One location, one supplier, one bought item (ITM) with an IPP row.

    Returns (loc_id, item_id). Lead time is irrelevant to reschedule need-date
    math (need date derives from demand-vs-on-hand-vs-safety, not lead time —
    see core._need_bucket_for_receipts), but IPP must exist so the loader reads
    reschedule_min_days (migration-061 DEFAULT 3) and safety_stock_qty for it.
    """
    loc_id = conn.execute(
        "INSERT INTO locations (name, location_type, external_id) "
        "VALUES (%s, %s, %s) RETURNING location_id",
        ("Resched Plant", "plant", "LOC-RS"),
    ).fetchone()[0]
    sup_id = conn.execute(
        "INSERT INTO suppliers (external_id, name, reliability_score, status) "
        "VALUES (%s, %s, %s, %s) RETURNING supplier_id",
        ("SUP-RS", "Resched Supplier", 0.95, "active"),
    ).fetchone()[0]
    item_id = conn.execute(
        "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING item_id",
        ("ITM", "Reschedule Item", "component", 40.0, "EUR"),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO item_planning_params "
        "(item_id, location_id, is_make, lead_time_sourcing_days, "
        " lead_time_manufacturing_days, lead_time_transit_days, safety_stock_qty, "
        " lot_size_rule, frozen_time_fence_days, slashed_time_fence_days, "
        " forecast_consumption_strategy) "
        "VALUES (%s,%s,FALSE,14,0,0,0,%s,0,1,%s)",
        (item_id, loc_id, "LOTFORLOT", "max_only"),
    )
    conn.execute(
        "INSERT INTO supplier_items "
        "(supplier_id, item_id, lead_time_days, unit_cost, currency, is_preferred) "
        "VALUES (%s,%s,14,4.0,%s,TRUE)",
        (sup_id, item_id, "EUR"),
    )
    return loc_id, item_id


def _node(conn, ntype, scenario, item_id, loc_id, today, days_out, qty, is_firm=False):
    conn.execute(
        "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
        " time_grain, time_ref, active, is_firm) "
        "VALUES (%s, %s, %s, %s, %s, 'exact_date', %s, TRUE, %s)",
        (ntype, str(scenario), item_id, loc_id, qty,
         today + _dt.timedelta(days=days_out), is_firm),
    )


def _onhand(conn, scenario, item_id, loc_id, today, qty):
    _node(conn, "OnHandSupply", scenario, item_id, loc_id, today, 0, qty)


def _demand(conn, scenario, item_id, loc_id, today, weeks_out, qty):
    _node(conn, "CustomerOrderDemand", scenario, item_id, loc_id, today, weeks_out * _WEEK, qty)


def _receipt(conn, scenario, item_id, loc_id, today, weeks_out, qty):
    """A firm PurchaseOrderSupply receipt (a committed order): enters
    sched_orders (FIRM_RECEIPT_TYPES) and is re-datable."""
    _node(conn, "PurchaseOrderSupply", scenario, item_id, loc_id, today,
          weeks_out * _WEEK, qty, is_firm=True)


# ===========================================================================
# 1. Emission — one mis-dated firm receipt -> one governed DRAFT.
# ===========================================================================


def test_emits_one_governed_draft_for_misdated_receipt(migrated_db):
    """A firm receipt arriving MONTHS before its need date -> a single DRAFT
    RESCHEDULE_OUT in `recommendations`, carrying the target node, both dates and
    the mapping-derived level. Never mrp_action_messages."""
    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True) as conn:
        today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn, today)
        # Demand of 100 at week 20; on-hand 0 => need bucket 20. Receipt lands at
        # week 4 => far too early => RESCHEDULE_OUT (push it to ~week 20).
        _onhand(conn, BASELINE, item_id, loc_id, today, 0)
        _demand(conn, BASELINE, item_id, loc_id, today, 20, 100)
        _receipt(conn, BASELINE, item_id, loc_id, today, 4, 100)
        node_id = conn.execute(
            "SELECT node_id FROM nodes WHERE node_type='PurchaseOrderSupply' "
            "AND scenario_id=%s", (BASELINE,)).fetchone()[0]

    assert _run(dsn) == 0
    rows = _drafts(dsn)
    assert len(rows) == 1, "exactly one reschedule DRAFT expected"
    r = rows[0]
    assert r["action"] == "RESCHEDULE_OUT"
    assert r["decision_level"] == decision_level("RESCHEDULE_OUT") == "L2"
    assert str(r["target_node_id"]) == str(node_id)
    assert r["current_receipt_date"] == today + _dt.timedelta(weeks=4)
    assert r["proposed_date"] == today + _dt.timedelta(weeks=20)
    assert r["proposed_date"] > r["current_receipt_date"]   # OUT = pushed later
    assert r["status"] == "DRAFT"
    assert r["item_external_id"] == "ITM"
    assert r["evidence"] is not None
    assert r["evidence"]["signal"] == "RESCHEDULE_OUT"

    # It must NOT have leaked into the legacy action-message table (governed
    # channel is `recommendations`, per the watcher docstring / architect call).
    with psycopg.connect(dsn) as conn:
        exists = conn.execute(
            "SELECT to_regclass('public.mrp_action_messages')").fetchone()[0]
        if exists is not None:
            n = conn.execute(
                "SELECT COUNT(*) FROM mrp_action_messages "
                "WHERE action LIKE 'RESCHEDULE%%'").fetchone()[0]
            assert n == 0, "reschedule watcher must not write mrp_action_messages"


# ===========================================================================
# 2. STABILITY — re-run on an unchanged plan inserts ZERO new rows.
# ===========================================================================


def test_rerun_on_unchanged_plan_inserts_zero_new_rows(migrated_db):
    """THE headline #346 invariant. After a first run, re-running on the exact
    same plan re-derives the SAME deterministic ids; ON CONFLICT DO NOTHING
    makes the second run a no-op. Row count before run 2 == after run 2."""
    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True) as conn:
        today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn, today)
        _onhand(conn, BASELINE, item_id, loc_id, today, 0)
        _demand(conn, BASELINE, item_id, loc_id, today, 20, 100)
        _receipt(conn, BASELINE, item_id, loc_id, today, 4, 100)

    assert _run(dsn) == 0
    after_run1 = _count_recos(dsn)
    assert after_run1 >= 1, "run 1 must emit at least one reschedule reco"
    drafts1 = {r["recommendation_id"] for r in _drafts(dsn)}

    # --- Second run on the identical plan: no new rows, same DRAFTs still active.
    assert _run(dsn) == 0
    after_run2 = _count_recos(dsn)
    assert after_run2 == after_run1, (
        f"stability broken: {after_run2 - after_run1} new rows on an unchanged plan"
    )
    drafts2 = {r["recommendation_id"] for r in _drafts(dsn)}
    assert drafts2 == drafts1, "the same DRAFTs must remain active (not superseded/re-minted)"

    # The run metrics must self-report the no-op (auditable idempotence).
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        run = conn.execute(
            "SELECT metrics FROM agent_runs WHERE agent_name=%s AND status='COMPLETED' "
            "ORDER BY started_at DESC LIMIT 1", (AGENT,)).fetchone()
        m = run["metrics"]
        assert m["recommendations_inserted"] == 0
        assert m["recommendations_idempotent_noop"] == m["recommendations_affirmed"]


# ===========================================================================
# 3. Scenario isolation — a fork overlay shifts the message; baseline unchanged.
# ===========================================================================


def test_fork_overlay_produces_different_message_baseline_untouched(migrated_db):
    """A safety_stock_qty overlay (#347) on a fork pulls the need date earlier,
    turning an on-time receipt into a RESCHEDULE_IN — a DIFFERENT message the
    baseline never sees. safety_stock_qty (not lead_time) is the overlay knob
    that actually moves a reschedule need date: core._need_bucket_for_receipts
    walks demand vs on-hand vs SAFETY STOCK, and is independent of lead time.

    Same nodes are seeded in BOTH scenarios (the loader reads nodes scoped by
    scenario_id, so a fork sees only its own graph); the ONLY difference is the
    fork's safety-stock override."""
    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        today = conn.execute("SELECT CURRENT_DATE").fetchone()["current_date"]
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn, today)

        # Fork scenario.
        fork_id = conn.execute(
            "INSERT INTO scenarios (name, is_baseline, status) "
            "VALUES ('resched-fork', FALSE, 'active') RETURNING scenario_id"
        ).fetchone()["scenario_id"]

        # Identical graph in both: demand 100 @ week 20, on-hand 0, receipt @
        # week 20 (ON its need date when ss=0 -> baseline emits NOTHING).
        for scen in (BASELINE, fork_id):
            _onhand(conn, scen, item_id, loc_id, today, 0)
            _demand(conn, scen, item_id, loc_id, today, 20, 100)
            _receipt(conn, scen, item_id, loc_id, today, 20, 100)

        # Overlay on the fork ONLY: raise safety stock so the balance dips below
        # ss at bucket 0 -> need date is pulled to now -> receipt is now LATE ->
        # RESCHEDULE_IN. set_param_override needs a dict_row conn (it is).
        set_param_override(
            conn, fork_id, item_id, "safety_stock_qty", "100", "reschedule-test",
        )

    # Run on baseline: the receipt is on its need date -> no message.
    assert _run(dsn, scenario=BASELINE) == 0
    assert _drafts(dsn, BASELINE) == [], "baseline receipt is on time -> no reschedule"

    # Run on the fork: the overlay shifted the need -> exactly one RESCHEDULE_IN.
    assert _run(dsn, scenario=fork_id) == 0
    fork_rows = _drafts(dsn, fork_id)
    assert len(fork_rows) == 1
    assert fork_rows[0]["action"] == "RESCHEDULE_IN"
    assert fork_rows[0]["proposed_date"] < fork_rows[0]["current_receipt_date"]
    assert str(fork_rows[0]["scenario_id"]) == str(fork_id)

    # Baseline stayed empty even after the fork run (no cross-scenario bleed).
    assert _drafts(dsn, BASELINE) == []


# ===========================================================================
# 4. CANCEL => L3 — an entirely-surplus firm receipt.
# ===========================================================================


def test_surplus_receipt_yields_cancel_at_l3(migrated_db):
    """A firm receipt with NO demand pulling it, sitting well inside the horizon
    (not on the edge), is entirely surplus -> action=CANCEL at decision_level
    L3 (the first watcher-emitted L3), proposed_date NULL."""
    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True) as conn:
        today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn, today)
        # No demand at all; receipt of 50 at week 10 (70 days out, far inside the
        # 540-day horizon) -> CANCEL.
        _onhand(conn, BASELINE, item_id, loc_id, today, 0)
        _receipt(conn, BASELINE, item_id, loc_id, today, 10, 50)

    assert _run(dsn) == 0
    rows = _drafts(dsn)
    assert len(rows) == 1
    r = rows[0]
    assert r["action"] == "CANCEL"
    assert r["decision_level"] == decision_level("CANCEL") == "L3"
    assert r["proposed_date"] is None
    # NOT-NULL shortage_date anchors on the current date for a CANCEL.
    assert r["current_receipt_date"] == today + _dt.timedelta(weeks=10)
    assert r["shortage_date"] == today + _dt.timedelta(weeks=10)
    assert r["evidence"]["signal"] == "CANCEL"
    assert r["evidence"]["delta_days"] is None


# ===========================================================================
# 5. ADR-021 — the watcher writes NOTHING into `shortages`.
# ===========================================================================


def test_watcher_never_writes_shortages(migrated_db):
    """ADR-021: `shortages` is ShortageDetector's alone. The reschedule watcher
    is read-only against it — the shortages row count is unchanged across a run
    that DOES emit reschedule recommendations."""
    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True) as conn:
        today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn, today)
        _onhand(conn, BASELINE, item_id, loc_id, today, 0)
        _demand(conn, BASELINE, item_id, loc_id, today, 20, 100)
        _receipt(conn, BASELINE, item_id, loc_id, today, 4, 100)
        before = conn.execute("SELECT COUNT(*) FROM shortages").fetchone()[0]

    assert _run(dsn) == 0
    assert _drafts(dsn), "run must actually emit a reschedule reco (else the test proves nothing)"

    with psycopg.connect(dsn) as conn:
        after = conn.execute("SELECT COUNT(*) FROM shortages").fetchone()[0]
    assert after == before == 0, "reschedule watcher must never touch `shortages` (ADR-021)"


# ===========================================================================
# 6. Expiration — a resolved DRAFT flips to EXPIRED, scoped to this agent+scen.
# ===========================================================================


def test_resolved_draft_is_expired_scoped_to_agent_and_scenario(migrated_db):
    """A DRAFT emitted at run 1 whose mis-date is resolved before run 2 (the
    receipt is re-dated onto its need date) must flip to EXPIRED at run 2 — and
    the expiration must touch ONLY this agent's DRAFTs in this scenario (a
    foreign agent's DRAFT on the same scenario is left alone)."""
    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True) as conn:
        today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn, today)
        _onhand(conn, BASELINE, item_id, loc_id, today, 0)
        _demand(conn, BASELINE, item_id, loc_id, today, 20, 100)
        _receipt(conn, BASELINE, item_id, loc_id, today, 4, 100)
        po_node = conn.execute(
            "SELECT node_id FROM nodes WHERE node_type='PurchaseOrderSupply' "
            "AND scenario_id=%s", (BASELINE,)).fetchone()[0]

    assert _run(dsn) == 0
    run1_drafts = _drafts(dsn)
    assert len(run1_drafts) == 1
    run1_id = run1_drafts[0]["recommendation_id"]

    # Seed a FOREIGN agent's DRAFT on the same scenario: the expiration must not
    # touch it (scoped to agent_name + scenario_id). Reuse the run1 agent_run_id
    # FK target so the row is valid.
    with psycopg.connect(dsn, autocommit=True) as conn:
        foreign_run = conn.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) "
            "VALUES ('other_watcher', %s, 'COMPLETED') RETURNING agent_run_id",
            (BASELINE,)).fetchone()[0]
        conn.execute(
            "INSERT INTO recommendations "
            "(agent_name, agent_run_id, scenario_id, item_id, item_external_id, "
            " shortage_date, deficit_qty, recommended_qty, action, decision_level, "
            " status, confidence, evidence) "
            "VALUES ('other_watcher', %s, %s, %s, 'ITM', %s, 1, 1, 'EXPEDITE', 'L2', "
            " 'DRAFT', 'HIGH', '{}'::jsonb)",
            (foreign_run, BASELINE, item_id, today),
        )
        # Resolve the mis-date: re-date the firm receipt ONTO its need date
        # (week 20). Now reschedule_signals fires nothing for it.
        conn.execute(
            "UPDATE nodes SET time_ref=%s WHERE node_id=%s",
            (today + _dt.timedelta(weeks=20), po_node),
        )

    # --- Run 2: the run-1 mis-date signal is gone -> its DRAFT must EXPIRE.
    assert _run(dsn) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        run1_row = conn.execute(
            "SELECT status FROM recommendations WHERE recommendation_id=%s",
            (run1_id,)).fetchone()
        assert run1_row["status"] == "EXPIRED", "resolved reschedule DRAFT must flip to EXPIRED"

        # The foreign agent's DRAFT on the same scenario is untouched.
        foreign = conn.execute(
            "SELECT status FROM recommendations WHERE agent_name='other_watcher' "
            "AND scenario_id=%s", (BASELINE,)).fetchone()
        assert foreign["status"] == "DRAFT", "expiration leaked onto another agent's rows"

    # After resolution there is no live reschedule DRAFT left for this agent.
    assert _drafts(dsn) == []
