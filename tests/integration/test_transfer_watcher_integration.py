"""
tests/integration/test_transfer_watcher_integration.py — chantier #395 PR2b.

DB-backed coverage of the GOVERNED DRP transfer emitter
(scripts/agent_transfer_watcher.py + engine.recommendation.transfer.
emit_transfer_recommendations) against a real Postgres, no mocks. The watcher is
a thin orchestrator over the pure DRP core (drp_core.transfer_signals, covered
by tests/test_drp_core_golden.py) + the pure mapping
(engine.recommendation.transfer, covered by tests/test_transfer_recommendation.py);
this file asserts the pieces the pure tests cannot: the real INSERT ... ON
CONFLICT idempotence, the stale-DRAFT expiration, scenario isolation through the
#347 overlay, and the ADR-021 no-write into `shortages` — end to end on a seeded
distribution plan. Plus the POST /v1/drp/run endpoint's auth/scope/kill-switch
floors and the seed_demo_data DRP opportunity.

The direct sibling of tests/integration/test_reschedule_watcher_integration.py
(#346) — same governed-watcher invariants, applied to the distribution echelon.

The seven areas (the governed-watcher contract):
  1. Emission        — a per-site deficit against a linked source's excess
                       yields ONE governed DRAFT TRANSFER in `recommendations`,
                       action='TRANSFER', level L1, both location coordinates
                       filled, recommended_qty > 0.
  2. Stability       — re-running on an UNCHANGED plan inserts ZERO new rows
                       (deterministic id + ON CONFLICT DO NOTHING), reflected in
                       recommendations_idempotent_noop. THE #346/#395 invariant.
  3. Scenario iso    — a fork with a safety_stock_qty overlay (#347) on the
                       destination shifts the deficit => a DIFFERENT transfer on
                       the fork; baseline is untouched.
  4. ADR-021         — the watcher writes NOTHING into `shortages`.
  5. Expiration      — a DRAFT whose deficit is resolved between run 1 and run 2
                       flips to EXPIRED, scoped to this agent + scenario +
                       action='TRANSFER' (a foreign agent's row is left alone).
  6. Endpoint        — POST /v1/drp/run: 401 without a token, 403 for a token
                       lacking recommend:draft, 503 with OOTILS_DRP_ENABLED=0,
                       forkable via ?scenario_id=, {recommendations_emitted,...}.
  7. Seed demo       — after seed_drp, a baseline run drafts VALVE-02
                       DC-ATL -> DC-LAX.

Determinism: every date is anchored on the DB-side CURRENT_DATE (never Python
now()), exactly like the DRP loader's horizon anchor and the sibling watcher
seeds. Every test seeds + cleans its OWN rows (uuid4-suffixed external_ids) so
the idempotence / expiration / isolation assertions never bleed across tests.
"""
from __future__ import annotations

import datetime as _dt
import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from .conftest import requires_db

# Import seam: mrp_core (shim) + the watchers live under scripts/ (outside the
# package), exactly as the reschedule watcher integration test does.
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_transfer_watcher  # noqa: E402
import mrp_core as core  # noqa: E402
from agent_governance import decision_level  # noqa: E402

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

from ootils_core.engine.scenario.param_overlay import set_param_override  # noqa: E402

pytestmark = [requires_db, pytest.mark.smoke]

BASELINE = core.BASELINE
AGENT = "transfer_watcher"

# Weekly bucket grid, same as the DRP core / loader (bucket N == +N*7 days).
_WEEK = 7


def _run(dsn, scenario=None):
    """Drive the watcher in-process (main(argv) -> int), like the fleet smoke.
    --allow-dev because guard_db refuses ootils_dev otherwise (the reschedule
    watcher smoke passes the same flag)."""
    argv = ["--dsn", dsn, "--allow-dev"]
    if scenario is not None:
        argv += ["--scenario", str(scenario)]
    return agent_transfer_watcher.main(argv)


def _drafts(dsn, scenario=BASELINE):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT * FROM recommendations WHERE agent_name=%s AND scenario_id=%s "
            "AND status='DRAFT'",
            (AGENT, str(scenario))).fetchall()


def _count_recos(dsn, scenario=BASELINE):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM recommendations WHERE agent_name=%s AND scenario_id=%s",
            (AGENT, str(scenario))).fetchone()["count"]


# ---------------------------------------------------------------------------
# Seed helpers. Each test gets a FUNCTION-scoped, freshly-truncated graph so the
# idempotence / expiration / isolation assertions never bleed across tests.
# Calqued on test_drp_loader_integration.py's per-(item, location) seeding.
# ---------------------------------------------------------------------------


def _reset_graph(conn):
    """Wipe the per-test graph + agent artifacts + distribution links. Keeps the
    migrated schema (and the migration-002 baseline scenario) intact; only
    clears rows this file seeds. TRUNCATE ... CASCADE so FK-linked recommendation
    rows go too."""
    conn.execute(
        "TRUNCATE nodes, edges, recommendations, agent_runs, "
        "item_planning_params, distribution_links, items, locations, "
        "scenario_planning_overrides RESTART IDENTITY CASCADE"
    )
    # `shortages` may or may not have rows; truncate so the ADR-021 delta test
    # starts from a known baseline.
    conn.execute("TRUNCATE shortages RESTART IDENTITY CASCADE")


def _item(conn, external_id):
    return conn.execute(
        "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING item_id",
        (external_id, "Transfer Item", "component", 40.0, "EUR"),
    ).fetchone()["item_id"]


def _location(conn, external_id):
    return conn.execute(
        "INSERT INTO locations (name, location_type, external_id) "
        "VALUES (%s, %s, %s) RETURNING location_id",
        (external_id, "distribution_center", external_id),
    ).fetchone()["location_id"]


def _planning_params(conn, item_id, location_id, safety_stock_qty=0):
    """One CURRENT (effective_to NULL) item_planning_params row for the
    (item, location) coordinate, so the overlay resolver returns a safety figure
    for it (excess/deficit both read resolved safety_stock_qty). Mirrors the DRP
    loader integration seed."""
    conn.execute(
        """
        INSERT INTO item_planning_params (
            item_id, location_id, effective_from, effective_to,
            lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days,
            safety_stock_qty, lot_size_rule
        ) VALUES (%s, %s, CURRENT_DATE, NULL, 14, 0, 0, %s, 'LOTFORLOT')
        """,
        (item_id, location_id, safety_stock_qty),
    )


def _link(conn, item_id, src_loc, dst_loc, *, min_qty=10, transfer_multiple=10, priority=1):
    """An ACTIVE item-specific distribution link src -> dst, transit 7d (1 weekly
    bucket). Columns/defaults read straight off migration 029 / 065."""
    return conn.execute(
        """
        INSERT INTO distribution_links (
            upstream_location_id, downstream_location_id, item_id,
            transit_lead_time_days, minimum_shipment_qty, maximum_shipment_qty,
            priority, transfer_multiple, active
        ) VALUES (%s, %s, %s, 7, %s, NULL, %s, %s, TRUE)
        RETURNING distribution_link_id
        """,
        (src_loc, dst_loc, item_id, min_qty, priority, transfer_multiple),
    ).fetchone()["distribution_link_id"]


def _onhand(conn, scenario, item_id, loc_id, qty):
    conn.execute(
        "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
        " time_grain, time_ref, active) "
        "VALUES ('OnHandSupply', %s, %s, %s, %s, 'exact_date', CURRENT_DATE, TRUE)",
        (str(scenario), item_id, loc_id, qty),
    )


def _demand(conn, scenario, item_id, loc_id, weeks_out, qty):
    conn.execute(
        "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
        " time_grain, time_ref, active) "
        "VALUES ('CustomerOrderDemand', %s, %s, %s, %s, 'exact_date', "
        " CURRENT_DATE + %s, TRUE)",
        (str(scenario), item_id, loc_id, qty, _dt.timedelta(days=weeks_out * _WEEK)),
    )


def _seed_transfer_opportunity(conn, scenario, *, src_ext, dst_ext, item_ext,
                               src_onhand=2000, dst_onhand=45, deficit_weeks=3,
                               deficit_qty=200, dst_safety=30):
    """Seed the nominal transfer opportunity for ONE scenario:

      * SOURCE (src): large on-hand, NO demand, safety 0 -> distributable excess.
      * DEST (dst): small on-hand + a future customer order that outruns
        on-hand + safety -> a projected per-site deficit.
      * an item-specific link src -> dst (transit 1 bucket, mult 10).

    Returns (item_id, src_loc, dst_loc). The bucket math is hand-checkable: the
    dst deficit lands at bucket `deficit_weeks`, the ship bucket is
    max(0, deficit_bucket - 1)."""
    item_id = _item(conn, item_ext)
    src_loc = _location(conn, src_ext)
    dst_loc = _location(conn, dst_ext)
    _planning_params(conn, item_id, src_loc, safety_stock_qty=0)
    _planning_params(conn, item_id, dst_loc, safety_stock_qty=dst_safety)
    _link(conn, item_id, src_loc, dst_loc)
    _onhand(conn, scenario, item_id, src_loc, src_onhand)
    _onhand(conn, scenario, item_id, dst_loc, dst_onhand)
    _demand(conn, scenario, item_id, dst_loc, deficit_weeks, deficit_qty)
    return item_id, src_loc, dst_loc


# ===========================================================================
# 1. Emission — a per-site deficit against a linked excess -> one governed DRAFT.
# ===========================================================================


def test_emits_one_governed_transfer_draft(migrated_db):
    """A DC projected short, a linked plant holding excess -> a single DRAFT
    TRANSFER in `recommendations`, action='TRANSFER' at decision_level L1, both
    the source and destination location coordinates filled, a positive
    recommended_qty and the fair-share evidence trail."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset_graph(conn)
        item_id, src_loc, dst_loc = _seed_transfer_opportunity(
            conn, BASELINE,
            src_ext=f"SRC-{suffix}", dst_ext=f"DST-{suffix}", item_ext=f"ITM-{suffix}",
        )

    assert _run(dsn) == 0
    rows = _drafts(dsn)
    assert len(rows) == 1, "exactly one transfer DRAFT expected"
    r = rows[0]
    assert r["action"] == "TRANSFER"
    assert r["decision_level"] == decision_level("TRANSFER") == "L1"
    assert r["status"] == "DRAFT"
    assert r["source_location_id"] is not None
    assert r["dest_location_id"] is not None
    assert str(r["source_location_id"]) == str(src_loc)
    assert str(r["dest_location_id"]) == str(dst_loc)
    assert str(r["item_id"]) == str(item_id)
    assert float(r["recommended_qty"]) > 0
    # transfer_multiple=10 -> the moved qty is a whole multiple of 10.
    assert float(r["recommended_qty"]) % 10 == 0
    assert r["evidence"] is not None
    assert r["evidence"]["signal"] == "TRANSFER"
    assert r["evidence"]["source_location"] == f"SRC-{suffix}"
    assert r["evidence"]["dest_location"] == f"DST-{suffix}"
    # proposed_date (ship) is on or before the deficit date (shortage_date):
    # ship_bucket = max(0, deficit_bucket - lead), so ship <= deficit always.
    assert r["proposed_date"] <= r["shortage_date"]


# ===========================================================================
# 2. STABILITY — re-run on an unchanged plan inserts ZERO new rows.
# ===========================================================================


def test_rerun_on_unchanged_plan_inserts_zero_new_rows(migrated_db):
    """THE #395/#346 headline invariant. After a first run, re-running on the
    exact same plan re-derives the SAME deterministic ids; ON CONFLICT DO NOTHING
    makes the second run a no-op. Row count before run 2 == after run 2, and the
    metrics self-report the idempotent no-op (auditable)."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset_graph(conn)
        _seed_transfer_opportunity(
            conn, BASELINE,
            src_ext=f"SRC-{suffix}", dst_ext=f"DST-{suffix}", item_ext=f"ITM-{suffix}",
        )

    assert _run(dsn) == 0
    after_run1 = _count_recos(dsn)
    assert after_run1 >= 1, "run 1 must emit at least one transfer reco"
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
# 3. Scenario isolation — a fork overlay shifts the transfer; baseline unchanged.
# ===========================================================================


def test_fork_overlay_produces_different_transfer_baseline_untouched(migrated_db):
    """A safety_stock_qty overlay (#347) on the DESTINATION in a fork raises the
    destination's deficit (safety pulls the projection below threshold sooner /
    by more), so the fork's transfer differs from baseline's — a DIFFERENT
    message the baseline never sees. safety_stock_qty is the overlay knob that
    actually moves a DRP deficit: projected_deficits triggers at the SAFETY
    threshold, and excess_by_location subtracts safety too.

    The SAME graph is seeded in BOTH scenarios (the loader reads nodes scoped by
    scenario_id); the ONLY difference is the fork's destination safety override.
    A larger deficit at the same bucket => a larger fair-share transfer =>
    (mult=10 rounding) a strictly larger recommended_qty than baseline."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset_graph(conn)

        fork_id = conn.execute(
            "INSERT INTO scenarios (name, is_baseline, status) "
            "VALUES (%s, FALSE, 'active') RETURNING scenario_id",
            (f"transfer-fork-{suffix}",),
        ).fetchone()["scenario_id"]

        # Identical opportunity in both scenarios. The master data (items,
        # locations, link, params) is scenario-agnostic and created ONCE by the
        # baseline seed; only the graph nodes are scenario-scoped, so the fork
        # just re-seeds the same on-hand/demand nodes under its own scenario_id.
        item_id, src_loc, dst_loc = _seed_transfer_opportunity(
            conn, BASELINE,
            src_ext=f"SRC-{suffix}", dst_ext=f"DST-{suffix}", item_ext=f"ITM-{suffix}",
        )
        _reseed_nodes_for_scenario(conn, fork_id, item_id, src_loc, dst_loc)

        # Overlay on the fork ONLY: raise the DESTINATION safety stock well above
        # baseline (30 -> 500), deepening the fork's deficit. Location-scoped
        # override (per-(item, location), the DRP planning key).
        set_param_override(
            conn, fork_id, item_id, "safety_stock_qty", "500", "transfer-test",
            location_id=dst_loc,
        )

    # Baseline run.
    assert _run(dsn, scenario=BASELINE) == 0
    base_rows = _drafts(dsn, BASELINE)
    assert len(base_rows) == 1
    base_qty = float(base_rows[0]["recommended_qty"])

    # Fork run: deeper deficit -> a strictly larger transfer.
    assert _run(dsn, scenario=fork_id) == 0
    fork_rows = _drafts(dsn, fork_id)
    assert len(fork_rows) == 1
    fork_qty = float(fork_rows[0]["recommended_qty"])
    assert str(fork_rows[0]["scenario_id"]) == str(fork_id)
    assert fork_qty > base_qty, (
        "a destination safety-stock overlay must deepen the fork's deficit and "
        "produce a larger transfer than baseline"
    )

    # Baseline stayed exactly as run (no cross-scenario bleed) — same single row,
    # same qty, after the fork run.
    base_after = _drafts(dsn, BASELINE)
    assert len(base_after) == 1
    assert float(base_after[0]["recommended_qty"]) == base_qty


def _reseed_nodes_for_scenario(conn, scenario, item_id, src_loc, dst_loc):
    """Seed the SAME on-hand/demand nodes for a second scenario, reusing the
    already-created master data (items/locations/link/params are scenario-
    agnostic; only the graph nodes are scenario-scoped). Returns the same
    (item_id, src_loc, dst_loc) triple so the caller's loop stays uniform."""
    _onhand(conn, scenario, item_id, src_loc, 2000)
    _onhand(conn, scenario, item_id, dst_loc, 45)
    _demand(conn, scenario, item_id, dst_loc, 3, 200)
    return item_id, src_loc, dst_loc


# ===========================================================================
# 4. ADR-021 — the watcher writes NOTHING into `shortages`.
# ===========================================================================


def test_watcher_never_writes_shortages(migrated_db):
    """ADR-021: `shortages` is ShortageDetector's alone. The DRP transfer watcher
    is read-only against it — the shortages row count is unchanged across a run
    that DOES emit a transfer recommendation."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset_graph(conn)
        _seed_transfer_opportunity(
            conn, BASELINE,
            src_ext=f"SRC-{suffix}", dst_ext=f"DST-{suffix}", item_ext=f"ITM-{suffix}",
        )
        before = conn.execute("SELECT COUNT(*) FROM shortages").fetchone()["count"]

    assert _run(dsn) == 0
    assert _drafts(dsn), "run must actually emit a transfer reco (else the test proves nothing)"

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        after = conn.execute("SELECT COUNT(*) FROM shortages").fetchone()["count"]
    assert after == before == 0, "transfer watcher must never touch `shortages` (ADR-021)"


# ===========================================================================
# 5. Expiration — a resolved DRAFT flips to EXPIRED, scoped to agent+scen+action.
# ===========================================================================


def test_resolved_draft_is_expired_scoped_to_agent_scenario_action(migrated_db):
    """A DRAFT emitted at run 1 whose deficit is resolved before run 2 (the
    destination on-hand is topped up so the site is no longer short) must flip to
    EXPIRED at run 2 — and the expiration must touch ONLY this agent's TRANSFER
    DRAFTs in this scenario. A foreign agent's DRAFT AND a non-TRANSFER row of
    the SAME agent on the same scenario are both left alone."""
    dsn = migrated_db
    suffix = uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        _reset_graph(conn)
        item_id, src_loc, dst_loc = _seed_transfer_opportunity(
            conn, BASELINE,
            src_ext=f"SRC-{suffix}", dst_ext=f"DST-{suffix}", item_ext=f"ITM-{suffix}",
        )

    assert _run(dsn) == 0
    run1_drafts = _drafts(dsn)
    assert len(run1_drafts) == 1
    run1_id = run1_drafts[0]["recommendation_id"]

    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        # A FOREIGN agent's DRAFT on the same scenario: expiration must not touch
        # it (scoped to agent_name). Reuse a valid agent_run FK target.
        foreign_run = conn.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) "
            "VALUES ('other_watcher', %s, 'COMPLETED') RETURNING agent_run_id",
            (BASELINE,)).fetchone()["agent_run_id"]
        conn.execute(
            "INSERT INTO recommendations "
            "(agent_name, agent_run_id, scenario_id, item_id, item_external_id, "
            " shortage_date, deficit_qty, recommended_qty, action, decision_level, "
            " status, confidence, evidence) "
            "VALUES ('other_watcher', %s, %s, %s, %s, CURRENT_DATE, 1, 1, 'EXPEDITE', 'L2', "
            " 'DRAFT', 'HIGH', '{}'::jsonb)",
            (foreign_run, BASELINE, item_id, f"ITM-{suffix}"),
        )
        # A NON-TRANSFER DRAFT of THIS agent on the same scenario: expiration is
        # scoped to action='TRANSFER', so this EXPEDITE stays DRAFT.
        same_run = conn.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) "
            "VALUES (%s, %s, 'COMPLETED') RETURNING agent_run_id",
            (AGENT, BASELINE)).fetchone()["agent_run_id"]
        conn.execute(
            "INSERT INTO recommendations "
            "(agent_name, agent_run_id, scenario_id, item_id, item_external_id, "
            " shortage_date, deficit_qty, recommended_qty, action, decision_level, "
            " status, confidence, evidence) "
            "VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, 1, 1, 'EXPEDITE', 'L2', "
            " 'DRAFT', 'HIGH', '{}'::jsonb)",
            (AGENT, same_run, BASELINE, item_id, f"ITM-{suffix}"),
        )
        # Resolve the deficit: top the destination on-hand up so the site is no
        # longer projected short. Now transfer_signals fires nothing for it.
        conn.execute(
            "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
            " time_grain, time_ref, active) "
            "VALUES ('OnHandSupply', %s, %s, %s, 100000, 'exact_date', CURRENT_DATE, TRUE)",
            (BASELINE, item_id, dst_loc),
        )

    # --- Run 2: the run-1 deficit is gone -> its TRANSFER DRAFT must EXPIRE.
    assert _run(dsn) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        run1_row = conn.execute(
            "SELECT status FROM recommendations WHERE recommendation_id=%s",
            (run1_id,)).fetchone()
        assert run1_row["status"] == "EXPIRED", "resolved transfer DRAFT must flip to EXPIRED"

        # The foreign agent's DRAFT is untouched.
        foreign = conn.execute(
            "SELECT status FROM recommendations WHERE agent_name='other_watcher' "
            "AND scenario_id=%s", (BASELINE,)).fetchone()
        assert foreign["status"] == "DRAFT", "expiration leaked onto another agent's rows"

        # THIS agent's non-TRANSFER (EXPEDITE) DRAFT is untouched (action-scoped).
        expedite = conn.execute(
            "SELECT status FROM recommendations WHERE agent_name=%s AND scenario_id=%s "
            "AND action='EXPEDITE'", (AGENT, BASELINE)).fetchone()
        assert expedite["status"] == "DRAFT", "expiration leaked onto a non-TRANSFER action"

    # After resolution there is no live transfer DRAFT left for this agent.
    live_transfers = [r for r in _drafts(dsn) if r["action"] == "TRANSFER"]
    assert live_transfers == []


# ===========================================================================
# 6. Endpoint POST /v1/drp/run — auth / scope / kill switch / forkable.
# ===========================================================================


LEGACY_TOKEN = "integration-test-token"


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB, get_db
    overridden onto it — mirrors test_recommendations_api_integration.py."""
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
    """The minted-token lookup is memoised in-process; clear it around every
    test so a mint/revoke never leaks a cached decision (and so a fresh scope is
    observable without a TTL sleep). Mirrors test_agent_floor_integration.py."""
    import ootils_core.api.auth as auth

    auth._token_cache.clear()
    yield
    auth._token_cache.clear()


def _db_conn(dsn):
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _mint_token(dsn, *, actor_kind, scopes):
    """Insert one api_tokens row (migration 064); return (cleartext, token_id).
    The cleartext exists ONLY here — the DB stores hash_token(clear). Mirrors
    test_agent_floor_integration.py::_mint_token."""
    from ootils_core.api.auth import hash_token, token_prefix

    clear = f"ootk_{actor_kind}_{uuid4().hex}"
    token_id = uuid4()
    with _db_conn(dsn) as conn:
        conn.execute(
            "INSERT INTO api_tokens (token_id, name, actor_kind, token_hash, "
            " token_prefix, scopes) VALUES (%s, %s, %s, %s, %s, %s)",
            (token_id, f"test-{actor_kind}-{token_id}", actor_kind,
             hash_token(clear), token_prefix(clear), scopes),
        )
    return clear, str(token_id)


def _bearer(token):
    return {"Authorization": f"Bearer {token}"}


def test_endpoint_requires_auth(api_client):
    """No Authorization header -> 401 (every /v1/* is authenticated; only
    /health is open)."""
    resp = api_client.post("/v1/drp/run", json={})
    assert resp.status_code == 401, resp.text


def test_endpoint_requires_recommend_draft_scope(api_client, migrated_db):
    """The run EMITS DRAFT recommendations, so it requires recommend:draft — an
    agent token holding only {read} is 403 on the scope floor. The legacy admin
    token (below) satisfies it, so no pre-#392 caller regresses."""
    clear, token_id = _mint_token(migrated_db, actor_kind="agent", scopes=["read"])
    try:
        resp = api_client.post("/v1/drp/run", json={}, headers=_bearer(clear))
        assert resp.status_code == 403, resp.text
        assert resp.json()["detail"] == "missing scope 'recommend:draft'"
    finally:
        with _db_conn(migrated_db) as conn:
            conn.execute("DELETE FROM api_tokens WHERE token_id = %s", (token_id,))


def test_endpoint_kill_switch_returns_503(api_client, monkeypatch):
    """OOTILS_DRP_ENABLED falsy -> 503 on the run verb, checked AFTER auth but
    BEFORE the DB pool (an operational escape hatch). The legacy admin token
    clears auth/scope, so the 503 is the kill switch, not an auth failure."""
    monkeypatch.setenv("OOTILS_DRP_ENABLED", "0")
    resp = api_client.post("/v1/drp/run", json={}, headers=_bearer(LEGACY_TOKEN))
    assert resp.status_code == 503, resp.text
    assert "OOTILS_DRP_ENABLED" in resp.json()["detail"]


def test_endpoint_runs_baseline_and_returns_metrics(api_client, migrated_db):
    """A baseline run over a seeded opportunity returns the response schema
    (recommendations_emitted et al.) and actually drafts a TRANSFER."""
    suffix = uuid4().hex[:8]
    with _db_conn(migrated_db) as conn:
        _reset_graph(conn)
        _seed_transfer_opportunity(
            conn, BASELINE,
            src_ext=f"SRC-{suffix}", dst_ext=f"DST-{suffix}", item_ext=f"ITM-{suffix}",
        )

    resp = api_client.post("/v1/drp/run", json={}, headers=_bearer(LEGACY_TOKEN))
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["decision_level"] == "L1"
    assert data["recommendations_emitted"] >= 1
    # Response schema keys the agent fleet consumes.
    for key in ("scenario_id", "agent_run_id", "signals",
                "recommendations_idempotent_noop", "expired_stale_drafts",
                "unresolved_coords", "message"):
        assert key in data
    assert str(data["scenario_id"]) == str(BASELINE)

    # The DRAFT is queryable by the drp_run agent identity.
    with _db_conn(migrated_db) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM recommendations "
            "WHERE action='TRANSFER' AND status='DRAFT' AND scenario_id=%s",
            (str(BASELINE),)).fetchone()["count"]
    assert n >= 1


def test_endpoint_is_forkable_via_scenario_id(api_client, migrated_db):
    """North Star forkability: ?scenario_id=<fork> runs the DRP through the fork
    and stamps the emitted recos with the fork's scenario_id (baseline untouched
    by a fork-only opportunity)."""
    suffix = uuid4().hex[:8]
    with _db_conn(migrated_db) as conn:
        _reset_graph(conn)
        fork_id = conn.execute(
            "INSERT INTO scenarios (name, is_baseline, status) "
            "VALUES (%s, FALSE, 'active') RETURNING scenario_id",
            (f"drp-endpoint-fork-{suffix}",),
        ).fetchone()["scenario_id"]
        # Opportunity seeded ONLY on the fork.
        _seed_transfer_opportunity(
            conn, fork_id,
            src_ext=f"SRC-{suffix}", dst_ext=f"DST-{suffix}", item_ext=f"ITM-{suffix}",
        )

    resp = api_client.post(
        f"/v1/drp/run?scenario_id={fork_id}", json={}, headers=_bearer(LEGACY_TOKEN)
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert str(data["scenario_id"]) == str(fork_id)
    assert data["recommendations_emitted"] >= 1

    with _db_conn(migrated_db) as conn:
        fork_n = conn.execute(
            "SELECT COUNT(*) FROM recommendations WHERE action='TRANSFER' "
            "AND scenario_id=%s", (str(fork_id),)).fetchone()["count"]
        base_n = conn.execute(
            "SELECT COUNT(*) FROM recommendations WHERE action='TRANSFER' "
            "AND scenario_id=%s", (str(BASELINE),)).fetchone()["count"]
    assert fork_n >= 1
    assert base_n == 0, "a fork-only opportunity must not draft any baseline transfer"


# ===========================================================================
# 7. Seed demo — after the DRP seed, a baseline run drafts VALVE-02 ATL->LAX.
# ===========================================================================


def test_seed_drp_produces_valve02_atl_to_lax_transfer(migrated_db):
    """After the demo DRP seed (scripts/seed_demo_data.py:seed_drp — an ACTIVE
    DC-ATL -> DC-LAX link for VALVE-02 + a source excess at DC-ATL against the
    destination's projected shortage at DC-LAX), a baseline run drafts exactly
    that inter-site transfer. Proves the wedge demo works out of the box."""
    dsn = migrated_db
    # The DRP seed depends on the enrichment (VALVE-02 @ DC-LAX shortage) + the
    # baseline master data; run the full demo seed pipeline the same way
    # seed_demo_data.__main__ does.
    import seed_demo_data as seed

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        _reset_graph(conn)
        conn.commit()
        seed.seed(conn)
        seed.seed_enrichment(conn)
        seed.seed_bom(conn)
        seed.seed_calendars(conn)
        seed.seed_drp(conn)

    # A baseline run must draft the VALVE-02 DC-ATL -> DC-LAX transfer.
    assert _run(dsn) == 0

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT r.recommended_qty, r.evidence,
                   su.external_id AS src_ext, du.external_id AS dst_ext,
                   it.external_id AS item_ext
            FROM recommendations r
            JOIN locations su ON su.location_id = r.source_location_id
            JOIN locations du ON du.location_id = r.dest_location_id
            JOIN items it ON it.item_id = r.item_id
            WHERE r.agent_name=%s AND r.action='TRANSFER' AND r.status='DRAFT'
              AND r.scenario_id=%s
            """,
            (AGENT, str(BASELINE)),
        ).fetchall()

    valve = [
        r for r in rows
        if r["item_ext"] == "VALVE-02"
        and r["src_ext"] == "DC-ATL"
        and r["dst_ext"] == "DC-LAX"
    ]
    assert len(valve) == 1, (
        "seed_drp must yield exactly one VALVE-02 DC-ATL -> DC-LAX transfer DRAFT "
        f"(got transfers: {[(r['item_ext'], r['src_ext'], r['dst_ext']) for r in rows]})"
    )
    assert float(valve[0]["recommended_qty"]) > 0
    # transfer_multiple=10 in the seed -> a whole-multiple-of-10 shipment.
    assert float(valve[0]["recommended_qty"]) % 10 == 0
