"""
tests/integration/test_param_overlay_propagation_integration.py — DB-backed
tests that the scenario planning-param overlay (#347 PR1 resolver) is actually
READ by the propagation call sites wired in PR3:

  - the SQL propagation engine (propagator_sql.SHORTAGES_SQL, alias ipp_ss),
  - the Python propagation engine (propagator.PropagationEngine — the
    safety_stock_cache preload + _get_safety_stock standalone fallback),
  - the 5th reader, api.routers.mrp._get_planning_params (lead-time recompose
    + min_order_qty + safety_stock_qty).

The chantier invariant here is the propagation-time half of the PR1 isolation
guarantee: an override set inside a fork must change ONLY that fork's computed
shortages / MRP params; a baseline propagation is byte-identical whether or not
a sibling fork carries an override.

Both propagation engines are real, distinct, invocable code paths (events.py
_build_propagation_engine selects between them via OOTILS_ENGINE). We drive
each directly via `_propagate` — the same low-level entry the seed calibrator
uses (seed/projection/calibration.py) — because it exercises exactly the SS
resolution under test without needing a full supply/demand graph: an isolated
bucket-0 PI with no replenishes/consumes edges projects to closing_stock=0, so
with base safety_stock_qty=SS_BASE it is a `below_safety_stock` shortage of
qty=SS_BASE, and raising the fork's safety_stock_qty override to SS_OVERRIDE
raises the fork's shortage qty to SS_OVERRIDE — a clean, deterministic signal
that the fork read its override.

Mirrors the seeding style of test_m4_shortage_integration.py (direct SQL seed,
function-scoped `conn`, commit so the engines' own reads see the rows). No
mocks — CLAUDE.md.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.engine.kernel.graph.traversal import GraphTraversal
from ootils_core.engine.kernel.calc.projection import ProjectionKernel
from ootils_core.engine.kernel.shortage.detector import ShortageDetector
from ootils_core.engine.orchestration.calc_run import CalcRunManager
from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine
from ootils_core.engine.scenario.param_overlay import set_param_override

from ootils_core.api.routers.mrp import _get_planning_params

from .conftest import requires_db

pytestmark = requires_db

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

# Base safety stock seeded on item_planning_params; the fork override lifts it.
SS_BASE = Decimal("10")
SS_OVERRIDE = Decimal("999")


# ---------------------------------------------------------------------------
# Seed helpers (item / location / planning-params / scenario fork / PI bucket)
# ---------------------------------------------------------------------------


def _seed_item(conn, name: str = "ppg-item") -> UUID:
    return conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s) RETURNING item_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["item_id"]


def _seed_location(conn, name: str = "ppg-loc") -> UUID:
    return conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s) RETURNING location_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["location_id"]


def _seed_planning_params(conn, item_id, location_id, **overrides) -> None:
    """One CURRENT (effective_to NULL) item_planning_params row. Defaults give
    safety_stock_qty=SS_BASE and lead_time_sourcing_days=30 with the other two
    lead-time components left NULL — the PR2 recompose trap in invariant #5."""
    defaults = dict(
        lead_time_sourcing_days=30,
        lead_time_manufacturing_days=None,
        lead_time_transit_days=None,
        safety_stock_qty=SS_BASE,
        min_order_qty=None,
        lot_size_rule="LOTFORLOT",
    )
    defaults.update(overrides)
    conn.execute(
        """
        INSERT INTO item_planning_params (
            item_id, location_id, effective_from, effective_to,
            lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days,
            safety_stock_qty, min_order_qty, lot_size_rule
        ) VALUES (
            %(item_id)s, %(location_id)s, %(effective_from)s, NULL,
            %(lead_time_sourcing_days)s, %(lead_time_manufacturing_days)s, %(lead_time_transit_days)s,
            %(safety_stock_qty)s, %(min_order_qty)s, %(lot_size_rule)s
        )
        """,
        {"item_id": item_id, "location_id": location_id, "effective_from": date.today(), **defaults},
    )


def _seed_fork(conn, name: str = "ppg-fork") -> UUID:
    """A non-baseline scenario (a fork). We do NOT need ScenarioManager's node
    deep-copy here: item_planning_params is scenario-agnostic (no scenario_id
    column — one shared base row), so the ONLY per-scenario differentiator of
    the resolved safety stock is a scenario_planning_overrides row."""
    return conn.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
        "VALUES (%s, %s, FALSE, 'active') RETURNING scenario_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["scenario_id"]


def _seed_pi_bucket(conn, *, scenario_id, item_id, location_id) -> UUID:
    """A bucket-0 ProjectedInventory node in its own projection_series, with no
    replenishes/consumes edges — it projects to closing_stock=0, which is
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


# ---------------------------------------------------------------------------
# Engine drivers — build + run one propagation over a single dirty PI bucket
# ---------------------------------------------------------------------------


def _build_engine(cls, conn):
    store = GraphStore(conn)
    return cls(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        shortage_detector=ShortageDetector(),
    )


def _propagate_bucket(engine, conn, *, scenario_id, node_id) -> UUID:
    """Start a calc_run for `scenario_id`, mark `node_id` dirty, run the
    engine's _propagate over it, resolve stale + close the run. Returns the
    calc_run_id (shortage rows are keyed by it). Same shape as
    seed/projection/calibration.py:_run_propagation."""
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
        "UPDATE calc_runs SET status = 'completed', completed_at = now() WHERE calc_run_id = %s",
        (calc_run.calc_run_id,),
    )
    conn.execute("SELECT pg_advisory_unlock(hashtext(%s)::bigint)", (str(scenario_id),))
    conn.commit()
    return calc_run.calc_run_id


def _shortage_for(conn, node_id) -> dict | None:
    return conn.execute(
        """
        SELECT shortage_qty, severity_class, status
        FROM shortages WHERE pi_node_id = %s AND status = 'active'
        """,
        (node_id,),
    ).fetchone()


# ===========================================================================
# Invariant 1 — SQL engine reads the fork's safety-stock override
# ===========================================================================


def test_sql_engine_fork_sees_safety_stock_override(conn):
    """SqlPropagationEngine (SHORTAGES_SQL, ipp_ss): a fork's safety_stock_qty
    override deepens the fork's shortage while baseline stays at the base SS."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id)
    base_pi = _seed_pi_bucket(conn, scenario_id=BASELINE, item_id=item_id, location_id=location_id)
    fork = _seed_fork(conn)
    fork_pi = _seed_pi_bucket(conn, scenario_id=fork, item_id=item_id, location_id=location_id)
    set_param_override(conn, fork, item_id, "safety_stock_qty", str(SS_OVERRIDE), "t",
                       location_id=location_id)
    conn.commit()

    engine = _build_engine(SqlPropagationEngine, conn)
    _propagate_bucket(engine, conn, scenario_id=BASELINE, node_id=base_pi)
    _propagate_bucket(engine, conn, scenario_id=fork, node_id=fork_pi)

    base = _shortage_for(conn, base_pi)
    fork_row = _shortage_for(conn, fork_pi)
    assert base is not None and fork_row is not None
    assert Decimal(str(base["shortage_qty"])) == SS_BASE, "baseline reads base SS"
    assert Decimal(str(fork_row["shortage_qty"])) == SS_OVERRIDE, "fork reads its override"
    assert fork_row["severity_class"] == "below_safety_stock"


# ===========================================================================
# Invariant 2 — Python engine reads the fork's safety-stock override
# ===========================================================================


def test_python_engine_fork_sees_safety_stock_override(conn):
    """PropagationEngine (Python, safety_stock_cache preload): distinct code
    path from the SQL engine — the fork's override reaches detect_with_params
    and deepens the fork's shortage while baseline stays at the base SS."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id)
    base_pi = _seed_pi_bucket(conn, scenario_id=BASELINE, item_id=item_id, location_id=location_id)
    fork = _seed_fork(conn)
    fork_pi = _seed_pi_bucket(conn, scenario_id=fork, item_id=item_id, location_id=location_id)
    set_param_override(conn, fork, item_id, "safety_stock_qty", str(SS_OVERRIDE), "t",
                       location_id=location_id)
    conn.commit()

    engine = _build_engine(PropagationEngine, conn)
    _propagate_bucket(engine, conn, scenario_id=BASELINE, node_id=base_pi)
    _propagate_bucket(engine, conn, scenario_id=fork, node_id=fork_pi)

    base = _shortage_for(conn, base_pi)
    fork_row = _shortage_for(conn, fork_pi)
    assert base is not None and fork_row is not None
    assert Decimal(str(base["shortage_qty"])) == SS_BASE, "baseline reads base SS"
    assert Decimal(str(fork_row["shortage_qty"])) == SS_OVERRIDE, "fork reads its override"
    assert fork_row["severity_class"] == "below_safety_stock"


# ===========================================================================
# Invariant 3 — a fork's override never perturbs a baseline propagation
# ===========================================================================


def test_baseline_propagation_identical_with_and_without_sibling_fork_override(conn):
    """Isolation for a WRITING engine: a baseline propagation produces the same
    baseline shortage (business columns) whether or not a sibling fork B carries
    a safety_stock_qty override — the override never leaks into baseline."""
    engine = _build_engine(SqlPropagationEngine, conn)

    # Run A: baseline propagation, NO override anywhere.
    item_a = _seed_item(conn)
    loc_a = _seed_location(conn)
    _seed_planning_params(conn, item_a, loc_a)
    pi_a = _seed_pi_bucket(conn, scenario_id=BASELINE, item_id=item_a, location_id=loc_a)
    conn.commit()
    _propagate_bucket(engine, conn, scenario_id=BASELINE, node_id=pi_a)
    row_a = _shortage_for(conn, pi_a)

    # Run B: identical baseline seed, but this time a sibling fork carries an
    # override on the SAME item/location. The baseline shortage must be identical.
    item_b = _seed_item(conn)
    loc_b = _seed_location(conn)
    _seed_planning_params(conn, item_b, loc_b)
    pi_b = _seed_pi_bucket(conn, scenario_id=BASELINE, item_id=item_b, location_id=loc_b)
    fork_b = _seed_fork(conn)
    set_param_override(conn, fork_b, item_b, "safety_stock_qty", str(SS_OVERRIDE), "t",
                       location_id=loc_b)
    conn.commit()
    _propagate_bucket(engine, conn, scenario_id=BASELINE, node_id=pi_b)
    row_b = _shortage_for(conn, pi_b)

    assert row_a is not None and row_b is not None
    # Compare business columns (shortage_id/pi_node_id differ by node — the SQL
    # engine mints a random shortage_id; equality is on the computed values).
    assert Decimal(str(row_a["shortage_qty"])) == Decimal(str(row_b["shortage_qty"])) == SS_BASE
    assert row_a["severity_class"] == row_b["severity_class"] == "below_safety_stock"


# ===========================================================================
# Invariant 4 — the SS cache is local to the calc_run, not shared across runs
# ===========================================================================


def test_python_engine_cache_not_shared_between_baseline_and_fork_runs(conn):
    """On the SAME PropagationEngine instance, a baseline run (SS=SS_BASE) then a
    fork run (SS override) must NOT let the fork reuse the baseline SS cached by
    the first run — safety_stock_cache is rebuilt per _propagate call."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id)
    base_pi = _seed_pi_bucket(conn, scenario_id=BASELINE, item_id=item_id, location_id=location_id)
    fork = _seed_fork(conn)
    fork_pi = _seed_pi_bucket(conn, scenario_id=fork, item_id=item_id, location_id=location_id)
    set_param_override(conn, fork, item_id, "safety_stock_qty", str(SS_OVERRIDE), "t",
                       location_id=location_id)
    conn.commit()

    engine = _build_engine(PropagationEngine, conn)  # ONE instance, two runs
    _propagate_bucket(engine, conn, scenario_id=BASELINE, node_id=base_pi)  # warms cache w/ SS_BASE
    _propagate_bucket(engine, conn, scenario_id=fork, node_id=fork_pi)

    fork_row = _shortage_for(conn, fork_pi)
    assert fork_row is not None
    assert Decimal(str(fork_row["shortage_qty"])) == SS_OVERRIDE, (
        "fork run reused the baseline SS cached by the first run — cache must be "
        "local to the calc_run, not the engine instance"
    )


# ===========================================================================
# Invariant 5 — the 5th reader (mrp._get_planning_params) resolves overrides,
# with the lead-time NULL-component recompose trap
# ===========================================================================


def test_get_planning_params_fork_resolves_overrides_and_recomposes_lead_time(conn):
    """mrp._get_planning_params: a fork override on lead_time_manufacturing_days
    (a NULL base component) and safety_stock_qty changes the resolved result;
    baseline stays put. Proves the COALESCE(component,0) recompose — base total
    is 30 (sourcing=30, mfg/transit NULL, NOT NULL-propagated), fork total is 35."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    # sourcing=30, manufacturing=NULL, transit=NULL -> base lead_time_total=30.
    _seed_planning_params(conn, item_id, location_id)
    fork = _seed_fork(conn)
    set_param_override(conn, fork, item_id, "lead_time_manufacturing_days", "5", "t",
                       location_id=location_id)
    set_param_override(conn, fork, item_id, "safety_stock_qty", str(SS_OVERRIDE), "t",
                       location_id=location_id)
    conn.commit()

    base = _get_planning_params(conn, item_id, location_id, BASELINE)
    forked = _get_planning_params(conn, item_id, location_id, fork)

    assert base["lead_time_total_days"] == 30, "base recompose: 30 + COALESCE(NULL,0)*2"
    assert base["safety_stock_qty"] == SS_BASE
    assert forked["lead_time_total_days"] == 35, "override lifts the NULL mfg component to 5"
    assert forked["safety_stock_qty"] == SS_OVERRIDE
