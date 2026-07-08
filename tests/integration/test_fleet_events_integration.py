"""
tests/integration/test_fleet_events_integration.py — THE North Star guard-rail
for chantier AN-1 (#401, "events emission"), against a real Postgres. No mocks.

WHY THIS FILE IS THE GUARD-RAIL. The North Star "Streamable" rule (CLAUDE.md,
ADR-027) is absolute: *every state-changing capability MUST write a typed
``events`` row*, so the agent fleet subscribes to deltas via ``GET /v1/stream``
(keyset cursor on ``events.stream_seq``, migration 063) instead of polling. A
governed write that emits no event is invisible to the fleet — it silently
breaks the substrate. This suite encodes the inverse as a red line:

    A GOVERNED WRITE WITHOUT ITS STREAM EVENT = RED.

The 26 pure unit tests (test_emit_stream_event.py, test_agent_subscribe.py)
prove the helper's column MAPPING and the drain's cursor LOGIC in isolation
against fake connections. They CANNOT prove the thing that actually matters end
to end: that each of the five real emission SITES, wired into five real
capabilities, lands exactly one correctly-typed row on the SAME transaction as
the business write it announces, that the migration-071 CHECK accepts every
type the emitter writes, and that the keyset SELECT the stream is built on
surfaces it. That is this file's job.

The seven cases (one per backend contract, per the AN-1 hand-off):
  1. THE GUARD-RAIL — table-driven capability -> event_type over ALL FIVE
     fleet emissions: run each once on a seeded fork, assert EXACTLY one row of
     the right type, RUN-level ``new_quantity`` (one event per run, the count
     travels in the column — never one event per item), and that the /v1/stream
     keyset SELECT (WHERE scenario_id=%s AND stream_seq>%s ORDER BY stream_seq)
     surfaces it. Every capability is driven through its REAL emission site
     (complete_calc_run / governed_run._close / the snapshot & outcome routers),
     NEVER a raw ``INSERT INTO events`` that would bypass the site under test.
  2. RUN granularity — a calc run with 0 shortages emits calc_run_finished
     ALONE; a run persisting N shortages adds ONE shortage_detected with
     new_quantity=N (proof it is run-level, not one-per-shortage).
  3. Atomicity — emitting on a transaction that then ROLLS BACK leaves NO event
     (a burned stream_seq is tolerated — the keyset only ever compares `>`).
  4. Idempotence of recommendation_created — the count is read back by the
     CURRENT run's agent_run_id, so an idempotent re-run that inserts 0 new
     recommendations (the transfer/reschedule ON CONFLICT DO NOTHING mechanic)
     emits NOTHING; the first run of N recos emits one event new_quantity=N.
  5. API request-transaction — the drp/snapshots/outcomes routers emit on the
     REQUEST's get_db transaction: after the POST the event is visible from a
     SEPARATE connection (proof it committed) and carries source='api'.
  6. --subscribe end to end — the shortage watcher's event-driven gate: the
     first subscribed run seeds "from now" and runs; a tick with no relevant
     event skips (no agent_runs row); a tick after a calc_run_finished runs and
     advances the persisted cursor; and WITHOUT the flag the metrics stay
     byte-identical to legacy (no cursor key).
  7. Migration-071 CHECK — emit_stream_event succeeds for each of the five
     FLEET_EVENT_TYPES against the real constraint, catching any drift between
     FLEET_EVENT_TYPES (emitter) <-> VALID_EVENT_TYPES (events router) <-> the
     events.event_type CHECK (migration 071).

Coverage note (justified level-below, case 5): the POST /v1/drp/run endpoint's
recommendation_created emission uses the IDENTICAL, already-proven shared helper
``emit_recommendation_created_for_run`` — exercised end to end here through
governed_run (cases 1 & 4). Driving the DRP planner itself to the point of
emitting requires a full inter-site distribution plan whose test harness
TRUNCATEs the whole graph (see test_transfer_watcher_integration.py), which
would be destructive to this module's shared DB and disproportionate for CI. The
DRP route's emission is therefore covered at the shared-helper level (its only
new-vs-governed_run code is the ``source='api'`` argument, whose behaviour is
proven by the snapshots/outcomes request-transaction tests below), not re-driven.

Conventions cloned from the passing suites: the ``migrated_db`` / ``conn``
fixtures + the app ``get_db``-override / minted-token pattern
(test_snapshot_integration.py, test_outcome_integration.py); the direct-engine
propagation driver (test_param_overlay_propagation_integration.py); the
in-process watcher driving on a DB-anchored baseline seed
(test_agent_fleet_smoke.py, test_scenario_backed_watchers_integration.py). Every
capability runs on its OWN fresh uuid4-suffixed scenario so the keyset
assertions never bleed across cases; case 6 is the ONLY user of BASELINE (the
watcher is baseline-only). Dates are anchored on the DB-side CURRENT_DATE.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import sys
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.events.emit import FLEET_EVENT_TYPES, emit_stream_event
from ootils_core.engine.kernel.calc.projection import ProjectionKernel
from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.engine.kernel.graph.traversal import GraphTraversal
from ootils_core.engine.kernel.shortage.detector import ShortageDetector
from ootils_core.engine.orchestration.calc_run import CalcRunManager
from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine
from ootils_core.models import Scenario

from .conftest import requires_db

# Import seam: mrp_core + the watchers + agent_subscribe/agent_governance live
# under scripts/ (outside the package); the watchers do a bare "import
# mrp_core", so scripts/ must be on sys.path — same seam as the fleet tests.
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_shortage_watcher  # noqa: E402
import agent_subscribe  # noqa: E402
from agent_governance import governed_run  # noqa: E402

pytestmark = requires_db

# Seeded by migration 002 (is_baseline=TRUE) — the only baseline scenario, and
# the only scenario the shortage watcher reads (case 6).
BASELINE = UUID("00000000-0000-0000-0000-000000000001")
LEGACY_TOKEN = "integration-test-token"


# ===========================================================================
# The /v1/stream keyset contract + typed-column readback helpers
# ===========================================================================

# The EXACT keyset SELECT GET /v1/stream is built on (migration 063 header:
# "the replayable truth"). Every event assertion below goes through it so the
# test proves the fleet-visible contract, not just that a row exists.
_KEYSET_SQL = (
    "SELECT stream_seq, event_id, event_type, scenario_id, new_quantity, "
    "       field_changed, new_text, old_text, new_date, source "
    "FROM events "
    "WHERE scenario_id = %s AND stream_seq > %s "
    "ORDER BY stream_seq"
)


def _keyset(conn, scenario_id, cursor: int = 0) -> list[dict]:
    """Drain the events stream for a scenario past ``cursor`` via the exact
    /v1/stream keyset SELECT. dict_row rows."""
    return conn.execute(_KEYSET_SQL, (scenario_id, cursor)).fetchall()


def _events_of_type(conn, scenario_id, event_type: str) -> list[dict]:
    """Every event of ``event_type`` for a scenario, read THROUGH the keyset
    SELECT (so a passing assertion also proves the stream surfaces it)."""
    return [r for r in _keyset(conn, scenario_id, 0) if r["event_type"] == event_type]


def _db_conn(dsn):
    """Autocommit dict_row connection for out-of-band seed / readback — a
    SEPARATE session from any request/engine transaction, so anything it can
    read is provably COMMITTED."""
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


# ===========================================================================
# Master-data + graph seed helpers (calqued on
# test_param_overlay_propagation_integration.py / test_snapshot_integration.py)
# ===========================================================================


def _seed_fork(conn, name: str = "fe-fork") -> UUID:
    """A fresh non-baseline scenario. Every capability runs on its own fork so
    its event stream is isolated (a clean exactly-one-of-type assertion)."""
    return conn.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
        "VALUES (%s, %s, FALSE, 'active') RETURNING scenario_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["scenario_id"]


def _seed_item(conn, name: str = "fe-item") -> UUID:
    return conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s) RETURNING item_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["item_id"]


def _seed_location(conn, name: str = "fe-loc") -> UUID:
    return conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s) RETURNING location_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["location_id"]


def _seed_planning_params(conn, item_id, location_id, *, safety_stock) -> None:
    """One CURRENT (effective_to NULL) item_planning_params row — the base row
    both propagation engines read for safety stock. safety_stock=0 gives a clean
    (no-shortage) bucket-0 PI; safety_stock>0 makes the bucket a
    below_safety_stock shortage of qty=safety_stock (the param-overlay
    propagation test's proven signal)."""
    conn.execute(
        """
        INSERT INTO item_planning_params (
            item_id, location_id, effective_from, effective_to,
            lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days,
            safety_stock_qty, min_order_qty, lot_size_rule
        ) VALUES (
            %s, %s, %s, NULL,
            30, NULL, NULL,
            %s, NULL, 'LOTFORLOT'
        )
        """,
        (item_id, location_id, _dt.date.today(), safety_stock),
    )


def _seed_pi_bucket(conn, *, scenario_id, item_id, location_id) -> UUID:
    """A bucket-0 ProjectedInventory node in its own projection_series, no
    replenish/consume edges — it projects to closing_stock=0, which is a
    below_safety_stock shortage for any safety_stock_qty > 0 and NO shortage for
    safety_stock_qty = 0. Verbatim shape from the param-overlay propagation
    test."""
    series_id = conn.execute(
        """
        INSERT INTO projection_series
            (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING series_id
        """,
        (uuid4(), item_id, location_id, scenario_id, _dt.date.today(), _dt.date.today()),
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
        (
            node_id, scenario_id, item_id, location_id,
            _dt.date.today(), _dt.date.today() + _dt.timedelta(days=7), series_id,
        ),
    )
    return node_id


def _seed_agent_run(conn, scenario_id, agent_name: str = "shortage_watcher") -> UUID:
    return conn.execute(
        "INSERT INTO agent_runs (agent_run_id, agent_name, scenario_id, status) "
        "VALUES (%s, %s, %s, 'COMPLETED') RETURNING agent_run_id",
        (uuid4(), agent_name, scenario_id),
    ).fetchone()["agent_run_id"]


def _seed_reco_row(conn, *, scenario_id, item_id, shortage_date, status: str = "APPROVED") -> UUID:
    """One `recommendations` row via direct SQL (NO governed_run, so NO
    recommendation_created event) — used only to give the outcome evaluator
    something to classify. Column set cloned from test_outcome_integration."""
    run_id = _seed_agent_run(conn, scenario_id)
    return conn.execute(
        """
        INSERT INTO recommendations (
            recommendation_id, agent_name, agent_run_id, scenario_id,
            item_id, item_external_id, shortage_date,
            deficit_qty, recommended_qty, estimated_cost, currency,
            action, status, confidence, evidence
        ) VALUES (
            %s, 'shortage_watcher', %s, %s,
            %s, %s, %s,
            100, 120, 4800, 'EUR',
            'EXPEDITE', %s, 'HIGH', %s
        )
        RETURNING recommendation_id
        """,
        (
            uuid4(), run_id, scenario_id,
            item_id, f"EXT-{uuid4().hex[:8]}", shortage_date,
            status, json.dumps({"unit_cost": 3.0}),
        ),
    ).fetchone()["recommendation_id"]


def _seed_snapshot_row(conn, *, scenario_id, item_id, location_id, as_of_date) -> None:
    conn.execute(
        """
        INSERT INTO inventory_snapshots (snapshot_id, scenario_id, item_id,
            location_id, as_of_date, on_hand_qty, source)
        VALUES (%s, %s, %s, %s, %s, 10, 'cli')
        """,
        (uuid4(), scenario_id, item_id, location_id, as_of_date),
    )


# ===========================================================================
# Calc-run engine driver — start -> mark dirty -> _propagate -> complete.
# complete_calc_run IS the emission site (it calls _emit_run_events), so this
# reaches the fleet emission through the REAL propagation path, exactly as
# test_param_overlay_propagation_integration.py drives the engine — only with the
# real complete_calc_run instead of a manual UPDATE.
# ===========================================================================


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


def _run_calc(conn, scenario_id, node_ids):
    """Run ONE real calc run over the given dirty PI buckets and CLOSE it through
    CalcRunManager.complete_calc_run — the fleet emission site. Returns the
    CalcRun (with nodes_recalculated set by _propagate). Commits."""
    engine = _build_sql_engine(conn)
    calc_mgr = CalcRunManager()
    calc_run = calc_mgr.start_calc_run(scenario_id=scenario_id, event_ids=[], db=conn)
    assert calc_run is not None, "could not acquire advisory lock for the fork"

    dirty = DirtyFlagManager()
    nodes = set(node_ids)
    dirty.mark_dirty(nodes, scenario_id, calc_run.calc_run_id, conn)
    dirty.flush_to_postgres(calc_run.calc_run_id, scenario_id, conn)

    engine._propagate(calc_run, nodes, conn)

    scenario_obj = Scenario(
        scenario_id=scenario_id, name="fe-calc", is_baseline=False,
        baseline_snapshot_id=None,
    )
    calc_mgr.complete_calc_run(calc_run, scenario_obj, conn)
    conn.commit()
    return calc_run


# ===========================================================================
# HTTP surface — app fixtures (the #392 test_snapshot / test_outcome pattern)
# ===========================================================================


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """TestClient with get_db overridden onto the test DB. Each request runs on a
    fresh OotilsDB connection bound to the test DSN (get_db owns commit/rollback),
    so an event emitted by a router lands on the request's OWN transaction —
    exactly what case 5 asserts."""
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
    """Clear the minted-token cache around every test so a seed in one test never
    leaks a cached auth decision into another."""
    import ootils_core.api.auth as auth

    auth._token_cache.clear()
    yield
    auth._token_cache.clear()


def _mint_token(dsn, *, actor_kind: str, scopes: list[str]) -> str:
    from ootils_core.api.auth import hash_token, token_prefix

    clear = f"ootk_{actor_kind}_{uuid4().hex}"
    token_id = uuid4()
    with _db_conn(dsn) as c:
        c.execute(
            """
            INSERT INTO api_tokens (
                token_id, name, actor_kind, token_hash, token_prefix, scopes
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                token_id,
                f"fe-{actor_kind}-{token_id}",
                actor_kind,
                hash_token(clear),
                token_prefix(clear),
                scopes,
            ),
        )
    return clear


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ===========================================================================
# Capability triggers — each drives ONE real emission site on its OWN fork and
# returns (scenario_id, event_type, expected_run_level_new_quantity).
# ===========================================================================


def _cap_calc_run_finished(api_client, dsn):
    """A calc run with 0 shortages (safety_stock=0) -> calc_run_finished ALONE,
    new_quantity = run-level nodes_recalculated."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        fork = _seed_fork(conn, "fe-calc")
        item = _seed_item(conn)
        loc = _seed_location(conn)
        _seed_planning_params(conn, item, loc, safety_stock=0)
        node = _seed_pi_bucket(conn, scenario_id=fork, item_id=item, location_id=loc)
        conn.commit()
        run = _run_calc(conn, fork, [node])
    return fork, "calc_run_finished", run.nodes_recalculated


def _cap_shortage_detected(api_client, dsn):
    """A calc run that persists ONE shortage (safety_stock>0) -> shortage_detected
    with new_quantity = 1 (one shortage persisted this run)."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        fork = _seed_fork(conn, "fe-short")
        item = _seed_item(conn)
        loc = _seed_location(conn)
        _seed_planning_params(conn, item, loc, safety_stock=10)
        node = _seed_pi_bucket(conn, scenario_id=fork, item_id=item, location_id=loc)
        conn.commit()
        _run_calc(conn, fork, [node])
    return fork, "shortage_detected", 1


def _cap_recommendation_created(api_client, dsn):
    """governed_run inserts ONE recommendation; _close(COMPLETED) emits ONE
    recommendation_created, new_quantity = 1 (the run's reco count)."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        fork = _seed_fork(conn, "fe-reco")
        item = _seed_item(conn)
        conn.commit()
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        with governed_run(conn, "shortage_watcher", str(fork)) as run:
            run.insert(
                "recommendations",
                ["agent_name", "agent_run_id", "scenario_id", "item_id",
                 "item_external_id", "shortage_date", "deficit_qty",
                 "recommended_qty", "action"],
                [("shortage_watcher", run.run_id, str(fork), item,
                  f"EXT-{uuid4().hex[:6]}", today, 10, 12, "ORDER_NOW")],
            )
            run.set_metrics({"recommendations": 1})
        # governed_run committed on exit; _close emitted recommendation_created.
    return fork, "recommendation_created", 1


def _cap_snapshot_captured(api_client, dsn):
    """POST /v1/snapshots on a fork with one on-hand coordinate -> ONE
    snapshot_captured, new_quantity = rows persisted."""
    with _db_conn(dsn) as c:
        fork = _seed_fork(c, "fe-snap")
        item = _seed_item(c)
        loc = _seed_location(c)
        c.execute(
            """
            INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id,
                quantity, time_grain, time_ref, active)
            VALUES (%s, 'OnHandSupply', %s, %s, %s, 25, 'exact_date', CURRENT_DATE, TRUE)
            """,
            (uuid4(), fork, item, loc),
        )
    clear = _mint_token(dsn, actor_kind="service", scopes=["ingest"])
    resp = api_client.post(
        "/v1/snapshots", params={"scenario_id": str(fork)}, json={}, headers=_bearer(clear)
    )
    assert resp.status_code == 201, resp.text
    written = resp.json()["snapshots_captured"]
    assert written >= 1
    return fork, "snapshot_captured", written


def _cap_outcome_evaluated(api_client, dsn):
    """POST /v1/outcomes/evaluate on a fork with one classifiable reco -> ONE
    outcome_evaluated, new_quantity = recos classified."""
    with _db_conn(dsn) as c:
        fork = _seed_fork(c, "fe-outcome")
        item = _seed_item(c)
        loc = _seed_location(c)
        today = c.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        _seed_reco_row(
            c, scenario_id=fork, item_id=item,
            shortage_date=today + _dt.timedelta(days=20), status="APPROVED",
        )
        # Snapshot present + NO shortage -> AVOIDED -> classified (evaluated>=1).
        _seed_snapshot_row(c, scenario_id=fork, item_id=item, location_id=loc, as_of_date=today)
    clear = _mint_token(dsn, actor_kind="service", scopes=["ingest"])
    resp = api_client.post(
        "/v1/outcomes/evaluate", params={"scenario_id": str(fork)}, json={}, headers=_bearer(clear)
    )
    assert resp.status_code == 201, resp.text
    evaluated = resp.json()["evaluated"]
    assert evaluated >= 1
    return fork, "outcome_evaluated", evaluated


_CAPABILITIES = [
    ("calc_run_finished", _cap_calc_run_finished),
    ("shortage_detected", _cap_shortage_detected),
    ("recommendation_created", _cap_recommendation_created),
    ("snapshot_captured", _cap_snapshot_captured),
    ("outcome_evaluated", _cap_outcome_evaluated),
]


# ===========================================================================
# Case 1 — THE GUARD-RAIL. Every fleet capability emits EXACTLY one run-level
# event, surfaced by the /v1/stream keyset SELECT.
# ===========================================================================


@pytest.mark.parametrize(
    "event_type,trigger", _CAPABILITIES, ids=[c[0] for c in _CAPABILITIES]
)
def test_every_governed_write_emits_exactly_one_run_level_stream_event(
    event_type, trigger, api_client, migrated_db
):
    scenario_id, emitted_type, expected_nq = trigger(api_client, migrated_db)
    assert emitted_type == event_type

    with _db_conn(migrated_db) as c:
        rows = _events_of_type(c, scenario_id, event_type)
        assert len(rows) == 1, (
            f"{event_type}: a governed write must emit EXACTLY one stream event, "
            f"got {len(rows)}"
        )
        ev = rows[0]

        # RUN granularity: one event per run, the count in new_quantity (never
        # one event per item — ADR-027).
        assert ev["new_quantity"] is not None, f"{event_type}: run-level count is NULL"
        assert int(ev["new_quantity"]) == int(expected_nq), (
            f"{event_type}: new_quantity {ev['new_quantity']} != run-level {expected_nq}"
        )

        # The /v1/stream keyset SELECT surfaces exactly this event, and returns
        # its rows strictly ordered by stream_seq (the cursor contract).
        keyset = _keyset(c, scenario_id, 0)
        assert any(
            k["event_id"] == ev["event_id"] and k["event_type"] == event_type
            for k in keyset
        ), f"{event_type}: keyset SELECT (stream_seq>cursor) did not surface the event"
        seqs = [int(k["stream_seq"]) for k in keyset]
        assert seqs == sorted(seqs), "keyset must return rows ordered by stream_seq"
        assert all(s > 0 for s in seqs), "migration-063 trigger must assign a stream_seq"


# ===========================================================================
# Case 2 — RUN granularity: 0 shortages -> calc_run_finished alone; N shortages
# -> one shortage_detected with new_quantity=N.
# ===========================================================================


def test_calc_run_zero_shortage_emits_only_calc_run_finished(migrated_db):
    """A clean calc run (safety_stock=0, closing_stock=0 -> no shortage) emits
    calc_run_finished ALONE — no shortage_detected."""
    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        fork = _seed_fork(conn, "fe-zero")
        item = _seed_item(conn)
        loc = _seed_location(conn)
        _seed_planning_params(conn, item, loc, safety_stock=0)
        node = _seed_pi_bucket(conn, scenario_id=fork, item_id=item, location_id=loc)
        conn.commit()
        run = _run_calc(conn, fork, [node])

    with _db_conn(migrated_db) as c:
        finished = _events_of_type(c, fork, "calc_run_finished")
        shortages = _events_of_type(c, fork, "shortage_detected")

    assert len(finished) == 1, "exactly one calc_run_finished per terminal run"
    assert finished[0]["field_changed"] == "completed"
    assert int(finished[0]["new_quantity"]) == run.nodes_recalculated
    assert shortages == [], "a run persisting 0 shortages must emit NO shortage_detected"


def test_calc_run_with_n_shortages_emits_one_shortage_detected_new_quantity_n(migrated_db):
    """ONE calc run persisting N shortages (N distinct items below safety stock)
    emits ONE run-level shortage_detected with new_quantity=N — proof the event
    is per-RUN, not per-shortage."""
    n = 2
    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        fork = _seed_fork(conn, "fe-nshort")
        loc = _seed_location(conn)
        nodes = []
        for _ in range(n):
            item = _seed_item(conn)
            _seed_planning_params(conn, item, loc, safety_stock=10)
            nodes.append(_seed_pi_bucket(conn, scenario_id=fork, item_id=item, location_id=loc))
        conn.commit()
        run = _run_calc(conn, fork, nodes)

    with _db_conn(migrated_db) as c:
        shortages = _events_of_type(c, fork, "shortage_detected")
        finished = _events_of_type(c, fork, "calc_run_finished")
        persisted = c.execute(
            "SELECT COUNT(*) AS n FROM shortages WHERE calc_run_id = %s",
            (run.calc_run_id,),
        ).fetchone()["n"]

    assert persisted == n, "seed precondition: the run persisted N shortages"
    assert len(shortages) == 1, "N shortages -> ONE run-level event, never N events"
    assert int(shortages[0]["new_quantity"]) == n
    assert len(finished) == 1
    assert int(finished[0]["new_quantity"]) == run.nodes_recalculated == n


# ===========================================================================
# Case 3 — Atomicity: an emission on a rolled-back transaction leaves NO event.
# ===========================================================================


def test_rolled_back_transaction_leaves_no_event(migrated_db):
    """emit_stream_event inserts on the CALLER's connection and never commits, so
    a rollback discards it entirely — the fleet never sees a phantom row for a
    change that did not happen. A burned stream_seq is tolerated (the keyset only
    ever compares with `>`)."""
    with _db_conn(migrated_db) as setup:
        fork = _seed_fork(setup, "fe-atomic")

    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:  # autocommit=False
        emit_stream_event(
            conn, "calc_run_finished", fork,
            field_changed="completed", new_text=str(uuid4()), new_quantity=7,
        )
        conn.rollback()  # cancel the transaction the event rode on

    with _db_conn(migrated_db) as c:
        rows = _events_of_type(c, fork, "calc_run_finished")
    assert rows == [], "a rolled-back emission must leave NO event row"


# ===========================================================================
# Case 4 — Idempotence of recommendation_created: the count is read back by the
# CURRENT run's agent_run_id, so an idempotent re-run inserting 0 new rows emits
# nothing (the transfer/reschedule ON CONFLICT DO NOTHING mechanic).
# ===========================================================================


def test_recommendation_created_idempotent_rerun_emits_nothing(migrated_db):
    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        fork = _seed_fork(conn, "fe-idem")
        item = _seed_item(conn)
        conn.commit()
        today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]

        # Run 1: inserts N=1 recommendation -> one recommendation_created, nq=1.
        with governed_run(conn, "shortage_watcher", str(fork)) as run1:
            run1.insert(
                "recommendations",
                ["agent_name", "agent_run_id", "scenario_id", "item_id",
                 "item_external_id", "shortage_date", "deficit_qty",
                 "recommended_qty", "action"],
                [("shortage_watcher", run1.run_id, str(fork), item,
                  f"EXT-{uuid4().hex[:6]}", today, 10, 12, "ORDER_NOW")],
            )
            run1.set_metrics({"recommendations": 1})

    with _db_conn(migrated_db) as c:
        first = _events_of_type(c, fork, "recommendation_created")
    assert len(first) == 1, "first run of 1 reco emits exactly one event"
    assert int(first[0]["new_quantity"]) == 1

    # Run 2: an idempotent re-run that inserts ZERO new recommendations — the
    # COUNT by run2's agent_run_id is 0, so nothing is announced.
    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        with governed_run(conn, "shortage_watcher", str(fork)) as run2:
            run2.set_metrics({"recommendations": 0})

    with _db_conn(migrated_db) as c:
        after = _events_of_type(c, fork, "recommendation_created")
    assert len(after) == 1, "an idempotent no-op re-run (0 new recos) emits NO new event"
    assert after[0]["event_id"] == first[0]["event_id"], "still only the first run's event"


# ===========================================================================
# Case 5 — API request-transaction: the routers emit on the request's own get_db
# transaction (visible from a SEPARATE connection => committed) with source='api'.
# The DRP route shares the identical, already-proven emit_recommendation_created_
# for_run helper (see module docstring) — covered at the helper level.
# ===========================================================================


def test_snapshots_endpoint_emits_on_request_transaction_source_api(api_client, migrated_db):
    with _db_conn(migrated_db) as c:
        fork = _seed_fork(c, "fe-snap-api")
        item = _seed_item(c)
        loc = _seed_location(c)
        c.execute(
            """
            INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id,
                quantity, time_grain, time_ref, active)
            VALUES (%s, 'OnHandSupply', %s, %s, %s, 40, 'exact_date', CURRENT_DATE, TRUE)
            """,
            (uuid4(), fork, item, loc),
        )
    clear = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])
    resp = api_client.post(
        "/v1/snapshots", params={"scenario_id": str(fork)}, json={}, headers=_bearer(clear)
    )
    assert resp.status_code == 201, resp.text

    # A SEPARATE connection sees the event => it committed on the request's txn.
    with _db_conn(migrated_db) as c:
        rows = _events_of_type(c, fork, "snapshot_captured")
    assert len(rows) == 1
    assert rows[0]["source"] == "api", "the router stamps source='api'"
    assert rows[0]["new_date"] is not None, "snapshot_captured carries as_of_date"
    assert int(rows[0]["new_quantity"]) == resp.json()["snapshots_captured"]


def test_outcomes_endpoint_emits_on_request_transaction_source_api(api_client, migrated_db):
    with _db_conn(migrated_db) as c:
        fork = _seed_fork(c, "fe-outcome-api")
        item = _seed_item(c)
        loc = _seed_location(c)
        today = c.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        _seed_reco_row(
            c, scenario_id=fork, item_id=item,
            shortage_date=today + _dt.timedelta(days=20), status="APPROVED",
        )
        _seed_snapshot_row(c, scenario_id=fork, item_id=item, location_id=loc, as_of_date=today)
    clear = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])
    resp = api_client.post(
        "/v1/outcomes/evaluate", params={"scenario_id": str(fork)}, json={}, headers=_bearer(clear)
    )
    assert resp.status_code == 201, resp.text

    with _db_conn(migrated_db) as c:
        rows = _events_of_type(c, fork, "outcome_evaluated")
    assert len(rows) == 1
    assert rows[0]["source"] == "api"
    assert int(rows[0]["new_quantity"]) == resp.json()["evaluated"]


# ===========================================================================
# Case 6 — --subscribe end to end (the shortage watcher's event-driven gate).
# ===========================================================================


@pytest.fixture(scope="module")
def seeded_baseline(migrated_db):
    """Minimal FG-SHORT baseline so the shortage watcher runs and completes
    deterministically (a proven-complete shape from test_agent_fleet_smoke:
    bought FG, thin on-hand, near-term customer order -> a shortage -> a DRAFT).
    All dates anchored on the DB CURRENT_DATE. BASELINE is used ONLY here (the
    watcher is baseline-only); every other case runs on its own fork."""
    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True) as conn:
        cur = conn.cursor()
        today = cur.execute("SELECT CURRENT_DATE").fetchone()[0]

        loc_id = cur.execute(
            "INSERT INTO locations (name, location_type, external_id) "
            "VALUES (%s, %s, %s) RETURNING location_id",
            ("Sub Plant", "plant", "LOC-SUB"),
        ).fetchone()[0]
        sup_id = cur.execute(
            "INSERT INTO suppliers (external_id, name, reliability_score, status) "
            "VALUES (%s, %s, %s, %s) RETURNING supplier_id",
            ("SUP-SUB", "Sub Supplier", 0.95, "active"),
        ).fetchone()[0]
        item_id = cur.execute(
            "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING item_id",
            ("FG-SUB", "FG Sub", "finished_good", 100.0, "EUR"),
        ).fetchone()[0]
        cur.execute(
            "INSERT INTO item_planning_params "
            "(item_id, location_id, is_make, lead_time_sourcing_days, "
            " lead_time_manufacturing_days, lead_time_transit_days, safety_stock_qty, "
            " lot_size_rule, frozen_time_fence_days, slashed_time_fence_days, "
            " forecast_consumption_strategy) "
            "VALUES (%s,%s,FALSE,14,0,0,0,'LOTFORLOT',0,1,'max_only')",
            (item_id, loc_id),
        )
        cur.execute(
            "INSERT INTO supplier_items "
            "(supplier_id, item_id, lead_time_days, unit_cost, currency, is_preferred) "
            "VALUES (%s,%s,14,100.0,'EUR',TRUE)",
            (sup_id, item_id),
        )
        cur.execute(
            "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
            " time_grain, time_ref, active) "
            "VALUES ('OnHandSupply', %s, %s, %s, 5, 'exact_date', %s, TRUE)",
            (str(BASELINE), item_id, loc_id, today),
        )
        cur.execute(
            "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
            " time_grain, time_ref, active) "
            "VALUES ('CustomerOrderDemand', %s, %s, %s, 50, 'exact_date', %s, TRUE)",
            (str(BASELINE), item_id, loc_id, today + _dt.timedelta(days=21)),
        )
    yield dsn
    # Teardown owned by migrated_db (drops all public tables).


def _baseline_completed_runs(dsn) -> int:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM agent_runs "
            "WHERE agent_name='shortage_watcher' AND scenario_id=%s AND status='COMPLETED'",
            (str(BASELINE),),
        ).fetchone()["n"]


def _baseline_last_run_metrics(dsn) -> dict:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT metrics FROM agent_runs "
            "WHERE agent_name='shortage_watcher' AND scenario_id=%s AND status='COMPLETED' "
            "ORDER BY finished_at DESC NULLS LAST, started_at DESC LIMIT 1",
            (str(BASELINE),),
        ).fetchone()
    return (row["metrics"] if row else None) or {}


@pytest.mark.smoke
def test_subscribe_gate_first_runs_then_skips_then_runs_advancing_cursor(seeded_baseline):
    dsn = seeded_baseline
    before = _baseline_completed_runs(dsn)

    # RUN 1 — first subscribed run: no prior cursor -> seeds "from now" -> RUNS.
    assert agent_shortage_watcher.main(["--dsn", dsn, "--subscribe", "--allow-dev"]) == 0
    assert _baseline_completed_runs(dsn) == before + 1, "first subscribed run must execute"
    cursor_1 = _baseline_last_run_metrics(dsn).get(agent_subscribe.STREAM_CURSOR_KEY)
    assert cursor_1 is not None, "a subscribed run persists its stream_cursor"

    # RUN 2 — a tick with no RELEVANT event since (the watcher's own
    # recommendation_created is drained but not relevant) -> SKIP, no new run.
    assert agent_shortage_watcher.main(["--dsn", dsn, "--subscribe", "--allow-dev"]) == 0
    assert _baseline_completed_runs(dsn) == before + 1, (
        "no relevant event -> skip, leaving NO agent_runs row"
    )

    # Inject a RELEVANT event (calc_run_finished) into BASELINE's stream.
    with psycopg.connect(dsn, row_factory=dict_row) as inj:
        emit_stream_event(
            inj, "calc_run_finished", str(BASELINE),
            field_changed="completed", new_text=str(uuid4()), new_quantity=1,
        )
        inj.commit()

    # RUN 3 — a relevant event since the cursor -> RUNS and advances the cursor.
    assert agent_shortage_watcher.main(["--dsn", dsn, "--subscribe", "--allow-dev"]) == 0
    assert _baseline_completed_runs(dsn) == before + 2, "a relevant event must trigger a run"
    cursor_3 = _baseline_last_run_metrics(dsn).get(agent_subscribe.STREAM_CURSOR_KEY)
    assert cursor_3 is not None
    assert int(cursor_3) > int(cursor_1), "the persisted cursor advanced past the relevant event"


@pytest.mark.smoke
def test_subscribe_flag_off_is_byte_identical_no_cursor(seeded_baseline):
    """Without --subscribe the watcher's metrics carry NO stream_cursor key —
    byte-identical to the legacy full-scan behaviour."""
    dsn = seeded_baseline
    assert agent_shortage_watcher.main(["--dsn", dsn, "--allow-dev"]) == 0
    metrics = _baseline_last_run_metrics(dsn)
    assert metrics, "a plain run still writes a metrics block"
    assert agent_subscribe.STREAM_CURSOR_KEY not in metrics, (
        "a non-subscribe run must not persist a stream_cursor (legacy shape)"
    )


# ===========================================================================
# Case 7 — Migration-071 CHECK: emit_stream_event succeeds for every fleet type,
# catching drift between the emitter set, the router set and the SQL CHECK.
# ===========================================================================


@pytest.mark.parametrize("event_type", sorted(FLEET_EVENT_TYPES))
def test_migration_071_check_accepts_every_fleet_type(event_type, migrated_db):
    """Each of the five FLEET_EVENT_TYPES must INSERT cleanly against the real
    events.event_type CHECK (migration 071). A CHECK that lags the emitter would
    surface here as a psycopg CheckViolation, not a silent miss."""
    with _db_conn(migrated_db) as c:
        fork = _seed_fork(c, "fe-071")
        eid = emit_stream_event(
            c, event_type, fork,
            field_changed=event_type, new_text=str(uuid4()),
            new_quantity=1, new_date=_dt.date.today(),
        )
        row = c.execute(
            "SELECT event_type, stream_seq FROM events WHERE event_id = %s", (eid,)
        ).fetchone()
    assert row is not None, f"{event_type}: emit_stream_event persisted no row"
    assert row["event_type"] == event_type
    assert row["stream_seq"] is not None, "migration-063 trigger must assign a stream_seq"


def test_fleet_event_types_do_not_drift_from_router_valid_set():
    """Static cross-check: every type the emitter writes is in the events
    router's VALID_EVENT_TYPES (which the /v1/events + /v1/stream surface use).
    Paired with the per-type CHECK test above, this pins the three lists
    (FLEET_EVENT_TYPES <-> VALID_EVENT_TYPES <-> migration-071 CHECK) together."""
    from ootils_core.api.routers.events import VALID_EVENT_TYPES

    assert FLEET_EVENT_TYPES <= VALID_EVENT_TYPES, (
        "an emitter type is missing from the router's VALID_EVENT_TYPES"
    )
    assert FLEET_EVENT_TYPES == {
        "recommendation_created", "shortage_detected", "calc_run_finished",
        "snapshot_captured", "outcome_evaluated",
    }
