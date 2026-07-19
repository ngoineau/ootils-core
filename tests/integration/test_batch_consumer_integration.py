"""
tests/integration/test_batch_consumer_integration.py — End-to-end proof of the
chantier C3 « moteur d'exception » PR2 BATCH CONSUMER (2026-07-19), written
AGAINST THE ARCHITECT CONTRACT while the backend is fabricated in parallel.

THE CONTRACT UNDER TEST (architect plan, applied verbatim):

  ``PropagationEngine.process_pending(scenario_id, db, *, full=False)`` on the
  BASE class (engine/orchestration/propagator.py) is the daily incremental
  recompute. It:
    1. calls the EXISTING ``CalcRunManager.start_calc_run`` — ONE advisory lock,
       coalesces ALL pending (``processed=FALSE``) events of the scenario, and
       stamps the C2 decision basis (anchor_date / engine_flavor / code_version)
       on the calc_run;
    2. INSERTs the ENTIRE SERIES (every active PI of the DISTINCT
       (item_id, location_id) pairs behind the pending events' trigger nodes)
       into ``dirty_nodes`` — whole series, not just the touched bucket, for the
       contiguity ``PROPAGATE_SQL`` needs from ``seed_seq=0`` and the migration-087
       ``pi_chain_continuity`` invariant; ``full=True`` dirties every active PI of
       the scenario;
    3. ANALYZEs ``dirty_nodes`` (#455), reads the dirty set back, calls the
       POLYMORPHIC ``self._propagate`` (sql/python/rust inherit unchanged), then
       ``_finish_run`` (detection + PR1's ``resolve_stale`` scoped to the
       recomputed series via ``nodes.last_calc_run_id``).
    → ONE calc_run, ONE lock, ONE ANALYZE, ONE ``calc_run_finished`` — never N.

  ``POST /v1/calc/run {full_recompute:false}`` is REWIRED (api/routers/calc.py)
  to call ``process_pending`` instead of the broken event-without-trigger path
  that skipped propagation; ``full_recompute:true`` keeps its existing inline
  path unchanged.

WHY THESE PROOFS ARE ENGINE-STABLE. The load-bearing "was this PI recomputed"
signal is ``nodes.last_calc_run_id`` — the uniform stamp BOTH engines write on
every PI they process (SQL: PROPAGATE_SQL's UPDATE; Python: update_pi_results*),
per the PR1 fix rationale. It does NOT use ``CalcRun.nodes_recalculated``, whose
semantics DIVERGE by engine (SQL = every dirty PI via UPDATE rowcount; Python =
only PIs whose values changed — models/__init__.py:377-390). The few response-
level ``nodes_recalculated`` assertions below (> 0 / == 0 / A==B equality) hold
under BOTH engines by the analysis noted at each site.

Propagation is driven through genuine engine-computed stockouts (a customer
order with no supply drives the projection negative), so every committed PI is
projection-balanced and stockout-flag coherent — the migration-087
``invariant_violations`` net (autouse teardown tripwire, conftest.py) stays
green. Isolation lesson (retraction module): every committed seed is neutralized
by a SAFE deactivating finalizer — never a DELETE cascade.

CASES:
  1. test_batch_coalesces_pending_events_into_one_run — N pending ingest events
     over 2 series → ONE process_pending → ONE calc_run, all events processed,
     BOTH series recomputed, exactly ONE calc_run_finished for that run, decision
     basis stamped (anchor_date / engine_flavor / code_version all non-NULL).
  2. test_rewired_non_full_post_propagates_real_batch — POST /v1/calc/run
     {full_recompute:false} with pending events really propagates
     (nodes_recalculated > 0); the touched series' shortages move to the new run
     while an UNTOUCHED series keeps its original calc_run_id (the PR1 scoping
     proof holds under the real batch path, not just the manual /v1/events path).
  3. test_rewired_non_full_post_without_pending_is_clean_noop — the same endpoint
     with zero pending events is a clean no-op: 0 recalculated, status completed,
     no error (not "locked").
  4. test_single_bucket_event_recomputes_whole_series — a pending event on ONE
     LATE bucket of a series recomputes the WHOLE series (every bucket, INCLUDING
     bucket 0 upstream of the trigger) — the whole-series dirtying the contract
     mandates, distinct from the old downstream-only expand_dirty_subgraph.
  5. test_second_immediate_batch_is_idempotent_noop — a second immediate
     process_pending sees 0 pending events and changes nothing (0 recalculated;
     the series and its shortages keep the first run's stamp).
  6. test_full_process_pending_equals_inline_full — process_pending(full=True)
     covers the same PI set and reports the same (recalculated, unchanged) counts
     as the existing inline full-recompute path.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.constants import BASELINE_SCENARIO_ID

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}

# Unique prefix per test run — seeds never collide across runs.
PREFIX = f"BATCH-{uuid4().hex[:8]}"

TODAY = date.today()

# One item per role; a customer order with no supply drives its series short.
_ITEMS = (
    "C1-ITEM-1", "C1-ITEM-2",   # case 1 — two coalesced series
    "C2-ITEM-A", "C2-ITEM-B",   # case 2 — touched vs untouched
    "C4-ITEM",                  # case 4 — whole-series
    "C5-ITEM",                  # case 5 — idempotence
    "C6-ITEM",                  # case 6 — full == inline full
)


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB (same pattern
    as test_resolve_stale_scoping_integration.py)."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = TOKEN

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from fastapi.testclient import TestClient

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def ootils_db(migrated_db, api_client):
    """One OotilsDB for the DIRECT process_pending calls (cases 1/4/5/6). Depends
    on api_client so OOTILS_API_TOKEN is already set (auth.py validates it at
    import, pulled in transitively by the events-router engine builder)."""
    from ootils_core.db.connection import OotilsDB

    return OotilsDB(migrated_db)


def _ext(base: str) -> str:
    return f"{PREFIX}-{base}"


@pytest.fixture(scope="module")
def seed(api_client):
    """Seed master data (items + one location) through the real ingest API,
    then NEUTRALIZE (never delete) everything committed under this module's
    PREFIX (isolation lesson: deactivate, keep the invariant net green)."""
    items = [
        {"external_id": _ext(name), "name": f"batch consumer {name}",
         "item_type": "finished_good", "uom": "EA", "status": "active"}
        for name in _ITEMS
    ]
    resp = api_client.post("/v1/ingest/items", json={"items": items}, headers=AUTH)
    assert resp.status_code == 200, resp.text

    resp = api_client.post(
        "/v1/ingest/locations",
        json={"locations": [{"external_id": _ext("LOC"), "name": "Batch consumer DC",
                             "location_type": "dc"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    yield {"loc": _ext("LOC")}

    # ── Safe neutralizing finalizer: deactivate, never DELETE — no cascade can
    # take innocent rows with it. Keeps committed state coherent for the
    # invariant-violations net that asserts at this module's teardown.
    like = PREFIX + "%"
    try:
        with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
            c.execute(
                """
                UPDATE edges SET active = FALSE
                WHERE scenario_id = %s AND from_node_id IN (
                    SELECT n.node_id FROM nodes n
                    JOIN items i ON i.item_id = n.item_id
                    WHERE i.external_id LIKE %s)
                """,
                (BASELINE_SCENARIO_ID, like),
            )
            c.execute(
                """
                UPDATE events SET processed = TRUE
                WHERE processed = FALSE AND trigger_node_id IN (
                    SELECT n.node_id FROM nodes n
                    JOIN items i ON i.item_id = n.item_id
                    WHERE i.external_id LIKE %s)
                """,
                (like,),
            )
            c.execute(
                """
                UPDATE nodes SET active = FALSE, is_dirty = FALSE
                WHERE scenario_id = %s AND item_id IN (
                    SELECT item_id FROM items WHERE external_id LIKE %s)
                """,
                (BASELINE_SCENARIO_ID, like),
            )
            c.execute(
                "UPDATE items SET status = 'obsolete' WHERE external_id LIKE %s",
                (like,),
            )
    except Exception:
        pass  # best-effort — migrated_db teardown is the backstop


# ─────────────────────────────────────────────────────────────
# DB read helpers (fresh connection: only committed state is asserted)
# ─────────────────────────────────────────────────────────────


def _fetchone(sql: str, params: tuple):
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as c:
        return c.execute(sql, params).fetchone()


def _fetchall(sql: str, params: tuple):
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as c:
        return c.execute(sql, params).fetchall()


def _uuid_or_none(value) -> UUID | None:
    return UUID(str(value)) if value is not None else None


def _shortage_rows(item_ext: str) -> list[dict]:
    """Every shortage row for an item on baseline, as {calc_run_id, status}."""
    rows = _fetchall(
        """
        SELECT s.calc_run_id, s.status
        FROM shortages s
        JOIN items i ON i.item_id = s.item_id
        WHERE i.external_id = %s AND s.scenario_id = %s
        ORDER BY s.calc_run_id, s.status
        """,
        (item_ext, BASELINE_SCENARIO_ID),
    )
    return [{"calc_run_id": _uuid_or_none(r["calc_run_id"]), "status": r["status"]} for r in rows]


def _pi_series(item_ext: str, loc_ext: str) -> list[dict]:
    """Active ProjectedInventory buckets of a series, ordered by bucket_sequence,
    each as {bucket_sequence, last_calc_run_id}."""
    rows = _fetchall(
        """
        SELECT n.bucket_sequence, n.last_calc_run_id
        FROM nodes n
        JOIN items i ON i.item_id = n.item_id
        JOIN locations l ON l.location_id = n.location_id
        WHERE i.external_id = %s AND l.external_id = %s
          AND n.node_type = 'ProjectedInventory'
          AND n.scenario_id = %s AND n.active = TRUE
        ORDER BY n.bucket_sequence ASC
        """,
        (item_ext, loc_ext, BASELINE_SCENARIO_ID),
    )
    return [
        {"bucket_sequence": r["bucket_sequence"],
         "last_calc_run_id": _uuid_or_none(r["last_calc_run_id"])}
        for r in rows
    ]


def _pi_node_at_sequence(item_ext: str, loc_ext: str, seq: int) -> UUID:
    """The node_id of a specific bucket of a series — used as a single-bucket
    trigger to prove whole-series dirtying."""
    row = _fetchone(
        """
        SELECT n.node_id
        FROM nodes n
        JOIN items i ON i.item_id = n.item_id
        JOIN locations l ON l.location_id = n.location_id
        WHERE i.external_id = %s AND l.external_id = %s
          AND n.node_type = 'ProjectedInventory'
          AND n.scenario_id = %s AND n.active = TRUE
          AND n.bucket_sequence = %s
        LIMIT 1
        """,
        (item_ext, loc_ext, BASELINE_SCENARIO_ID, seq),
    )
    assert row is not None, f"no PI bucket seq={seq} for {item_ext}@{loc_ext}"
    return UUID(str(row["node_id"]))


def _baseline_active_pi_count() -> int:
    row = _fetchone(
        """
        SELECT COUNT(*) AS n FROM nodes
        WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE
        """,
        (BASELINE_SCENARIO_ID,),
    )
    return int(row["n"]) if row else 0


def _baseline_pi_stamped_count(run_id: UUID) -> int:
    """Active baseline PI whose last_calc_run_id is this run — the whole-scenario
    coverage probe for the full-recompute comparison (case 6)."""
    row = _fetchone(
        """
        SELECT COUNT(*) AS n FROM nodes
        WHERE scenario_id = %s AND node_type = 'ProjectedInventory'
          AND active = TRUE AND last_calc_run_id = %s
        """,
        (BASELINE_SCENARIO_ID, run_id),
    )
    return int(row["n"]) if row else 0


def _baseline_pending_event_count() -> int:
    row = _fetchone(
        "SELECT COUNT(*) AS n FROM events WHERE scenario_id = %s AND processed = FALSE",
        (BASELINE_SCENARIO_ID,),
    )
    return int(row["n"]) if row else 0


def _my_event_counts(item_like: str) -> tuple[int, int]:
    """(pending, processed) counts of every event whose trigger node belongs to
    an item matching ``item_like`` — scopes the assertion to this test's series."""
    row = _fetchone(
        """
        SELECT
            COUNT(*) FILTER (WHERE e.processed = FALSE) AS pending,
            COUNT(*) FILTER (WHERE e.processed = TRUE)  AS processed
        FROM events e
        WHERE e.trigger_node_id IN (
            SELECT n.node_id FROM nodes n
            JOIN items i ON i.item_id = n.item_id
            WHERE i.external_id LIKE %s)
        """,
        (item_like,),
    )
    return (int(row["pending"]), int(row["processed"])) if row else (0, 0)


def _calc_run_count() -> int:
    row = _fetchone(
        "SELECT COUNT(*) AS n FROM calc_runs WHERE scenario_id = %s",
        (BASELINE_SCENARIO_ID,),
    )
    return int(row["n"]) if row else 0


def _calc_run_row(run_id: UUID) -> dict | None:
    return _fetchone(
        """
        SELECT status, anchor_date, engine_flavor, code_version
        FROM calc_runs WHERE calc_run_id = %s
        """,
        (run_id,),
    )


def _calc_run_finished_count(run_id: UUID) -> int:
    """Number of calc_run_finished stream events for a specific run
    (complete_calc_run stamps new_text = str(calc_run_id))."""
    row = _fetchone(
        "SELECT COUNT(*) AS n FROM events WHERE event_type = 'calc_run_finished' AND new_text = %s",
        (str(run_id),),
    )
    return int(row["n"]) if row else 0


# ─────────────────────────────────────────────────────────────
# Write helpers
# ─────────────────────────────────────────────────────────────


def _ingest_demand_only_order(api_client, seed, *, co_ext: str, item_ext: str,
                              qty: int, req_date: date, status: str = "open") -> None:
    """Ingest a customer order with NO matching supply → the series projects
    negative from req_date onward. A NEW order deposits ONE unprocessed
    ``ingestion_complete`` event (trigger = the demand node) — a pending event
    the batch consumer coalesces; ingest itself never propagates."""
    resp = api_client.post(
        "/v1/ingest/customer-orders",
        json={"customer_orders": [{
            "external_id": co_ext,
            "item_external_id": item_ext,
            "location_external_id": seed["loc"],
            "quantity": qty,
            "requested_delivery_date": req_date.isoformat(),
            "status": status,
        }]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text


def _full_recompute(api_client) -> UUID:
    """Full recompute through the real production path (full_recompute:true —
    the UNCHANGED inline path). Drains every pending event and stamps every
    active PI; returns its calc_run_id."""
    resp = api_client.post("/v1/calc/run", json={"full_recompute": True}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "completed", body
    assert body["calc_run_id"] is not None, body
    return UUID(str(body["calc_run_id"]))


def _insert_pending_event_on_node(trigger_node_id: UUID) -> None:
    """Commit ONE unprocessed ``ingestion_complete`` event on a specific node,
    on a fresh autocommit connection so process_pending (a separate connection)
    coalesces it. start_calc_run coalesces every processed=FALSE row regardless
    of type."""
    with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
        c.execute(
            """
            INSERT INTO events (event_id, event_type, scenario_id, trigger_node_id,
                                processed, source, created_at)
            VALUES (%s, 'ingestion_complete', %s, %s, FALSE, 'ingestion', now())
            """,
            (uuid4(), BASELINE_SCENARIO_ID, trigger_node_id),
        )


def _run_process_pending(ootils_db, *, full: bool = False):
    """Drive the batch consumer through the REAL engine on its own committed
    connection (OotilsDB.conn commits on clean exit → the batch is visible to
    the fresh-connection read helpers). Returns the base-class CalcRun (None
    only if the scenario advisory lock is already held).

    NOTE — rust-svc (ADR-017) is OUT OF SCOPE for process_pending: it overrides
    process_event wholesale and never touches _propagate, so it is not built by
    the default (sql) engine builder these tests exercise.
    """
    from ootils_core.api.routers.events import _build_propagation_engine

    with ootils_db.conn() as c:
        engine = _build_propagation_engine(c)
        run = engine.process_pending(BASELINE_SCENARIO_ID, c, full=full)
    return run


# ─────────────────────────────────────────────────────────────
# (1) N pending events over 2 series → ONE process_pending.
# ─────────────────────────────────────────────────────────────


def test_batch_coalesces_pending_events_into_one_run(api_client, ootils_db, seed):
    item1, item2 = _ext("C1-ITEM-1"), _ext("C1-ITEM-2")
    like = PREFIX + "-C1-ITEM-%"

    # M=4 pending events across 2 distinct series (2 orders each). Each new order
    # deposits one unprocessed ingestion_complete event; ingest does not propagate.
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C1-CO-1a"),
                              item_ext=item1, qty=20, req_date=TODAY + timedelta(days=8))
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C1-CO-1b"),
                              item_ext=item1, qty=15, req_date=TODAY + timedelta(days=15))
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C1-CO-2a"),
                              item_ext=item2, qty=30, req_date=TODAY + timedelta(days=9))
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C1-CO-2b"),
                              item_ext=item2, qty=12, req_date=TODAY + timedelta(days=20))

    pending_before, _ = _my_event_counts(like)
    assert pending_before == 4, f"expected 4 pending ingest events, got {pending_before}"

    runs_before = _calc_run_count()

    # ── ONE batch consume of everything pending.
    run = _run_process_pending(ootils_db)
    assert run is not None, "process_pending returned None — the scenario lock was held"
    run_id = UUID(str(run.calc_run_id))

    # ONE calc_run — never N (one per coalesced event would be the regression).
    assert _calc_run_count() - runs_before == 1, "batch consume created more than one calc_run"

    # Every pending event of this test's series is now processed (coalesced).
    pending_after, processed_after = _my_event_counts(like)
    assert pending_after == 0, "some ingest events were left unprocessed by the batch"
    assert processed_after >= 4

    # BOTH series fully recomputed: every active bucket carries this run's stamp.
    for item in (item1, item2):
        series = _pi_series(item, seed["loc"])
        total = len(series)
        stamped = sum(1 for r in series if r["last_calc_run_id"] == run_id)
        assert total > 1, f"{item}: series has no buckets"
        assert stamped == total, f"{item}: only {stamped}/{total} buckets carry the batch run"

    # Exactly ONE calc_run_finished for this run — never N.
    assert _calc_run_finished_count(run_id) == 1, "expected exactly one calc_run_finished event"

    # Decision basis (C2) stamped by start_calc_run on the process_pending run.
    row = _calc_run_row(run_id)
    assert row is not None, "the calc_run row is missing"
    assert row["status"] in ("completed", "completed_stale"), row
    assert row["anchor_date"] is not None, "anchor_date not stamped (decision basis)"
    assert row["engine_flavor"] is not None, "engine_flavor not stamped (decision basis)"
    assert row["code_version"] is not None, "code_version not stamped (decision basis)"


# ─────────────────────────────────────────────────────────────
# (2) Rewired POST /v1/calc/run {full_recompute:false} really propagates a
#     batch; untouched series keep their calc_run_id (PR1 proof, batch path).
# ─────────────────────────────────────────────────────────────


def test_rewired_non_full_post_propagates_real_batch(api_client, seed):
    item_a, item_b = _ext("C2-ITEM-A"), _ext("C2-ITEM-B")
    req = TODAY + timedelta(days=10)

    # Bank a baseline: both series short under run0.
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C2-CO-A"),
                              item_ext=item_a, qty=25, req_date=req)
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C2-CO-B"),
                              item_ext=item_b, qty=18, req_date=req)
    run0 = _full_recompute(api_client)

    a_before = _shortage_rows(item_a)
    b_before = _shortage_rows(item_b)
    assert a_before and b_before, "both series must be short after the full recompute"
    assert all(r["status"] == "active" and r["calc_run_id"] == run0 for r in a_before + b_before)

    # Create a pending event on ONLY series A (a fresh order adds demand). Series
    # B has NO pending event.
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C2-CO-A2"),
                              item_ext=item_a, qty=9, req_date=TODAY + timedelta(days=14))

    # ── The REWIRED batch path: full_recompute:false now calls process_pending.
    resp = api_client.post("/v1/calc/run", json={"full_recompute": False}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "completed", body
    # Really propagated (not the old propagation-skipped path). Holds on both
    # engines: SQL counts every dirty PI (rowcount); Python counts the changed
    # buckets, and A gained real demand so at least one bucket changed.
    assert body["nodes_recalculated"] > 0, body
    run_new = UUID(str(body["calc_run_id"]))
    assert run_new != run0

    # Series A recomputed: its whole series now carries run_new, and it has fresh
    # active shortages under run_new (still genuinely short — demand only grew).
    a_series = _pi_series(item_a, seed["loc"])
    assert a_series and all(r["last_calc_run_id"] == run_new for r in a_series), \
        "series A was not fully recomputed by the batch"
    a_after = _shortage_rows(item_a)
    assert any(r["status"] == "active" and r["calc_run_id"] == run_new for r in a_after), \
        "series A's shortages were not moved to the batch run"

    # Series B UNTOUCHED — the PR1 scoping proof, now under the real batch path:
    # every B shortage is still active AND still carries run0 (nothing resolved,
    # nothing re-stamped). The old resolve_stale would have resolved them all.
    b_after = _shortage_rows(item_b)
    assert b_after == b_before, "the batch wrongly touched the untouched series B"
    assert all(r["status"] == "active" and r["calc_run_id"] == run0 for r in b_after)
    b_series = _pi_series(item_b, seed["loc"])
    assert all(r["last_calc_run_id"] == run0 for r in b_series), \
        "series B's PI were re-stamped by a batch that never touched them"


# ─────────────────────────────────────────────────────────────
# (3) Rewired non-full POST with no pending events → clean no-op.
# ─────────────────────────────────────────────────────────────


def test_rewired_non_full_post_without_pending_is_clean_noop(api_client, seed):
    # Drain: a full recompute consumes every pending baseline event.
    _full_recompute(api_client)
    assert _baseline_pending_event_count() == 0, "drain failed — pending events remain before the no-op"

    resp = api_client.post("/v1/calc/run", json={"full_recompute": False}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # A completed run (NOT "locked" — the lock was free), zero work, no error.
    assert body["status"] == "completed", body
    assert body["nodes_recalculated"] == 0, body
    assert body["calc_run_id"] is not None, body


# ─────────────────────────────────────────────────────────────
# (4) A single-bucket event recomputes the WHOLE series (not just downstream).
# ─────────────────────────────────────────────────────────────


def test_single_bucket_event_recomputes_whole_series(api_client, ootils_db, seed):
    item = _ext("C4-ITEM")

    # A series with a genuine late-horizon shortage, banked under run0 (every
    # bucket stamped run0 by the full recompute).
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C4-CO"),
                              item_ext=item, qty=40, req_date=TODAY + timedelta(days=40))
    run0 = _full_recompute(api_client)

    series0 = _pi_series(item, seed["loc"])
    total = len(series0)
    assert total > 60, "series too short to prove upstream-of-trigger recompute"
    assert all(r["last_calc_run_id"] == run0 for r in series0), "setup: series not fully stamped run0"

    # A pending event on ONE LATE bucket (seq 60). The old expand_dirty_subgraph
    # would only dirty buckets 60..end (downstream); process_pending dirties the
    # WHOLE series of that (item, location).
    trigger = _pi_node_at_sequence(item, seed["loc"], 60)
    _insert_pending_event_on_node(trigger)

    run = _run_process_pending(ootils_db)
    assert run is not None
    run1 = UUID(str(run.calc_run_id))
    assert run1 != run0

    series1 = _pi_series(item, seed["loc"])
    stamped = sum(1 for r in series1 if r["last_calc_run_id"] == run1)
    assert stamped == len(series1), \
        f"only {stamped}/{len(series1)} buckets recomputed — not the whole series"

    # The sharp proof: bucket 0 — strictly UPSTREAM of the seq-60 trigger — moved
    # from run0 to run1. Downstream-only expansion would have left it at run0.
    first = next(r for r in series1 if r["bucket_sequence"] == 0)
    assert first["last_calc_run_id"] == run1, \
        "bucket 0 (upstream of the trigger) was not recomputed — whole-series dirtying failed"


# ─────────────────────────────────────────────────────────────
# (5) A second immediate batch is an idempotent no-op.
# ─────────────────────────────────────────────────────────────


def test_second_immediate_batch_is_idempotent_noop(api_client, ootils_db, seed):
    item = _ext("C5-ITEM")

    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C5-CO"),
                              item_ext=item, qty=22, req_date=TODAY + timedelta(days=12))

    run1 = _run_process_pending(ootils_db)
    assert run1 is not None
    run1_id = UUID(str(run1.calc_run_id))
    series_after_1 = _pi_series(item, seed["loc"])
    assert series_after_1 and all(r["last_calc_run_id"] == run1_id for r in series_after_1), \
        "first batch did not recompute the series"
    shortages_after_1 = _shortage_rows(item)

    # ── Second immediate consume: nothing pending → nothing to do.
    run2 = _run_process_pending(ootils_db)
    recalculated2 = (run2.nodes_recalculated or 0) if run2 is not None else 0
    assert recalculated2 == 0, "the idempotent second batch recomputed phantom work"

    # Load-bearing idempotence: the series still carries run1's stamp — the empty
    # second run stamped nothing — and its shortages are untouched.
    series_after_2 = _pi_series(item, seed["loc"])
    assert all(r["last_calc_run_id"] == run1_id for r in series_after_2), \
        "the idempotent second batch wrongly re-stamped the series"
    assert _shortage_rows(item) == shortages_after_1, \
        "the idempotent second batch wrongly touched the shortages"


# ─────────────────────────────────────────────────────────────
# (6) process_pending(full=True) == the inline full-recompute path.
# ─────────────────────────────────────────────────────────────


def test_full_process_pending_equals_inline_full(api_client, ootils_db, seed):
    item = _ext("C6-ITEM")

    # A committed, STABLE baseline: ingest then full-recompute so every PI is
    # already computed. On stable state the two full paths agree on BOTH engines
    # (SQL: recalc = all dirty PI, unchanged = 0; Python: recalc = 0, unchanged =
    # all) — the divergence only appears when state is not pre-stabilized.
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("C6-CO"),
                              item_ext=item, qty=17, req_date=TODAY + timedelta(days=11))
    _full_recompute(api_client)

    total_pi = _baseline_active_pi_count()
    assert total_pi > 0, "no active baseline PI to full-recompute"

    # Path A — full=True via process_pending (direct base-class call). Count its
    # coverage BEFORE path B re-stamps every PI with its own run.
    run_a = _run_process_pending(ootils_db, full=True)
    assert run_a is not None
    run_a_id = UUID(str(run_a.calc_run_id))
    recalc_a, unchanged_a = run_a.nodes_recalculated, run_a.nodes_unchanged
    count_a = _baseline_pi_stamped_count(run_a_id)

    # Path B — the UNCHANGED inline full path (full_recompute:true).
    resp = api_client.post("/v1/calc/run", json={"full_recompute": True}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    run_b_id = UUID(str(body["calc_run_id"]))
    recalc_b, unchanged_b = body["nodes_recalculated"], body["nodes_unchanged"]
    count_b = _baseline_pi_stamped_count(run_b_id)

    # Same coverage: each full run stamped every active baseline PI.
    assert count_a == total_pi, f"process_pending(full=True) stamped {count_a}/{total_pi} PI"
    assert count_b == total_pi, f"inline full stamped {count_b}/{total_pi} PI"

    # Same reported counters — process_pending(full=True) IS the inline full.
    assert (recalc_a, unchanged_a) == (recalc_b, unchanged_b), (
        f"full-recompute counters diverge: process_pending={(recalc_a, unchanged_a)} "
        f"inline={(recalc_b, unchanged_b)}"
    )
    assert recalc_a + unchanged_a == total_pi, "full recompute did not account for every PI"
