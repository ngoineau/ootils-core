"""
tests/integration/test_resolve_stale_scoping_integration.py — End-to-end proof
of the chantier C3 « moteur d'exception » PR1 fix (2026-07-19), against real
Postgres.

THE BUG. ``ShortageDetector.resolve_stale`` (called by
``PropagationEngine._finish_run`` at the end of EVERY run) used to mark
``resolved`` every active shortage of the scenario whose ``calc_run_id`` differed
from the current run — a full-run-shaped assumption. A run that recomputes only a
sub-graph (or, worse, one that skips propagation entirely) would then wrongly
resolve the shortages of every series it never touched, and re-detect none.

The corruption reachable TODAY without any incremental feature:
``POST /v1/calc/run`` with ``full_recompute=false`` creates a ``calc_triggered``
event with no ``trigger_node_id``; ``process_event`` SKIPS propagation
(orchestration/propagator.py) but ``_finish_run`` still runs, so the old
``resolve_stale`` resolved ALL active shortages and re-detected NONE.

THE FIX. ``resolve_stale`` now retires a shortage ONLY when its ProjectedInventory
node carries ``last_calc_run_id = calc_run_id`` — the uniform stamp both engines
write on every PI they recompute (Python: GraphStore.update_pi_result*; SQL:
PROPAGATE_SQL). The strict generalisation of the full-run behaviour:

  * FULL recompute — every PI carries the current run → whole-scenario scope,
    byte-for-byte the historical behaviour.
  * INCREMENTAL run — only the recomputed series are in scope; untouched series
    keep their shortages.
  * SKIPPED propagation — zero PI stamped → zero resolved (corruption
    neutralised BY CONSTRUCTION, no change to calc.py).

Three proofs here, each on its OWN pair/one of items so scenario-wide
``resolve_stale`` calls never interfere:

  (A) test_non_full_calc_run_resolves_nothing — the accessible corruption:
      after a full run detects shortages on X and Y, POST /v1/calc/run
      {full_recompute:false} recomputes 0 nodes and resolves NOTHING (before the
      fix it resolved every active shortage).
  (B) test_incremental_run_only_resolves_touched_series — the core scoping proof:
      a full run detects shortages on A and B; an INCREMENTAL event touching
      ONLY series A (POST /v1/events with a series-A PI as trigger_node_id, same
      pattern as test_ingest_retraction_integration.test_incremental_event_sees_retraction)
      leaves B's shortages active with their original calc_run_id (NONE resolved),
      while A's follow the real result of A's recompute (stale rows retired, fresh
      rows detected).
  (C) test_full_run_resolves_disappeared_shortage — non-regression: a full run
      still resolves a shortage that genuinely disappeared (Z's demand retracted).

Propagation is driven through the REAL production path (POST /v1/calc/run and
POST /v1/events), against genuine engine-computed stockouts (a customer order
with no supply drives the projection negative), so every committed node is
projection-balanced and stockout-flag coherent — the migration-087
``invariant_violations`` net stays green.

Isolation lesson (from the retraction module): every committed seed is
neutralized by a SAFE module finalizer — deactivation only, never a DELETE
cascade.
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
PREFIX = f"RSCOPE-{uuid4().hex[:8]}"

TODAY = date.today()


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB (same pattern
    as test_ingest_retraction_integration.py)."""
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


def _ext(base: str) -> str:
    return f"{PREFIX}-{base}"


# One item per role; a customer order with no supply drives its series short.
_ITEMS = ("ITEM-A", "ITEM-B", "ITEM-X", "ITEM-Y", "ITEM-Z")


@pytest.fixture(scope="module")
def seed(api_client):
    """Seed master data (items + one location) through the real ingest API,
    then NEUTRALIZE (never delete) everything committed under this module's
    PREFIX."""
    items = [
        {"external_id": _ext(name), "name": f"resolve_stale scoping {name}",
         "item_type": "finished_good", "uom": "EA", "status": "active"}
        for name in _ITEMS
    ]
    resp = api_client.post("/v1/ingest/items", json={"items": items}, headers=AUTH)
    assert resp.status_code == 200, resp.text

    resp = api_client.post(
        "/v1/ingest/locations",
        json={"locations": [{"external_id": _ext("LOC"), "name": "Scoping test DC",
                             "location_type": "dc"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    yield {"loc": _ext("LOC")}

    # ── Safe neutralizing finalizer (isolation lesson): deactivate, never
    # DELETE — no cascade can take innocent rows with it. The module-scoped
    # migrated_db teardown drops the schema afterwards anyway; this keeps the
    # DB coherent for the invariant-violations net that asserts at teardown.
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


def _shortage_rows(item_ext: str) -> list[dict]:
    """Every shortage row for an item on baseline, as {calc_run_id, status}."""
    return _fetchall(
        """
        SELECT s.calc_run_id, s.status
        FROM shortages s
        JOIN items i ON i.item_id = s.item_id
        WHERE i.external_id = %s AND s.scenario_id = %s
        ORDER BY s.calc_run_id, s.status
        """,
        (item_ext, BASELINE_SCENARIO_ID),
    )


def _first_pi_node_id(item_ext: str, loc_ext: str) -> UUID:
    """The earliest active ProjectedInventory bucket of a series — a trigger
    that, via the feeds_forward chain, dirties the WHOLE series and nothing
    outside it."""
    row = _fetchone(
        """
        SELECT n.node_id
        FROM nodes n
        JOIN items i ON i.item_id = n.item_id
        JOIN locations l ON l.location_id = n.location_id
        WHERE i.external_id = %s AND l.external_id = %s
          AND n.node_type = 'ProjectedInventory'
          AND n.scenario_id = %s AND n.active = TRUE
        ORDER BY n.bucket_sequence ASC
        LIMIT 1
        """,
        (item_ext, loc_ext, BASELINE_SCENARIO_ID),
    )
    assert row is not None, f"no PI bucket for {item_ext}@{loc_ext}"
    return UUID(str(row["node_id"]))


def _ingest_demand_only_order(api_client, seed, *, co_ext: str, item_ext: str,
                              qty: int, req_date: date, status: str = "open") -> None:
    """Ingest a customer order with NO matching supply → the series projects
    negative from req_date onward → a genuine stockout on every bucket from
    then on."""
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
    """Full recompute through the real production path; returns its calc_run_id.
    Every active PI is re-derived and stamped with this run — whole-scenario
    resolve_stale scope."""
    resp = api_client.post(
        "/v1/calc/run", json={"full_recompute": True}, headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "completed", body
    assert body["calc_run_id"] is not None, body
    return UUID(str(body["calc_run_id"]))


# ─────────────────────────────────────────────────────────────
# (A) The accessible corruption: POST /v1/calc/run {full_recompute:false}
#     skips propagation → must resolve NOTHING.
# ─────────────────────────────────────────────────────────────


def test_non_full_calc_run_resolves_nothing(api_client, seed):
    req_date = TODAY + timedelta(days=8)
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("CO-X"),
                              item_ext=_ext("ITEM-X"), qty=30, req_date=req_date)
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("CO-Y"),
                              item_ext=_ext("ITEM-Y"), qty=12, req_date=req_date)

    run1 = _full_recompute(api_client)

    # Both series are short, all rows active under run1.
    x_before = _shortage_rows(_ext("ITEM-X"))
    y_before = _shortage_rows(_ext("ITEM-Y"))
    assert x_before, "ITEM-X should be short after the full recompute"
    assert y_before, "ITEM-Y should be short after the full recompute"
    assert all(r["status"] == "active" and UUID(str(r["calc_run_id"])) == run1
               for r in x_before + y_before)

    # The corruption path: a non-full run creates an event with NO trigger node,
    # process_event skips propagation, _finish_run still calls resolve_stale.
    resp = api_client.post(
        "/v1/calc/run", json={"full_recompute": False}, headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "completed", body
    # Propagation was skipped: not a single node recalculated → not a single PI
    # stamped with this run → resolve_stale can match nothing.
    assert body["nodes_recalculated"] == 0, body

    # THE proof: every shortage is still active, still under run1. Before the
    # fix, resolve_stale would have resolved every one of them (calc_run_id !=
    # the skipped run) and re-detected none.
    x_after = _shortage_rows(_ext("ITEM-X"))
    y_after = _shortage_rows(_ext("ITEM-Y"))
    assert x_after == x_before, "a non-full (skipped) run wrongly touched ITEM-X"
    assert y_after == y_before, "a non-full (skipped) run wrongly touched ITEM-Y"
    assert all(r["status"] == "active" for r in x_after + y_after)
    assert not any(r["status"] == "resolved" for r in x_after + y_after)


# ─────────────────────────────────────────────────────────────
# (B) The core scoping proof: an incremental run touching ONLY series A must
#     leave series B's shortages untouched.
# ─────────────────────────────────────────────────────────────


def test_incremental_run_only_resolves_touched_series(api_client, seed):
    req_date = TODAY + timedelta(days=9)
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("CO-A"),
                              item_ext=_ext("ITEM-A"), qty=20, req_date=req_date)
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("CO-B"),
                              item_ext=_ext("ITEM-B"), qty=10, req_date=req_date)

    run1 = _full_recompute(api_client)

    a_before = _shortage_rows(_ext("ITEM-A"))
    b_before = _shortage_rows(_ext("ITEM-B"))
    assert a_before and b_before, "both series must be short after the full run"
    assert all(r["status"] == "active" and UUID(str(r["calc_run_id"])) == run1
               for r in a_before + b_before)

    # Incremental run touching ONLY series A: fire an event whose trigger is
    # series A's earliest PI bucket. expand_dirty_subgraph walks the
    # feeds_forward chain forward, dirtying every A bucket and NOTHING outside
    # series A — so only A's PIs get stamped with the new run. No old_date/
    # new_date ⇒ full downstream window (no bucket is filtered out).
    trigger = _first_pi_node_id(_ext("ITEM-A"), seed["loc"])
    resp = api_client.post(
        "/v1/events",
        json={"event_type": "ingestion_complete", "trigger_node_id": str(trigger)},
        headers=AUTH,
    )
    assert resp.status_code == 202, resp.text
    # Engine-coupled guard: under the (locked) default OOTILS_ENGINE=sql,
    # nodes_recalculated counts every dirty PI (PROPAGATE_SQL rowcount), so
    # this is > 0 even when no value changes. The python flavour counts only
    # CHANGED nodes and would read 0 here — the load-bearing proof below
    # (b_after == b_before) does not depend on this guard.
    assert resp.json()["affected_nodes_estimate"] > 0, (
        "the incremental event recomputed nothing — series A was not dirtied"
    )

    # SERIES B — untouched by the incremental run: every shortage row is still
    # active AND still carries run1. This is the corruption the fix prevents:
    # the old resolve_stale would have resolved them all (calc_run_id != run2).
    b_after = _shortage_rows(_ext("ITEM-B"))
    assert b_after == b_before, "an incremental run on A wrongly touched B"
    assert all(r["status"] == "active" and UUID(str(r["calc_run_id"])) == run1
               for r in b_after)

    # SERIES A — recomputed: its run1 rows are now retired (resolved) and fresh
    # rows were detected under the incremental run (calc_run_id != run1). This
    # is resolve_stale doing its real job on a series that WAS recalculated.
    a_after = _shortage_rows(_ext("ITEM-A"))
    resolved_run1 = [r for r in a_after
                     if r["status"] == "resolved" and UUID(str(r["calc_run_id"])) == run1]
    active_new = [r for r in a_after
                  if r["status"] == "active" and UUID(str(r["calc_run_id"])) != run1]
    assert resolved_run1, "A's stale run1 shortages were not retired by its recompute"
    assert active_new, "A's recompute did not re-detect its (still real) shortage"
    # And no run1 row for A survived as active — every one was superseded.
    assert not any(r["status"] == "active" and UUID(str(r["calc_run_id"])) == run1
                   for r in a_after)


# ─────────────────────────────────────────────────────────────
# (C) Non-regression: a full run still resolves a shortage that genuinely
#     disappeared.
# ─────────────────────────────────────────────────────────────


def test_full_run_resolves_disappeared_shortage(api_client, seed):
    req_date = TODAY + timedelta(days=11)
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("CO-Z"),
                              item_ext=_ext("ITEM-Z"), qty=15, req_date=req_date,
                              status="open")

    run1 = _full_recompute(api_client)
    z_short = _shortage_rows(_ext("ITEM-Z"))
    assert z_short, "ITEM-Z should be short while its demand is open"
    assert all(r["status"] == "active" and UUID(str(r["calc_run_id"])) == run1
               for r in z_short)

    # Retract the demand: re-ingest the SAME customer order as 'shipped' — a
    # terminal status that deactivates the demand node, so the series projects
    # back to zero and is no longer short.
    _ingest_demand_only_order(api_client, seed, co_ext=_ext("CO-Z"),
                              item_ext=_ext("ITEM-Z"), qty=15, req_date=req_date,
                              status="shipped")

    run2 = _full_recompute(api_client)
    assert run2 != run1

    # The full run recomputed every Z bucket (stamped run2) and found no
    # shortage → resolve_stale retires the run1 rows. Non-regression of the
    # historical full-run semantics.
    z_after = _shortage_rows(_ext("ITEM-Z"))
    assert not any(r["status"] == "active" for r in z_after), (
        "a disappeared shortage must be resolved by the next full run"
    )
    assert any(r["status"] == "resolved" and UUID(str(r["calc_run_id"])) == run1
               for r in z_after), "the former run1 shortage rows must be resolved"
