"""
tests/integration/test_ingest_retraction_integration.py — End-to-end proof of
the 2026-07-16 ingest lifecycle fix, against real Postgres.

The bug family: terminal statuses were only half-honoured. A PO re-ingested as
'received' (or a CO as 'shipped') kept counting in the projection because the
old blacklists (`status != 'cancelled'` / `not in ('delivered','cancelled')`)
never retracted it, and `_wire_node_to_pi` accumulated a duplicate edge on
every re-ingest (double-counting the quantity via inflows_agg/outflows_agg).

Proof cycles here (each on its own item so projections never interfere):

  (a)+(b) PO confirmed → projection counts the inflow; re-ingest the SAME
          external_id as 'received' → node inactive AND the projection has
          FORGOTTEN the quantity (closing stock back to the no-PO state).
  (c)     CO open → outflow counted; re-ingest 'shipped' → demand gone.
  (d)     PO confirmed → in_transit (non-terminal): stays active, quantity
          counted EXACTLY once (the duplicate-edge regression proof), one
          single replenishes edge.
  (e)     planning params: CREATED with omitted cells → DB DEFAULTs applied
          (forecast_consumption_strategy='max_only', consumption_window_days=7);
          SCD2 rotation with an omitted cell → previous value preserved.

Propagation is driven through the REAL production path after a TSV load:
POST /v1/calc/run {"full_recompute": true} (all active PI buckets re-derived;
inflows_agg/outflows_agg filter on node+edge active — the retraction axis).

test_incremental_event_sees_retraction proves the INCREMENTAL path too
(POST /v1/events with trigger_node_id, no full recompute): until 2026-07-17,
GraphStore.get_node filtered active=TRUE unconditionally, so
expand_dirty_subgraph (and process_event's item/location window fallback)
returned an empty dirty set for a trigger node deactivated by the same write
that raised the event — the retraction was invisible outside a full
recompute. Fixed via GraphStore.get_node(include_inactive=True), scoped to
the trigger-resolution call sites only (traversal.py, propagator.py).

Isolation lesson applied: every committed seed is neutralized by a SAFE
module finalizer — deactivation only (nodes/edges active=FALSE, events
processed=TRUE, items obsoleted), never a DELETE cascade.
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
PREFIX = f"RETR-{uuid4().hex[:8]}"

TODAY = date.today()


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB
    (same pattern as test_ingest_planning_params_integration.py)."""
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


@pytest.fixture(scope="module")
def seed(api_client):
    """Seed master data through the real ingest API, then NEUTRALIZE (never
    delete) everything committed under this module's PREFIX."""
    items = [
        {"external_id": _ext("ITEM-PO"), "name": "Retraction PO item",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("ITEM-CO"), "name": "Retraction CO item",
         "item_type": "finished_good", "uom": "EA", "status": "active"},
        {"external_id": _ext("ITEM-NOREG"), "name": "Non-terminal PO item",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("ITEM-INCR"), "name": "Incremental-event retraction item",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("ITEM-PP1"), "name": "Planning params item 1",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("ITEM-PP2"), "name": "Planning params item 2",
         "item_type": "component", "uom": "EA", "status": "active"},
    ]
    resp = api_client.post("/v1/ingest/items", json={"items": items}, headers=AUTH)
    assert resp.status_code == 200, resp.text

    resp = api_client.post(
        "/v1/ingest/locations",
        json={"locations": [{"external_id": _ext("LOC"), "name": "Retraction test DC",
                             "location_type": "dc"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    resp = api_client.post(
        "/v1/ingest/suppliers",
        json={"suppliers": [{"external_id": _ext("SUP"), "name": "Retraction supplier"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    yield {
        "item_po": _ext("ITEM-PO"),
        "item_co": _ext("ITEM-CO"),
        "item_noreg": _ext("ITEM-NOREG"),
        "item_incr": _ext("ITEM-INCR"),
        "item_pp1": _ext("ITEM-PP1"),
        "item_pp2": _ext("ITEM-PP2"),
        "loc": _ext("LOC"),
        "sup": _ext("SUP"),
    }

    # ── Safe neutralizing finalizer (isolation lesson): deactivate, never
    # DELETE — no cascade can take innocent rows with it. The module-scoped
    # migrated_db teardown drops the schema afterwards anyway; this keeps the
    # DB coherent for any test that still runs in between.
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


def _node_for(entity_type: str, external_id: str) -> dict:
    row = _fetchone(
        """
        SELECT n.node_id, n.active, n.quantity, n.time_ref
        FROM external_references er
        JOIN nodes n ON n.node_id = er.internal_id
        WHERE er.entity_type = %s AND er.external_id = %s
        """,
        (entity_type, external_id),
    )
    assert row is not None, f"no node for {entity_type} '{external_id}'"
    return row


def _pi_bucket(item_ext: str, loc_ext: str, day: date) -> dict:
    row = _fetchone(
        """
        SELECT n.node_id, n.inflows, n.outflows, n.closing_stock
        FROM nodes n
        JOIN items i ON i.item_id = n.item_id
        JOIN locations l ON l.location_id = n.location_id
        WHERE i.external_id = %s AND l.external_id = %s
          AND n.node_type = 'ProjectedInventory'
          AND n.scenario_id = %s AND n.active = TRUE
          AND n.time_span_start <= %s AND n.time_span_end > %s
        """,
        (item_ext, loc_ext, BASELINE_SCENARIO_ID, day, day),
    )
    assert row is not None, f"no PI bucket for {item_ext}@{loc_ext} on {day}"
    return row


def _last_pi_closing(item_ext: str, loc_ext: str):
    row = _fetchone(
        """
        SELECT n.closing_stock
        FROM nodes n
        JOIN items i ON i.item_id = n.item_id
        JOIN locations l ON l.location_id = n.location_id
        WHERE i.external_id = %s AND l.external_id = %s
          AND n.node_type = 'ProjectedInventory'
          AND n.scenario_id = %s AND n.active = TRUE
        ORDER BY n.bucket_sequence DESC
        LIMIT 1
        """,
        (item_ext, loc_ext, BASELINE_SCENARIO_ID),
    )
    assert row is not None
    return row["closing_stock"]


def _edges_from(node_id) -> list[dict]:
    return _fetchall(
        """
        SELECT edge_id, to_node_id, active FROM edges
        WHERE from_node_id = %s AND scenario_id = %s
        """,
        (node_id, BASELINE_SCENARIO_ID),
    )


def _recalc(api_client) -> None:
    """Full recompute through the real production path (all PI re-derived)."""
    resp = api_client.post(
        "/v1/calc/run", json={"full_recompute": True}, headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "completed", body  # 'locked' would mean a stuck run


# ─────────────────────────────────────────────────────────────
# (a) + (b) — PO confirmed counts, PO received is FORGOTTEN
# ─────────────────────────────────────────────────────────────


def test_po_received_retracts_supply_from_projection(api_client, seed):
    po_ext = _ext("PO-1")
    delivery = TODAY + timedelta(days=5)
    qty = 40

    def po_payload(status: str) -> dict:
        return {
            "purchase_orders": [{
                "external_id": po_ext,
                "item_external_id": seed["item_po"],
                "location_external_id": seed["loc"],
                "supplier_external_id": seed["sup"],
                "quantity": qty,
                "uom": "EA",
                "expected_delivery_date": delivery.isoformat(),
                "status": status,
            }]
        }

    # (a) confirmed → the projection must count the inflow
    resp = api_client.post("/v1/ingest/purchase-orders", json=po_payload("confirmed"), headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "inserted"

    node = _node_for("purchase_order", po_ext)
    assert node["active"] is True

    _recalc(api_client)
    bucket = _pi_bucket(seed["item_po"], seed["loc"], delivery)
    assert bucket["inflows"] == qty
    assert _last_pi_closing(seed["item_po"], seed["loc"]) == qty

    # (b) SAME external_id re-ingested as 'received' → retraction
    resp = api_client.post("/v1/ingest/purchase-orders", json=po_payload("received"), headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "updated"

    node = _node_for("purchase_order", po_ext)
    assert node["active"] is False, (
        "terminal status 'received' must deactivate the supply node "
        "(whitelist applied on the UPDATE arm of the upsert)"
    )

    _recalc(api_client)
    bucket = _pi_bucket(seed["item_po"], seed["loc"], delivery)
    assert bucket["inflows"] == 0, "projection still counts a received PO"
    assert _last_pi_closing(seed["item_po"], seed["loc"]) == 0, (
        "closing stock must return to the no-PO state after retraction"
    )

    # The edge is kept (single, still active) — retraction rides the NODE
    # active flag; inflows_agg filters s.active = TRUE.
    edges = _edges_from(node["node_id"])
    assert len(edges) == 1


# ─────────────────────────────────────────────────────────────
# (b') — the INCREMENTAL path (POST /v1/events, no full recompute) must
# see the retraction too — GraphStore.get_node(include_inactive=True) fix
# ─────────────────────────────────────────────────────────────


def test_incremental_event_sees_retraction(api_client, seed):
    """Before the 2026-07-17 fix this failed: expand_dirty_subgraph resolved
    the trigger node through GraphStore.get_node, which unconditionally
    filtered active=TRUE. A PO re-ingested as 'received' deactivates its
    own node in the SAME write that raises the 'ingestion_complete' event,
    so the very next lookup of that node — done to seed the dirty-subgraph
    BFS — returned None, the dirty set came back empty, and POST /v1/events
    recomputed nothing: the projection kept the retracted PO's quantity
    forever, until someone ran a full recompute. This test drives ONLY the
    incremental path (no /v1/calc/run after the retraction) and asserts the
    projection has forgotten the quantity anyway.
    """
    po_ext = _ext("PO-INCR")
    delivery = TODAY + timedelta(days=6)
    qty = 15

    def po_payload(status: str) -> dict:
        return {
            "purchase_orders": [{
                "external_id": po_ext,
                "item_external_id": seed["item_incr"],
                "location_external_id": seed["loc"],
                "supplier_external_id": seed["sup"],
                "quantity": qty,
                "uom": "EA",
                "expected_delivery_date": delivery.isoformat(),
                "status": status,
            }]
        }

    # Seed: confirmed PO, established via a full recompute (unrelated to
    # the axis under test — just gets the baseline projection in place).
    resp = api_client.post("/v1/ingest/purchase-orders", json=po_payload("confirmed"), headers=AUTH)
    assert resp.status_code == 200, resp.text
    _recalc(api_client)
    bucket = _pi_bucket(seed["item_incr"], seed["loc"], delivery)
    assert bucket["inflows"] == qty
    assert _last_pi_closing(seed["item_incr"], seed["loc"]) == qty

    # Retract: same external_id, terminal status. The node is deactivated
    # synchronously by this write; propagation is NOT run yet.
    resp = api_client.post("/v1/ingest/purchase-orders", json=po_payload("received"), headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "updated"

    node = _node_for("purchase_order", po_ext)
    assert node["active"] is False

    # The incremental path, driven directly — no /v1/calc/run anywhere
    # from here on. trigger_node_id is the now-INACTIVE PO node.
    resp = api_client.post(
        "/v1/events",
        json={
            "event_type": "ingestion_complete",
            "trigger_node_id": str(node["node_id"]),
        },
        headers=AUTH,
    )
    assert resp.status_code == 202, resp.text
    assert resp.json()["affected_nodes_estimate"] > 0, (
        "incremental propagation recomputed nothing — the dirty subgraph "
        "expansion did not see the retracted trigger node"
    )

    bucket = _pi_bucket(seed["item_incr"], seed["loc"], delivery)
    assert bucket["inflows"] == 0, (
        "incremental propagation still counts a received PO — the "
        "retraction is invisible outside a full recompute"
    )
    assert _last_pi_closing(seed["item_incr"], seed["loc"]) == 0, (
        "closing stock must return to the no-PO state via the incremental "
        "path alone"
    )


# ─────────────────────────────────────────────────────────────
# (c) — CO open counts as demand, CO shipped disappears
# ─────────────────────────────────────────────────────────────


def test_co_shipped_retracts_demand_from_projection(api_client, seed):
    co_ext = _ext("CO-1")
    req_date = TODAY + timedelta(days=10)
    qty = 7

    def co_payload(status: str) -> dict:
        return {
            "customer_orders": [{
                "external_id": co_ext,
                "item_external_id": seed["item_co"],
                "location_external_id": seed["loc"],
                "quantity": qty,
                "requested_delivery_date": req_date.isoformat(),
                "status": status,
            }]
        }

    resp = api_client.post("/v1/ingest/customer-orders", json=co_payload("open"), headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert _node_for("customer_order", co_ext)["active"] is True

    _recalc(api_client)
    bucket = _pi_bucket(seed["item_co"], seed["loc"], req_date)
    assert bucket["outflows"] == qty
    # No supply on this item — the demand drives the projection negative.
    assert _last_pi_closing(seed["item_co"], seed["loc"]) == -qty

    # shipped → the outflow already happened; future demand must vanish
    resp = api_client.post("/v1/ingest/customer-orders", json=co_payload("shipped"), headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "updated"
    assert _node_for("customer_order", co_ext)["active"] is False

    _recalc(api_client)
    bucket = _pi_bucket(seed["item_co"], seed["loc"], req_date)
    assert bucket["outflows"] == 0, "projection still counts a shipped CO as demand"
    assert _last_pi_closing(seed["item_co"], seed["loc"]) == 0


# ─────────────────────────────────────────────────────────────
# (d) — non-terminal transition stays active, counted exactly ONCE
# ─────────────────────────────────────────────────────────────


def test_po_confirmed_to_in_transit_stays_active_no_double_count(api_client, seed):
    po_ext = _ext("PO-2")
    delivery = TODAY + timedelta(days=7)
    qty = 25

    def po_payload(status: str) -> dict:
        return {
            "purchase_orders": [{
                "external_id": po_ext,
                "item_external_id": seed["item_noreg"],
                "location_external_id": seed["loc"],
                "supplier_external_id": seed["sup"],
                "quantity": qty,
                "uom": "EA",
                "expected_delivery_date": delivery.isoformat(),
                "status": status,
            }]
        }

    resp = api_client.post("/v1/ingest/purchase-orders", json=po_payload("confirmed"), headers=AUTH)
    assert resp.status_code == 200, resp.text
    _recalc(api_client)
    assert _pi_bucket(seed["item_noreg"], seed["loc"], delivery)["inflows"] == qty

    # confirmed → in_transit: same date, same qty — must stay active.
    resp = api_client.post("/v1/ingest/purchase-orders", json=po_payload("in_transit"), headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "updated"

    node = _node_for("purchase_order", po_ext)
    assert node["active"] is True, "non-terminal in_transit must NOT retract the PO"

    _recalc(api_client)
    bucket = _pi_bucket(seed["item_noreg"], seed["loc"], delivery)
    # THE duplicate-edge regression proof: before the _wire_node_to_pi fix,
    # each re-ingest at an unchanged date INSERTed a parallel replenishes
    # edge and inflows_agg summed the quantity once PER EDGE (2 × qty here).
    assert bucket["inflows"] == qty, (
        f"expected inflows == {qty} exactly once; a duplicated edge would "
        f"double-count (got {bucket['inflows']})"
    )
    assert _last_pi_closing(seed["item_noreg"], seed["loc"]) == qty

    edges = _edges_from(node["node_id"])
    assert len(edges) == 1, (
        f"exactly ONE replenishes edge expected after re-ingest, got {len(edges)}"
    )
    assert edges[0]["active"] is True


# ─────────────────────────────────────────────────────────────
# (e) — planning params: DB DEFAULTs at CREATED, carry-over at ROTATED
# ─────────────────────────────────────────────────────────────


def _active_pp_row(item_ext: str, loc_ext: str) -> dict:
    row = _fetchone(
        """
        SELECT p.*
        FROM item_planning_params p
        JOIN items i ON i.item_id = p.item_id
        JOIN locations l ON l.location_id = p.location_id
        WHERE i.external_id = %s AND l.external_id = %s
          AND p.effective_to IS NULL
        ORDER BY p.effective_from DESC
        LIMIT 1
        """,
        (item_ext, loc_ext),
    )
    assert row is not None, f"no active planning-params row for {item_ext}"
    return row


def test_planning_params_created_with_empty_cells_gets_db_defaults(api_client, seed):
    """CREATED with omitted fields: the INSERT must OMIT the columns so the
    DB DEFAULTs apply (migrations 007/021) — the old code pushed explicit
    NULLs that short-circuited them."""
    resp = api_client.post(
        "/v1/ingest/planning-params",
        json={"params": [{
            "item_external_id": seed["item_pp1"],
            "location_external_id": seed["loc"],
            "safety_stock_qty": 50,
        }]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "created"

    row = _active_pp_row(seed["item_pp1"], seed["loc"])
    # The explicitly pushed value:
    assert row["safety_stock_qty"] == 50
    # DB DEFAULTs that the old explicit-NULL INSERT used to wipe out:
    assert row["forecast_consumption_strategy"] == "max_only"
    assert row["consumption_window_days"] == 7
    # Python-side floor defaults (NOT NULL columns), unchanged by the fix:
    assert row["lot_size_rule"] == "LOTFORLOT"
    assert row["planning_horizon_days"] == 90
    assert row["is_make"] is False
    # Nullable columns with no DEFAULT stay honestly NULL:
    assert row["lead_time_sourcing_days"] is None
    assert row["safety_stock_days"] is None


def test_planning_params_rotation_empty_cell_keeps_previous_value(api_client, seed):
    """SCD2 rotation: an omitted cell means 'do not touch' — the previous
    value (including one that came from a DB DEFAULT) is carried over."""
    # First push: two explicit values; the rest take DB DEFAULTs.
    resp = api_client.post(
        "/v1/ingest/planning-params",
        json={"params": [{
            "item_external_id": seed["item_pp2"],
            "location_external_id": seed["loc"],
            "safety_stock_qty": 50,
            "lead_time_sourcing_days": 5,
        }]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "created"
    param_id = UUID(resp.json()["results"][0]["param_id"])

    # Backdate the active row so the next differing push ROTATES instead of
    # updating in place (decide_action: same-day change = UPDATED_INPLACE).
    with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
        c.execute(
            "UPDATE item_planning_params SET effective_from = %s WHERE param_id = %s",
            (TODAY - timedelta(days=5), param_id),
        )

    # Rotation push: ONLY lead_time_sourcing_days; safety_stock_qty omitted
    # (the empty TSV cell) → must be preserved from the previous version.
    resp = api_client.post(
        "/v1/ingest/planning-params",
        json={"params": [{
            "item_external_id": seed["item_pp2"],
            "location_external_id": seed["loc"],
            "lead_time_sourcing_days": 9,
        }]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "rotated"

    row = _active_pp_row(seed["item_pp2"], seed["loc"])
    assert row["lead_time_sourcing_days"] == 9          # the pushed change
    assert row["safety_stock_qty"] == 50                # empty cell → untouched
    assert row["consumption_window_days"] == 7          # DB DEFAULT carried over
    assert row["forecast_consumption_strategy"] == "max_only"

    # The previous version was closed at today (half-open interval), never lost.
    old = _fetchone(
        "SELECT effective_to FROM item_planning_params WHERE param_id = %s",
        (param_id,),
    )
    assert old["effective_to"] == TODAY

    history = _fetchall(
        """
        SELECT p.param_id FROM item_planning_params p
        JOIN items i ON i.item_id = p.item_id
        WHERE i.external_id = %s
        """,
        (seed["item_pp2"],),
    )
    assert len(history) == 2
