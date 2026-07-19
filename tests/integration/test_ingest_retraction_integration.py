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
  (d)     PO confirmed → in_transit (non-terminal) AND re-dated: stays
          active, the ONE replenishes edge is RETARGETED to the new bucket
          (old bucket drained, quantity counted EXACTLY once — the
          duplicate-edge regression proof, re-date flavour; the
          identical-columns re-ingest is a C2 no-op proven in
          test_c2_changed_only_ingest_integration.py).
  (e)     planning params: CREATED with omitted cells → DB DEFAULTs applied
          (forecast_consumption_strategy='max_only', consumption_window_days=7);
          SCD2 rotation with an omitted cell → previous value preserved.
  (f)     feeds_forward chain SHAPE: a series created through the real ingest
          path carries exactly N-1 edges strictly chaining consecutive
          bucket_sequence values (no loop, no skip, no duplicate), all
          active, weight_ratio=1.0 — the 2026-07-17 `_ensure_projection_series`
          fix (before it, ingest-created series had ZERO feeds_forward edges
          and incremental propagation never cascaded past the first bucket).
  (g)     migration 080 backfill: a pre-fix series (PI nodes, no edges —
          INSERTed directly) gets its exact N-1 chain from the migration SQL,
          and a re-execution adds nothing (NOT EXISTS guard, idempotent).

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
from pathlib import Path
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
        {"external_id": _ext("ITEM-CHAIN"), "name": "Feeds-forward chain item",
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
        "item_chain": _ext("ITEM-CHAIN"),
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
    # C2 (changed-only ingest) note: a re-ingest whose compared node columns
    # are all identical is now a structural no-op ("unchanged" — no UPDATE, no
    # event, no is_dirty, no re-wire); that semantics has its own proof in
    # test_c2_changed_only_ingest_integration.py. To keep exercising the #468
    # anti-duplicate-edge guard, THIS re-ingest changes the delivery date, so
    # the UPDATE path + _wire_node_to_pi really run — and the proof is now
    # STRONGER: the re-date must RETARGET the one replenishes edge to the new
    # bucket (dedup #468), never insert a parallel edge nor leave the old one.
    po_ext = _ext("PO-2")
    delivery = TODAY + timedelta(days=7)
    delivery2 = TODAY + timedelta(days=9)
    qty = 25

    def po_payload(status: str, when) -> dict:
        return {
            "purchase_orders": [{
                "external_id": po_ext,
                "item_external_id": seed["item_noreg"],
                "location_external_id": seed["loc"],
                "supplier_external_id": seed["sup"],
                "quantity": qty,
                "uom": "EA",
                "expected_delivery_date": when.isoformat(),
                "status": status,
            }]
        }

    resp = api_client.post(
        "/v1/ingest/purchase-orders", json=po_payload("confirmed", delivery), headers=AUTH
    )
    assert resp.status_code == 200, resp.text
    _recalc(api_client)
    assert _pi_bucket(seed["item_noreg"], seed["loc"], delivery)["inflows"] == qty

    # confirmed → in_transit AND re-dated: must stay active, must be "updated".
    resp = api_client.post(
        "/v1/ingest/purchase-orders", json=po_payload("in_transit", delivery2), headers=AUTH
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "updated"

    node = _node_for("purchase_order", po_ext)
    assert node["active"] is True, "non-terminal in_transit must NOT retract the PO"

    _recalc(api_client)
    # THE duplicate-edge regression proof (#468), re-date flavour: before the
    # _wire_node_to_pi dedup, a re-ingest INSERTed a parallel replenishes edge
    # and inflows_agg summed the quantity once PER EDGE. After the fix the ONE
    # edge is retargeted: full qty lands in the NEW bucket, ZERO stays behind.
    new_bucket = _pi_bucket(seed["item_noreg"], seed["loc"], delivery2)
    assert new_bucket["inflows"] == qty, (
        f"expected inflows == {qty} exactly once in the re-dated bucket; a "
        f"duplicated edge would double-count (got {new_bucket['inflows']})"
    )
    old_bucket = _pi_bucket(seed["item_noreg"], seed["loc"], delivery)
    assert old_bucket["inflows"] == 0, (
        f"old bucket must be drained after the re-date retarget "
        f"(got {old_bucket['inflows']} — stale edge left behind)"
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


# ─────────────────────────────────────────────────────────────
# (f) — feeds_forward chain SHAPE on a series created by the real ingest path
# ─────────────────────────────────────────────────────────────


def _series_id_for(item_ext: str, loc_ext: str) -> UUID:
    row = _fetchone(
        """
        SELECT ps.series_id
        FROM projection_series ps
        JOIN items i ON i.item_id = ps.item_id
        JOIN locations l ON l.location_id = ps.location_id
        WHERE i.external_id = %s AND l.external_id = %s AND ps.scenario_id = %s
        """,
        (item_ext, loc_ext, BASELINE_SCENARIO_ID),
    )
    assert row is not None, f"no projection_series for {item_ext}@{loc_ext}"
    return UUID(str(row["series_id"]))


def test_new_series_feeds_forward_chain_shape(api_client, seed):
    """Structural proof of the `_ensure_projection_series` fix: an ingest that
    creates a NEW series must chain its N buckets with exactly N-1
    feeds_forward edges — from=seq i → to=seq i+1, strictly consecutive, no
    self-loop, no skip, no duplicate, never leaving the series — all active,
    weight_ratio=1.0 (migration 019's contract). Before the fix this SELECT
    returned ZERO rows for every ingest-created series."""
    po_ext = _ext("PO-CHAIN")
    resp = api_client.post(
        "/v1/ingest/purchase-orders",
        json={"purchase_orders": [{
            "external_id": po_ext,
            "item_external_id": seed["item_chain"],
            "location_external_id": seed["loc"],
            "supplier_external_id": seed["sup"],
            "quantity": 10,
            "uom": "EA",
            "expected_delivery_date": (TODAY + timedelta(days=3)).isoformat(),
            "status": "confirmed",
        }]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["results"][0]["action"] == "inserted"

    series_id = _series_id_for(seed["item_chain"], seed["loc"])

    n_buckets = _fetchone(
        """
        SELECT COUNT(*) AS n FROM nodes
        WHERE projection_series_id = %s
          AND node_type = 'ProjectedInventory' AND active = TRUE
        """,
        (series_id,),
    )["n"]
    assert n_buckets == 90, f"expected the 90 daily PI buckets, got {n_buckets}"

    # Every feeds_forward edge LEAVING a node of the series, with both ends'
    # coordinates — the raw material for every structural assertion below.
    rows = _fetchall(
        """
        SELECT n1.bucket_sequence AS from_seq,
               n2.bucket_sequence AS to_seq,
               n2.projection_series_id AS to_series,
               e.active AS edge_active,
               e.weight_ratio,
               e.scenario_id
        FROM edges e
        JOIN nodes n1 ON n1.node_id = e.from_node_id
        JOIN nodes n2 ON n2.node_id = e.to_node_id
        WHERE e.edge_type = 'feeds_forward'
          AND n1.projection_series_id = %s
        ORDER BY n1.bucket_sequence
        """,
        (series_id,),
    )

    # Exactly N-1 edges for N buckets.
    assert len(rows) == n_buckets - 1, (
        f"expected {n_buckets - 1} feeds_forward edges for {n_buckets} "
        f"buckets, got {len(rows)}"
    )

    # Strict consecutive chaining: the ordered (from, to) list IS the ideal
    # chain — one comparison rules out loops (i,i), skips (i,i+2), backward
    # edges, and duplicates all at once.
    pairs = [(r["from_seq"], r["to_seq"]) for r in rows]
    assert pairs == [(i, i + 1) for i in range(n_buckets - 1)], (
        f"chain is not strictly consecutive: {pairs[:5]}... "
    )

    for r in rows:
        # An edge must never jump to another series' bucket.
        assert UUID(str(r["to_series"])) == series_id
        assert r["edge_active"] is True
        assert r["weight_ratio"] == 1
        assert UUID(str(r["scenario_id"])) == BASELINE_SCENARIO_ID

    # And nothing feeds INTO the series from outside it (no foreign edge
    # masquerading as part of the chain).
    inbound_foreign = _fetchone(
        """
        SELECT COUNT(*) AS n
        FROM edges e
        JOIN nodes n2 ON n2.node_id = e.to_node_id
        JOIN nodes n1 ON n1.node_id = e.from_node_id
        WHERE e.edge_type = 'feeds_forward'
          AND n2.projection_series_id = %s
          AND (n1.projection_series_id IS DISTINCT FROM %s)
        """,
        (series_id, series_id),
    )["n"]
    assert inbound_foreign == 0


# ─────────────────────────────────────────────────────────────
# (g) — migration 080: exact backfill of a pre-fix series + idempotence
# ─────────────────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_080 = (
    _REPO_ROOT / "src" / "ootils_core" / "db" / "migrations"
    / "080_backfill_feeds_forward_edges.sql"
)


class TestMigration080Backfill:
    @pytest.mark.invariants_exempt(
        reason="2026-07-18 (chantier moteur-d'exception CHANTIER 1): this test "
        "DELIBERATELY seeds a pre-fix ProjectedInventory series with ZERO "
        "feeds_forward edges (what every ingest-created series looked like "
        "before the _ensure_projection_series fix) to prove the migration-080 "
        "backfill. That pre-fix shape is exactly what the invariant_violations "
        "net (migration 087) exists to flag as a broken chain, so the seed is "
        "exempted at its source. The seed rides the function-scoped `conn` "
        "(rolled back, all-zero balanced buckets), so no committed residue is "
        "expected today — the marker documents the intentional pre-fix shape "
        "and future-proofs the net against a variant that commits it."
    )
    def test_backfill_exact_then_reexecution_is_noop(self, migrated_db, conn):
        """Backfill + defensive-idempotence contract of migration 080 (same
        NOT EXISTS guard as 019). Triple execution overall, mirroring
        test_reexecuting_078_sql_is_noop — with ONE adaptation: unlike 078,
        the 080 file carries NO internal BEGIN/COMMIT (a single INSERT ...
        SELECT), so it runs INSIDE this test's transaction on the `conn`
        fixture and everything — seed included — rolls back at teardown.
        Execution #1 was the migrated_db boot (fresh DB, zero series: no-op);
        #2 backfills the pre-fix series seeded below; #3 must add nothing.

        Isolation lesson: nothing here ever COMMITs, so no finalizer is
        needed — the rollback IS the cleanup (strictly safer than the
        neutralizing finalizer used for the module's committed seeds)."""
        today = date.today()
        n_buckets = 5

        # ── Seed the PRE-FIX state: item + location + series + N active PI
        # buckets, and deliberately ZERO feeds_forward edges (what every
        # ingest-created series looked like before `_ensure_projection_series`
        # learned to chain them).
        item_id, location_id, series_id = uuid4(), uuid4(), uuid4()
        conn.execute(
            """
            INSERT INTO items (item_id, name, item_type, uom, status, external_id)
            VALUES (%s, 'Mig080 backfill item', 'component', 'EA', 'active', %s)
            """,
            (item_id, _ext("MIG080-ITEM")),
        )
        conn.execute(
            """
            INSERT INTO locations (location_id, name, location_type, external_id)
            VALUES (%s, 'Mig080 backfill DC', 'dc', %s)
            """,
            (location_id, _ext("MIG080-LOC")),
        )
        conn.execute(
            """
            INSERT INTO projection_series
                (series_id, item_id, location_id, scenario_id,
                 horizon_start, horizon_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (series_id, item_id, location_id, BASELINE_SCENARIO_ID,
             today, today + timedelta(days=n_buckets)),
        )
        for i in range(n_buckets):
            day_start = today + timedelta(days=i)
            conn.execute(
                """
                INSERT INTO nodes (
                    node_id, node_type, scenario_id, item_id, location_id,
                    time_grain, time_span_start, time_span_end, time_ref,
                    projection_series_id, bucket_sequence,
                    opening_stock, inflows, outflows, closing_stock,
                    is_dirty, active
                ) VALUES (
                    %s, 'ProjectedInventory', %s, %s, %s,
                    'day', %s, %s, %s,
                    %s, %s,
                    0, 0, 0, 0,
                    FALSE, TRUE
                )
                """,
                (uuid4(), BASELINE_SCENARIO_ID, item_id, location_id,
                 day_start, day_start + timedelta(days=1), day_start,
                 series_id, i),
            )

        def _series_pairs() -> list[tuple[int, int]]:
            return [
                (r["from_seq"], r["to_seq"])
                for r in conn.execute(
                    """
                    SELECT n1.bucket_sequence AS from_seq,
                           n2.bucket_sequence AS to_seq
                    FROM edges e
                    JOIN nodes n1 ON n1.node_id = e.from_node_id
                    JOIN nodes n2 ON n2.node_id = e.to_node_id
                    WHERE e.edge_type = 'feeds_forward'
                      AND n1.projection_series_id = %s
                    ORDER BY n1.bucket_sequence
                    """,
                    (series_id,),
                ).fetchall()
            ]

        def _global_ff_count() -> int:
            return conn.execute(
                "SELECT COUNT(*) AS n FROM edges WHERE edge_type = 'feeds_forward'"
            ).fetchone()["n"]

        assert _series_pairs() == [], "seed must start with ZERO edges (pre-fix state)"
        n_global_before = _global_ff_count()

        sql_text = MIGRATION_080.read_text(encoding="utf-8")

        # ── Execution #2 (the backfill): exactly N-1 edges, strictly chained.
        conn.execute(sql_text)
        assert _series_pairs() == [(i, i + 1) for i in range(n_buckets - 1)]
        # ... and it touched NOTHING else: every other series in this DB was
        # created by the FIXED ingest path, already chained, skipped by the
        # NOT EXISTS guard.
        assert _global_ff_count() == n_global_before + (n_buckets - 1)

        # The backfilled edges honour migration 019's contract.
        edge_rows = conn.execute(
            """
            SELECT e.active, e.weight_ratio, e.scenario_id
            FROM edges e
            JOIN nodes n1 ON n1.node_id = e.from_node_id
            WHERE e.edge_type = 'feeds_forward'
              AND n1.projection_series_id = %s
            """,
            (series_id,),
        ).fetchall()
        for r in edge_rows:
            assert r["active"] is True
            assert r["weight_ratio"] == 1
            assert UUID(str(r["scenario_id"])) == BASELINE_SCENARIO_ID

        # ── Execution #3: a clean no-op — not one duplicated edge, anywhere.
        conn.execute(sql_text)
        assert _series_pairs() == [(i, i + 1) for i in range(n_buckets - 1)], (
            "re-executing migration 080 duplicated or altered chain edges"
        )
        assert _global_ff_count() == n_global_before + (n_buckets - 1)
