"""
tests/integration/test_c2_changed_only_ingest_integration.py — C2 (moteur
d'exception, chantier 2), VOLET 3 proofs 1 & 2, against real Postgres.

Doctrine C2 §1 (CHANGED-ONLY) + §2 (EVENTS = VALUE LEDGER) + architect plan §A.

  Proof 1 — a strictly identical re-push is a NO-OP: action="unchanged", no new
            event, the node is not re-dirtied, and the propagation that follows
            recomputes nothing.

  Proof 2 — a re-push with exactly ONE business field changed emits exactly the
            expected typed event(s), with old_*/new_* filled correctly, and
            dirties the node.

Contract matrix under test (architect plan §A — ONE event per changed field):

  on_hand (node-key lookup)     quantity  -> onhand_updated        (old/new_quantity)
                                qty_uom   -> onhand_updated        (old/new_text)
                                as_of_date-> EXCLUDED (deliberate: the timeless
                                             balance's as-of moves every day; a
                                             re-push touching only it is unchanged)
  purchase_order (extref lookup) quantity -> supply_qty_changed    (old/new_quantity)
                                time_ref  -> supply_date_changed   (old/new_date)
                                active    -> supply_status_changed (old/new_text true/false)
                                qty_uom   -> supply_uom_changed     (old/new_text)
  customer_order (extref lookup) quantity -> demand_qty_changed    (old/new_quantity)
                                time_ref  -> demand_date_changed   (old/new_date)
                                active    -> demand_status_changed (old/new_text)
  forecast (node-key lookup)     quantity -> demand_qty_changed    (old/new_quantity)

Creation (INSERT) keeps ONE ``ingestion_complete`` with new_* filled, old_* NULL
(doctrine C2 §2, architect plan §A).

RED UNTIL VOLET 1: on the pre-C2 tree every re-push returns "updated" and emits
a bare ``ingestion_complete`` with empty ledger columns — these proofs fail
until the changed-only + value-ledger emission lands in the ingest router.

Isolation follows test_ingest_retraction_integration.py exactly: committed seeds
under a per-run PREFIX, neutralized (deactivated, never DELETEd) by a module
finalizer; committed state asserted through fresh connections.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.constants import BASELINE_SCENARIO_ID

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
PREFIX = f"C2CH-{uuid4().hex[:8]}"
TODAY = date.today()

# The typed value-ledger event types C2 emits on a change (never the creation
# event). Used to isolate "did a change fire" from the creation ingestion_complete.
_C2_CHANGE_TYPES = (
    "onhand_updated",
    "supply_qty_changed", "supply_date_changed",
    "supply_status_changed", "supply_uom_changed",
    "demand_qty_changed", "demand_date_changed", "demand_status_changed",
)


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB
    (same pattern as test_ingest_retraction_integration.py)."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = TOKEN

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


def _ext(base: str) -> str:
    return f"{PREFIX}-{base}"


@pytest.fixture(scope="module")
def seed(api_client):
    """Master data through the real ingest API; NEUTRALIZE (never delete) under
    this module's PREFIX at teardown."""
    items = [
        {"external_id": _ext("ITEM-OH"), "name": "C2 on-hand item",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("ITEM-PO"), "name": "C2 PO item",
         "item_type": "component", "uom": "EA", "status": "active"},
        {"external_id": _ext("ITEM-CO"), "name": "C2 CO item",
         "item_type": "finished_good", "uom": "EA", "status": "active"},
        {"external_id": _ext("ITEM-FC"), "name": "C2 forecast item",
         "item_type": "finished_good", "uom": "EA", "status": "active"},
    ]
    assert api_client.post("/v1/ingest/items", json={"items": items}, headers=AUTH).status_code == 200
    assert api_client.post(
        "/v1/ingest/locations",
        json={"locations": [{"external_id": _ext("LOC"), "name": "C2 DC", "location_type": "dc"}]},
        headers=AUTH,
    ).status_code == 200
    assert api_client.post(
        "/v1/ingest/suppliers",
        json={"suppliers": [{"external_id": _ext("SUP"), "name": "C2 supplier"}]},
        headers=AUTH,
    ).status_code == 200

    yield {
        "item_oh": _ext("ITEM-OH"), "item_po": _ext("ITEM-PO"),
        "item_co": _ext("ITEM-CO"), "item_fc": _ext("ITEM-FC"),
        "loc": _ext("LOC"), "sup": _ext("SUP"),
    }

    like = PREFIX + "%"
    try:
        with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
            c.execute(
                """
                UPDATE nodes SET active = FALSE, is_dirty = FALSE
                WHERE scenario_id = %s AND item_id IN (
                    SELECT item_id FROM items WHERE external_id LIKE %s)
                """,
                (BASELINE_SCENARIO_ID, like),
            )
            c.execute("UPDATE items SET status = 'obsolete' WHERE external_id LIKE %s", (like,))
    except Exception:
        pass  # migrated_db teardown is the backstop


# ─────────────────────────────────────────────────────────────
# Read / mutate helpers (fresh connection: only committed state)
# ─────────────────────────────────────────────────────────────

def _one(sql: str, params: tuple):
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as c:
        return c.execute(sql, params).fetchone()


def _all(sql: str, params: tuple):
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as c:
        return c.execute(sql, params).fetchall()


def _node_by_extref(entity_type: str, external_id: str) -> dict:
    row = _one(
        """
        SELECT n.node_id, n.active, n.is_dirty, n.quantity, n.qty_uom, n.time_ref
        FROM external_references er
        JOIN nodes n ON n.node_id = er.internal_id
        WHERE er.entity_type = %s AND er.external_id = %s
        """,
        (entity_type, external_id),
    )
    assert row is not None, f"no node for {entity_type} '{external_id}'"
    return row


def _onhand_node(item_ext: str, loc_ext: str) -> dict:
    row = _one(
        """
        SELECT n.node_id, n.active, n.is_dirty, n.quantity, n.qty_uom, n.time_ref
        FROM nodes n
        JOIN items i ON i.item_id = n.item_id
        JOIN locations l ON l.location_id = n.location_id
        WHERE i.external_id = %s AND l.external_id = %s
          AND n.node_type = 'OnHandSupply' AND n.scenario_id = %s AND n.active = TRUE
        ORDER BY n.updated_at DESC LIMIT 1
        """,
        (item_ext, loc_ext, BASELINE_SCENARIO_ID),
    )
    assert row is not None, f"no OnHandSupply node for {item_ext}@{loc_ext}"
    return row


def _forecast_node(item_ext: str, loc_ext: str, bucket: date, grain: str) -> dict:
    row = _one(
        """
        SELECT n.node_id, n.is_dirty, n.quantity, n.time_ref
        FROM nodes n
        JOIN items i ON i.item_id = n.item_id
        JOIN locations l ON l.location_id = n.location_id
        WHERE i.external_id = %s AND l.external_id = %s
          AND n.node_type = 'ForecastDemand' AND n.scenario_id = %s
          AND n.time_ref = %s AND n.time_grain = %s AND n.active = TRUE
        LIMIT 1
        """,
        (item_ext, loc_ext, BASELINE_SCENARIO_ID, bucket, grain),
    )
    assert row is not None, f"no ForecastDemand node for {item_ext}@{loc_ext} {bucket}/{grain}"
    return row


def _events_for_node(node_id) -> list[dict]:
    return _all(
        """
        SELECT event_id, event_type, field_changed, processed,
               old_quantity, new_quantity, old_date, new_date, old_text, new_text
        FROM events
        WHERE trigger_node_id = %s
        ORDER BY created_at, stream_seq
        """,
        (node_id,),
    )


def _new_events(node_id, before_ids: set) -> list[dict]:
    return [e for e in _events_for_node(node_id) if e["event_id"] not in before_ids]


def _is_dirty(node_id) -> bool:
    return _one("SELECT is_dirty FROM nodes WHERE node_id = %s", (node_id,))["is_dirty"]


def _dirty_nonpi_count(item_ext: str) -> int:
    return _one(
        """
        SELECT COUNT(*) AS n FROM nodes n
        JOIN items i ON i.item_id = n.item_id
        WHERE i.external_id = %s AND n.scenario_id = %s
          AND n.is_dirty = TRUE AND n.node_type <> 'ProjectedInventory'
        """,
        (item_ext, BASELINE_SCENARIO_ID),
    )["n"]


def _reset_dirty(item_ext: str) -> None:
    """Clear is_dirty on every node of an item — a deterministic clean slate so
    '0 dirty' / '1 dirty' assertions never inherit a prior step's flag (fresh
    autocommit connection, same out-of-band pattern the retraction test uses)."""
    with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
        c.execute(
            """
            UPDATE nodes SET is_dirty = FALSE
            WHERE scenario_id = %s AND item_id = (
                SELECT item_id FROM items WHERE external_id = %s)
            """,
            (BASELINE_SCENARIO_ID, item_ext),
        )


def _post(api_client, path: str, payload: dict) -> dict:
    resp = api_client.post(path, json=payload, headers=AUTH)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _drain_full(api_client) -> None:
    """Full recompute: processes ALL unprocessed events so the next incremental
    run has a clean, drained baseline (proof-1 no-op assertion)."""
    resp = api_client.post("/v1/calc/run", json={"full_recompute": True}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "completed", resp.json()


def _recalc_incremental(api_client) -> dict:
    resp = api_client.post("/v1/calc/run", json={}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _qeq(value, expected: float) -> bool:
    """NUMERIC columns come back as Decimal — compare numerically."""
    return value is not None and float(value) == float(expected)


# ═════════════════════════════════════════════════════════════
# Creation event contract (doctrine C2 §2 / plan §A)
# ═════════════════════════════════════════════════════════════

def test_creation_event_fills_new_only(api_client, seed):
    """INSERT keeps ONE ingestion_complete carrying new_quantity/new_date, with
    old_* and field_changed NULL — the CDC 'insert' idiom."""
    po_ext = _ext("PO-CREATE")
    delivery = TODAY + timedelta(days=5)
    _post(api_client, "/v1/ingest/purchase-orders", {"purchase_orders": [{
        "external_id": po_ext, "item_external_id": seed["item_po"],
        "location_external_id": seed["loc"], "supplier_external_id": seed["sup"],
        "quantity": 40, "uom": "EA",
        "expected_delivery_date": delivery.isoformat(), "status": "confirmed",
    }]})

    node = _node_by_extref("purchase_order", po_ext)
    evts = _events_for_node(node["node_id"])
    assert len(evts) == 1, f"exactly one creation event expected, got {len(evts)}"
    e = evts[0]
    assert e["event_type"] == "ingestion_complete"
    assert _qeq(e["new_quantity"], 40), "creation event must record new_quantity"
    assert e["new_date"] == delivery, "creation event must record new_date"
    assert e["old_quantity"] is None and e["old_date"] is None and e["old_text"] is None
    assert e["field_changed"] is None


# ═════════════════════════════════════════════════════════════
# Proof 1 — identical re-push is a no-op
# ═════════════════════════════════════════════════════════════

def test_proof1_onhand_identical_repush_is_noop(api_client, seed):
    payload = {"on_hand": [{
        "item_external_id": seed["item_oh"], "location_external_id": seed["loc"],
        "quantity": 100, "uom": "EA", "as_of_date": TODAY.isoformat(),
    }]}
    assert _post(api_client, "/v1/ingest/on-hand", payload)["results"][0]["action"] == "inserted"
    node_id = _onhand_node(seed["item_oh"], seed["loc"])["node_id"]

    _drain_full(api_client)
    _reset_dirty(seed["item_oh"])
    before = {e["event_id"] for e in _events_for_node(node_id)}

    body = _post(api_client, "/v1/ingest/on-hand", payload)
    assert body["results"][0]["action"] == "unchanged", (
        "a strictly identical on-hand re-push must be a changed-only no-op"
    )
    assert _new_events(node_id, before) == [], "identical re-push emitted an event"
    assert _is_dirty(node_id) is False, "identical re-push re-dirtied the node"
    assert _dirty_nonpi_count(seed["item_oh"]) == 0

    assert _recalc_incremental(api_client)["nodes_recalculated"] == 0, (
        "propagation after an identical re-push must recompute nothing"
    )


def test_proof1_onhand_as_of_date_only_change_is_noop(api_client, seed):
    """The deliberate on-hand time_ref EXCLUSION (plan §A): only as_of_date
    differs -> unchanged, no event, nothing dirtied. Protects the gain on the
    most voluminous daily feed."""
    item = _ext("ITEM-OH2")
    assert api_client.post("/v1/ingest/items", json={"items": [{
        "external_id": item, "name": "C2 on-hand asof item",
        "item_type": "component", "uom": "EA", "status": "active"}]}, headers=AUTH).status_code == 200

    def payload(as_of: date) -> dict:
        return {"on_hand": [{
            "item_external_id": item, "location_external_id": seed["loc"],
            "quantity": 100, "uom": "EA", "as_of_date": as_of.isoformat()}]}

    assert _post(api_client, "/v1/ingest/on-hand", payload(TODAY))["results"][0]["action"] == "inserted"
    node_id = _onhand_node(item, seed["loc"])["node_id"]
    _drain_full(api_client)
    _reset_dirty(item)
    before = {e["event_id"] for e in _events_for_node(node_id)}

    body = _post(api_client, "/v1/ingest/on-hand", payload(TODAY + timedelta(days=1)))
    assert body["results"][0]["action"] == "unchanged", (
        "on-hand re-push differing only in as_of_date must be unchanged "
        "(time_ref is excluded from on-hand change detection)"
    )
    assert _new_events(node_id, before) == []
    assert _is_dirty(node_id) is False


def test_proof1_po_identical_repush_is_noop(api_client, seed):
    po_ext = _ext("PO-IDENT")
    delivery = TODAY + timedelta(days=8)
    payload = {"purchase_orders": [{
        "external_id": po_ext, "item_external_id": seed["item_po"],
        "location_external_id": seed["loc"], "supplier_external_id": seed["sup"],
        "quantity": 25, "uom": "EA",
        "expected_delivery_date": delivery.isoformat(), "status": "confirmed",
    }]}
    assert _post(api_client, "/v1/ingest/purchase-orders", payload)["results"][0]["action"] == "inserted"
    node_id = _node_by_extref("purchase_order", po_ext)["node_id"]

    _drain_full(api_client)
    _reset_dirty(seed["item_po"])
    before = {e["event_id"] for e in _events_for_node(node_id)}

    body = _post(api_client, "/v1/ingest/purchase-orders", payload)
    assert body["results"][0]["action"] == "unchanged"
    assert _new_events(node_id, before) == []
    assert _is_dirty(node_id) is False


def test_proof1_po_status_confirmed_to_in_transit_is_noop(api_client, seed):
    """confirmed -> in_transit: raw source status changed but BOTH map to
    active=TRUE and qty/date/uom are identical, so NO compared field changed.
    Encodes the 'compare derived active, never the raw status string' decision
    (plan §A) — this same transition returned "updated" pre-C2."""
    po_ext = _ext("PO-NONTERM")
    delivery = TODAY + timedelta(days=9)

    def payload(status: str) -> dict:
        return {"purchase_orders": [{
            "external_id": po_ext, "item_external_id": seed["item_po"],
            "location_external_id": seed["loc"], "supplier_external_id": seed["sup"],
            "quantity": 30, "uom": "EA",
            "expected_delivery_date": delivery.isoformat(), "status": status}]}

    assert _post(api_client, "/v1/ingest/purchase-orders", payload("confirmed"))["results"][0]["action"] == "inserted"
    node_id = _node_by_extref("purchase_order", po_ext)["node_id"]
    _drain_full(api_client)
    _reset_dirty(seed["item_po"])
    before = {e["event_id"] for e in _events_for_node(node_id)}

    body = _post(api_client, "/v1/ingest/purchase-orders", payload("in_transit"))
    assert body["results"][0]["action"] == "unchanged", (
        "confirmed->in_transit changes no compared field (both active, same "
        "qty/date/uom) — must be a changed-only no-op"
    )
    assert _new_events(node_id, before) == []
    assert _is_dirty(node_id) is False


# ═════════════════════════════════════════════════════════════
# Proof 2 — a single changed field emits exactly the right typed event
# ═════════════════════════════════════════════════════════════

def _settle_and_capture(api_client, item_ext: str, node_id):
    _drain_full(api_client)
    _reset_dirty(item_ext)
    return {e["event_id"] for e in _events_for_node(node_id)}


def test_proof2_onhand_quantity_change(api_client, seed):
    item = _ext("ITEM-OH-Q")
    assert api_client.post("/v1/ingest/items", json={"items": [{
        "external_id": item, "name": "OH qty", "item_type": "component",
        "uom": "EA", "status": "active"}]}, headers=AUTH).status_code == 200

    def payload(q: float) -> dict:
        return {"on_hand": [{"item_external_id": item, "location_external_id": seed["loc"],
                             "quantity": q, "uom": "EA", "as_of_date": TODAY.isoformat()}]}

    _post(api_client, "/v1/ingest/on-hand", payload(100))
    node_id = _onhand_node(item, seed["loc"])["node_id"]
    before = _settle_and_capture(api_client, item, node_id)

    body = _post(api_client, "/v1/ingest/on-hand", payload(150))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1, f"exactly one change event expected, got {[e['event_type'] for e in new]}"
    e = new[0]
    assert e["event_type"] == "onhand_updated"
    assert e["field_changed"] == "quantity"
    assert _qeq(e["old_quantity"], 100) and _qeq(e["new_quantity"], 150)
    assert e["old_date"] is None and e["new_date"] is None and e["old_text"] is None
    assert _is_dirty(node_id) is True


def test_proof2_onhand_uom_change(api_client, seed):
    item = _ext("ITEM-OH-U")
    assert api_client.post("/v1/ingest/items", json={"items": [{
        "external_id": item, "name": "OH uom", "item_type": "component",
        "uom": "EA", "status": "active"}]}, headers=AUTH).status_code == 200

    def payload(uom: str) -> dict:
        return {"on_hand": [{"item_external_id": item, "location_external_id": seed["loc"],
                             "quantity": 100, "uom": uom, "as_of_date": TODAY.isoformat()}]}

    _post(api_client, "/v1/ingest/on-hand", payload("EA"))
    node_id = _onhand_node(item, seed["loc"])["node_id"]
    before = _settle_and_capture(api_client, item, node_id)

    body = _post(api_client, "/v1/ingest/on-hand", payload("BOX"))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1
    e = new[0]
    assert e["event_type"] == "onhand_updated"
    assert e["field_changed"] == "qty_uom"
    assert e["old_text"] == "EA" and e["new_text"] == "BOX"
    assert e["old_quantity"] is None and e["new_quantity"] is None
    assert _is_dirty(node_id) is True


def test_proof2_po_quantity_change(api_client, seed):
    po_ext = _ext("PO-Q")
    delivery = TODAY + timedelta(days=10)

    def payload(q: float) -> dict:
        return {"purchase_orders": [{
            "external_id": po_ext, "item_external_id": seed["item_po"],
            "location_external_id": seed["loc"], "supplier_external_id": seed["sup"],
            "quantity": q, "uom": "EA",
            "expected_delivery_date": delivery.isoformat(), "status": "confirmed"}]}

    _post(api_client, "/v1/ingest/purchase-orders", payload(50))
    node_id = _node_by_extref("purchase_order", po_ext)["node_id"]
    before = _settle_and_capture(api_client, seed["item_po"], node_id)

    body = _post(api_client, "/v1/ingest/purchase-orders", payload(75))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1
    e = new[0]
    assert e["event_type"] == "supply_qty_changed"
    assert e["field_changed"] == "quantity"
    assert _qeq(e["old_quantity"], 50) and _qeq(e["new_quantity"], 75)
    assert _is_dirty(node_id) is True


def test_proof2_po_date_change(api_client, seed):
    po_ext = _ext("PO-D")
    d1, d2 = TODAY + timedelta(days=10), TODAY + timedelta(days=17)

    def payload(when: date) -> dict:
        return {"purchase_orders": [{
            "external_id": po_ext, "item_external_id": seed["item_po"],
            "location_external_id": seed["loc"], "supplier_external_id": seed["sup"],
            "quantity": 50, "uom": "EA",
            "expected_delivery_date": when.isoformat(), "status": "confirmed"}]}

    _post(api_client, "/v1/ingest/purchase-orders", payload(d1))
    node_id = _node_by_extref("purchase_order", po_ext)["node_id"]
    before = _settle_and_capture(api_client, seed["item_po"], node_id)

    body = _post(api_client, "/v1/ingest/purchase-orders", payload(d2))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1
    e = new[0]
    assert e["event_type"] == "supply_date_changed"
    assert e["field_changed"] == "time_ref"
    assert e["old_date"] == d1 and e["new_date"] == d2
    assert e["old_quantity"] is None and e["new_quantity"] is None
    assert _is_dirty(node_id) is True


def test_proof2_po_status_change_deactivates(api_client, seed):
    """confirmed -> cancelled: active TRUE -> FALSE -> supply_status_changed with
    old/new_text 'true'/'false'."""
    po_ext = _ext("PO-S")
    delivery = TODAY + timedelta(days=12)

    def payload(status: str) -> dict:
        return {"purchase_orders": [{
            "external_id": po_ext, "item_external_id": seed["item_po"],
            "location_external_id": seed["loc"], "supplier_external_id": seed["sup"],
            "quantity": 50, "uom": "EA",
            "expected_delivery_date": delivery.isoformat(), "status": status}]}

    _post(api_client, "/v1/ingest/purchase-orders", payload("confirmed"))
    node_id = _node_by_extref("purchase_order", po_ext)["node_id"]
    before = _settle_and_capture(api_client, seed["item_po"], node_id)

    body = _post(api_client, "/v1/ingest/purchase-orders", payload("cancelled"))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1
    e = new[0]
    assert e["event_type"] == "supply_status_changed"
    assert e["field_changed"] == "active"
    assert e["old_text"] == "true" and e["new_text"] == "false"
    assert _node_by_extref("purchase_order", po_ext)["active"] is False
    assert _is_dirty(node_id) is True


def test_proof2_po_uom_change(api_client, seed):
    po_ext = _ext("PO-U")
    delivery = TODAY + timedelta(days=13)

    def payload(uom: str) -> dict:
        return {"purchase_orders": [{
            "external_id": po_ext, "item_external_id": seed["item_po"],
            "location_external_id": seed["loc"], "supplier_external_id": seed["sup"],
            "quantity": 50, "uom": uom,
            "expected_delivery_date": delivery.isoformat(), "status": "confirmed"}]}

    _post(api_client, "/v1/ingest/purchase-orders", payload("EA"))
    node_id = _node_by_extref("purchase_order", po_ext)["node_id"]
    before = _settle_and_capture(api_client, seed["item_po"], node_id)

    body = _post(api_client, "/v1/ingest/purchase-orders", payload("PAL"))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1
    e = new[0]
    assert e["event_type"] == "supply_uom_changed"
    assert e["field_changed"] == "qty_uom"
    assert e["old_text"] == "EA" and e["new_text"] == "PAL"
    assert _is_dirty(node_id) is True


def test_proof2_po_multi_field_change_emits_one_event_per_field(api_client, seed):
    """qty AND date change together -> EXACTLY two events (one per field), each
    filling only its own typed couple. The 'one event per changed field'
    granularity (plan §A)."""
    po_ext = _ext("PO-MULTI")
    d1, d2 = TODAY + timedelta(days=10), TODAY + timedelta(days=20)

    def payload(q: float, when: date) -> dict:
        return {"purchase_orders": [{
            "external_id": po_ext, "item_external_id": seed["item_po"],
            "location_external_id": seed["loc"], "supplier_external_id": seed["sup"],
            "quantity": q, "uom": "EA",
            "expected_delivery_date": when.isoformat(), "status": "confirmed"}]}

    _post(api_client, "/v1/ingest/purchase-orders", payload(50, d1))
    node_id = _node_by_extref("purchase_order", po_ext)["node_id"]
    before = _settle_and_capture(api_client, seed["item_po"], node_id)

    body = _post(api_client, "/v1/ingest/purchase-orders", payload(80, d2))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 2, f"expected one event per changed field, got {[e['event_type'] for e in new]}"
    by_type = {e["event_type"]: e for e in new}
    assert set(by_type) == {"supply_qty_changed", "supply_date_changed"}
    assert _qeq(by_type["supply_qty_changed"]["old_quantity"], 50)
    assert _qeq(by_type["supply_qty_changed"]["new_quantity"], 80)
    assert by_type["supply_date_changed"]["old_date"] == d1
    assert by_type["supply_date_changed"]["new_date"] == d2
    assert _is_dirty(node_id) is True


def test_proof2_customer_order_quantity_change(api_client, seed):
    co_ext = _ext("CO-Q")
    req = TODAY + timedelta(days=15)

    def payload(q: float) -> dict:
        return {"customer_orders": [{
            "external_id": co_ext, "item_external_id": seed["item_co"],
            "location_external_id": seed["loc"], "quantity": q,
            "requested_delivery_date": req.isoformat(), "status": "open"}]}

    _post(api_client, "/v1/ingest/customer-orders", payload(7))
    node_id = _node_by_extref("customer_order", co_ext)["node_id"]
    before = _settle_and_capture(api_client, seed["item_co"], node_id)

    body = _post(api_client, "/v1/ingest/customer-orders", payload(12))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1
    e = new[0]
    assert e["event_type"] == "demand_qty_changed"
    assert e["field_changed"] == "quantity"
    assert _qeq(e["old_quantity"], 7) and _qeq(e["new_quantity"], 12)
    assert _is_dirty(node_id) is True


def test_proof2_customer_order_date_change(api_client, seed):
    co_ext = _ext("CO-D")
    d1, d2 = TODAY + timedelta(days=15), TODAY + timedelta(days=25)

    def payload(when: date) -> dict:
        return {"customer_orders": [{
            "external_id": co_ext, "item_external_id": seed["item_co"],
            "location_external_id": seed["loc"], "quantity": 7,
            "requested_delivery_date": when.isoformat(), "status": "open"}]}

    _post(api_client, "/v1/ingest/customer-orders", payload(d1))
    node_id = _node_by_extref("customer_order", co_ext)["node_id"]
    before = _settle_and_capture(api_client, seed["item_co"], node_id)

    body = _post(api_client, "/v1/ingest/customer-orders", payload(d2))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1
    e = new[0]
    assert e["event_type"] == "demand_date_changed"
    assert e["field_changed"] == "time_ref"
    assert e["old_date"] == d1 and e["new_date"] == d2
    assert _is_dirty(node_id) is True


def test_proof2_customer_order_status_change_deactivates(api_client, seed):
    co_ext = _ext("CO-S")
    req = TODAY + timedelta(days=15)

    def payload(status: str) -> dict:
        return {"customer_orders": [{
            "external_id": co_ext, "item_external_id": seed["item_co"],
            "location_external_id": seed["loc"], "quantity": 7,
            "requested_delivery_date": req.isoformat(), "status": status}]}

    _post(api_client, "/v1/ingest/customer-orders", payload("open"))
    node_id = _node_by_extref("customer_order", co_ext)["node_id"]
    before = _settle_and_capture(api_client, seed["item_co"], node_id)

    body = _post(api_client, "/v1/ingest/customer-orders", payload("cancelled"))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1
    e = new[0]
    assert e["event_type"] == "demand_status_changed"
    assert e["field_changed"] == "active"
    assert e["old_text"] == "true" and e["new_text"] == "false"
    assert _node_by_extref("customer_order", co_ext)["active"] is False
    assert _is_dirty(node_id) is True


def test_proof2_forecast_quantity_change(api_client, seed):
    bucket = TODAY + timedelta(days=14)
    grain = "week"

    def payload(q: float) -> dict:
        return {"forecasts": [{
            "item_external_id": seed["item_fc"], "location_external_id": seed["loc"],
            "quantity": q, "bucket_date": bucket.isoformat(), "time_grain": grain}]}

    _post(api_client, "/v1/ingest/forecast-demand", payload(30))
    node_id = _forecast_node(seed["item_fc"], seed["loc"], bucket, grain)["node_id"]
    before = _settle_and_capture(api_client, seed["item_fc"], node_id)

    # identical re-push first: a forecast whose qty is unchanged is a no-op.
    noop = _post(api_client, "/v1/ingest/forecast-demand", payload(30))
    assert noop["results"][0]["action"] == "unchanged"
    assert _new_events(node_id, before) == []

    body = _post(api_client, "/v1/ingest/forecast-demand", payload(45))
    assert body["results"][0]["action"] == "updated"

    new = _new_events(node_id, before)
    assert len(new) == 1
    e = new[0]
    assert e["event_type"] == "demand_qty_changed"
    assert e["field_changed"] == "quantity"
    assert _qeq(e["old_quantity"], 30) and _qeq(e["new_quantity"], 45)
    assert _is_dirty(node_id) is True
