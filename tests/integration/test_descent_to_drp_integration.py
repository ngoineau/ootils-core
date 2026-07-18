"""
tests/integration/test_descent_to_drp_integration.py — LE TEST DE PREUVE DRP
(DESC-1, ADR-043 x #395): the end-to-end chain that was the plan's "done"
criterion — national demand on a virtual channel, descended onto real DCs,
recomputed, then THE DRP WAKES UP and drafts a governed inter-site TRANSFER
from the surplus DC toward the deficit DC.

The scenario, driven through the REAL surface only (module-scoped TestClient,
pattern of test_demand_descent_run_integration.py):

  SEED  — one virtual channel (``locations.is_stocking = FALSE``), two DCs;
          baseline ``demand_split_pct`` 60/40 + ``item_dc_eligibility`` TRUE
          for one item (direct INSERT — migration 083 has no ingest surface);
          on-hand via /v1/ingest/on-hand: DC1 = 500 (excédentaire), DC2 = 0
          (vide); ONE lane DC1 -> DC2 through the NEW
          POST /v1/ingest/distribution-links endpoint (generic lane, transit
          7 d = 1 weekly bucket); ONE national CustomerOrderDemand of 100 on
          the channel at DB-CURRENT_DATE + 14 (bucket 2) via
          /v1/ingest/customer-orders.
  RUN   — POST /v1/demand/descend (kill switch OOTILS_DESCENT_ENABLED armed
          via monkeypatch): 60 land on DC1, 40 on DC2, the national source is
          deactivated. Then POST /v1/calc/run {full_recompute: true} (the
          descent never recomputes — its own contract). Then POST /v1/drp/run
          (OOTILS_DRP_ENABLED armed via monkeypatch).
  PROOF — at least one governed recommendation with action='TRANSFER',
          status DRAFT, decision_level L1, source = the surplus DC1,
          dest = the deficit DC2, recommended_qty == 40 (the hand-checkable
          fair-share: deficit 40 = safety 0 - (0 on-hand - 40 demand),
          against DC1's excess 500 - 60 = 440; transfer_multiple 1). The DRP
          woke up on data that entered EXCLUSIVELY through the half-interface
          chain. Plus ADR-021: the DRP run itself wrote NOTHING into
          ``shortages`` (the shortage rows of the calc run are the
          ShortageDetector's, counted before/after the DRP call).

Isolation (pattern of the sibling descent test): referential seeds under a
unique PREFIX via the real API, neutralized by DEACTIVATION in a module
finalizer (nodes off, items obsoleted, eligibility revoked, channel
is_stocking restored, lanes deactivated, drp_run DRAFTs expired) — never a
DELETE. The module-scoped ``migrated_db`` teardown drops the schema as the
backstop.
"""
from __future__ import annotations

import os
from datetime import timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}

PREFIX = f"D2DRP-{uuid4().hex[:8]}"

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

CO_QTY = 100.0        # national demand on the channel
DC1_ONHAND = 500.0    # surplus DC
DC2_ONHAND = 0.0      # empty DC
CO_WEEKS_OUT = 2      # CO at DB_TODAY + 14 d -> weekly bucket 2


def _ext(base: str) -> str:
    return f"{PREFIX}-{base}"


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB. The two
    kill switches (OOTILS_DESCENT_ENABLED / OOTILS_DRP_ENABLED) are read PER
    REQUEST by their routers and are deliberately NOT set here — the proof
    test arms them via monkeypatch (the plan's own wording)."""
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


@pytest.fixture(scope="module")
def seed(api_client, request, migrated_db):
    """The full DESC-1 seed: channel + DCs + shares + eligibility + on-hand +
    lane (via the NEW endpoint) + national demand. Every referential row goes
    through the REAL ingest endpoints; migration 083's tables (shares,
    eligibility) by direct INSERT — no ingest surface yet (PR-F is TSV spec).
    Neutralized by DEACTIVATION, never a DELETE."""
    resp = api_client.post(
        "/v1/ingest/items",
        json={"items": [{"external_id": _ext("ITEM"), "name": "Descent-to-DRP item",
                         "item_type": "finished_good", "uom": "EA",
                         "status": "active"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    resp = api_client.post(
        "/v1/ingest/locations",
        json={"locations": [
            {"external_id": _ext("NAT"), "name": "National channel (virtual)"},
            {"external_id": _ext("DC1"), "name": "Surplus DC", "location_type": "dc"},
            {"external_id": _ext("DC2"), "name": "Deficit DC", "location_type": "dc"},
        ]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        db_today = c.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]
        item = c.execute(
            "SELECT item_id FROM items WHERE external_id = %s", (_ext("ITEM"),)
        ).fetchone()["item_id"]
        locs = {
            r["external_id"]: r["location_id"]
            for r in c.execute(
                "SELECT external_id, location_id FROM locations "
                "WHERE external_id LIKE %s",
                (PREFIX + "%",),
            ).fetchall()
        }
        channel = locs[_ext("NAT")]
        dc1, dc2 = locs[_ext("DC1")], locs[_ext("DC2")]

        # The channel is a VIRTUAL demand-only location (migration 081) — the
        # descent's national-demand scope key AND the is_stocking detection
        # exclusion of ADR-021.
        c.execute(
            "UPDATE locations SET is_stocking = FALSE WHERE location_id = %s",
            (channel,),
        )
        # Eligibility: explicit TRUE rows (an absent pair is NOT eligible).
        for dc in (dc1, dc2):
            c.execute(
                "INSERT INTO item_dc_eligibility (item_id, dc_location_id, eligible, source) "
                "VALUES (%s, %s, TRUE, 'manual')",
                (item, dc),
            )
        # Baseline split 60/40 (scenario_id NULL = baseline, migration 083).
        for dc, pct in ((dc1, Decimal("0.6")), (dc2, Decimal("0.4"))):
            c.execute(
                "INSERT INTO demand_split_pct (scenario_id, item_id, dc_location_id, pct, method) "
                "VALUES (NULL, %s, %s, %s, 'manual')",
                (item, dc, pct),
            )
        c.commit()

    co_date = db_today + timedelta(days=7 * CO_WEEKS_OUT)

    # On-hand through the REAL endpoint: DC1 excédentaire, DC2 vide (an
    # explicit 0 row, so the empty DC exists as a coordinate with stock 0).
    resp = api_client.post(
        "/v1/ingest/on-hand",
        json={"on_hand": [
            {"item_external_id": _ext("ITEM"), "location_external_id": _ext("DC1"),
             "quantity": DC1_ONHAND, "as_of_date": db_today.isoformat()},
            {"item_external_id": _ext("ITEM"), "location_external_id": _ext("DC2"),
             "quantity": DC2_ONHAND, "as_of_date": db_today.isoformat()},
        ]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    # THE LANE between the two DCs — through the NEW endpoint (DESC-1 PR-D).
    # Generic lane (item empty): the DRP specificity rule serves any item on
    # it. Transit 7 d -> lead 1 weekly bucket.
    resp = api_client.post(
        "/v1/ingest/distribution-links",
        json={"distribution_links": [
            {"upstream_external_id": _ext("DC1"),
             "downstream_external_id": _ext("DC2"),
             "item_external_id": "",
             "transit_lead_time_days": 7},
        ]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["summary"]["inserted"] == 1

    # National demand on the channel: ONE CustomerOrderDemand of 100.
    resp = api_client.post(
        "/v1/ingest/customer-orders",
        json={"customer_orders": [
            {"external_id": _ext("CO-1"), "item_external_id": _ext("ITEM"),
             "location_external_id": _ext("NAT"), "quantity": CO_QTY,
             "requested_delivery_date": co_date.isoformat(), "status": "open"},
        ]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    def _neutralize():
        with psycopg.connect(migrated_db, autocommit=True) as c:
            c.execute(
                "UPDATE nodes SET active = FALSE WHERE item_id = %s", (item,)
            )
            c.execute(
                "UPDATE items SET status = 'obsolete' WHERE external_id LIKE %s",
                (PREFIX + "%",),
            )
            c.execute(
                "UPDATE item_dc_eligibility SET eligible = FALSE WHERE item_id = %s",
                (item,),
            )
            c.execute(
                "UPDATE locations SET is_stocking = TRUE WHERE location_id = %s",
                (channel,),
            )
            c.execute(
                "UPDATE distribution_links SET active = FALSE "
                "WHERE upstream_location_id = %s AND downstream_location_id = %s",
                (dc1, dc2),
            )
            c.execute(
                "UPDATE recommendations SET status = 'EXPIRED', updated_at = now() "
                "WHERE agent_name = 'drp_run' AND status = 'DRAFT' AND item_id = %s",
                (item,),
            )

    request.addfinalizer(_neutralize)
    return {
        "item": item, "channel": channel, "dc1": dc1, "dc2": dc2,
        "db_today": db_today, "co_date": co_date,
    }


def test_descent_then_recompute_then_drp_wakes_up(
    api_client, seed, migrated_db, monkeypatch
):
    """Le critère de done du plan: après descente + full recompute, le DRP se
    réveille — au moins une recommendation TRANSFER L1 DRAFT du DC
    excédentaire vers le DC en déficit."""
    monkeypatch.setenv("OOTILS_DESCENT_ENABLED", "1")
    monkeypatch.setenv("OOTILS_DRP_ENABLED", "1")

    # ---- 1. Descent: 100 national -> 60 DC1 / 40 DC2, source deactivated.
    resp = api_client.post("/v1/demand/descend", json={"dry_run": False}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source_nodes_considered"] == 1
    assert body["source_nodes_deactivated"] == 1
    assert body["derived_nodes_created"] == 2
    assert body["lines_written"] == 2
    assert body["items_without_shares"] == []
    assert body["recompute_triggered"] is False  # its own contract — step 2 is on us

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        derived = {
            r["location_id"]: float(r["quantity"])
            for r in c.execute(
                "SELECT location_id, quantity FROM nodes "
                "WHERE item_id = %s AND node_type = 'CustomerOrderDemand' "
                "AND active = TRUE AND location_id = ANY(%s)",
                (seed["item"], [seed["dc1"], seed["dc2"]]),
            ).fetchall()
        }
        assert derived == {seed["dc1"]: 60.0, seed["dc2"]: 40.0}

    # ---- 2. Full recompute (the descent never recomputes — its contract).
    resp = api_client.post(
        "/v1/calc/run", json={"full_recompute": True}, headers=AUTH
    )
    assert resp.status_code == 200, resp.text
    calc = resp.json()
    assert calc["status"] == "completed"
    assert calc["calc_run_id"] is not None
    # The seeded PI series (channel + both DCs) were all walked.
    assert calc["nodes_recalculated"] + calc["nodes_unchanged"] > 0

    # Snapshot the shortages count AFTER the calc run (the ShortageDetector's
    # rows) and BEFORE the DRP — ADR-021 says the DRP adds none.
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        shortages_before_drp_baseline = c.execute(
            "SELECT COUNT(*) AS n FROM shortages"
        ).fetchone()["n"]

    # ---- 3. DRP run — LE RÉVEIL.
    resp = api_client.post("/v1/drp/run", json={}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    drp = resp.json()
    assert drp["scenario_id"] == str(BASELINE)
    assert drp["decision_level"] == "L1"
    assert drp["signals"] >= 1
    assert drp["recommendations_emitted"] >= 1
    assert drp["unresolved_coords"] == 0

    # ---- 4. THE PROOF: a governed TRANSFER L1 DRAFT, surplus DC -> deficit DC.
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        recos = c.execute(
            "SELECT * FROM recommendations "
            "WHERE agent_name = 'drp_run' AND scenario_id = %s "
            "AND action = 'TRANSFER' AND status = 'DRAFT' AND item_id = %s",
            (str(BASELINE), seed["item"]),
        ).fetchall()
        assert len(recos) >= 1, "le DRP ne s'est pas réveillé — aucune reco TRANSFER"
        r = recos[0]
        assert r["decision_level"] == "L1"
        assert r["source_location_id"] == seed["dc1"]  # from the surplus DC ...
        assert r["dest_location_id"] == seed["dc2"]    # ... toward the deficit DC
        # Hand-checkable fair-share: deficit 40 (0 on-hand - 40 demand, safety
        # 0) against DC1's excess 500 - 60 = 440, transfer_multiple 1 -> 40.
        assert float(r["recommended_qty"]) == 40.0
        assert float(r["deficit_qty"]) == 40.0
        # Ship before (or at) the deficit date: lead 1 bucket ahead of bucket 2.
        assert r["proposed_date"] < r["shortage_date"]
        # Evidence trail carries the human-readable lane coordinates.
        assert r["evidence"]["signal"] == "TRANSFER"
        assert r["evidence"]["source_location"] == _ext("DC1")
        assert r["evidence"]["dest_location"] == _ext("DC2")
        assert r["evidence"]["item"] == _ext("ITEM")

        # ADR-021: the DRP run wrote NOTHING into `shortages`.
        assert c.execute(
            "SELECT COUNT(*) AS n FROM shortages"
        ).fetchone()["n"] == shortages_before_drp_baseline


def test_drp_rerun_on_unchanged_plan_is_idempotent(api_client, seed, migrated_db, monkeypatch):
    """Bonus invariant (#395): re-running the DRP on the unchanged plan
    re-derives the SAME deterministic ids — zero new rows, the no-op is
    self-reported. Runs AFTER the proof test (module order is definition
    order; the plan has not moved in between)."""
    monkeypatch.setenv("OOTILS_DRP_ENABLED", "1")

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        before = c.execute(
            "SELECT COUNT(*) AS n FROM recommendations WHERE agent_name = 'drp_run'"
        ).fetchone()["n"]
    assert before >= 1, "the proof test must have emitted first"

    resp = api_client.post("/v1/drp/run", json={}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    drp = resp.json()
    assert drp["recommendations_emitted"] == 0
    assert drp["recommendations_idempotent_noop"] >= 1

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        after = c.execute(
            "SELECT COUNT(*) AS n FROM recommendations WHERE agent_name = 'drp_run'"
        ).fetchone()["n"]
    assert after == before
