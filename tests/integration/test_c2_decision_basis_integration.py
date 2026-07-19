"""
tests/integration/test_c2_decision_basis_integration.py — C2 (moteur
d'exception, chantier 2), VOLET 3 proofs 3 & 4, against real Postgres.

Doctrine C2 §3 (DECISION BASIS — "que savait le moteur quand il a recommandé
X ?") + architect plan §B (migration 088 columns).

  Proof 3 — a calc_run carries its decision basis: after a full recompute the
            calc_runs row stamps anchor_date (the as-of date it computed
            against), engine_flavor (which OOTILS_ENGINE ran), and code_version
            (the code identity, resolved once at import).

  Proof 4 — a supply watcher's recommendation carries anchor_date (d.horizon_start)
            and stream_seq_hwm (the events high-water mark the watcher had seen
            when it decided) — the event-sourced "as-of" seal on the reco.

RED UNTIL VOLET 1/2: on the pre-C2 tree migration 088 is absent, so calc_runs
and recommendations have none of these columns and every SELECT below raises
UndefinedColumn — the proofs turn green once the migration + the calc_run.py /
watcher stamps land.

Isolation: proof 3 seeds through the real ingest API; proof 4 seeds directly in
SQL (the proven shape from test_scenario_backed_watchers_integration.py). Both
lean on the module-scoped migrated_db teardown (drops all public tables).
"""
from __future__ import annotations

import datetime as _dt
import os
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.constants import BASELINE_SCENARIO_ID

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
PREFIX = f"C2DB-{uuid4().hex[:8]}"

_VALID_ENGINE_FLAVORS = {"sql", "python", "rust", "rust-svc"}


def _ext(base: str) -> str:
    return f"{PREFIX}-{base}"


def _current_date():
    with psycopg.connect(TEST_DB_URL) as c:
        return c.execute("SELECT CURRENT_DATE").fetchone()[0]


# ═════════════════════════════════════════════════════════════
# Proof 3 — calc_run carries anchor_date / engine_flavor / code_version
# ═════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def api_client(migrated_db):
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
def basis_seed(api_client):
    """A minimal graph through the real ingest path so a full recompute has PI
    buckets to recalculate."""
    assert api_client.post("/v1/ingest/items", json={"items": [{
        "external_id": _ext("ITEM"), "name": "C2 basis item",
        "item_type": "finished_good", "uom": "EA", "status": "active"}]},
        headers=AUTH).status_code == 200
    assert api_client.post("/v1/ingest/locations", json={"locations": [{
        "external_id": _ext("LOC"), "name": "C2 basis DC", "location_type": "dc"}]},
        headers=AUTH).status_code == 200
    assert api_client.post("/v1/ingest/on-hand", json={"on_hand": [{
        "item_external_id": _ext("ITEM"), "location_external_id": _ext("LOC"),
        "quantity": 100, "uom": "EA", "as_of_date": _dt.date.today().isoformat()}]},
        headers=AUTH).status_code == 200
    assert api_client.post("/v1/ingest/forecast-demand", json={"forecasts": [{
        "item_external_id": _ext("ITEM"), "location_external_id": _ext("LOC"),
        "quantity": 30, "bucket_date": (_dt.date.today() + _dt.timedelta(days=14)).isoformat(),
        "time_grain": "week"}]}, headers=AUTH).status_code == 200
    return {"item": _ext("ITEM"), "loc": _ext("LOC")}


def _latest_calc_run() -> dict:
    row = None
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as c:
        row = c.execute(
            """
            SELECT calc_run_id, status, anchor_date, engine_flavor, code_version
            FROM calc_runs
            WHERE scenario_id = %s
            ORDER BY created_at DESC, started_at DESC
            LIMIT 1
            """,
            (BASELINE_SCENARIO_ID,),
        ).fetchone()
    assert row is not None, "no calc_runs row for baseline"
    return row


def test_proof3_full_recompute_stamps_decision_basis(api_client, basis_seed):
    resp = api_client.post("/v1/calc/run", json={"full_recompute": True}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "completed", resp.json()

    run = _latest_calc_run()
    assert run["status"] in ("completed", "completed_stale")

    # anchor_date = COALESCE(scenarios.as_of_date, CURRENT_DATE); baseline has no
    # as_of_date, so it is the DB's current date — the PAST-principle as-of the
    # run computed against (prerequisite for bit-identical replay).
    assert run["anchor_date"] is not None, "calc_run did not stamp anchor_date (C2 §3)"
    assert run["anchor_date"] == _current_date()

    # engine_flavor = the resolved OOTILS_ENGINE that ran (provenance, no CHECK).
    assert run["engine_flavor"] is not None, "calc_run did not stamp engine_flavor (C2 §3)"
    assert run["engine_flavor"] in _VALID_ENGINE_FLAVORS, (
        f"engine_flavor {run['engine_flavor']!r} is not a known flavour"
    )

    # code_version = OOTILS_CODE_VERSION | git short sha | 'unknown' — resolved
    # ONCE at import, never a subprocess per run. Value is env-dependent; the
    # contract is only that it is stamped and non-empty.
    assert run["code_version"] is not None, "calc_run did not stamp code_version (C2 §3)"
    assert isinstance(run["code_version"], str) and run["code_version"].strip()


# ═════════════════════════════════════════════════════════════
# Proof 4 — a supply watcher reco carries anchor_date + stream_seq_hwm
# ═════════════════════════════════════════════════════════════

# Import seam: mrp_core + watchers live under scripts/ (outside the package) —
# same insertion as test_scenario_backed_watchers_integration.py.
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


@pytest.fixture(scope="module")
def watcher_seed(migrated_db):
    """Minimal past-due EXPEDITE scenario (one bought finished good with
    independent demand + a firm receipt) — enough for the shortage watcher to
    emit at least one governed DRAFT recommendation. Shape reprised from the
    scenario-backed watcher battery; all dates anchored on the DB CURRENT_DATE."""
    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True) as conn:
        cur = conn.cursor()
        today = cur.execute("SELECT CURRENT_DATE").fetchone()[0]

        loc_id = cur.execute(
            "INSERT INTO locations (name, location_type, external_id) "
            "VALUES (%s, %s, %s) RETURNING location_id",
            ("C2 Watcher Plant", "plant", _ext("W-LOC")),
        ).fetchone()[0]
        sup_id = cur.execute(
            "INSERT INTO suppliers (external_id, name, reliability_score, status) "
            "VALUES (%s, %s, %s, %s) RETURNING supplier_id",
            (_ext("W-SUP"), "C2 Watcher Supplier", 0.95, "active"),
        ).fetchone()[0]
        item_ext = _ext("W-FG")
        item_id = cur.execute(
            "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING item_id",
            (item_ext, "C2 Watcher FG", "finished_good", 100.0, "EUR"),
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
            "VALUES (%s,%s,14,10.0,'EUR',TRUE)",
            (sup_id, item_id),
        )

        def _node(ntype, days_out, qty):
            cur.execute(
                "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
                " time_grain, time_ref, active) "
                "VALUES (%s, %s, %s, %s, %s, 'exact_date', %s, TRUE)",
                (ntype, BASELINE_SCENARIO_ID, item_id, loc_id, qty,
                 today + _dt.timedelta(days=days_out)),
            )

        _node("OnHandSupply", 0, 2)
        _node("CustomerOrderDemand", 7, 60)     # near-term -> past-due PO -> EXPEDITE
        _node("PurchaseOrderSupply", 40, 100)   # firm receipt after the need -> simulable

        yield {"dsn": dsn, "item": item_ext}
        # Teardown owned by migrated_db.


def _draft_recos(agent_name: str) -> list[dict]:
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as c:
        return c.execute(
            """
            SELECT item_external_id, action, decision_level, anchor_date, stream_seq_hwm
            FROM recommendations
            WHERE agent_name = %s AND scenario_id = %s AND status = 'DRAFT'
            """,
            (agent_name, BASELINE_SCENARIO_ID),
        ).fetchall()


def test_proof4_shortage_watcher_reco_carries_anchor_and_hwm(watcher_seed):
    import agent_shortage_watcher  # noqa: E402  (scripts/ on sys.path)

    rc = agent_shortage_watcher.main(["--dsn", watcher_seed["dsn"], "--allow-dev"])
    assert rc == 0, "shortage watcher run failed"

    rows = _draft_recos("shortage_watcher")
    assert rows, "seed must produce at least one shortage_watcher DRAFT recommendation"

    by_ext = {r["item_external_id"]: r for r in rows}
    assert watcher_seed["item"] in by_ext, "the seeded past-due item produced no reco"
    assert by_ext[watcher_seed["item"]]["action"] == "EXPEDITE"

    today = _current_date()
    for r in rows:
        # anchor_date = d.horizon_start = the run's as-of (CURRENT_DATE anchor).
        assert r["anchor_date"] is not None, (
            f"{r['item_external_id']}: reco did not stamp anchor_date (C2 §3)"
        )
        assert r["anchor_date"] == today, (
            f"{r['item_external_id']}: anchor_date {r['anchor_date']} != run anchor {today}"
        )
        # stream_seq_hwm = the events high-water mark seen at decision time
        # (seed_cursor in --subscribe, else current MAX(stream_seq)). Opaque,
        # non-negative; the contract is that it is stamped.
        assert r["stream_seq_hwm"] is not None, (
            f"{r['item_external_id']}: reco did not stamp stream_seq_hwm (C2 §3)"
        )
        assert int(r["stream_seq_hwm"]) >= 0
