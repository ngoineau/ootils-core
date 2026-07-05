"""
tests/integration/test_param_overlay_agent_integration.py — DB-backed tests
for the AGENT PATH and the REST endpoint of the scenario planning-param
overlay (chantier #347 PR4), against real Postgres (no mocks — CLAUDE.md).

What PR4 wires on top of the PR1-3 resolver:
  - api/routers/param_overrides.py : POST/GET/DELETE /v1/scenarios/{id}/param-overrides,
    the OOTILS_PARAM_OVERLAY_ENABLED kill switch, ParamOverlayError -> 422 carve-out.
  - tools/agent_tools.py:simulate_param_overrides : ONE fork, N overlay overrides,
    propagate, shortage delta (fail-loudly, same shape as simulate_overrides).
  - scripts/agent_simulation.py:simulate_param_run : the one-fork-per-run harness
    that archives its fork (TTL, never DELETE, never promoted onto baseline).

Invariants asserted here (one test = one invariant):
  1. ISOLATION — an override POSTed into fork A leaves baseline AND sibling fork
     B resolving bit-identically to the pre-override state (the closure test).
  2. Endpoint contract — 422 on baseline/whitelist-miss/illegal-value (with NO
     DSN / psycopg / file-path leak in the message), 201 set, GET list, DELETE
     204 idempotent.
  3. simulate_param_overrides — a real shortage delta when the override actually
     moves the plan; delta_computed True on success, and NO fabricated delta.
  4. Fork archived, never promoted — after a simulate_param_run the fork is
     status='archived' and baseline's resolved params are unchanged.
  5. lot_policy scenario-backing — evidence carries a per-item delta, decision
     levels are coherent, and the harness writes NOTHING into shortages (ADR-021).
  6. Kill switch — disabled overlay -> 503 on all three verbs via TestClient.

Seeding a shortage that MOVES with an override (invariants 3/5): the resolver
reads safety_stock_qty per scenario, and the shortage detector flags
`below_safety_stock` when 0 <= closing_stock < safety_stock_qty. We seed a
baseline PI bucket that projects to closing_stock=0 with base safety_stock_qty=0
(NO baseline shortage), then override safety_stock_qty to a positive value in
the fork — the deep-copied fork bucket now trips a NEW shortage. That makes the
delta unambiguously override-driven (new_shortages non-empty), not a fork-node-id
artifact. Mirrors the seeding of test_param_overlay_propagation_integration.py.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.scenario.param_overlay import resolved_params_sql
from ootils_core.tools.agent_tools import simulate_param_overrides

from .conftest import requires_db

pytestmark = requires_db

# ---------------------------------------------------------------------------
# Import seam: the simulation harness + governance live under scripts/ and do
# bare "import mrp_core" / "import agent_simulation" — same pattern as
# tests/integration/test_agent_fleet_smoke.py.
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_simulation  # noqa: E402  (after sys.path mutation, by design)
from agent_governance import decision_level  # noqa: E402
from agent_lot_policy_watcher import build_param_override  # noqa: E402

AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}
BASELINE = "00000000-0000-0000-0000-000000000001"
BASELINE_UUID = UUID(BASELINE)

# Positive override value that turns a closing_stock=0, SS=0 bucket (no
# shortage) into a below_safety_stock shortage of this magnitude.
SS_OVERRIDE = "999"


# ---------------------------------------------------------------------------
# Fixtures — TestClient bound to the real test DB (mirrors
# test_recommendations_api_integration.py) + a dedicated dict_row connection.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

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


@pytest.fixture
def db(migrated_db):
    """Function-scoped dedicated dict_row connection (autocommit=False) for
    direct seeding + as the dedicated connection simulate_param_overrides owns."""
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        yield c
        try:
            c.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Seed helpers (item / location / planning-params / fork / PI bucket)
# ---------------------------------------------------------------------------


def _seed_item(conn, name: str = "pa-item") -> UUID:
    return conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s) RETURNING item_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["item_id"]


def _seed_location(conn, name: str = "pa-loc") -> UUID:
    return conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s) RETURNING location_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["location_id"]


def _seed_planning_params(conn, item_id, location_id, **overrides) -> None:
    """One CURRENT (effective_to NULL) item_planning_params row. Default
    safety_stock_qty=0 so a closing_stock=0 bucket has NO baseline shortage —
    the fork override is then the ONLY thing that can create one."""
    defaults = dict(
        lead_time_sourcing_days=14,
        lead_time_manufacturing_days=None,
        lead_time_transit_days=None,
        safety_stock_qty=0,
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
        {"item_id": item_id, "location_id": location_id,
         "effective_from": date.today(), **defaults},
    )


def _seed_fork(conn, name: str = "pa-fork", parent: str = BASELINE) -> UUID:
    """A non-baseline scenario. For pure resolver-isolation tests this bare row
    is enough (item_planning_params is scenario-agnostic — the only per-scenario
    differentiator is a scenario_planning_overrides row)."""
    return conn.execute(
        "INSERT INTO scenarios (scenario_id, name, parent_scenario_id, is_baseline, status) "
        "VALUES (%s, %s, %s, FALSE, 'active') RETURNING scenario_id",
        (uuid4(), f"{name}-{uuid4()}", parent),
    ).fetchone()["scenario_id"]


def _seed_pi_bucket(conn, *, scenario_id, item_id, location_id) -> UUID:
    """A bucket-0 ProjectedInventory node with no edges — projects to
    closing_stock=0, which is below_safety_stock for any safety_stock_qty>0."""
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
         date.today(), date.today() + timedelta(days=7), series_id),
    )
    return node_id


def _resolve_one(conn, scenario_id, item_id, location_id) -> dict:
    """Full resolved planning-param row for one (scenario, item, location)."""
    fragment = resolved_params_sql("ipp")
    sql = (
        f"SELECT rp.* FROM ({fragment}) rp "
        "WHERE rp.item_id = %(item_id)s AND rp.location_id = %(location_id)s"
    )
    rows = conn.execute(
        sql, {"scenario_id": scenario_id, "item_id": item_id, "location_id": location_id}
    ).fetchall()
    assert len(rows) == 1, f"expected exactly one resolved row, got {len(rows)}"
    return rows[0]


# ===========================================================================
# Invariant 1 — ISOLATION (the closure test)
# ===========================================================================


def test_endpoint_override_in_fork_a_leaves_baseline_and_fork_b_identical(api_client, db):
    """A POST override into fork A must leave baseline AND sibling fork B
    resolving bit-identically to the pre-override state; only fork A sees it."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id, safety_stock_qty=10)
    fork_a = _seed_fork(db, "pa-iso-a")
    fork_b = _seed_fork(db, "pa-iso-b")
    db.commit()  # the endpoint uses its own pooled connection — must see these rows

    baseline_before = _resolve_one(db, None, item_id, location_id)
    fork_b_before = _resolve_one(db, fork_b, item_id, location_id)

    resp = api_client.post(
        f"/v1/scenarios/{fork_a}/param-overrides",
        headers=AUTH_HEADERS,
        json={"item_id": str(item_id), "location_id": str(location_id),
              "field_name": "safety_stock_qty", "value": "999",
              "applied_by": "agent:test"},
    )
    assert resp.status_code == 201, resp.text

    # Fork A sees the override; baseline and fork B are byte-identical to before.
    assert _resolve_one(db, fork_a, item_id, location_id)["safety_stock_qty"] == 999
    assert _resolve_one(db, None, item_id, location_id) == baseline_before
    assert _resolve_one(db, fork_b, item_id, location_id) == fork_b_before


# ===========================================================================
# Invariant 2 — Endpoint contract (422 no-leak, 201/GET/DELETE)
# ===========================================================================

_LEAK_MARKERS = ("postgresql://", "psycopg", ".py", "\\", "Traceback")


def _assert_no_leak(detail: str) -> None:
    """A ParamOverlayError message is hand-authored from UUIDs/fields/enums only
    (carve-out, module docstring) — it must never leak a DSN, a psycopg text,
    or a file path/traceback."""
    low = detail.lower()
    assert "postgresql://" not in low
    assert "psycopg" not in low
    assert ".py" not in low
    assert "traceback" not in low
    assert "\\" not in detail  # a Windows path separator would signal a leak


def test_endpoint_set_on_baseline_is_422_without_leak(api_client, db):
    """Setting an override on the baseline scenario is refused 422 — and the
    error message carries no DSN/psycopg/path leak."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id)
    db.commit()

    resp = api_client.post(
        f"/v1/scenarios/{BASELINE}/param-overrides",
        headers=AUTH_HEADERS,
        json={"item_id": str(item_id), "location_id": str(location_id),
              "field_name": "safety_stock_qty", "value": "42",
              "applied_by": "agent:test"},
    )
    assert resp.status_code == 422, resp.text
    _assert_no_leak(resp.json()["detail"])


def test_endpoint_set_non_whitelisted_field_is_422_without_leak(api_client, db):
    """A field outside ALLOWED_PARAM_FIELDS is refused 422, no leak. reorder_point_qty
    is a REAL column deliberately absent from the V1 whitelist."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id)
    fork = _seed_fork(db, "pa-wl")
    db.commit()

    resp = api_client.post(
        f"/v1/scenarios/{fork}/param-overrides",
        headers=AUTH_HEADERS,
        json={"item_id": str(item_id), "location_id": str(location_id),
              "field_name": "reorder_point_qty", "value": "42",
              "applied_by": "agent:test"},
    )
    assert resp.status_code == 422, resp.text
    _assert_no_leak(resp.json()["detail"])


def test_endpoint_set_illegal_value_is_422_without_leak(api_client, db):
    """An out-of-bounds value (negative lead time, CHECK >= 0) is refused 422,
    no leak — fail-loudly at the write, not at an innocent reader's cast."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id)
    fork = _seed_fork(db, "pa-badval")
    db.commit()

    resp = api_client.post(
        f"/v1/scenarios/{fork}/param-overrides",
        headers=AUTH_HEADERS,
        json={"item_id": str(item_id), "location_id": str(location_id),
              "field_name": "lead_time_sourcing_days", "value": "-3",
              "applied_by": "agent:test"},
    )
    assert resp.status_code == 422, resp.text
    _assert_no_leak(resp.json()["detail"])


def test_endpoint_set_then_get_lists_the_override(api_client, db):
    """201 on set; GET returns the posed override; a fresh baseline GET is []."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id)
    fork = _seed_fork(db, "pa-getlist")
    db.commit()

    set_resp = api_client.post(
        f"/v1/scenarios/{fork}/param-overrides",
        headers=AUTH_HEADERS,
        json={"item_id": str(item_id), "location_id": str(location_id),
              "field_name": "safety_stock_qty", "value": "42",
              "applied_by": "agent:test"},
    )
    assert set_resp.status_code == 201, set_resp.text

    list_resp = api_client.get(
        f"/v1/scenarios/{fork}/param-overrides", headers=AUTH_HEADERS,
    )
    assert list_resp.status_code == 200
    body = list_resp.json()
    assert body["total"] == 1
    assert body["overrides"][0]["field_name"] == "safety_stock_qty"
    assert body["overrides"][0]["value"] == "42"

    # Baseline carries no override and never can -> legitimate empty 200.
    base_resp = api_client.get(
        f"/v1/scenarios/{BASELINE}/param-overrides", headers=AUTH_HEADERS,
    )
    assert base_resp.status_code == 200
    assert base_resp.json()["overrides"] == []


def test_endpoint_delete_is_204_and_idempotent(api_client, db):
    """DELETE removes the override (204); a second DELETE on the now-missing
    override is a no-op 204 (idempotent), not an error."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id)
    fork = _seed_fork(db, "pa-del")
    db.commit()

    api_client.post(
        f"/v1/scenarios/{fork}/param-overrides",
        headers=AUTH_HEADERS,
        json={"item_id": str(item_id), "location_id": str(location_id),
              "field_name": "safety_stock_qty", "value": "42",
              "applied_by": "agent:test"},
    )

    params = {"item_id": str(item_id), "location_id": str(location_id)}
    first = api_client.request(
        "DELETE", f"/v1/scenarios/{fork}/param-overrides/safety_stock_qty",
        headers=AUTH_HEADERS, params=params,
    )
    assert first.status_code == 204
    second = api_client.request(
        "DELETE", f"/v1/scenarios/{fork}/param-overrides/safety_stock_qty",
        headers=AUTH_HEADERS, params=params,
    )
    assert second.status_code == 204  # idempotent no-op, not 404/500


# ===========================================================================
# Invariant 3 — simulate_param_overrides computes a real, override-driven delta
# ===========================================================================


def test_simulate_param_overrides_delta_moves_with_override(db):
    """An override that trips a NEW shortage in the fork yields a non-empty
    new_shortages delta with delta_computed=True — never a fabricated one."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    # base SS=0 -> baseline PI bucket (closing=0) has NO shortage.
    _seed_planning_params(db, item_id, location_id, safety_stock_qty=0)
    _seed_pi_bucket(db, scenario_id=BASELINE_UUID, item_id=item_id, location_id=location_id)
    db.commit()

    result = simulate_param_overrides(
        db,
        [{"item_id": str(item_id), "location_id": str(location_id),
          "field_name": "safety_stock_qty", "value": SS_OVERRIDE}],
        scenario_name=f"what-if-pa-{uuid4().hex[:8]}",
        base_scenario_id=BASELINE,
        applied_by="agent:test",
    )

    assert result["override_count"] == 1
    assert result["failed_overrides"] == []
    assert result["propagation_status"] == "ok"
    assert result["delta_computed"] is True
    # The fork bucket (deep-copied) now trips below_safety_stock at SS_OVERRIDE
    # while baseline had none -> at least one NEW shortage, net change > 0.
    assert len(result["delta"]["new_shortages"]) >= 1
    assert result["delta"]["net_shortage_change"] >= 1


def test_identical_fork_yields_empty_new_and_resolved_even_with_baseline_shortage(db):
    """fix/counterfactual-delta-keying — THE invariant.

    Seed a baseline that ALREADY has a shortage (safety_stock_qty=999 on a
    closing_stock=0 bucket -> below_safety_stock), then apply an override that
    sets safety_stock_qty to that SAME value: the fork is functionally identical
    to the baseline. The fork's shortage carries a FRESH pi_node_id (deep-copy),
    so the old raw-node-id set-difference reported it as BOTH new AND resolved.
    Keyed by (item, location, shortage_date) it matches its baseline
    counterpart -> new=[] and resolved=[]. net_shortage_change is 0.
    """
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    # Baseline itself is short: SS=999 on a closing=0 bucket.
    _seed_planning_params(db, item_id, location_id, safety_stock_qty=999)
    _seed_pi_bucket(db, scenario_id=BASELINE_UUID, item_id=item_id, location_id=location_id)
    db.commit()

    result = simulate_param_overrides(
        db,
        # Same value the baseline already resolves to -> a no-op override that
        # still forks + propagates, so the delta path runs against a real
        # baseline shortage.
        [{"item_id": str(item_id), "location_id": str(location_id),
          "field_name": "safety_stock_qty", "value": "999"}],
        scenario_name=f"what-if-identical-{uuid4().hex[:8]}",
        base_scenario_id=BASELINE,
        applied_by="agent:test",
    )

    assert result["propagation_status"] == "ok"
    assert result["delta_computed"] is True
    assert result["delta"]["new_shortages"] == [], (
        "an identical fork must report NO new shortages (pre-fix: reported all)"
    )
    assert result["delta"]["resolved_shortages"] == [], (
        "an identical fork must report NO resolved shortages (pre-fix: reported all)"
    )
    assert result["delta"]["net_shortage_change"] == 0


def test_simulate_param_overrides_rejected_override_carries_no_delta(db):
    """An override rejected by ParamOverlayError (illegal value) is recorded in
    failed_overrides with the typed message; with no applied override the run
    stays propagation_status='skipped' and delta_computed=False — no fabricated
    delta, and the failure message carries no DSN/psycopg/path leak."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id)
    db.commit()

    result = simulate_param_overrides(
        db,
        [{"item_id": str(item_id), "location_id": str(location_id),
          "field_name": "lead_time_sourcing_days", "value": "-3"}],  # CHECK >= 0
        scenario_name=f"what-if-pa-bad-{uuid4().hex[:8]}",
        base_scenario_id=BASELINE,
        applied_by="agent:test",
    )

    assert result["override_count"] == 0
    assert len(result["failed_overrides"]) == 1
    _assert_no_leak(result["failed_overrides"][0]["error"])
    assert result["propagation_status"] == "skipped"
    assert result["delta_computed"] is False
    assert result["delta"]["new_shortages"] == []
    assert result["delta"]["net_shortage_change"] == 0


# ===========================================================================
# Invariant 4 — the fork is archived, never promoted onto baseline
# ===========================================================================


def test_simulate_param_run_archives_fork_and_leaves_baseline_unchanged(migrated_db, db):
    """After simulate_param_run the what-if fork is status='archived' (TTL,
    never DELETE) and baseline's resolved params are UNCHANGED — no override is
    ever replayed onto baseline."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id, safety_stock_qty=0)
    _seed_pi_bucket(db, scenario_id=BASELINE_UUID, item_id=item_id, location_id=location_id)
    db.commit()

    baseline_before = _resolve_one(db, None, item_id, location_id)

    override = build_param_override(item_id, "RENEGOTIATE_MOQ", "50")[0]
    candidates = [{"item": str(item_id), "simulable": True,
                   "param_override": override, "reason": None}]
    summary, results = agent_simulation.simulate_param_run(
        migrated_db, "lot_policy_watcher", candidates, applied_by="agent:test",
    )

    assert summary["scenario_id"] is not None
    assert summary["archived"] is True

    # The fork row itself is archived, not deleted.
    fork_row = db.execute(
        "SELECT status FROM scenarios WHERE scenario_id = %s",
        (summary["scenario_id"],),
    ).fetchone()
    assert fork_row is not None, "the fork must survive (never DELETE)"
    assert fork_row["status"] == "archived"

    # Baseline resolves EXACTLY as before — no override replayed onto baseline.
    assert _resolve_one(db, None, item_id, location_id) == baseline_before
    base_overrides = db.execute(
        "SELECT count(*) AS n FROM scenario_planning_overrides WHERE scenario_id = %s",
        (BASELINE_UUID,),
    ).fetchone()
    assert base_overrides["n"] == 0


# ===========================================================================
# Invariant 5 — lot_policy scenario-backing: per-item delta evidence, coherent
# decision levels, and ADR-021 (the harness writes NOTHING into shortages)
# ===========================================================================


def test_simulate_param_run_stamps_per_item_delta_and_touches_no_shortages(migrated_db, db):
    """A lot_policy-shaped candidate whose override trips a shortage gets a
    per-item delta stamped in its result; and simulate_param_run writes NOTHING
    into the BASELINE shortages (ADR-021 — the watcher path never writes baseline
    shortage rows; the fork's own counter-factual rows are written by
    ShortageDetector, scoped to the archived fork — PR3 scoped persistence)."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id, safety_stock_qty=0)
    _seed_pi_bucket(db, scenario_id=BASELINE_UUID, item_id=item_id, location_id=location_id)
    db.commit()

    baseline_shortages_before = db.execute(
        "SELECT count(*) AS n FROM shortages WHERE scenario_id = %s", (BASELINE_UUID,)
    ).fetchone()["n"]

    # safety_stock_qty is a whitelisted lot-policy-adjacent field; use it as the
    # candidate override so the fork trips a real, attributable shortage.
    override = {"item_id": str(item_id), "location_id": str(location_id),
                "field_name": "safety_stock_qty", "value": SS_OVERRIDE}
    candidates = [{"item": str(item_id), "simulable": True,
                   "param_override": override, "reason": None}]
    summary, results = agent_simulation.simulate_param_run(
        migrated_db, "lot_policy_watcher", candidates, applied_by="agent:test",
    )

    assert summary["propagation_status"] == "ok"
    assert summary["delta_computed"] is True
    # Per-item delta share stamped on the (single) simulated candidate.
    ev = agent_simulation.simulation_evidence(summary, results[0])
    assert ev["simulated"] is True
    assert ev["delta"] is not None
    assert ev["delta"]["new_shortages"] >= 1

    # ADR-021: the watcher/harness path never writes BASELINE shortages. The
    # fork's own counter-factual rows are written by ShortageDetector, scoped to
    # the (archived) fork scenario — expected PR3 scoped-persistence behaviour,
    # not a baseline write. So the baseline-scoped count must be unchanged.
    baseline_shortages_after = db.execute(
        "SELECT count(*) AS n FROM shortages WHERE scenario_id = %s", (BASELINE_UUID,)
    ).fetchone()["n"]
    assert baseline_shortages_after == baseline_shortages_before, (
        "simulate_param_run must not write BASELINE shortages (ADR-021 — the "
        "baseline shortage truth is owned exclusively by ShortageDetector; the "
        "fork's scoped rows are expected and archived)"
    )


def test_lot_policy_change_types_map_to_expected_decision_levels():
    """decision_level is coherent with each change_type the lot_policy watcher
    emits: all three parameter-adjustment drafts are L1 (never hardcoded)."""
    assert decision_level("RENEGOTIATE_MOQ") == "L1"
    assert decision_level("REVIEW_MULTIPLE") == "L1"
    assert decision_level("SET_LOT_RULE") == "L1"


# ===========================================================================
# Invariant 6 — kill switch: disabled overlay -> 503 on all three verbs
# ===========================================================================


def test_kill_switch_disables_all_three_verbs(api_client, db, monkeypatch):
    """OOTILS_PARAM_OVERLAY_ENABLED falsy -> 503 on POST, GET and DELETE, before
    any DB work (an operational escape hatch independent of auth/whitelist)."""
    item_id = _seed_item(db)
    location_id = _seed_location(db)
    _seed_planning_params(db, item_id, location_id)
    fork = _seed_fork(db, "pa-kill")
    db.commit()

    monkeypatch.setenv("OOTILS_PARAM_OVERLAY_ENABLED", "0")

    post_resp = api_client.post(
        f"/v1/scenarios/{fork}/param-overrides",
        headers=AUTH_HEADERS,
        json={"item_id": str(item_id), "location_id": str(location_id),
              "field_name": "safety_stock_qty", "value": "42",
              "applied_by": "agent:test"},
    )
    assert post_resp.status_code == 503

    get_resp = api_client.get(
        f"/v1/scenarios/{fork}/param-overrides", headers=AUTH_HEADERS,
    )
    assert get_resp.status_code == 503

    del_resp = api_client.request(
        "DELETE", f"/v1/scenarios/{fork}/param-overrides/safety_stock_qty",
        headers=AUTH_HEADERS,
        params={"item_id": str(item_id), "location_id": str(location_id)},
    )
    assert del_resp.status_code == 503
