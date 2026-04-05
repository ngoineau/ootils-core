"""
test_m6_api.py — Sprint M6 REST API tests.

All DB calls are mocked via dependency overrides.
Tests cover: auth, events, projection, issues, explain, simulate, graph.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Generator
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

# Set env token before importing app
os.environ["OOTILS_API_TOKEN"] = "test-token"

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.models import (
    CausalStep,
    Edge,
    Explanation,
    Node,
    Scenario,
    ScenarioOverride,
    ShortageRecord,
    ProjectionSeries,
)

# ─────────────────────────── Fixtures ───────────────────────────

BASELINE_ID = UUID("00000000-0000-0000-0000-000000000001")
SCENARIO_ID = uuid4()
ITEM_ID = uuid4()
LOCATION_ID = uuid4()
SERIES_ID = uuid4()
NODE_ID = uuid4()
EXPLANATION_ID = uuid4()
CALC_RUN_ID = uuid4()
SHORTAGE_ID = uuid4()


def _mock_db() -> MagicMock:
    """Return a mock psycopg3 Connection."""
    conn = MagicMock()
    conn.execute.return_value = MagicMock(rowcount=1)
    return conn


@pytest.fixture
def app():
    application = create_app()
    return application


@pytest.fixture
def client(app) -> Generator:
    """TestClient with mocked DB."""
    mock_conn = _mock_db()

    def override_db():
        yield mock_conn

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers():
    return {"Authorization": "Bearer test-token"}


# ─────────────────────────── Auth Tests ───────────────────────────


def test_health_no_auth(client):
    """Health endpoint requires no auth."""
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["version"] == "1.0.0"


def test_events_401_without_token(client):
    """All protected endpoints return 401 without token."""
    resp = client.post("/v1/events", json={"event_type": "supply_date_changed"})
    assert resp.status_code == 401


def test_events_401_wrong_token(client):
    resp = client.post(
        "/v1/events",
        json={"event_type": "supply_date_changed"},
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 401


def test_projection_401_without_token(client):
    resp = client.get("/v1/projection?item_id=abc&location_id=xyz")
    assert resp.status_code == 401


def test_issues_401_without_token(client):
    resp = client.get("/v1/issues")
    assert resp.status_code == 401


# ─────────────────────────── POST /events ───────────────────────────


def test_post_event_success(app, auth_headers):
    mock_conn = _mock_db()
    mock_conn.execute.return_value = MagicMock(rowcount=1)

    def override_db():
        yield mock_conn

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as c:
        resp = c.post(
            "/v1/events",
            json={
                "event_type": "supply_date_changed",
                "trigger_node_id": str(NODE_ID),
                "source": "erp-sync",
                "field_changed": "due_date",
                "new_date": "2026-04-18",
            },
            headers=auth_headers,
        )

    app.dependency_overrides.clear()
    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["status"] == "queued"
    assert "event_id" in data
    assert "scenario_id" in data


def test_post_event_invalid_type(app, auth_headers):
    mock_conn = _mock_db()

    def override_db():
        yield mock_conn

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as c:
        resp = c.post(
            "/v1/events",
            json={"event_type": "invalid_type_xyz"},
            headers=auth_headers,
        )

    app.dependency_overrides.clear()
    assert resp.status_code == 422


def test_post_event_inserts_to_db(app, auth_headers):
    """Verify that execute() is called (DB insert happens)."""
    mock_conn = _mock_db()

    def override_db():
        yield mock_conn

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as c:
        c.post(
            "/v1/events",
            json={"event_type": "onhand_updated", "source": "manual"},
            headers=auth_headers,
        )

    app.dependency_overrides.clear()
    assert mock_conn.execute.called


# ─────────────────────────── GET /projection ───────────────────────────


def _make_pi_node(seq: int) -> Node:
    return Node(
        node_id=uuid4(),
        node_type="ProjectedInventory",
        scenario_id=BASELINE_ID,
        item_id=ITEM_ID,
        location_id=LOCATION_ID,
        projection_series_id=SERIES_ID,
        bucket_sequence=seq,
        time_span_start=date(2026, 4, seq),
        time_span_end=date(2026, 4, seq + 1),
        time_grain="day",
        opening_stock=Decimal("100"),
        inflows=Decimal("0"),
        outflows=Decimal("20"),
        closing_stock=Decimal("80"),
        has_shortage=False,
        shortage_qty=Decimal("0"),
    )


def test_get_projection_success(app, auth_headers):
    mock_conn = _mock_db()
    series = ProjectionSeries(
        series_id=SERIES_ID,
        item_id=ITEM_ID,
        location_id=LOCATION_ID,
        scenario_id=BASELINE_ID,
        horizon_start=date(2026, 4, 1),
        horizon_end=date(2026, 4, 30),
    )
    nodes = [_make_pi_node(i) for i in range(1, 4)]

    # Simulate UUID parse succeeding (item_id is a UUID)
    from ootils_core.engine.kernel.graph.store import GraphStore

    with patch.object(GraphStore, "get_projection_series", return_value=series), \
         patch.object(GraphStore, "get_nodes_by_series", return_value=nodes):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.get(
                f"/v1/projection?item_id={ITEM_ID}&location_id={LOCATION_ID}",
                headers=auth_headers,
            )

    app.dependency_overrides.clear()
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["series_id"] == str(SERIES_ID)
    assert len(data["buckets"]) == 3
    assert data["buckets"][0]["bucket_sequence"] == 1


def test_get_projection_not_found(app, auth_headers):
    mock_conn = _mock_db()

    from ootils_core.engine.kernel.graph.store import GraphStore

    with patch.object(GraphStore, "get_projection_series", return_value=None):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.get(
                f"/v1/projection?item_id={ITEM_ID}&location_id={LOCATION_ID}",
                headers=auth_headers,
            )

    app.dependency_overrides.clear()
    assert resp.status_code == 404


# ─────────────────────────── GET /issues ───────────────────────────


def _make_shortage(score: Decimal) -> ShortageRecord:
    return ShortageRecord(
        shortage_id=uuid4(),
        scenario_id=BASELINE_ID,
        pi_node_id=uuid4(),
        item_id=ITEM_ID,
        location_id=LOCATION_ID,
        shortage_date=date(2026, 4, 8),
        shortage_qty=Decimal("130"),
        severity_score=score,
        explanation_id=None,
        calc_run_id=CALC_RUN_ID,
        status="active",
    )


def test_get_issues_all(app, auth_headers):
    mock_conn = _mock_db()
    shortages = [
        _make_shortage(Decimal("50")),   # low
        _make_shortage(Decimal("500")),  # medium
        _make_shortage(Decimal("2000")), # high
    ]

    from ootils_core.engine.kernel.shortage.detector import ShortageDetector

    with patch.object(ShortageDetector, "get_active_shortages", return_value=shortages):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.get("/v1/issues?severity=all&horizon_days=90", headers=auth_headers)

    app.dependency_overrides.clear()
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3


def test_get_issues_filter_high(app, auth_headers):
    mock_conn = _mock_db()
    shortages = [
        _make_shortage(Decimal("50")),
        _make_shortage(Decimal("2000")),
    ]

    from ootils_core.engine.kernel.shortage.detector import ShortageDetector

    with patch.object(ShortageDetector, "get_active_shortages", return_value=shortages):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.get("/v1/issues?severity=high&horizon_days=90", headers=auth_headers)

    app.dependency_overrides.clear()
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["issues"][0]["severity"] == "high"


def test_get_issues_horizon_filter(app, auth_headers):
    """Shortages beyond horizon should be excluded."""
    mock_conn = _mock_db()
    # shortage date far in the future
    far_shortage = ShortageRecord(
        shortage_id=uuid4(),
        scenario_id=BASELINE_ID,
        pi_node_id=uuid4(),
        item_id=ITEM_ID,
        location_id=LOCATION_ID,
        shortage_date=date(2030, 1, 1),  # far future
        shortage_qty=Decimal("130"),
        severity_score=Decimal("2000"),
        explanation_id=None,
        calc_run_id=CALC_RUN_ID,
        status="active",
    )
    near_shortage = _make_shortage(Decimal("2000"))

    from ootils_core.engine.kernel.shortage.detector import ShortageDetector

    with patch.object(
        ShortageDetector, "get_active_shortages", return_value=[far_shortage, near_shortage]
    ):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.get("/v1/issues?severity=all&horizon_days=90", headers=auth_headers)

    app.dependency_overrides.clear()
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1  # only the near one


# ─────────────────────────── GET /explain ───────────────────────────


def _make_explanation() -> Explanation:
    return Explanation(
        explanation_id=EXPLANATION_ID,
        calc_run_id=CALC_RUN_ID,
        target_node_id=NODE_ID,
        target_type="Shortage",
        root_cause_node_id=uuid4(),
        causal_path=[
            CausalStep(
                step=1,
                node_id=uuid4(),
                node_type="CustomerOrderDemand",
                edge_type="consumes",
                fact="Order CO-778 requires 150u due April 8",
            ),
            CausalStep(
                step=2,
                node_id=uuid4(),
                node_type="PurchaseOrderSupply",
                edge_type="replenishes",
                fact="PO-991 provides 200u due April 18",
            ),
        ],
        summary="Shortage: demand exceeds supply by 130 units.",
    )


def test_get_explain_success(app, auth_headers):
    mock_conn = _mock_db()
    explanation = _make_explanation()

    from ootils_core.engine.kernel.explanation.builder import ExplanationBuilder

    with patch.object(ExplanationBuilder, "get_explanation", return_value=explanation):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.get(f"/v1/explain?node_id={NODE_ID}", headers=auth_headers)

    app.dependency_overrides.clear()
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["explanation_id"] == str(EXPLANATION_ID)
    assert data["summary"] == explanation.summary
    assert len(data["causal_path"]) == 2
    assert data["causal_path"][0]["step"] == 1
    assert data["causal_path"][0]["node_type"] == "CustomerOrderDemand"


def test_get_explain_not_found(app, auth_headers):
    mock_conn = _mock_db()

    from ootils_core.engine.kernel.explanation.builder import ExplanationBuilder

    with patch.object(ExplanationBuilder, "get_explanation", return_value=None):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.get(f"/v1/explain?node_id={NODE_ID}", headers=auth_headers)

    app.dependency_overrides.clear()
    assert resp.status_code == 404


def test_get_explain_invalid_uuid(app, auth_headers):
    mock_conn = _mock_db()

    def override_db():
        yield mock_conn

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as c:
        resp = c.get("/v1/explain?node_id=not-a-uuid", headers=auth_headers)

    app.dependency_overrides.clear()
    assert resp.status_code == 422


# ─────────────────────────── POST /simulate ───────────────────────────


def test_post_simulate_success(app, auth_headers):
    mock_conn = _mock_db()
    new_scenario = Scenario(
        scenario_id=uuid4(),
        name="sim-test",
        parent_scenario_id=BASELINE_ID,
        is_baseline=False,
        status="active",
    )
    override_result = ScenarioOverride(
        override_id=uuid4(),
        scenario_id=new_scenario.scenario_id,
        node_id=NODE_ID,
        field_name="quantity",
        old_value="100",
        new_value="200",
    )

    from ootils_core.engine.scenario.manager import ScenarioManager

    with patch.object(ScenarioManager, "create_scenario", return_value=new_scenario), \
         patch.object(ScenarioManager, "apply_override", return_value=override_result):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.post(
                "/v1/simulate",
                json={
                    "scenario_name": "sim-test",
                    "base_scenario_id": "baseline",
                    "overrides": [
                        {"node_id": str(NODE_ID), "field_name": "quantity", "new_value": "200"}
                    ],
                },
                headers=auth_headers,
            )

    app.dependency_overrides.clear()
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["status"] == "created"
    assert data["override_count"] == 1
    assert data["scenario_id"] == str(new_scenario.scenario_id)


def test_post_simulate_no_overrides(app, auth_headers):
    mock_conn = _mock_db()
    new_scenario = Scenario(
        scenario_id=uuid4(),
        name="empty-sim",
        parent_scenario_id=BASELINE_ID,
        is_baseline=False,
        status="active",
    )

    from ootils_core.engine.scenario.manager import ScenarioManager

    with patch.object(ScenarioManager, "create_scenario", return_value=new_scenario):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.post(
                "/v1/simulate",
                json={"scenario_name": "empty-sim"},
                headers=auth_headers,
            )

    app.dependency_overrides.clear()
    assert resp.status_code == 201
    data = resp.json()
    assert data["override_count"] == 0


# ─────────────────────────── GET /graph ───────────────────────────


def test_get_graph_success(app, auth_headers):
    mock_conn = _mock_db()

    nodes_result = [
        {
            "node_id": str(NODE_ID),
            "node_type": "ProjectedInventory",
            "scenario_id": str(BASELINE_ID),
            "item_id": str(ITEM_ID),
            "location_id": str(LOCATION_ID),
            "quantity": None,
            "qty_uom": None,
            "time_grain": "day",
            "time_ref": None,
            "time_span_start": "2026-04-01",
            "time_span_end": "2026-04-02",
            "is_dirty": False,
            "last_calc_run_id": None,
            "active": True,
            "projection_series_id": str(SERIES_ID),
            "bucket_sequence": 1,
            "opening_stock": "100",
            "inflows": "0",
            "outflows": "20",
            "closing_stock": "80",
            "has_shortage": False,
            "shortage_qty": "0",
            "has_exact_date_inputs": False,
            "has_week_inputs": False,
            "has_month_inputs": False,
            "created_at": None,
            "updated_at": None,
        }
    ]
    mock_conn.execute.return_value.fetchall.return_value = nodes_result

    from ootils_core.engine.kernel.graph.store import GraphStore

    with patch.object(GraphStore, "get_all_edges", return_value=[]):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.get(
                f"/v1/graph?item_id={ITEM_ID}&location_id={LOCATION_ID}",
                headers=auth_headers,
            )

    app.dependency_overrides.clear()
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "nodes" in data
    assert "edges" in data
    assert data["depth"] == 2  # default


# ─────────────────────────── OpenAPI schema ───────────────────────────


def test_openapi_schema_accessible(client, auth_headers):
    """OpenAPI schema should be reachable without auth."""
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    assert schema["info"]["title"] == "Ootils Core API"
    assert schema["info"]["version"] == "1.0.0"


def test_post_simulate_invalid_node_returns_422(app, auth_headers):
    """
    Non-regression #48: POST /v1/simulate with an invalid node_id must return 422,
    not 500. All overrides fail → 422 with failed_overrides detail.
    """
    mock_conn = _mock_db()
    new_scenario = Scenario(
        scenario_id=uuid4(),
        name="bad-node-sim",
        parent_scenario_id=BASELINE_ID,
        is_baseline=False,
        status="active",
    )

    from ootils_core.engine.scenario.manager import ScenarioManager

    with patch.object(ScenarioManager, "create_scenario", return_value=new_scenario), \
         patch.object(
             ScenarioManager,
             "apply_override",
             side_effect=ValueError("Node 00000000-0000-0000-0000-000000000000 not found in scenario"),
         ):

        def override_db():
            yield mock_conn

        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            resp = c.post(
                "/v1/simulate",
                json={
                    "scenario_name": "bad-node-sim",
                    "overrides": [
                        {
                            "node_id": "00000000-0000-0000-0000-000000000000",
                            "field_name": "shortage_qty",
                            "new_value": "0",
                        }
                    ],
                },
                headers=auth_headers,
            )

    app.dependency_overrides.clear()
    assert resp.status_code == 422, f"Expected 422, got {resp.status_code}: {resp.text}"
    data = resp.json()
    detail = data.get("detail", {})
    assert "failed_overrides" in detail, f"Expected failed_overrides in detail: {detail}"
    assert len(detail["failed_overrides"]) == 1
    assert "not found" in detail["failed_overrides"][0]["error"].lower()
