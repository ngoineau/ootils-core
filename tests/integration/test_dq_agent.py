"""
tests/integration/test_dq_agent.py — Integration tests for DQ Agent V1.

Tests:
  1.  STAT_LEAD_TIME_SPIKE détecté
  2.  STAT_NEGATIVE_ONHAND détecté
  3.  TEMP_PO_DATE_PAST détecté
  4.  TEMP_DUPLICATE_BATCH détecté
  5.  impact_scorer enrichit les issues
  6.  POST /v1/dq/agent/run/{batch_id} fonctionne
  7.  GET /v1/dq/agent/report/{batch_id} retourne le rapport
  8.  GET /v1/dq/agent/runs retourne l'historique
  9.  Fallback LLM fonctionne (mock API indisponible)
  10. dq_agent_runs créé en DB

Requires PostgreSQL. Set DATABASE_URL before running.
"""
from __future__ import annotations

import json
import os
from datetime import date, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def agent_client(migrated_db):
    """Module-scoped TestClient with migrated DB for agent tests."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "test-token"

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
def auth():
    return {"Authorization": "Bearer test-token"}


@pytest.fixture(scope="module")
def agent_db_conn(migrated_db):
    """Direct psycopg connection for DB assertions."""
    conn = psycopg.connect(migrated_db, row_factory=dict_row)
    yield conn
    conn.close()


PREFIX = "agt-" + str(uuid4())[:8]


def uid(base: str) -> str:
    return f"{PREFIX}-{base}"


def _create_batch(db, entity_type: str, rows: list[dict], status: str = "processing") -> str:
    """Insert an ingest_batch + ingest_rows, return batch_id."""
    batch_id = str(uuid4())
    db.execute(
        """
        INSERT INTO ingest_batches
            (batch_id, entity_type, source_system, status, total_rows, submitted_by)
        VALUES (%s, %s, 'test', %s, %s, 'pytest')
        """,
        (batch_id, entity_type, status, len(rows)),
    )
    for i, row in enumerate(rows):
        db.execute(
            """
            INSERT INTO ingest_rows (row_id, batch_id, row_number, raw_content)
            VALUES (%s, %s, %s, %s)
            """,
            (str(uuid4()), batch_id, i + 1, json.dumps(row)),
        )
    db.commit()
    return batch_id


def _mark_batch_validated(db, batch_id: str) -> None:
    db.execute(
        "UPDATE ingest_batches SET dq_status = 'validated' WHERE batch_id = %s",
        (batch_id,),
    )
    db.commit()


def _insert_item(db, external_id: str) -> str:
    item_id = str(uuid4())
    db.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        VALUES (%s, %s, 'Test Item', 'component', 'EA', 'active')
        ON CONFLICT (external_id) DO NOTHING
        """,
        (item_id, external_id),
    )
    db.commit()
    return item_id


def _insert_supplier(db, external_id: str) -> str:
    sup_id = str(uuid4())
    db.execute(
        """
        INSERT INTO suppliers (supplier_id, external_id, name, status)
        VALUES (%s, %s, 'Test Supplier', 'active')
        ON CONFLICT (external_id) DO NOTHING
        """,
        (sup_id, external_id),
    )
    db.commit()
    return sup_id


# ─────────────────────────────────────────────────────────────
# Test 1: STAT_LEAD_TIME_SPIKE detected
# ─────────────────────────────────────────────────────────────

@requires_db
def test_stat_lead_time_spike_detected(agent_db_conn, migrated_db):
    """STAT_LEAD_TIME_SPIKE : lead_time_days 8σ above historical avg → issue generated."""
    from ootils_core.engine.dq.agent.stat_rules import run_stat_rules
    import psycopg as _psycopg

    item_ext = uid("lt-spike-item")
    sup_ext = uid("lt-spike-sup")

    # Create historical batches with lead_time=14 days (validated)
    for i in range(4):
        b_id = _create_batch(agent_db_conn, "supplier_items", [
            {"item_external_id": item_ext, "supplier_external_id": sup_ext, "lead_time_days": 14},
        ])
        _mark_batch_validated(agent_db_conn, b_id)

    # Create current batch with lead_time=200 (spike)
    batch_id = _create_batch(agent_db_conn, "supplier_items", [
        {"item_external_id": item_ext, "supplier_external_id": sup_ext, "lead_time_days": 200},
    ])

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        issues = run_stat_rules(conn, batch_id)

    spike_issues = [i for i in issues if i.rule_code == "STAT_LEAD_TIME_SPIKE"]
    assert len(spike_issues) >= 1
    assert spike_issues[0].severity == "error"
    assert spike_issues[0].field_name == "lead_time_days"


# ─────────────────────────────────────────────────────────────
# Test 2: STAT_NEGATIVE_ONHAND detected
# ─────────────────────────────────────────────────────────────

@requires_db
def test_stat_negative_onhand_detected(agent_db_conn, migrated_db):
    """STAT_NEGATIVE_ONHAND : quantity < 0 in on_hand batch → issue generated."""
    from ootils_core.engine.dq.agent.stat_rules import run_stat_rules
    import psycopg as _psycopg

    item_ext = uid("neg-oh-item")
    loc_ext = uid("neg-oh-loc")

    # Note: L1 rejects negative qty, but STAT_NEGATIVE_ONHAND checks the raw content
    batch_id = _create_batch(agent_db_conn, "on_hand", [
        {
            "item_external_id": item_ext,
            "location_external_id": loc_ext,
            "quantity": -50,
            "uom": "EA",
            "as_of_date": "2026-04-08",
        }
    ])

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        issues = run_stat_rules(conn, batch_id)

    neg_issues = [i for i in issues if i.rule_code == "STAT_NEGATIVE_ONHAND"]
    assert len(neg_issues) >= 1
    assert neg_issues[0].severity == "error"


# ─────────────────────────────────────────────────────────────
# Test 3: TEMP_PO_DATE_PAST detected
# ─────────────────────────────────────────────────────────────

@requires_db
def test_temp_po_date_past_detected(agent_db_conn, migrated_db):
    """TEMP_PO_DATE_PAST : PO expected_date in the past + status != received → issue."""
    from ootils_core.engine.dq.agent.temporal_rules import run_temporal_rules
    import psycopg as _psycopg

    past_date = (date.today() - timedelta(days=30)).isoformat()

    batch_id = _create_batch(agent_db_conn, "purchase_orders", [
        {
            "external_id": uid("po-past"),
            "item_external_id": uid("po-item"),
            "location_external_id": uid("po-loc"),
            "supplier_external_id": uid("po-sup"),
            "quantity": 100,
            "uom": "EA",
            "expected_delivery_date": past_date,
            "status": "confirmed",  # not 'received'
        }
    ])

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        issues = run_temporal_rules(conn, batch_id)

    past_issues = [i for i in issues if i.rule_code == "TEMP_PO_DATE_PAST"]
    assert len(past_issues) >= 1
    assert past_issues[0].severity == "warning"
    assert past_issues[0].field_name == "expected_delivery_date"


# ─────────────────────────────────────────────────────────────
# Test 4: TEMP_DUPLICATE_BATCH detected
# ─────────────────────────────────────────────────────────────

@requires_db
def test_temp_duplicate_batch_detected(agent_db_conn, migrated_db):
    """TEMP_DUPLICATE_BATCH : >95% identical rows to previous batch → issue."""
    from ootils_core.engine.dq.agent.temporal_rules import run_temporal_rules
    import psycopg as _psycopg

    item_ext = uid("dup-item")
    loc_ext = uid("dup-loc")

    # Row data — same in both batches
    row_data = [
        {
            "item_external_id": item_ext,
            "location_external_id": loc_ext,
            "quantity": 100,
            "uom": "EA",
            "as_of_date": "2026-04-08",
        }
        for _ in range(5)
    ]

    # Create first batch (validated = previous)
    prev_batch_id = _create_batch(agent_db_conn, "on_hand", row_data)
    _mark_batch_validated(agent_db_conn, prev_batch_id)

    # Create second batch (identical)
    current_batch_id = _create_batch(agent_db_conn, "on_hand", row_data)

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        issues = run_temporal_rules(conn, current_batch_id)

    dup_issues = [i for i in issues if i.rule_code == "TEMP_DUPLICATE_BATCH"]
    assert len(dup_issues) >= 1
    assert dup_issues[0].severity == "warning"


# ─────────────────────────────────────────────────────────────
# Test 5: impact_scorer enrichit les issues
# ─────────────────────────────────────────────────────────────

@requires_db
def test_impact_scorer_enriches_issues(agent_db_conn, migrated_db):
    """impact_scorer assigns impact_score to all issues."""
    from ootils_core.engine.dq.agent.stat_rules import AgentIssue
    from ootils_core.engine.dq.agent.impact_scorer import score_issues
    import psycopg as _psycopg

    batch_id_uuid = uuid4()
    batch_id_str = str(batch_id_uuid)

    # Create a dummy batch in DB
    agent_db_conn.execute(
        """
        INSERT INTO ingest_batches
            (batch_id, entity_type, source_system, status, total_rows, submitted_by)
        VALUES (%s, 'on_hand', 'test', 'processing', 1, 'pytest')
        """,
        (batch_id_str,),
    )
    row_id = uuid4()
    agent_db_conn.execute(
        """
        INSERT INTO ingest_rows (row_id, batch_id, row_number, raw_content)
        VALUES (%s, %s, 1, %s)
        """,
        (str(row_id), batch_id_str, json.dumps({"item_external_id": uid("scorer-item"), "quantity": -10})),
    )
    agent_db_conn.commit()

    issue = AgentIssue(
        issue_id=uuid4(),
        batch_id=batch_id_uuid,
        row_id=row_id,
        row_number=1,
        dq_level=3,
        rule_code="STAT_NEGATIVE_ONHAND",
        severity="error",
        field_name="quantity",
        raw_value="-10",
        message="test issue",
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        scored = score_issues(conn, batch_id_uuid, [issue])

    assert len(scored) == 1
    # impact_score must be set (>= severity_weight × 1.0)
    assert scored[0].impact_score is not None
    assert scored[0].impact_score >= 1.5  # error weight is 3.0 × log(1+0)=3.0×1=3.0


# ─────────────────────────────────────────────────────────────
# Test 6: POST /v1/dq/agent/run/{batch_id} fonctionne
# ─────────────────────────────────────────────────────────────

@requires_db
def test_post_agent_run_endpoint(agent_client, auth, agent_db_conn):
    """POST /v1/dq/agent/run/{batch_id} triggers agent and returns run_id."""
    batch_id = _create_batch(agent_db_conn, "on_hand", [
        {
            "item_external_id": uid("api-run-item"),
            "location_external_id": uid("api-run-loc"),
            "quantity": 100,
            "uom": "EA",
            "as_of_date": "2026-04-08",
        }
    ])

    resp = agent_client.post(f"/v1/dq/agent/run/{batch_id}", headers=auth)
    assert resp.status_code == 200
    data = resp.json()
    assert data["batch_id"] == batch_id
    assert data["status"] == "completed"
    assert "agent_run_id" in data
    assert data["agent_run_id"] is not None


# ─────────────────────────────────────────────────────────────
# Test 7: GET /v1/dq/agent/report/{batch_id} retourne le rapport
# ─────────────────────────────────────────────────────────────

@requires_db
def test_get_agent_report_endpoint(agent_client, auth, agent_db_conn):
    """GET /v1/dq/agent/report/{batch_id} returns full agent report."""
    batch_id = _create_batch(agent_db_conn, "on_hand", [
        {
            "item_external_id": uid("report-item"),
            "location_external_id": uid("report-loc"),
            "quantity": -5,  # will trigger STAT_NEGATIVE_ONHAND
            "uom": "EA",
            "as_of_date": "2026-04-08",
        }
    ])

    # Run agent first
    agent_client.post(f"/v1/dq/agent/run/{batch_id}", headers=auth)

    resp = agent_client.get(f"/v1/dq/agent/report/{batch_id}", headers=auth)
    assert resp.status_code == 200
    data = resp.json()
    assert data["batch_id"] == batch_id
    assert data["entity_type"] == "on_hand"
    assert "issues" in data
    assert "summary" in data
    assert "priority_actions" in data
    assert isinstance(data["issues"], list)


# ─────────────────────────────────────────────────────────────
# Test 8: GET /v1/dq/agent/runs retourne l'historique
# ─────────────────────────────────────────────────────────────

@requires_db
def test_get_agent_runs_history(agent_client, auth, agent_db_conn):
    """GET /v1/dq/agent/runs returns list of agent run records."""
    # Ensure at least one run exists
    batch_id = _create_batch(agent_db_conn, "on_hand", [
        {
            "item_external_id": uid("runs-hist-item"),
            "location_external_id": uid("runs-hist-loc"),
            "quantity": 50,
            "uom": "EA",
            "as_of_date": "2026-04-08",
        }
    ])
    agent_client.post(f"/v1/dq/agent/run/{batch_id}", headers=auth)

    resp = agent_client.get("/v1/dq/agent/runs", headers=auth)
    assert resp.status_code == 200
    data = resp.json()
    assert "runs" in data
    assert "total" in data
    assert data["total"] >= 1
    assert len(data["runs"]) >= 1
    run = data["runs"][0]
    assert "run_id" in run
    assert "batch_id" in run
    assert "status" in run


# ─────────────────────────────────────────────────────────────
# Test 9: Fallback LLM fonctionne (mock API indisponible)
# ─────────────────────────────────────────────────────────────

@requires_db
def test_llm_fallback_when_api_unavailable(agent_db_conn, migrated_db):
    """LLM fallback generates a structured report when OPENAI_API_KEY is not set."""
    from ootils_core.engine.dq.agent.llm_reporter import generate_llm_report
    from ootils_core.engine.dq.agent.stat_rules import AgentIssue
    import psycopg as _psycopg

    # Temporarily remove API key
    original_key = os.environ.pop("OPENAI_API_KEY", None)

    try:
        issue = AgentIssue(
            issue_id=uuid4(),
            batch_id=uuid4(),
            row_id=None,
            row_number=None,
            dq_level=3,
            rule_code="STAT_NEGATIVE_ONHAND",
            severity="error",
            field_name="quantity",
            raw_value="-10",
            message="on_hand_qty=-10",
            impact_score=3.0,
        )

        report = generate_llm_report(
            issues=[issue],
            entity_type="on_hand",
            batch_id=uuid4(),
            total_rows=1,
        )

        assert report.llm_available is False
        assert report.narrative is not None
        assert len(report.narrative) > 0
        assert report.model_used is None
    finally:
        if original_key:
            os.environ["OPENAI_API_KEY"] = original_key


# ─────────────────────────────────────────────────────────────
# Test 10: dq_agent_runs created in DB
# ─────────────────────────────────────────────────────────────

@requires_db
def test_dq_agent_run_created_in_db(agent_db_conn, migrated_db):
    """run_dq_agent creates a row in dq_agent_runs with status=completed."""
    from ootils_core.engine.dq.agent import run_dq_agent
    import psycopg as _psycopg

    batch_id = _create_batch(agent_db_conn, "on_hand", [
        {
            "item_external_id": uid("db-run-item"),
            "location_external_id": uid("db-run-loc"),
            "quantity": 200,
            "uom": "EA",
            "as_of_date": "2026-04-08",
        }
    ])

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq_agent(conn, batch_id)
        conn.commit()

    # Verify in DB
    row = agent_db_conn.execute(
        "SELECT run_id, status, completed_at FROM dq_agent_runs WHERE run_id = %s",
        (str(result.run_id),),
    ).fetchone()

    assert row is not None
    assert row["status"] == "completed"
    assert row["completed_at"] is not None
