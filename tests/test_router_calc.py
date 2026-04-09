"""
test_router_calc.py — Unit tests for src/ootils_core/api/routers/calc.py.

Covers POST /v1/calc/run for both branches (full_recompute=True/False),
the locked-scenario fallback, and unhappy paths.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import get_db


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

AUTH = {"Authorization": "Bearer test-token"}


def _make_db_mock() -> MagicMock:
    conn = MagicMock(name="psycopg_conn")
    conn.execute.return_value = MagicMock()
    return conn


def _make_client(db_mock: MagicMock) -> TestClient:
    app = create_app()

    def override_db():
        yield db_mock

    app.dependency_overrides[get_db] = override_db
    return TestClient(app)


# ─────────────────────────────────────────────────────────────
# POST /v1/calc/run — incremental (full_recompute=False)
# ─────────────────────────────────────────────────────────────

def test_calc_run_incremental_success():
    db = _make_db_mock()
    db.execute.return_value = MagicMock()  # INSERT events row

    fake_calc_run = MagicMock()
    fake_calc_run.calc_run_id = uuid4()
    fake_calc_run.nodes_recalculated = 4
    fake_calc_run.nodes_unchanged = 1

    fake_engine = MagicMock()
    fake_engine.process_event.return_value = fake_calc_run

    with patch(
        "ootils_core.api.routers.events._build_propagation_engine",
        return_value=fake_engine,
    ):
        client = _make_client(db)
        resp = client.post("/v1/calc/run", json={"full_recompute": False}, headers=AUTH)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "completed"
    assert body["nodes_recalculated"] == 4
    assert body["nodes_unchanged"] == 1


def test_calc_run_incremental_locked_returns_locked_status():
    """engine.process_event returns None → locked."""
    db = _make_db_mock()
    db.execute.return_value = MagicMock()

    fake_engine = MagicMock()
    fake_engine.process_event.return_value = None

    with patch(
        "ootils_core.api.routers.events._build_propagation_engine",
        return_value=fake_engine,
    ):
        client = _make_client(db)
        resp = client.post("/v1/calc/run", json={"full_recompute": False}, headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "locked"
    assert body["calc_run_id"] is None


# ─────────────────────────────────────────────────────────────
# POST /v1/calc/run — full_recompute=True
# ─────────────────────────────────────────────────────────────

def test_calc_run_full_recompute_success():
    db = _make_db_mock()

    # 1) INSERT events trigger
    # 2) SELECT node_id FROM nodes WHERE ... ProjectedInventory
    pi_id_1 = uuid4()
    pi_id_2 = uuid4()
    cur_insert = MagicMock()
    cur_select_pi = MagicMock()
    cur_select_pi.fetchall.return_value = [
        {"node_id": pi_id_1},
        {"node_id": pi_id_2},
    ]
    db.execute.side_effect = [cur_insert, cur_select_pi]

    fake_calc_run = MagicMock()
    fake_calc_run.calc_run_id = uuid4()
    fake_calc_run.nodes_recalculated = 9
    fake_calc_run.nodes_unchanged = 0

    fake_engine = MagicMock()
    fake_calc_mgr = MagicMock()
    fake_calc_mgr.start_calc_run.return_value = fake_calc_run

    with patch(
        "ootils_core.api.routers.events._build_propagation_engine",
        return_value=fake_engine,
    ), patch(
        "ootils_core.engine.orchestration.calc_run.CalcRunManager",
        return_value=fake_calc_mgr,
    ), patch(
        "ootils_core.engine.kernel.graph.dirty.DirtyFlagManager"
    ) as mk_dirty:
        client = _make_client(db)
        resp = client.post("/v1/calc/run", json={"full_recompute": True}, headers=AUTH)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "completed"
    assert body["nodes_recalculated"] == 9
    fake_engine._propagate.assert_called_once()
    fake_engine._finish_run.assert_called_once()
    mk_dirty.assert_called()


def test_calc_run_full_recompute_no_pi_nodes():
    """No PI nodes → branch where dirty/propagate is skipped."""
    db = _make_db_mock()

    cur_insert = MagicMock()
    cur_select_pi = MagicMock()
    cur_select_pi.fetchall.return_value = []  # no PI nodes
    db.execute.side_effect = [cur_insert, cur_select_pi]

    fake_calc_run = MagicMock()
    fake_calc_run.calc_run_id = uuid4()
    fake_calc_run.nodes_recalculated = 0
    fake_calc_run.nodes_unchanged = 0

    fake_engine = MagicMock()
    fake_calc_mgr = MagicMock()
    fake_calc_mgr.start_calc_run.return_value = fake_calc_run

    with patch(
        "ootils_core.api.routers.events._build_propagation_engine",
        return_value=fake_engine,
    ), patch(
        "ootils_core.engine.orchestration.calc_run.CalcRunManager",
        return_value=fake_calc_mgr,
    ):
        client = _make_client(db)
        resp = client.post("/v1/calc/run", json={"full_recompute": True}, headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["nodes_recalculated"] == 0
    # _propagate should NOT have been called
    fake_engine._propagate.assert_not_called()
    fake_engine._finish_run.assert_called_once()


def test_calc_run_full_recompute_locked():
    """CalcRunManager.start_calc_run returns None → status='locked'."""
    db = _make_db_mock()
    db.execute.return_value = MagicMock()

    fake_engine = MagicMock()
    fake_calc_mgr = MagicMock()
    fake_calc_mgr.start_calc_run.return_value = None

    with patch(
        "ootils_core.api.routers.events._build_propagation_engine",
        return_value=fake_engine,
    ), patch(
        "ootils_core.engine.orchestration.calc_run.CalcRunManager",
        return_value=fake_calc_mgr,
    ):
        client = _make_client(db)
        resp = client.post("/v1/calc/run", json={"full_recompute": True}, headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "locked"
    assert body["calc_run_id"] is None


def test_calc_run_default_body_is_incremental():
    """Empty JSON body → defaults to full_recompute=False."""
    db = _make_db_mock()
    db.execute.return_value = MagicMock()

    fake_calc_run = MagicMock()
    fake_calc_run.calc_run_id = uuid4()
    fake_calc_run.nodes_recalculated = 0
    fake_calc_run.nodes_unchanged = 0

    fake_engine = MagicMock()
    fake_engine.process_event.return_value = fake_calc_run

    with patch(
        "ootils_core.api.routers.events._build_propagation_engine",
        return_value=fake_engine,
    ):
        client = _make_client(db)
        resp = client.post("/v1/calc/run", json={}, headers=AUTH)
    assert resp.status_code == 200


def test_calc_run_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post("/v1/calc/run", json={})
    assert resp.status_code == 401
