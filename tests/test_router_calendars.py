"""
test_router_calendars.py — Unit tests for src/ootils_core/api/routers/calendars.py.

Covers:
  - POST /v1/ingest/calendars (insert + update + dry_run + 422 for unknown location + validators)
  - GET  /v1/calendars/{location_external_id} (basic + filters + 404)
  - POST /v1/calendars/working-days (success + 422 + non-working day skipping)
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock
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
# POST /v1/ingest/calendars
# ─────────────────────────────────────────────────────────────

def test_ingest_calendars_unknown_location_422():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None  # location lookup fails
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": "MISSING",
            "entries": [{"calendar_date": "2026-04-08", "is_working_day": False}],
        },
        headers=AUTH,
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert detail[0]["field"] == "location_external_id"


def test_ingest_calendars_dry_run_no_writes():
    db = _make_db_mock()
    loc_id = uuid4()
    cur = MagicMock()
    cur.fetchone.return_value = {"location_id": loc_id}
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": "WAREHOUSE-1",
            "entries": [
                {"calendar_date": "2026-04-08", "is_working_day": False},
                {"calendar_date": "2026-04-09", "is_working_day": True},
            ],
            "dry_run": True,
        },
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "dry_run"
    assert body["summary"]["total"] == 2
    assert body["summary"]["inserted"] == 0
    assert body["summary"]["updated"] == 0


def test_ingest_calendars_inserts_new_records():
    """All entries are new (existing returns None)."""
    db = _make_db_mock()
    loc_id = uuid4()

    # 1) location lookup
    # 2..) for each entry: existing-row check (None) + INSERT
    cursors = [
        MagicMock(fetchone=MagicMock(return_value={"location_id": loc_id})),
        MagicMock(fetchone=MagicMock(return_value=None)),  # existing #1 -> None
        MagicMock(),  # INSERT #1
        MagicMock(fetchone=MagicMock(return_value=None)),  # existing #2 -> None
        MagicMock(),  # INSERT #2
    ]
    db.execute.side_effect = cursors

    client = _make_client(db)
    resp = client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": "WAREHOUSE-1",
            "entries": [
                {"calendar_date": "2026-04-08", "is_working_day": False, "notes": "holiday"},
                {
                    "calendar_date": "2026-04-09",
                    "is_working_day": True,
                    "shift_count": 2,
                    "capacity_factor": 1.5,
                },
            ],
        },
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    assert body["summary"]["total"] == 2
    assert body["summary"]["inserted"] == 2
    assert body["summary"]["updated"] == 0


def test_ingest_calendars_updates_existing_records():
    """Existing record found → update branch."""
    db = _make_db_mock()
    loc_id = uuid4()

    cursors = [
        MagicMock(fetchone=MagicMock(return_value={"location_id": loc_id})),
        MagicMock(fetchone=MagicMock(return_value={"calendar_id": uuid4()})),  # existing
        MagicMock(),  # UPSERT
    ]
    db.execute.side_effect = cursors

    client = _make_client(db)
    resp = client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": "WAREHOUSE-1",
            "entries": [{"calendar_date": "2026-04-08", "is_working_day": False}],
        },
        headers=AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["summary"]["inserted"] == 0
    assert body["summary"]["updated"] == 1


def test_ingest_calendars_empty_external_id_422():
    """Empty location_external_id triggers field_validator."""
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/ingest/calendars",
        json={"location_external_id": "   ", "entries": []},
        headers=AUTH,
    )
    assert resp.status_code == 422


def test_ingest_calendars_invalid_capacity_factor_422():
    """capacity_factor > 2 should fail Pydantic validation."""
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": "WAREHOUSE-1",
            "entries": [
                {"calendar_date": "2026-04-08", "is_working_day": True, "capacity_factor": 5.0}
            ],
        },
        headers=AUTH,
    )
    assert resp.status_code == 422


def test_calendar_entry_validator_rejects_out_of_range_capacity_factor():
    """Direct call to the @field_validator covers the raise branch."""
    from ootils_core.api.routers.calendars import CalendarEntryInput

    with pytest.raises(ValueError, match=r"capacity_factor must be in \[0, 2\]"):
        CalendarEntryInput.validate_capacity_factor(5.0)


def test_calendar_entry_validator_passes_none():
    from ootils_core.api.routers.calendars import CalendarEntryInput
    assert CalendarEntryInput.validate_capacity_factor(None) is None


def test_calendar_entry_validator_passes_in_range():
    from ootils_core.api.routers.calendars import CalendarEntryInput
    assert CalendarEntryInput.validate_capacity_factor(1.0) == 1.0


def test_ingest_calendars_invalid_shift_count_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": "WAREHOUSE-1",
            "entries": [
                {"calendar_date": "2026-04-08", "is_working_day": True, "shift_count": 99}
            ],
        },
        headers=AUTH,
    )
    assert resp.status_code == 422


def test_ingest_calendars_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/ingest/calendars",
        json={"location_external_id": "WAREHOUSE-1", "entries": []},
    )
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# GET /v1/calendars/{location_external_id}
# ─────────────────────────────────────────────────────────────

def _calendar_row(d=None, working=True, factor=1.0):
    return {
        "calendar_date": d or date(2026, 4, 8),
        "is_working_day": working,
        "shift_count": 1,
        "capacity_factor": factor,
        "notes": None,
    }


def test_get_calendars_success_no_filters():
    db = _make_db_mock()
    loc_id = uuid4()

    cursors = [
        MagicMock(fetchone=MagicMock(return_value={"location_id": loc_id})),
        MagicMock(fetchall=MagicMock(return_value=[_calendar_row(), _calendar_row(working=False)])),
    ]
    db.execute.side_effect = cursors

    client = _make_client(db)
    resp = client.get("/v1/calendars/WAREHOUSE-1", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 2
    assert body["location_external_id"] == "WAREHOUSE-1"


def test_get_calendars_with_all_query_filters():
    db = _make_db_mock()
    loc_id = uuid4()
    cursors = [
        MagicMock(fetchone=MagicMock(return_value={"location_id": loc_id})),
        MagicMock(fetchall=MagicMock(return_value=[_calendar_row()])),
    ]
    db.execute.side_effect = cursors

    client = _make_client(db)
    resp = client.get(
        "/v1/calendars/WAREHOUSE-1?from_date=2026-01-01&to_date=2026-12-31&working_only=true",
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


def test_get_calendars_capacity_factor_none_branch():
    """Row where capacity_factor is None — exercises None coercion in mapper."""
    db = _make_db_mock()
    loc_id = uuid4()
    row = _calendar_row()
    row["capacity_factor"] = None
    cursors = [
        MagicMock(fetchone=MagicMock(return_value={"location_id": loc_id})),
        MagicMock(fetchall=MagicMock(return_value=[row])),
    ]
    db.execute.side_effect = cursors

    client = _make_client(db)
    resp = client.get("/v1/calendars/WAREHOUSE-1", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["entries"][0]["capacity_factor"] is None


def test_get_calendars_404_unknown_location():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get("/v1/calendars/MISSING", headers=AUTH)
    assert resp.status_code == 404


def test_get_calendars_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/calendars/WAREHOUSE-1")
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# POST /v1/calendars/working-days
# ─────────────────────────────────────────────────────────────

def test_working_days_success_no_skips():
    db = _make_db_mock()
    loc_id = uuid4()
    cursors = [
        MagicMock(fetchone=MagicMock(return_value={"location_id": loc_id})),
        MagicMock(fetchall=MagicMock(return_value=[])),
    ]
    db.execute.side_effect = cursors

    client = _make_client(db)
    resp = client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": "WAREHOUSE-1",
            "start_date": "2026-04-08",
            "add_working_days": 5,
        },
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["working_days_found"] == 5
    assert body["non_working_days_skipped"] == 0
    assert body["result_date"] == "2026-04-13"


def test_working_days_skips_non_working_days():
    """One non-working day in the window — should be skipped + counted."""
    db = _make_db_mock()
    loc_id = uuid4()
    nwd = date(2026, 4, 10)
    cursors = [
        MagicMock(fetchone=MagicMock(return_value={"location_id": loc_id})),
        MagicMock(fetchall=MagicMock(return_value=[{"calendar_date": nwd}])),
    ]
    db.execute.side_effect = cursors

    client = _make_client(db)
    resp = client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": "WAREHOUSE-1",
            "start_date": "2026-04-08",
            "add_working_days": 3,
        },
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["working_days_found"] == 3
    assert body["non_working_days_skipped"] == 1
    assert body["result_date"] == "2026-04-12"


def test_working_days_zero_add():
    """Adding 0 days should return start_date itself."""
    db = _make_db_mock()
    loc_id = uuid4()
    cursors = [
        MagicMock(fetchone=MagicMock(return_value={"location_id": loc_id})),
        MagicMock(fetchall=MagicMock(return_value=[])),
    ]
    db.execute.side_effect = cursors

    client = _make_client(db)
    resp = client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": "WAREHOUSE-1",
            "start_date": "2026-04-08",
            "add_working_days": 0,
        },
        headers=AUTH,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["working_days_found"] == 0
    assert body["result_date"] == "2026-04-08"


def test_working_days_unknown_location_422():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": "MISSING",
            "start_date": "2026-04-08",
            "add_working_days": 1,
        },
        headers=AUTH,
    )
    assert resp.status_code == 422


def test_working_days_negative_count_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": "WAREHOUSE-1",
            "start_date": "2026-04-08",
            "add_working_days": -1,
        },
        headers=AUTH,
    )
    assert resp.status_code == 422


def test_working_days_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": "WAREHOUSE-1",
            "start_date": "2026-04-08",
            "add_working_days": 1,
        },
    )
    assert resp.status_code == 401
