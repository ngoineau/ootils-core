"""
tests/integration/test_calendars.py — Integration tests for operational calendar endpoints.

Endpoints covered:
  POST /v1/ingest/calendars
  GET  /v1/calendars/{location_external_id}
  POST /v1/calendars/working-days

Requires a running PostgreSQL instance with migrations applied.
Set DATABASE_URL before running:
    DATABASE_URL=postgresql://ootils:ootils@localhost:5432/ootils_test pytest tests/integration/test_calendars.py -v
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import uuid4

import pytest

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL

# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def cal_client(migrated_db):
    """Module-scoped TestClient wired to a migrated test DB."""
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


# Unique prefix to avoid cross-test contamination
PREFIX = str(uuid4())[:8]


def uid(base: str) -> str:
    return f"{PREFIX}-{base}"


@pytest.fixture(scope="module")
def test_location(cal_client, auth):
    """Create a location to use across calendar tests."""
    ext_id = uid("LOC-CAL")
    resp = cal_client.post(
        "/v1/ingest/locations",
        json={
            "locations": [{"external_id": ext_id, "name": "Calendar Test DC", "location_type": "dc"}],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    return ext_id


# ─────────────────────────────────────────────────────────────
# POST /v1/ingest/calendars — basic insert
# ─────────────────────────────────────────────────────────────

@requires_db
def test_ingest_calendars_basic(cal_client, auth, test_location):
    """Import 3 non-working days → verify summary and response."""
    resp = cal_client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": test_location,
            "entries": [
                {"calendar_date": "2026-12-25", "is_working_day": False, "notes": "Christmas"},
                {"calendar_date": "2026-01-01", "is_working_day": False, "notes": "New Year"},
                {"calendar_date": "2026-07-04", "is_working_day": False, "capacity_factor": 0.0, "notes": "Independence Day"},
            ],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    assert body["location_external_id"] == test_location
    assert body["location_id"] is not None
    summary = body["summary"]
    assert summary["total"] == 3
    assert summary["inserted"] == 3
    assert summary["updated"] == 0
    assert summary["errors"] == 0


@requires_db
def test_ingest_calendars_upsert(cal_client, auth, test_location):
    """Re-inserting same dates → summary shows updated, not inserted again."""
    resp = cal_client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": test_location,
            "entries": [
                {"calendar_date": "2026-12-25", "is_working_day": False, "notes": "Christmas (updated)"},
                {"calendar_date": "2026-11-26", "is_working_day": False, "notes": "Thanksgiving"},
            ],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    summary = body["summary"]
    assert summary["total"] == 2
    # 2026-12-25 exists (updated) + 2026-11-26 is new (inserted)
    assert summary["updated"] == 1
    assert summary["inserted"] == 1


# ─────────────────────────────────────────────────────────────
# POST /v1/ingest/calendars — dry_run
# ─────────────────────────────────────────────────────────────

@requires_db
def test_ingest_calendars_dry_run(cal_client, auth, test_location):
    """dry_run=True → status=dry_run, nothing written to DB."""
    new_date = "2027-06-15"
    resp = cal_client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": test_location,
            "entries": [
                {"calendar_date": new_date, "is_working_day": False, "notes": "dry run only"},
            ],
            "dry_run": True,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "dry_run"
    assert body["summary"]["inserted"] == 0
    assert body["summary"]["updated"] == 0

    # Confirm nothing was written
    get_resp = cal_client.get(
        f"/v1/calendars/{test_location}",
        params={"from_date": new_date, "to_date": new_date},
        headers=auth,
    )
    assert get_resp.status_code == 200
    assert get_resp.json()["total"] == 0


# ─────────────────────────────────────────────────────────────
# POST /v1/ingest/calendars — unknown location → 422
# ─────────────────────────────────────────────────────────────

@requires_db
def test_ingest_calendars_unknown_location(cal_client, auth):
    """Unknown location_external_id → HTTP 422."""
    resp = cal_client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": uid("NO-SUCH-LOC"),
            "entries": [
                {"calendar_date": "2026-12-25", "is_working_day": False},
            ],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp.status_code == 422, resp.text


# ─────────────────────────────────────────────────────────────
# GET /v1/calendars/{location_external_id}
# ─────────────────────────────────────────────────────────────

@requires_db
def test_get_calendars(cal_client, auth, test_location):
    """GET returns calendar entries for the location."""
    resp = cal_client.get(
        f"/v1/calendars/{test_location}",
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["location_external_id"] == test_location
    assert body["total"] >= 3  # at least the entries from test_ingest_calendars_basic
    assert isinstance(body["entries"], list)
    # Entries should be sorted by date
    dates = [e["calendar_date"] for e in body["entries"]]
    assert dates == sorted(dates)


@requires_db
def test_get_calendars_date_filter(cal_client, auth, test_location):
    """from_date/to_date filtering works correctly."""
    resp = cal_client.get(
        f"/v1/calendars/{test_location}",
        params={"from_date": "2026-12-01", "to_date": "2026-12-31"},
        headers=auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    for entry in body["entries"]:
        assert "2026-12" in entry["calendar_date"]


@requires_db
def test_get_calendars_unknown_location(cal_client, auth):
    """Unknown location → 404."""
    resp = cal_client.get(
        f"/v1/calendars/{uid('NO-SUCH-LOC')}",
        headers=auth,
    )
    assert resp.status_code == 404


# ─────────────────────────────────────────────────────────────
# POST /v1/calendars/working-days
# ─────────────────────────────────────────────────────────────

@requires_db
def test_working_days_calculation(cal_client, auth):
    """
    start=Friday 2026-04-03, add=3 working days.
    Insert Sat+Sun as non-working → result should be Wed 2026-04-08.
    (Fri→Sat skip, Sun skip, Mon=1, Tue=2, Wed=3)
    """
    loc_ext_id = uid("LOC-WD-TEST")
    # Create location
    cal_client.post(
        "/v1/ingest/locations",
        json={
            "locations": [{"external_id": loc_ext_id, "name": "WD Test DC", "location_type": "dc"}],
            "dry_run": False,
        },
        headers=auth,
    )

    # Mark Saturday 2026-04-04 and Sunday 2026-04-05 as non-working
    cal_client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": loc_ext_id,
            "entries": [
                {"calendar_date": "2026-04-04", "is_working_day": False, "notes": "Saturday"},
                {"calendar_date": "2026-04-05", "is_working_day": False, "notes": "Sunday"},
            ],
            "dry_run": False,
        },
        headers=auth,
    )

    resp = cal_client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": loc_ext_id,
            "start_date": "2026-04-03",
            "add_working_days": 3,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["result_date"] == "2026-04-08"  # Mon=1, Tue=2, Wed=3
    assert body["working_days_found"] == 3
    assert body["non_working_days_skipped"] == 2  # Sat + Sun


@requires_db
def test_working_days_no_calendar(cal_client, auth):
    """
    Location with no calendar entries → all days are working days (safe-by-default).
    start=2026-04-03 (Friday), add=3 → result=2026-04-06 (Monday? No: +3 calendar days)
    With no non-working days: result = 2026-04-06 (Fri+1=Sat, Sat+1=Sun, Sun+1=Mon → Mon 2026-04-06)
    Wait: +3 days with zero skips = 2026-04-03 + 3 = 2026-04-06.
    """
    loc_ext_id = uid("LOC-WD-EMPTY")
    cal_client.post(
        "/v1/ingest/locations",
        json={
            "locations": [{"external_id": loc_ext_id, "name": "Empty Calendar DC", "location_type": "dc"}],
            "dry_run": False,
        },
        headers=auth,
    )

    resp = cal_client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": loc_ext_id,
            "start_date": "2026-04-03",
            "add_working_days": 3,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # No non-working days → advance 3 calendar days
    assert body["result_date"] == "2026-04-06"
    assert body["working_days_found"] == 3
    assert body["non_working_days_skipped"] == 0


@requires_db
def test_working_days_zero(cal_client, auth, test_location):
    """add_working_days=0 → result_date = start_date."""
    resp = cal_client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": test_location,
            "start_date": "2026-06-01",
            "add_working_days": 0,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["result_date"] == "2026-06-01"


# ─────────────────────────────────────────────────────────────
# Auth tests — 401 without token
# ─────────────────────────────────────────────────────────────

@requires_db
def test_calendars_401_ingest(cal_client, test_location):
    """POST /v1/ingest/calendars without auth → 401."""
    resp = cal_client.post(
        "/v1/ingest/calendars",
        json={
            "location_external_id": test_location,
            "entries": [{"calendar_date": "2026-12-25", "is_working_day": False}],
            "dry_run": False,
        },
    )
    assert resp.status_code == 401, resp.text


@requires_db
def test_calendars_401_get(cal_client, test_location):
    """GET /v1/calendars/{loc} without auth → 401."""
    resp = cal_client.get(f"/v1/calendars/{test_location}")
    assert resp.status_code == 401, resp.text


@requires_db
def test_calendars_401_working_days(cal_client, test_location):
    """POST /v1/calendars/working-days without auth → 401."""
    resp = cal_client.post(
        "/v1/calendars/working-days",
        json={
            "location_external_id": test_location,
            "start_date": "2026-04-03",
            "add_working_days": 5,
        },
    )
    assert resp.status_code == 401, resp.text
