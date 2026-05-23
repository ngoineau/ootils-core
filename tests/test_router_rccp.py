"""
Slim tests for the RCCP FastAPI router (src/ootils_core/api/routers/rccp.py).

Keeps:
  - Pure helper unit tests for `_bucket_start`, `_bucket_end`,
    `_next_bucket_start`, `_generate_buckets`, `_count_working_days`
    (no-location heuristic only — the location/calendar variants need a DB
    and are ported to the integration file).
  - 401 auth tests.
  - 422 validation tests for the get_rccp endpoint (invalid grain, to_date
    before from_date — both fire before any DB call).
  - Router-introspection tests.

Every test that mocked DB responses for resource lookups, load aggregation,
capacity overrides or operational_calendars was ported to
tests/integration/test_router_rccp_integration.py, per the "no mocks"
rule (CLAUDE.md).
"""
from __future__ import annotations

import os
from datetime import date

import pytest
from fastapi import status
from fastapi.testclient import TestClient

# Must set token BEFORE importing the app
os.environ["OOTILS_API_TOKEN"] = "test-token"

from ootils_core.api.app import create_app
from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db
from ootils_core.api.routers.rccp import (
    _bucket_start,
    _bucket_end,
    _next_bucket_start,
    _generate_buckets,
    _count_working_days,
)


AUTH_HEADERS = {"Authorization": "Bearer test-token"}


class _DBAccessForbidden(AssertionError):
    """Raised if a test that should fail validation tries to use the DB."""


class _ForbiddenDB:
    """Sentinel: any attribute access raises. Proves 422 / 401 fires before DB use."""

    def __getattr__(self, name):
        raise _DBAccessForbidden(
            f"Validation test reached DB (accessed {name!r}) — 422/401 should have fired first"
        )


def _make_client_no_db() -> TestClient:
    app = create_app()

    def override_db():
        yield _ForbiddenDB()

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


def _make_client_no_auth_override() -> TestClient:
    app = create_app()

    def override_db():
        yield _ForbiddenDB()

    app.dependency_overrides[get_db] = override_db
    return TestClient(app)


# ─────────────────────────────────────────────────────────────
# Auth (401)
# ─────────────────────────────────────────────────────────────


class TestRCCPAuth:
    def test_rccp_requires_auth(self):
        client = _make_client_no_auth_override()
        resp = client.get("/v1/rccp/R1")
        assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# Bucket helpers — direct pure-Python unit tests
# ─────────────────────────────────────────────────────────────


class TestBucketStart:
    def test_day(self):
        d = date(2025, 1, 15)  # Wednesday
        assert _bucket_start(d, "day") == d

    def test_week(self):
        d = date(2025, 1, 15)  # Wednesday
        assert _bucket_start(d, "week") == date(2025, 1, 13)  # Monday

    def test_month(self):
        d = date(2025, 1, 15)
        assert _bucket_start(d, "month") == date(2025, 1, 1)


class TestBucketEnd:
    def test_day(self):
        d = date(2025, 1, 15)
        assert _bucket_end(d, "day") == d

    def test_week(self):
        monday = date(2025, 1, 13)
        assert _bucket_end(monday, "week") == date(2025, 1, 19)

    def test_month_january(self):
        assert _bucket_end(date(2025, 1, 1), "month") == date(2025, 1, 31)

    def test_month_december(self):
        """December branch: start.month == 12 → start.replace(day=31)."""
        assert _bucket_end(date(2025, 12, 1), "month") == date(2025, 12, 31)

    def test_month_february(self):
        assert _bucket_end(date(2025, 2, 1), "month") == date(2025, 2, 28)


class TestNextBucketStart:
    def test_day(self):
        assert _next_bucket_start(date(2025, 1, 15), "day") == date(2025, 1, 16)

    def test_week(self):
        monday = date(2025, 1, 13)
        assert _next_bucket_start(monday, "week") == date(2025, 1, 20)

    def test_month(self):
        assert _next_bucket_start(date(2025, 1, 1), "month") == date(2025, 2, 1)

    def test_month_december_year_rollover(self):
        """December branch: start.month == 12 → year+1, month=1."""
        assert _next_bucket_start(date(2025, 12, 1), "month") == date(2026, 1, 1)


class TestGenerateBuckets:
    def test_single_day(self):
        buckets = _generate_buckets(date(2025, 1, 13), date(2025, 1, 13), "day")
        assert buckets == [(date(2025, 1, 13), date(2025, 1, 13))]

    def test_week_truncated_at_to_date(self):
        """end > to_date branch: week extends past to_date and gets clipped."""
        buckets = _generate_buckets(date(2025, 1, 15), date(2025, 1, 17), "week")
        assert buckets[0] == (date(2025, 1, 13), date(2025, 1, 17))

    def test_multiple_weeks(self):
        buckets = _generate_buckets(date(2025, 1, 13), date(2025, 1, 26), "week")
        assert len(buckets) == 2
        assert buckets[0] == (date(2025, 1, 13), date(2025, 1, 19))
        assert buckets[1] == (date(2025, 1, 20), date(2025, 1, 26))


class TestCountWorkingDaysNoLocation:
    """Pure Mon–Fri heuristic, no DB query — safe to test against _ForbiddenDB.

    The location/calendar variants live in the integration test file.
    """

    def test_full_week(self):
        # 2025-01-13 (Mon) … 2025-01-19 (Sun) → 5 working days
        assert _count_working_days(_ForbiddenDB(), None, date(2025, 1, 13), date(2025, 1, 19)) == 5

    def test_weekend_only(self):
        # 2025-01-18 (Sat), 2025-01-19 (Sun) → 0 working days
        assert _count_working_days(_ForbiddenDB(), None, date(2025, 1, 18), date(2025, 1, 19)) == 0

    def test_single_weekday(self):
        # Monday only
        assert _count_working_days(_ForbiddenDB(), None, date(2025, 1, 13), date(2025, 1, 13)) == 1


# ─────────────────────────────────────────────────────────────
# Endpoint validation (422) — fires before DB access
# ─────────────────────────────────────────────────────────────


class TestRCCPEndpointValidation:
    def test_invalid_grain(self):
        client = _make_client_no_db()
        resp = client.get(
            "/v1/rccp/R1",
            params={"grain": "fortnight"},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "grain" in resp.json()["detail"]

    def test_to_date_before_from_date(self):
        client = _make_client_no_db()
        resp = client.get(
            "/v1/rccp/R1",
            params={
                "from_date": "2025-02-01",
                "to_date": "2025-01-01",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "to_date" in resp.json()["detail"]


# ─────────────────────────────────────────────────────────────
# Router introspection
# ─────────────────────────────────────────────────────────────


class TestRCCPRouterConfiguration:
    def test_router_prefix(self):
        from ootils_core.api.routers import rccp
        assert rccp.router.prefix == "/v1/rccp"

    def test_router_tags(self):
        from ootils_core.api.routers import rccp
        assert "rccp" in rccp.router.tags

    def test_router_registered_in_app(self):
        app = create_app()
        rccp_routes = [r for r in app.routes if hasattr(r, "path") and "/rccp/" in r.path]
        assert len(rccp_routes) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
