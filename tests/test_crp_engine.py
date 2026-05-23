"""
Tests for CRP (Capacity Requirements Planning) Engine — pure (no-DB) layer.

Mock-heavy DB-driven tests previously in this file were ported to
``tests/integration/test_crp_engine_integration.py`` per the project rule
(CLAUDE.md): tests run against real PostgreSQL, no mocks. This slim file
keeps only tests that exercise the in-memory dataclasses, helper methods,
and constructor behaviour of CRPEngine itself.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from ootils_core.crp.engine import (
    CRPEngine,
    CRPResult,
    LoadBucket,
    LoadProfile,
    Overload,
)
from ootils_core.crp.models import Operation, Routing, WorkCenter


# ─────────────────────────────────────────────────────────────
# Fixtures (pure data only — no DB)
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def sample_work_center():
    """Create a sample work center."""
    return WorkCenter(
        work_center_id=uuid4(),
        code="WC-001",
        description="Assembly Line 1",
        capacity_per_day=Decimal("8.0"),
        efficiency=Decimal("0.9"),
        calendar_id=None,
        active=True,
    )


@pytest.fixture
def sample_routing():
    """Create a sample routing with operations."""
    routing = Routing(
        routing_id=uuid4(),
        item_id=uuid4(),
        sequence=1,
        description="Assembly Routing",
        active=True,
    )

    op1 = Operation(
        operation_id=uuid4(),
        routing_id=routing.routing_id,
        sequence=10,
        work_center_id=uuid4(),
        setup_time=Decimal("1.0"),
        run_time_per_unit=Decimal("0.5"),
        description="Operation 10",
        active=True,
    )
    op2 = Operation(
        operation_id=uuid4(),
        routing_id=routing.routing_id,
        sequence=20,
        work_center_id=uuid4(),
        setup_time=Decimal("0.5"),
        run_time_per_unit=Decimal("0.25"),
        description="Operation 20",
        active=True,
    )
    routing.add_operation(op1)
    routing.add_operation(op2)
    return routing


# ─────────────────────────────────────────────────────────────
# Test LoadBucket
# ─────────────────────────────────────────────────────────────

class TestLoadBucket:
    """Tests for LoadBucket class."""

    def test_load_bucket_creation(self, sample_work_center):
        bucket = LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("4.0"),
            capacity_hours=Decimal("8.0"),
        )

        assert bucket.work_center_id == sample_work_center.work_center_id
        assert bucket.load_hours == Decimal("4.0")
        assert bucket.capacity_hours == Decimal("8.0")
        assert bucket.is_overloaded is False
        assert bucket.overload_hours == Decimal("0")

    def test_load_bucket_overload_detection(self, sample_work_center):
        bucket = LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("10.0"),
            capacity_hours=Decimal("8.0"),
        )

        assert bucket.is_overloaded is True
        assert bucket.overload_hours == Decimal("2.0")

    def test_load_bucket_add_load(self, sample_work_center):
        bucket = LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("5.0"),
            capacity_hours=Decimal("8.0"),
        )
        bucket.add_load(Decimal("4.0"))

        assert bucket.load_hours == Decimal("9.0")
        assert bucket.is_overloaded is True
        assert bucket.overload_hours == Decimal("1.0")

    def test_load_bucket_set_capacity(self, sample_work_center):
        bucket = LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("6.0"),
            capacity_hours=Decimal("8.0"),
        )
        bucket.set_capacity(Decimal("5.0"))

        assert bucket.capacity_hours == Decimal("5.0")
        assert bucket.is_overloaded is True
        assert bucket.overload_hours == Decimal("1.0")

    def test_load_bucket_to_dict(self, sample_work_center):
        bucket = LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("10.0"),
            capacity_hours=Decimal("8.0"),
        )
        result = bucket.to_dict()

        assert result["work_center_id"] == str(sample_work_center.work_center_id)
        assert result["bucket_date"] == date.today().isoformat()
        assert result["load_hours"] == 10.0
        assert result["capacity_hours"] == 8.0
        assert result["overload_hours"] == 2.0
        assert result["is_overloaded"] is True


# ─────────────────────────────────────────────────────────────
# Test LoadProfile
# ─────────────────────────────────────────────────────────────

class TestLoadProfile:
    """Tests for LoadProfile class."""

    def test_load_profile_creation(self, sample_work_center):
        profile = LoadProfile(
            sample_work_center.work_center_id,
            sample_work_center.code,
        )

        assert profile.work_center_id == sample_work_center.work_center_id
        assert profile.work_center_code == sample_work_center.code
        assert len(profile.buckets) == 0

    def test_load_profile_add_bucket(self, sample_work_center):
        profile = LoadProfile(
            sample_work_center.work_center_id,
            sample_work_center.code,
        )
        profile.add_bucket(LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("8.0"),
            capacity_hours=Decimal("8.0"),
        ))
        profile.add_bucket(LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today() + timedelta(days=1),
            load_hours=Decimal("10.0"),
            capacity_hours=Decimal("8.0"),
        ))

        assert len(profile.buckets) == 2
        assert profile.get_total_load() == Decimal("18.0")
        assert profile.get_total_capacity() == Decimal("16.0")
        assert profile.get_overload_count() == 1

    def test_load_profile_get_overloads(self, sample_work_center):
        profile = LoadProfile(
            sample_work_center.work_center_id,
            sample_work_center.code,
        )
        profile.add_bucket(LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("6.0"),
            capacity_hours=Decimal("8.0"),
        ))
        profile.add_bucket(LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today() + timedelta(days=1),
            load_hours=Decimal("12.0"),
            capacity_hours=Decimal("8.0"),
        ))
        overloads = profile.get_overloads()

        assert len(overloads) == 1
        assert overloads[0].overload_date == date.today() + timedelta(days=1)
        assert overloads[0].excess_hours == Decimal("4.0")

    def test_load_profile_to_dict(self, sample_work_center):
        profile = LoadProfile(
            sample_work_center.work_center_id,
            sample_work_center.code,
        )
        profile.add_bucket(LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("8.0"),
            capacity_hours=Decimal("8.0"),
        ))
        result = profile.to_dict()

        assert result["work_center_id"] == str(sample_work_center.work_center_id)
        assert result["work_center_code"] == sample_work_center.code
        assert len(result["buckets"]) == 1
        assert result["total_load_hours"] == 8.0
        assert result["overload_count"] == 0


# ─────────────────────────────────────────────────────────────
# Test Overload
# ─────────────────────────────────────────────────────────────

class TestOverload:
    """Tests for Overload class."""

    def test_overload_creation(self):
        wc_id = uuid4()
        overload_date = date.today()
        overload = Overload(
            work_center_id=wc_id,
            work_center_code="WC-001",
            overload_date=overload_date,
            load_hours=Decimal("12.0"),
            capacity_hours=Decimal("8.0"),
            excess_hours=Decimal("4.0"),
        )

        assert overload.work_center_id == wc_id
        assert overload.work_center_code == "WC-001"
        assert overload.overload_date == overload_date
        assert overload.load_hours == Decimal("12.0")
        assert overload.capacity_hours == Decimal("8.0")
        assert overload.excess_hours == Decimal("4.0")

    def test_overload_to_dict(self):
        wc_id = uuid4()
        overload_date = date.today()
        overload = Overload(
            work_center_id=wc_id,
            work_center_code="WC-001",
            overload_date=overload_date,
            load_hours=Decimal("12.0"),
            capacity_hours=Decimal("8.0"),
            excess_hours=Decimal("4.0"),
        )
        result = overload.to_dict()

        assert result["work_center_id"] == str(wc_id)
        assert result["work_center_code"] == "WC-001"
        assert result["overload_date"] == overload_date.isoformat()
        assert result["load_hours"] == 12.0
        assert result["excess_hours"] == 4.0


# ─────────────────────────────────────────────────────────────
# Test CRPEngine — constructor / property behaviour (no DB calls)
# ─────────────────────────────────────────────────────────────

class TestCRPEngine:
    """Tests for CRPEngine class — pure constructor & guard behaviour."""

    def test_engine_initialization(self):
        """Engine starts with no connection; the connection setter assigns it."""
        engine = CRPEngine()
        assert engine.connection is None

        # Assign an opaque sentinel — we never touch SQL here, we just
        # verify the property getter/setter round-trips correctly. Using
        # `object()` rather than a Mock keeps this test mock-free.
        sentinel = object()
        engine.connection = sentinel
        assert engine.connection is sentinel

    def test_engine_requires_connection(self):
        """calculate() must raise when no connection has been wired up."""
        engine = CRPEngine()

        with pytest.raises(ValueError, match="Database connection not set"):
            engine.calculate(horizon_days=30)


# ─────────────────────────────────────────────────────────────
# Test pure model methods (no DB)
# ─────────────────────────────────────────────────────────────

class TestCRPEngineLoadCalculation:
    """Tests for pure load-related model methods."""

    def test_operation_total_time(self):
        """Operation.total_time = setup + run_time * quantity."""
        op = Operation(
            operation_id=uuid4(),
            routing_id=uuid4(),
            sequence=10,
            work_center_id=uuid4(),
            setup_time=Decimal("2.0"),
            run_time_per_unit=Decimal("0.5"),
            description="Test Operation",
            active=True,
        )
        total = op.total_time(Decimal("100"))
        assert total == Decimal("52.0")  # 2.0 + (0.5 * 100)

    def test_work_center_effective_capacity(self, sample_work_center):
        """WorkCenter.effective_capacity_per_day = capacity * efficiency."""
        effective = sample_work_center.effective_capacity_per_day()
        assert effective == Decimal("7.2")  # 8.0 * 0.9


# ─────────────────────────────────────────────────────────────
# Test CRPResult — pure in-memory aggregation
# ─────────────────────────────────────────────────────────────

class TestCRPResult:
    """Tests for CRPResult class."""

    def test_crp_result_creation(self):
        calc_id = uuid4()
        start = date.today()
        end = start + timedelta(days=90)
        result = CRPResult(calc_id, start, end)

        assert result.calculation_id == calc_id
        assert result.horizon_start == start
        assert result.horizon_end == end
        assert result.planned_orders_count == 0
        assert result.work_centers_count == 0
        assert len(result.load_profiles) == 0
        assert len(result.overloads) == 0

    def test_crp_result_add_load_profile(self, sample_work_center):
        calc_id = uuid4()
        start = date.today()
        end = start + timedelta(days=90)
        result = CRPResult(calc_id, start, end)

        profile = LoadProfile(sample_work_center.work_center_id, sample_work_center.code)
        result.add_load_profile(profile)

        assert result.work_centers_count == 1
        assert sample_work_center.work_center_id in result.load_profiles

    def test_crp_result_collect_overloads(self, sample_work_center):
        calc_id = uuid4()
        start = date.today()
        end = start + timedelta(days=90)
        result = CRPResult(calc_id, start, end)

        profile = LoadProfile(sample_work_center.work_center_id, sample_work_center.code)
        profile.add_bucket(LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("12.0"),
            capacity_hours=Decimal("8.0"),
        ))
        result.add_load_profile(profile)
        result.collect_overloads()

        assert len(result.overloads) == 1
        assert result.overloads[0].work_center_id == sample_work_center.work_center_id

    def test_crp_result_to_dict(self, sample_work_center):
        calc_id = uuid4()
        start = date.today()
        end = start + timedelta(days=90)
        result = CRPResult(calc_id, start, end)
        result.planned_orders_count = 10
        result.calculation_time_ms = 150.5

        profile = LoadProfile(sample_work_center.work_center_id, sample_work_center.code)
        result.add_load_profile(profile)
        result.collect_overloads()

        result_dict = result.to_dict()
        assert result_dict["calculation_id"] == str(calc_id)
        assert result_dict["horizon_start"] == start.isoformat()
        assert result_dict["horizon_end"] == end.isoformat()
        assert result_dict["planned_orders_count"] == 10
        assert result_dict["work_centers_count"] == 1
        assert result_dict["calculation_time_ms"] == 150.5
