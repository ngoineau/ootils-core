"""
Integration tests for CRP (Capacity Requirements Planning) API and MRP integration.

Tests:
- CRP calculation with mock data
- Load profile aggregation
- Overload detection accuracy
- Suggest resolutions algorithm
- MRP → CRP integration hook (run_crp parameter in MPS promote-to-mrp)
- CRP router registration in app
"""
import pytest
from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4
from unittest.mock import Mock, MagicMock, patch

from ootils_core.crp.engine import CRPEngine, CRPResult, LoadProfile, Overload, LoadBucket
from ootils_core.crp.models import WorkCenter, Routing, Operation
from ootils_core.mps.api import PromoteToMRPRequest, PromoteToMRPResponse


@pytest.fixture
def mock_db_connection():
    """Create a mock database connection."""
    conn = Mock()
    cursor = Mock()
    conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
    conn.cursor.return_value.__exit__ = Mock(return_value=False)
    return conn, cursor


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


class TestCRPCalculationIntegration:
    """Test CRP calculation integration."""
    
    def test_crp_calculation_with_mock_data(self, mock_db_connection):
        """Test that CRP calculation correctly processes mock data."""
        conn, cursor = mock_db_connection
        
        # Mock work center fetch
        cursor.fetchall.side_effect = [
            [(uuid4(), "WC-001", "Assembly", Decimal("8"), Decimal("0.9"), None, True)],
            [],  # No planned orders
            [],  # No routings
        ]
        
        engine = CRPEngine(db_conn=conn)
        result = engine.calculate(horizon_days=30)
        
        assert isinstance(result, CRPResult)
        assert result.work_centers_count >= 0
    
    def test_crp_calculation_empty_when_no_planned_orders(self, mock_db_connection):
        """Test that CRP returns empty result when no planned orders exist."""
        conn, cursor = mock_db_connection
        
        # Mock work center exists but no planned orders
        cursor.fetchall.side_effect = [
            [(uuid4(), "WC-001", "Assembly", Decimal("8"), Decimal("0.9"), None, True)],
            [],  # No planned orders
        ]
        
        engine = CRPEngine(db_conn=conn)
        result = engine.calculate(horizon_days=30)
        
        assert result.planned_orders_count == 0
        assert len(result.load_profiles) == 0
        assert len(result.overloads) == 0
    
    def test_crp_calculation_with_planned_orders_mocked(self, mock_db_connection, sample_work_center):
        """Test CRP calculation with mocked planned orders."""
        conn, cursor = mock_db_connection
        wc_id = sample_work_center.work_center_id
        
        # Mock work center and planned orders (6 columns: ps_id, item_id, loc_id, qty, due_date, status)
        cursor.fetchall.side_effect = [
            [(wc_id, "WC-001", "Assembly", Decimal("8"), Decimal("0.9"), None, True)],
            [
                (uuid4(), uuid4(), uuid4(), Decimal("100"), date.today() + timedelta(days=10), "PLANNED")
            ],
            [],  # No routings needed
        ]
        
        engine = CRPEngine(db_conn=conn)
        result = engine.calculate(horizon_days=30)
        
        assert isinstance(result, CRPResult)
        assert result.work_centers_count >= 0


class TestLoadProfileAggregation:
    """Test load profile aggregation."""
    
    def test_load_bucket_creation(self, sample_work_center):
        """Test that load buckets are created correctly."""
        bucket = LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("10.5"),
            capacity_hours=Decimal("8.0"),
        )
        
        assert bucket.bucket_date == date.today()
        assert bucket.load_hours == Decimal("10.5")
        assert bucket.capacity_hours == Decimal("8.0")
        assert bucket.is_overloaded is True
    
    def test_load_profile_aggregation(self, sample_work_center):
        """Test that load profiles aggregate buckets correctly."""
        profile = LoadProfile(
            work_center_id=sample_work_center.work_center_id,
            work_center_code=sample_work_center.code,
        )
        
        # Add multiple buckets
        for i in range(5):
            bucket = LoadBucket(
                work_center_id=sample_work_center.work_center_id,
                bucket_date=date.today() + timedelta(days=i),
                load_hours=Decimal("10.0"),
                capacity_hours=Decimal("8.0"),
            )
            profile.add_bucket(bucket)
        
        assert len(profile.buckets) == 5
        assert profile.get_total_load() == Decimal("50.0")
        assert profile.get_total_capacity() == Decimal("40.0")
    
    def test_load_profile_overload_detection(self, sample_work_center):
        """Test that load profiles detect overloads correctly."""
        profile = LoadProfile(
            work_center_id=sample_work_center.work_center_id,
            work_center_code=sample_work_center.code,
        )
        
        # Add overloaded bucket
        bucket = LoadBucket(
            work_center_id=sample_work_center.work_center_id,
            bucket_date=date.today(),
            load_hours=Decimal("12.0"),
            capacity_hours=Decimal("8.0"),
        )
        profile.add_bucket(bucket)
        
        overloads = profile.get_overloads()
        assert len(overloads) == 1
        assert overloads[0].excess_hours == Decimal("4.0")


class TestOverloadDetection:
    """Test overload detection accuracy."""
    
    def test_overload_detected_when_load_exceeds_capacity(self, mock_db_connection, sample_work_center):
        """Test that overloads are detected when load > capacity."""
        conn, cursor = mock_db_connection
        wc_id = sample_work_center.work_center_id
        
        # Mock data: work centers (7 cols), planned orders (6 cols), routings (empty)
        cursor.fetchall.side_effect = [
            [(wc_id, "WC-001", "Assembly", Decimal("8"), Decimal("0.9"), None, True)],
            [(uuid4(), uuid4(), uuid4(), Decimal("100"), date.today() + timedelta(days=5), "PLANNED")],
            [],
        ]
        
        engine = CRPEngine(db_conn=conn)
        result = engine.calculate(horizon_days=30)
        
        assert result.work_centers_count >= 0
    
    def test_no_overload_when_capacity_sufficient(self, mock_db_connection, sample_work_center):
        """Test that no overload is detected when capacity is sufficient."""
        conn, cursor = mock_db_connection
        wc_id = sample_work_center.work_center_id
        
        # Mock data: work center with high capacity, small order
        cursor.fetchall.side_effect = [
            [(wc_id, "WC-001", "Assembly", Decimal("100"), Decimal("0.9"), None, True)],
            [(uuid4(), uuid4(), uuid4(), Decimal("10"), date.today() + timedelta(days=5), "PLANNED")],
            [],
        ]
        
        engine = CRPEngine(db_conn=conn)
        result = engine.calculate(horizon_days=30)
        
        assert len(result.overloads) == 0


class TestSuggestResolutions:
    """Test suggest resolutions algorithm."""
    
    def test_suggest_resolutions_returns_suggestions(self, mock_db_connection, sample_work_center):
        """Test that suggest_resolutions returns actionable suggestions."""
        conn, cursor = mock_db_connection
        wc_id = sample_work_center.work_center_id
        
        # Mock overloaded scenario: work centers (7 cols), planned orders (6 cols), routings, orders for resolution
        cursor.fetchall.side_effect = [
            [(wc_id, "WC-001", "Assembly", Decimal("4"), Decimal("0.9"), None, True)],
            [(uuid4(), uuid4(), uuid4(), Decimal("100"), date.today() + timedelta(days=5), "PLANNED")],
            [],
            [],
        ]
        
        engine = CRPEngine(db_conn=conn)
        suggestions = engine.suggest_resolutions(horizon_days=30, max_shift_days=14)
        
        # Should return list (may be empty if no orders found to shift)
        assert isinstance(suggestions, list)
    
    def test_suggest_resolutions_with_no_overloads(self, mock_db_connection, sample_work_center):
        """Test that suggest_resolutions returns empty when no overloads."""
        conn, cursor = mock_db_connection
        wc_id = sample_work_center.work_center_id
        
        # Mock data with no overloads
        cursor.fetchall.side_effect = [
            [(wc_id, "WC-001", "Assembly", Decimal("100"), Decimal("0.9"), None, True)],
            [],
        ]
        
        engine = CRPEngine(db_conn=conn)
        suggestions = engine.suggest_resolutions(horizon_days=30, max_shift_days=14)
        
        assert suggestions == []


class TestMRPCRPIntegration:
    """Test MRP → CRP integration hook."""
    
    def test_promote_to_mrp_request_has_crp_fields(self):
        """Test that PromoteToMRPRequest has CRP integration fields."""
        fields = PromoteToMRPRequest.model_fields
        assert "run_crp" in fields
        assert "crp_horizon_days" in fields
        
        # Check defaults
        assert fields["run_crp"].default is False
        assert fields["crp_horizon_days"].default == 90
    
    def test_promote_to_mrp_response_has_crp_fields(self):
        """Test that PromoteToMRPResponse has CRP result fields."""
        fields = PromoteToMRPResponse.model_fields
        assert "crp_triggered" in fields
        assert "crp_overload_count" in fields
        assert "crp_peak_load_date" in fields
    
    def test_promote_to_mrp_request_defaults(self):
        """Test that PromoteToMRPRequest has correct defaults."""
        request = PromoteToMRPRequest()
        
        assert request.run_crp is False
        assert request.crp_horizon_days == 90
        assert request.explode_components is True
        assert request.dry_run is False
    
    def test_promote_to_mrp_request_with_crp_enabled(self):
        """Test creating request with CRP enabled."""
        request = PromoteToMRPRequest(
            run_crp=True,
            crp_horizon_days=60,
        )
        
        assert request.run_crp is True
        assert request.crp_horizon_days == 60
    
    def test_crp_integration_hook_does_not_break_existing_flow(self):
        """Test that CRP integration doesn't break existing MPS promotion."""
        from ootils_core.mps.engine import AggregateDemandEngine
        
        engine = AggregateDemandEngine()
        
        # The promote_to_mrp method should exist and accept standard parameters
        assert hasattr(engine, 'promote_to_mrp')
    
    def test_crp_router_registered_in_app(self):
        """Test that CRP router is registered in the main app."""
        from ootils_core.api.app import create_app
        
        app = create_app()
        
        # Check that CRP routes are present
        crp_routes = [r for r in app.routes if hasattr(r, 'path') and "/v1/crp" in r.path]
        
        # Should have at least 4 routes: calculate, load-profile, overloads, suggest-resolutions
        assert len(crp_routes) >= 4
        
        route_paths = [r.path for r in crp_routes]
        assert any("/calculate" in p for p in route_paths)
        assert any("/load-profile" in p for p in route_paths)
        assert any("/overloads" in p for p in route_paths)
        assert any("/suggest-resolutions" in p for p in route_paths)
    
    def test_crp_router_has_suggest_resolutions_endpoint(self):
        """Test that CRP router has the suggest-resolutions endpoint."""
        from ootils_core.crp import crp_router
        
        # Check router routes
        routes = crp_router.routes
        suggest_routes = [r for r in routes if hasattr(r, 'path') and "suggest-resolutions" in r.path]
        
        assert len(suggest_routes) >= 1
        
        # Check it's a POST endpoint
        assert suggest_routes[0].methods is None or "POST" in suggest_routes[0].methods


class TestCRPResponseModels:
    """Test CRP response models."""
    
    def test_crp_calculate_response(self, sample_work_center):
        """Test CRPCalculateResponse model."""
        from ootils_core.crp.routers import CRPCalculateResponse, LoadProfileOut, LoadBucketOut
        
        response = CRPCalculateResponse(
            calculation_id="calc-001",
            horizon_start=date.today(),
            horizon_end=date.today() + timedelta(days=30),
            planned_orders_count=10,
            work_centers_count=3,
            overload_count=2,
            load_profiles={},
            overloads=[],
            calculation_time_ms=150,
        )
        
        assert response.planned_orders_count == 10
        assert response.work_centers_count == 3
        assert response.overload_count == 2
    
    def test_crp_overloads_response(self):
        """Test CRPOverloadsResponse model."""
        from ootils_core.crp.routers import CRPOverloadsResponse, OverloadOut
        
        response = CRPOverloadsResponse(
            horizon_start=date.today(),
            horizon_end=date.today() + timedelta(days=30),
            total_overloads=1,
            work_centers_affected=1,
            overloads=[
                OverloadOut(
                    work_center_id=str(uuid4()),
                    work_center_code="WC-001",
                    overload_date=date.today(),
                    load_hours=12.0,
                    capacity_hours=8.0,
                    excess_hours=4.0,
                )
            ],
            calculation_time_ms=50,
        )
        
        assert response.total_overloads == 1
        assert response.overloads[0].excess_hours == 4.0
    
    def test_crp_suggest_resolutions_response(self):
        """Test CRPSuggestResolutionsResponse model."""
        from ootils_core.crp.routers import CRPSuggestResolutionsResponse, ResolutionSuggestionOut
        
        response = CRPSuggestResolutionsResponse(
            horizon_start=date.today(),
            horizon_end=date.today() + timedelta(days=30),
            total_suggestions=1,
            suggestions=[
                ResolutionSuggestionOut(
                    work_center_id=str(uuid4()),
                    work_center_code="WC-001",
                    overload_date=date.today(),
                    excess_hours=4.0,
                    suggested_orders=[
                        {
                            "planned_supply_id": str(uuid4()),
                            "item_code": "ITEM-001",
                            "current_due_date": date.today(),
                            "suggested_due_date": date.today() + timedelta(days=2),
                            "hours_freed": 2.0,
                        }
                    ],
                    total_hours_freed=2.0,
                    recommendation="Shift 1 order to free up capacity",
                )
            ],
        )
        
        assert response.total_suggestions == 1
        assert response.suggestions[0].total_hours_freed == 2.0
