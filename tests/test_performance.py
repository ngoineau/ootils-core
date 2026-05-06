"""
Performance Benchmark Tests for Phase 1 Modules.

Targets:
- ATP check: <100ms for 1-year horizon
- CRP calculation: <5s for 1000 orders
- Forecast generation: <50ms per item
- MPS aggregation: <200ms for 90-day horizon

Run with:
    pytest tests/test_performance.py -v --tb=short
"""
from __future__ import annotations

import time
from datetime import date, timedelta
from decimal import Decimal
from typing import List
from uuid import UUID, uuid4
from unittest.mock import Mock, patch

import pytest

from ootils_core.forecasting.engine import ForecastingEngine, ForecastMethod, ForecastResult
from ootils_core.mps.engine import AggregateDemandEngine, AggregateDemandRequest, AggregateDemandResult
from ootils_core.mps.models import MPSNode, MPSStatus
from ootils_core.crp.engine import CRPEngine, CRPResult
from ootils_core.crp.models import WorkCenter, Routing, Operation
from ootils_core.atp.engine import ATPEngine
from ootils_core.atp.models import ATPConfig, ATPResult


# ─────────────────────────────────────────────────────────────
# Performance Test Configuration
# ─────────────────────────────────────────────────────────────

class PerformanceConfig:
    """Performance test thresholds."""
    ATP_1YEAR_HORIZON_MS = 100
    CRP_1000_ORDERS_S = 5.0
    FORECAST_PER_ITEM_MS = 50
    MPS_90DAY_HORIZON_MS = 200
    CRP_100_ORDERS_S = 0.5
    ATP_30DAY_HORIZON_MS = 20


# ─────────────────────────────────────────────────────────────
# ATP Performance Tests
# ─────────────────────────────────────────────────────────────

class TestATPPerformance:
    """ATP performance benchmarks."""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Create a mock database connection."""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        cursor.fetchall.return_value = []
        return conn
    
    def test_atp_30day_horizon_performance(self, mock_db_connection):
        """ATP check with 30-day horizon (target: <20ms)."""
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None
        cursor.fetchall.return_value = []
        
        engine = ATPEngine()
        engine.connection = mock_db_connection
        
        start_time = time.perf_counter()
        
        result = engine.calculate(
            item_id=uuid4(),
            location_id=uuid4(),
            quantity=Decimal("100"),
            request_date=date.today(),
            horizon_days=30,
        )
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        assert isinstance(result, ATPResult)
        assert elapsed_ms < PerformanceConfig.ATP_30DAY_HORIZON_MS, \
            f"ATP 30-day took {elapsed_ms:.2f}ms (target: <{PerformanceConfig.ATP_30DAY_HORIZON_MS}ms)"
    
    def test_atp_365day_horizon_performance(self, mock_db_connection):
        """ATP check with 365-day horizon (target: <100ms)."""
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None
        cursor.fetchall.return_value = []
        
        engine = ATPEngine(config=ATPConfig(default_horizon_days=365))
        engine.connection = mock_db_connection
        
        start_time = time.perf_counter()
        
        result = engine.calculate(
            item_id=uuid4(),
            location_id=uuid4(),
            quantity=Decimal("100"),
            request_date=date.today(),
            horizon_days=365,
        )
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        assert isinstance(result, ATPResult)
        assert elapsed_ms < PerformanceConfig.ATP_1YEAR_HORIZON_MS, \
            f"ATP 365-day took {elapsed_ms:.2f}ms (target: <{PerformanceConfig.ATP_1YEAR_HORIZON_MS}ms)"
    
    def test_atp_with_supplies_performance(self, mock_db_connection):
        """ATP check with multiple supply sources (target: <50ms)."""
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        item_id = uuid4()
        location_id = uuid4()
        
        # Mock on-hand: 5 fields (on_hand_id, item_id, location_id, quantity, as_of_date)
        on_hand = (uuid4(), item_id, location_id, 100, date.today())
        
        # Mock planned supplies: 6 fields (planned_supply_id, item_id, location_id, quantity, due_date, priority)
        planned_supplies = [
            (uuid4(), item_id, location_id, 100, date.today() + timedelta(days=i), 999)
            for i in range(1, 50)
        ]
        
        cursor.fetchone.return_value = on_hand
        cursor.fetchall.side_effect = [
            planned_supplies,  # Planned supplies
            [],  # No demand
        ]
        
        engine = ATPEngine()
        engine.connection = mock_db_connection
        
        start_time = time.perf_counter()
        
        result = engine.calculate(
            item_id=item_id,
            location_id=location_id,
            quantity=Decimal("500"),
            request_date=date.today() + timedelta(days=30),
            horizon_days=90,
        )
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        assert isinstance(result, ATPResult)
        assert elapsed_ms < 50, f"ATP with supplies took {elapsed_ms:.2f}ms (target: <50ms)"


# ─────────────────────────────────────────────────────────────
# CRP Performance Tests
# ─────────────────────────────────────────────────────────────

class TestCRPPerformance:
    """CRP performance benchmarks."""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Create a mock database connection."""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        return conn
    
    def test_crp_100_orders_performance(self, mock_db_connection):
        """CRP with 100 planned orders (target: <0.5s)."""
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        wc_id = uuid4()
        item_id = uuid4()
        
        # Planned orders: 6 fields (planned_supply_id, item_id, location_id, quantity, due_date, status)
        planned_orders = [
            (uuid4(), item_id, uuid4(), Decimal("10"), date.today() + timedelta(days=i % 30), "PLANNED")
            for i in range(100)
        ]
        
        cursor.fetchall.side_effect = [
            [(wc_id, "WC-001", "Assembly", Decimal("8"), Decimal("0.9"), None, True)],  # Work centers
            planned_orders,  # Planned orders
            [],  # Routings (empty for this test)
        ]
        
        engine = CRPEngine()
        engine.connection = mock_db_connection
        
        start_time = time.perf_counter()
        result = engine.calculate(horizon_days=30)
        elapsed_seconds = time.perf_counter() - start_time
        
        assert isinstance(result, CRPResult)
        assert result.planned_orders_count == 100
        assert elapsed_seconds < PerformanceConfig.CRP_100_ORDERS_S, \
            f"CRP 100 orders took {elapsed_seconds:.2f}s (target: <{PerformanceConfig.CRP_100_ORDERS_S}s)"
    
    def test_crp_1000_orders_performance(self, mock_db_connection):
        """CRP with 1000 planned orders (target: <5s)."""
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        wc_id = uuid4()
        item_id = uuid4()
        
        # Planned orders: 6 fields (planned_supply_id, item_id, location_id, quantity, due_date, status)
        planned_orders = [
            (uuid4(), item_id, uuid4(), Decimal("10"), date.today() + timedelta(days=i % 30), "PLANNED")
            for i in range(1000)
        ]
        
        cursor.fetchall.side_effect = [
            [(wc_id, "WC-001", "Assembly", Decimal("8"), Decimal("0.9"), None, True)],  # Work centers
            planned_orders,  # Planned orders
            [],  # Routings
        ]
        
        engine = CRPEngine()
        engine.connection = mock_db_connection
        
        start_time = time.perf_counter()
        result = engine.calculate(horizon_days=30)
        elapsed_seconds = time.perf_counter() - start_time
        
        assert isinstance(result, CRPResult)
        assert result.planned_orders_count == 1000
        assert elapsed_seconds < PerformanceConfig.CRP_1000_ORDERS_S, \
            f"CRP 1000 orders took {elapsed_seconds:.2f}s (target: <{PerformanceConfig.CRP_1000_ORDERS_S}s)"
    
    def test_crp_multiple_work_centers_performance(self, mock_db_connection):
        """CRP with 10 work centers and 500 orders (target: <2s)."""
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        
        work_centers = [
            (uuid4(), f"WC-{i:03d}", f"Work Center {i}", Decimal("8"), Decimal("0.9"), None, True)
            for i in range(10)
        ]
        wc_ids = [wc[0] for wc in work_centers]
        
        item_id = uuid4()
        planned_orders = [
            (uuid4(), item_id, uuid4(), Decimal("10"), date.today() + timedelta(days=i % 30), "PLANNED")
            for i in range(500)
        ]
        
        # Mock routings and operations so load gets distributed
        # Routing: routing_id, item_id, sequence, description, active
        # Operations: operation_id, routing_id, sequence, work_center_id, setup_time, run_time_per_unit, description, active
        routing_id = uuid4()
        cursor.fetchall.side_effect = [
            work_centers,  # Work centers
            planned_orders,  # Planned orders
            [(routing_id, item_id, 1, "Assembly", True)],  # Routings
            [(uuid4(), routing_id, 1, wc_ids[0], Decimal("0.5"), Decimal("0.1"), "Assembly Op", True)],  # Operations (8 fields)
        ]
        
        engine = CRPEngine()
        engine.connection = mock_db_connection
        
        start_time = time.perf_counter()
        result = engine.calculate(horizon_days=30)
        elapsed_seconds = time.perf_counter() - start_time
        
        assert isinstance(result, CRPResult)
        assert result.planned_orders_count == 500
        # At least 1 work center should have load (the one in the routing)
        assert result.work_centers_count >= 1
        assert elapsed_seconds < 2.0, f"CRP multi-WC took {elapsed_seconds:.2f}s (target: <2s)"


# ─────────────────────────────────────────────────────────────
# Forecast Performance Tests
# ─────────────────────────────────────────────────────────────

class TestForecastPerformance:
    """Forecast generation performance benchmarks."""
    
    def test_forecast_ma_performance(self):
        """Moving Average forecast (target: <10ms)."""
        engine = ForecastingEngine()
        history = [100 + i * 5 for i in range(50)]
        
        start_time = time.perf_counter()
        
        result = engine.generate(
            item_history=history,
            method=ForecastMethod.MA,
            params={"window": 5},
        )
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        assert isinstance(result, ForecastResult)
        assert elapsed_ms < 10, f"MA forecast took {elapsed_ms:.2f}ms (target: <10ms)"
    
    def test_forecast_exp_smoothing_performance(self):
        """Exponential Smoothing forecast (target: <20ms)."""
        engine = ForecastingEngine()
        history = [100 + i * 5 for i in range(50)]
        
        start_time = time.perf_counter()
        
        result = engine.generate(
            item_history=history,
            method=ForecastMethod.EXP_SMOOTHING,
            params={"alpha": 0.3},
        )
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        assert isinstance(result, ForecastResult)
        assert elapsed_ms < 20, f"ES forecast took {elapsed_ms:.2f}ms (target: <20ms)"
    
    def test_forecast_croston_performance(self):
        """Croston's method forecast (target: <30ms)."""
        engine = ForecastingEngine()
        # Intermittent demand with zeros
        history = [0 if i % 3 == 0 else 50 + i for i in range(50)]
        
        start_time = time.perf_counter()
        
        result = engine.generate(
            item_history=history,
            method=ForecastMethod.CROSTON,
        )
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        assert isinstance(result, ForecastResult)
        assert elapsed_ms < 30, f"Croston forecast took {elapsed_ms:.2f}ms (target: <30ms)"
    
    def test_forecast_batch_100_items_performance(self):
        """Batch forecast for 100 items (target: <5s total)."""
        engine = ForecastingEngine()
        history = [100 + i * 5 for i in range(50)]
        
        start_time = time.perf_counter()
        
        results = []
        for _ in range(100):
            result = engine.generate(
                item_history=history,
                method=ForecastMethod.MA,
                params={"window": 5},
            )
            results.append(result)
        
        elapsed_seconds = time.perf_counter() - start_time
        
        assert len(results) == 100
        assert elapsed_seconds < 5.0, f"Batch 100 items took {elapsed_seconds:.2f}s (target: <5s)"
        assert elapsed_seconds / 100 * 1000 < PerformanceConfig.FORECAST_PER_ITEM_MS, \
            f"Per-item avg {elapsed_seconds / 100 * 1000:.2f}ms (target: <{PerformanceConfig.FORECAST_PER_ITEM_MS}ms)"


# ─────────────────────────────────────────────────────────────
# MPS Performance Tests
# ─────────────────────────────────────────────────────────────

class TestMPSPerformance:
    """MPS aggregation performance benchmarks."""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Create a mock database connection."""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        return conn
    
    def test_mps_90day_horizon_performance(self, mock_db_connection):
        """MPS aggregation for 90-day horizon (target: <200ms)."""
        engine = AggregateDemandEngine()
        
        # Mock the internal methods to avoid DB calls
        with patch.object(engine, '_fetch_forecast_demand', return_value=[]), \
             patch.object(engine, '_fetch_sales_orders_demand', return_value=[]), \
             patch.object(engine, '_generate_time_buckets', return_value=[]):
            
            request = AggregateDemandRequest(
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                horizon_start=date.today(),
                horizon_end=date.today() + timedelta(days=90),
                time_grain="weekly",
            )
            
            start_time = time.perf_counter()
            result = engine.aggregate(
                db=mock_db_connection,
                item_id=request.item_id,
                location_id=request.location_id,
                scenario_id=request.scenario_id,
                horizon_start=request.horizon_start,
                horizon_end=request.horizon_end,
                time_grain=request.time_grain,
            )
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            
            assert isinstance(result, AggregateDemandResult)
            assert elapsed_ms < PerformanceConfig.MPS_90DAY_HORIZON_MS, \
                f"MPS 90-day took {elapsed_ms:.2f}ms (target: <{PerformanceConfig.MPS_90DAY_HORIZON_MS}ms)"
    
    def test_mps_node_batch_creation_performance(self):
        """Batch creation of 100 MPS nodes (target: <100ms)."""
        start_time = time.perf_counter()
        
        nodes = []
        for i in range(100):
            node = MPSNode(
                mps_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                time_bucket=f"2026-W{i % 52 + 15:02d}",
                time_bucket_start=date(2026, 4, 6) + timedelta(days=i * 7),
                time_bucket_end=date(2026, 4, 12) + timedelta(days=i * 7),
                forecast_quantity=Decimal("100"),
                sales_orders_quantity=Decimal("50"),
            )
            node.compute_total_demand()
            nodes.append(node)
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        assert len(nodes) == 100
        assert elapsed_ms < 100, f"Batch 100 MPS nodes took {elapsed_ms:.2f}ms (target: <100ms)"


# ─────────────────────────────────────────────────────────────
# End-to-End Performance Tests
# ─────────────────────────────────────────────────────────────

class TestEndToEndPerformance:
    """End-to-end flow performance benchmarks."""
    
    @pytest.fixture
    def mock_db_connection(self):
        """Create a mock database connection."""
        conn = Mock()
        cursor = Mock()
        conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        cursor.fetchall.return_value = []
        return conn
    
    def test_forecast_to_atp_flow_performance(self, mock_db_connection):
        """Complete flow: Forecast → MPS → ATP (target: <300ms)."""
        cursor = mock_db_connection.cursor.return_value.__enter__.return_value
        item_id = uuid4()
        location_id = uuid4()
        
        cursor.fetchone.return_value = None  # No on-hand
        # Planned supply: 6 fields (planned_supply_id, item_id, location_id, quantity, due_date, priority)
        cursor.fetchall.side_effect = [
            [  # Planned supply
                (uuid4(), item_id, location_id, 500, date.today() + timedelta(days=7), 999)
            ],
            [],  # No demand
        ]
        
        start_time = time.perf_counter()
        
        # Step 1: Forecast
        forecast_engine = ForecastingEngine()
        history = [100, 105, 98, 102, 101, 99, 103, 100, 102, 98]
        forecast_result = forecast_engine.generate(
            item_history=history,
            method=ForecastMethod.MA,
            params={"window": 3},
        )
        
        # Step 2: MPS node creation
        mps_node = MPSNode(
            mps_id=uuid4(),
            item_id=item_id,
            location_id=location_id,
            scenario_id=uuid4(),
            time_bucket="2026-W15",
            time_bucket_start=date.today(),
            time_bucket_end=date.today() + timedelta(days=7),
            forecast_quantity=forecast_result.forecast_value,
            sales_orders_quantity=Decimal("50"),
        )
        mps_node.compute_total_demand()
        
        # Step 3: ATP check
        atp_engine = ATPEngine()
        atp_engine.connection = mock_db_connection
        atp_result = atp_engine.calculate(
            item_id=item_id,
            location_id=location_id,
            quantity=Decimal("50"),
            request_date=date.today() + timedelta(days=7),
            horizon_days=30,
        )
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        assert isinstance(forecast_result, ForecastResult)
        assert isinstance(mps_node, MPSNode)
        assert isinstance(atp_result, ATPResult)
        assert elapsed_ms < 300, f"E2E flow took {elapsed_ms:.2f}ms (target: <300ms)"


# ─────────────────────────────────────────────────────────────
# Run with: pytest tests/test_performance.py -v --tb=short
# ─────────────────────────────────────────────────────────────
