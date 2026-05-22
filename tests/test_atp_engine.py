"""
Unit tests for ATP Engine.

Tests cover:
- Standard ATP calculation
- Partial shortage scenarios
- Backorder scenarios
- Edge cases (no supply, no demand, horizon boundaries)
- Performance requirements (<100ms for 1 year horizon)
"""

import unittest
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch
from uuid import uuid4

from ootils_core.atp.engine import ATPEngine


class TestATPEngineCalculate(unittest.TestCase):
    """Test ATP calculation with various scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.item_id = uuid4()
        self.location_id = uuid4()
        self.request_date = date.today()
        self.horizon_end = self.request_date + timedelta(days=30)
        
        # Mock database connection
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(return_value=self.mock_cursor)
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        
        self.engine = ATPEngine(db_conn=self.mock_conn)
    
    def test_basic_atp_calculation_with_onhand(self):
        """Test ATP calculation with on-hand supply only."""
        # Setup: 100 units on hand, no demand
        on_hand_id = uuid4()
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 100, self.request_date
        )
        self.mock_cursor.fetchall.return_value = []  # No planned supply, no demand
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("50"), self.request_date
        )
        
        # Should return full quantity available on request date
        self.assertEqual(result.available_quantity, Decimal("50"))
        self.assertEqual(result.available_date, self.request_date)
        self.assertTrue(result.is_fully_available)
        self.assertEqual(result.backorder_quantity, Decimal("0"))
    
    def test_basic_atp_calculation_with_demand(self):
        """Test ATP calculation with on-hand supply and customer orders."""
        # Setup: 100 units on hand, 40 units committed demand
        on_hand_id = uuid4()
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 100, self.request_date
        )
        self.mock_cursor.fetchall.side_effect = [
            [],  # No planned supply
            [(uuid4(), self.item_id, self.location_id, 40, self.request_date, 0, False)]  # Demand
        ]
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("50"), self.request_date
        )
        
        # ATP = 100 - 40 = 60, request is 50, so fully available
        self.assertEqual(result.available_quantity, Decimal("50"))
        self.assertTrue(result.is_fully_available)
    
    def test_partial_shortage_scenario(self):
        """Test scenario where only partial quantity is available."""
        # Setup: 100 units on hand, 80 units committed demand, request 50
        on_hand_id = uuid4()
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 100, self.request_date
        )
        self.mock_cursor.fetchall.side_effect = [
            [],  # No planned supply
            [(uuid4(), self.item_id, self.location_id, 80, self.request_date, 0, False)]  # Demand
        ]
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("50"), self.request_date
        )
        
        # ATP = 100 - 80 = 20, request is 50, so only 20 available
        self.assertEqual(result.available_quantity, Decimal("20"))
        self.assertFalse(result.is_fully_available)
        self.assertEqual(result.backorder_quantity, Decimal("30"))
    
    def test_backorder_scenario_with_planned_supply(self):
        """Test backorder scenario where planned supply fulfills later."""
        # Setup: 50 on hand, 80 demand today, 100 planned supply in 5 days
        on_hand_id = uuid4()
        future_date = self.request_date + timedelta(days=5)
        
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 50, self.request_date
        )
        self.mock_cursor.fetchall.side_effect = [
            [(uuid4(), self.item_id, self.location_id, 100, future_date, 0)],  # Planned supply
            [(uuid4(), self.item_id, self.location_id, 80, self.request_date, 0, False)]  # Demand
        ]
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("50"), self.request_date
        )
        
        # Today: ATP = 50 - 80 = -30 (shortage)
        # Day 5: ATP = -30 + 100 = 70 (available)
        # Request 50 should be available on day 5
        self.assertEqual(result.available_quantity, Decimal("50"))
        self.assertEqual(result.available_date, future_date)
        self.assertFalse(result.is_fully_available)  # Not available on request_date
    
    def test_no_supply_scenario(self):
        """Test scenario with no supply at all."""
        self.mock_cursor.fetchone.return_value = None  # No on-hand
        self.mock_cursor.fetchall.return_value = []  # No planned supply, no demand
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("50"), self.request_date
        )
        
        self.assertEqual(result.available_quantity, Decimal("0"))
        self.assertIsNone(result.available_date)
        self.assertFalse(result.is_fully_available)
    
    def test_no_demand_scenario(self):
        """Test scenario with supply but no demand."""
        on_hand_id = uuid4()
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 100, self.request_date
        )
        self.mock_cursor.fetchall.return_value = []
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("50"), self.request_date
        )
        
        self.assertEqual(result.available_quantity, Decimal("50"))
        self.assertEqual(result.available_date, self.request_date)
        self.assertTrue(result.is_fully_available)
    
    def test_multiple_planned_supplies(self):
        """Test with multiple planned supply receipts."""
        on_hand_id = uuid4()
        ps1_id = uuid4()
        ps2_id = uuid4()
        
        day5 = self.request_date + timedelta(days=5)
        day10 = self.request_date + timedelta(days=10)
        
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 50, self.request_date
        )
        self.mock_cursor.fetchall.side_effect = [
            [
                (ps1_id, self.item_id, self.location_id, 30, day5, 0),
                (ps2_id, self.item_id, self.location_id, 50, day10, 0),
            ],
            []  # No demand
        ]
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("100"), self.request_date
        )
        
        # Day 0: 50, Day 5: 50+30=80, Day 10: 80+50=130
        # Request 100 should be available on day 10
        self.assertEqual(result.available_quantity, Decimal("100"))
        self.assertEqual(result.available_date, day10)
    
    def test_cumulative_atp_netting(self):
        """Test that ATP is calculated cumulatively with proper netting."""
        on_hand_id = uuid4()
        
        day1 = self.request_date + timedelta(days=1)
        day2 = self.request_date + timedelta(days=2)
        
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 100, self.request_date
        )
        self.mock_cursor.fetchall.side_effect = [
            [
                (uuid4(), self.item_id, self.location_id, 50, day2, 0),  # Supply on day 2
            ],
            [
                (uuid4(), self.item_id, self.location_id, 60, self.request_date, 0, False),  # Demand day 0
                (uuid4(), self.item_id, self.location_id, 40, day1, 0, False),  # Demand day 1
            ]
        ]
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("50"), self.request_date
        )
        
        # Day 0: 100 - 60 = 40
        # Day 1: 40 - 40 = 0
        # Day 2: 0 + 50 = 50
        # Request 50 should be available on day 2
        self.assertEqual(result.available_quantity, Decimal("50"))
        self.assertEqual(result.available_date, day2)
    
    def test_request_beyond_horizon(self):
        """Test request date beyond calculation horizon."""
        self.mock_cursor.fetchone.return_value = None
        self.mock_cursor.fetchall.return_value = []
        
        # Request date 400 days out (beyond default 365 day horizon)
        future_date = self.request_date + timedelta(days=400)
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("50"), future_date
        )
        
        self.assertEqual(result.available_quantity, Decimal("0"))
        self.assertIsNone(result.available_date)


class TestATPEngineEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.item_id = uuid4()
        self.location_id = uuid4()
        self.request_date = date.today()
        
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value.__enter__ = Mock(return_value=self.mock_cursor)
        self.mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        
        self.engine = ATPEngine(db_conn=self.mock_conn)
    
    def test_zero_quantity_request(self):
        """Test request for zero quantity."""
        self.mock_cursor.fetchone.return_value = None
        self.mock_cursor.fetchall.return_value = []
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("0"), self.request_date
        )
        
        self.assertEqual(result.available_quantity, Decimal("0"))
    
    def test_exact_atp_match(self):
        """Test when request exactly matches available ATP."""
        on_hand_id = uuid4()
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 100, self.request_date
        )
        self.mock_cursor.fetchall.side_effect = [
            [],
            [(uuid4(), self.item_id, self.location_id, 50, self.request_date, 0, False)]
        ]
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("50"), self.request_date
        )
        
        # ATP = 100 - 50 = 50, request is 50 -> exact match
        self.assertEqual(result.available_quantity, Decimal("50"))
        self.assertTrue(result.is_fully_available)
    
    def test_negative_atp_scenario(self):
        """Test scenario where ATP goes negative."""
        on_hand_id = uuid4()
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 50, self.request_date
        )
        self.mock_cursor.fetchall.side_effect = [
            [],
            [(uuid4(), self.item_id, self.location_id, 100, self.request_date, 0, False)]
        ]
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("10"), self.request_date
        )
        
        # ATP = 50 - 100 = -50 (negative)
        # No availability without planned supply
        self.assertEqual(result.available_quantity, Decimal("0"))
        self.assertIsNone(result.available_date)
    
    def test_bucket_calculation(self):
        """Test that buckets are calculated correctly."""
        on_hand_id = uuid4()
        self.mock_cursor.fetchone.return_value = (
            on_hand_id, self.item_id, self.location_id, 100, self.request_date
        )
        self.mock_cursor.fetchall.return_value = []
        
        result = self.engine.calculate(
            self.item_id, self.location_id, Decimal("10"), self.request_date, horizon_days=5
        )
        
        # Should have 5 daily buckets
        self.assertEqual(len(result.buckets), 5)
        
        # First bucket should start on request_date
        self.assertEqual(result.buckets[0].bucket_start, self.request_date)
        
        # First bucket should have opening ATP = 100 (on-hand)
        self.assertEqual(result.buckets[0].opening_atp, Decimal("100"))


class TestATPEngineNoDatabase(unittest.TestCase):
    """Test engine behavior without database connection."""
    
    def test_no_connection_raises_error(self):
        """Test that calculate() raises error without DB connection."""
        engine = ATPEngine(db_conn=None)
        
        with self.assertRaises(ValueError) as context:
            engine.calculate(
                uuid4(), uuid4(), Decimal("10"), date.today()
            )
        
        self.assertIn("Database connection not set", str(context.exception))
    
    def test_connection_setter(self):
        """Test that connection can be set after initialization."""
        engine = ATPEngine(db_conn=None)
        mock_conn = MagicMock()
        
        engine.connection = mock_conn
        
        self.assertEqual(engine.connection, mock_conn)


class TestATPEnginePerformance(unittest.TestCase):
    """Test performance requirements."""
    
    def test_performance_one_year_horizon(self):
        """Test that calculation completes in <100ms for 1 year horizon."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        
        # Mock on-hand supply
        mock_cursor.fetchone.return_value = (
            uuid4(), uuid4(), uuid4(), 1000, date.today()
        )
        # Mock some planned supplies and demands
        mock_cursor.fetchall.side_effect = [
            [(uuid4(), uuid4(), uuid4(), 100, date.today() + timedelta(days=i*30), 0) for i in range(1, 13)],
            [(uuid4(), uuid4(), uuid4(), 50, date.today() + timedelta(days=i*15), 0, False) for i in range(1, 25)]
        ]
        
        engine = ATPEngine(db_conn=mock_conn)
        
        result = engine.calculate(
            uuid4(), uuid4(), Decimal("100"), date.today(), horizon_days=365
        )
        
        # Performance requirement: <100ms
        self.assertLess(result.calculation_time_ms, 100, 
            f"Calculation took {result.calculation_time_ms:.2f}ms, should be <100ms")


class TestATPEngineHelpers(unittest.TestCase):
    """Test helper methods."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_conn = MagicMock()
        self.engine = ATPEngine(db_conn=self.mock_conn)
    
    def test_check_available_true(self):
        """Test check_available returns True when available."""
        with patch.object(self.engine, 'calculate') as mock_calc:
            mock_calc.return_value = MagicMock(
                is_fully_available=True,
                available_quantity=Decimal("50")
            )
            
            result = self.engine.check_available(
                uuid4(), uuid4(), Decimal("50"), date.today()
            )
            
            self.assertTrue(result)
    
    def test_check_available_false(self):
        """Test check_available returns False when not available."""
        with patch.object(self.engine, 'calculate') as mock_calc:
            mock_calc.return_value = MagicMock(
                is_fully_available=False,
                available_quantity=Decimal("20")
            )
            
            result = self.engine.check_available(
                uuid4(), uuid4(), Decimal("50"), date.today()
            )
            
            self.assertFalse(result)
    
    def test_get_available_date(self):
        """Test get_available_date returns correct date."""
        expected_date = date.today() + timedelta(days=5)
        
        with patch.object(self.engine, 'calculate') as mock_calc:
            mock_calc.return_value = MagicMock(
                available_date=expected_date
            )
            
            result = self.engine.get_available_date(
                uuid4(), uuid4(), Decimal("50")
            )
            
            self.assertEqual(result, expected_date)


if __name__ == '__main__':
    unittest.main()
