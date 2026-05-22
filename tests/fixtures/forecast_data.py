"""
Test fixtures for forecasting module.

Provides reusable test datasets for forecast generation, BOM structures,
and routing definitions used in Phase 1 integration tests.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import List
from uuid import UUID, uuid4


@dataclass
class ForecastFixture:
    """Test fixture for forecast data."""
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    historical_demand: List[Decimal]
    forecast_horizon_days: int = 90
    expected_method: str = "MA"
    expected_value_range: tuple = (Decimal("0"), Decimal("999999"))


@dataclass
class BOMFixture:
    """Test fixture for Bill of Materials structure."""
    parent_item_id: UUID
    component_item_id: UUID
    quantity_per: Decimal
    effective_date: date = field(default_factory=date.today)
    expiration_date: date = None


@dataclass
class RoutingFixture:
    """Test fixture for manufacturing routing."""
    item_id: UUID
    operation_sequence: int
    work_center_id: UUID
    setup_time_hours: Decimal
    run_time_per_unit_hours: Decimal
    description: str = ""


# ─────────────────────────────────────────────────────────────
# Pre-built Test Datasets
# ─────────────────────────────────────────────────────────────

class ForecastDatasets:
    """Pre-built forecast test datasets."""
    
    @staticmethod
    def stable_demand() -> ForecastFixture:
        """Stable demand pattern - ideal for Moving Average."""
        return ForecastFixture(
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            historical_demand=[
                Decimal("100"), Decimal("105"), Decimal("98"), Decimal("102"),
                Decimal("101"), Decimal("99"), Decimal("103"), Decimal("100"),
                Decimal("102"), Decimal("98"), Decimal("101"), Decimal("100"),
            ],
            forecast_horizon_days=30,
            expected_method="MA",
            expected_value_range=(Decimal("95"), Decimal("105")),
        )
    
    @staticmethod
    def trending_demand() -> ForecastFixture:
        """Upward trending demand - tests exponential smoothing."""
        return ForecastFixture(
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            historical_demand=[
                Decimal("100"), Decimal("110"), Decimal("120"), Decimal("130"),
                Decimal("140"), Decimal("150"), Decimal("160"), Decimal("170"),
                Decimal("180"), Decimal("190"), Decimal("200"), Decimal("210"),
            ],
            forecast_horizon_days=30,
            expected_method="EXP_SMOOTHING",
            expected_value_range=(Decimal("200"), Decimal("250")),
        )
    
    @staticmethod
    def intermittent_demand() -> ForecastFixture:
        """Intermittent demand with zeros - tests Croston's method."""
        return ForecastFixture(
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            historical_demand=[
                Decimal("0"), Decimal("50"), Decimal("0"), Decimal("0"),
                Decimal("30"), Decimal("0"), Decimal("0"), Decimal("45"),
                Decimal("0"), Decimal("0"), Decimal("0"), Decimal("60"),
            ],
            forecast_horizon_days=60,
            expected_method="CROSTON",
            expected_value_range=(Decimal("0"), Decimal("100")),
        )
    
    @staticmethod
    def seasonal_demand() -> ForecastFixture:
        """Seasonal demand pattern - tests seasonal decomposition."""
        return ForecastFixture(
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            historical_demand=[
                Decimal("200"), Decimal("150"), Decimal("100"), Decimal("120"),
                Decimal("220"), Decimal("160"), Decimal("110"), Decimal("130"),
                Decimal("210"), Decimal("155"), Decimal("105"), Decimal("125"),
            ],
            forecast_horizon_days=90,
            expected_method="SEASONAL",
            expected_value_range=(Decimal("100"), Decimal("250")),
        )


class BOMDatasets:
    """Pre-built BOM test datasets."""
    
    @staticmethod
    def simple_bom() -> List[BOMFixture]:
        """Simple 2-level BOM: Finished Good → Component."""
        parent_id = uuid4()
        component_id = uuid4()
        
        return [
            BOMFixture(
                parent_item_id=parent_id,
                component_item_id=component_id,
                quantity_per=Decimal("2.0"),
                description="2 units of component per finished good",
            ),
        ]
    
    @staticmethod
    def multi_level_bom() -> List[BOMFixture]:
        """3-level BOM: FG → Sub-assembly → Raw material."""
        fg_id = uuid4()
        subassembly_id = uuid4()
        raw_material_id = uuid4()
        
        return [
            BOMFixture(
                parent_item_id=fg_id,
                component_item_id=subassembly_id,
                quantity_per=Decimal("1.0"),
                description="1 sub-assembly per finished good",
            ),
            BOMFixture(
                parent_item_id=subassembly_id,
                component_item_id=raw_material_id,
                quantity_per=Decimal("4.0"),
                description="4 raw materials per sub-assembly",
            ),
        ]
    
    @staticmethod
    def complex_bom() -> List[BOMFixture]:
        """Complex BOM with multiple components at each level."""
        fg_id = uuid4()
        subassembly_a_id = uuid4()
        subassembly_b_id = uuid4()
        raw_material_x_id = uuid4()
        raw_material_y_id = uuid4()
        raw_material_z_id = uuid4()
        
        return [
            # Level 1: FG → Sub-assemblies
            BOMFixture(
                parent_item_id=fg_id,
                component_item_id=subassembly_a_id,
                quantity_per=Decimal("2.0"),
                description="2 sub-assembly A per FG",
            ),
            BOMFixture(
                parent_item_id=fg_id,
                component_item_id=subassembly_b_id,
                quantity_per=Decimal("1.0"),
                description="1 sub-assembly B per FG",
            ),
            # Level 2: Sub-assembly A → Raw materials
            BOMFixture(
                parent_item_id=subassembly_a_id,
                component_item_id=raw_material_x_id,
                quantity_per=Decimal("3.0"),
                description="3 raw material X per sub-assembly A",
            ),
            BOMFixture(
                parent_item_id=subassembly_a_id,
                component_item_id=raw_material_y_id,
                quantity_per=Decimal("2.0"),
                description="2 raw material Y per sub-assembly A",
            ),
            # Level 2: Sub-assembly B → Raw materials
            BOMFixture(
                parent_item_id=subassembly_b_id,
                component_item_id=raw_material_z_id,
                quantity_per=Decimal("5.0"),
                description="5 raw material Z per sub-assembly B",
            ),
        ]


class RoutingDatasets:
    """Pre-built routing test datasets."""
    
    @staticmethod
    def single_operation() -> List[RoutingFixture]:
        """Single operation routing."""
        item_id = uuid4()
        wc_id = uuid4()
        
        return [
            RoutingFixture(
                item_id=item_id,
                operation_sequence=10,
                work_center_id=wc_id,
                setup_time_hours=Decimal("0.5"),
                run_time_per_unit_hours=Decimal("0.1"),
                description="Assembly operation",
            ),
        ]
    
    @staticmethod
    def multi_operation() -> List[RoutingFixture]:
        """Multi-operation routing with 3 steps."""
        item_id = uuid4()
        wc_cutting = uuid4()
        wc_welding = uuid4()
        wc_painting = uuid4()
        
        return [
            RoutingFixture(
                item_id=item_id,
                operation_sequence=10,
                work_center_id=wc_cutting,
                setup_time_hours=Decimal("1.0"),
                run_time_per_unit_hours=Decimal("0.25"),
                description="Cutting operation",
            ),
            RoutingFixture(
                item_id=item_id,
                operation_sequence=20,
                work_center_id=wc_welding,
                setup_time_hours=Decimal("2.0"),
                run_time_per_unit_hours=Decimal("0.5"),
                description="Welding operation",
            ),
            RoutingFixture(
                item_id=item_id,
                operation_sequence=30,
                work_center_id=wc_painting,
                setup_time_hours=Decimal("0.5"),
                run_time_per_unit_hours=Decimal("0.15"),
                description="Painting operation",
            ),
        ]
    
    @staticmethod
    def high_volume_routing() -> List[RoutingFixture]:
        """Routing optimized for high-volume production."""
        item_id = uuid4()
        wc_auto = uuid4()
        
        return [
            RoutingFixture(
                item_id=item_id,
                operation_sequence=10,
                work_center_id=wc_auto,
                setup_time_hours=Decimal("0.1"),
                run_time_per_unit_hours=Decimal("0.02"),
                description="Automated high-speed operation",
            ),
        ]
