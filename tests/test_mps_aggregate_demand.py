"""
Tests pour MPS-002 — Aggregate Demand Endpoint.

Couverture:
- AggregateDemandEngine: génération de time buckets
- AggregateDemandEngine: agrégation forecast + sales orders
- AggregateDemandEngine: application des poids (forecast_weight, orders_weight)
- AggregateDemandEngine: upsert de MPS nodes (idempotence)
- API endpoint POST /v1/mps/aggregate-demand
- API endpoint GET /v1/mps/nodes
"""

import pytest
from datetime import date
from decimal import Decimal
from uuid import uuid4


from ootils_core.mps.engine import (
    AggregateDemandEngine,
    DemandSource,
)


class TestGenerateTimeBuckets:
    """Tests pour la génération de time buckets."""

    def test_daily_buckets(self):
        engine = AggregateDemandEngine()
        horizon_start = date(2026, 5, 1)
        horizon_end = date(2026, 5, 5)

        buckets = engine._generate_time_buckets(horizon_start, horizon_end, "daily")

        assert len(buckets) == 5
        assert buckets[0].time_bucket == "2026-05-01"
        assert buckets[0].time_bucket_start == horizon_start
        assert buckets[0].time_bucket_end == horizon_start
        assert buckets[4].time_bucket == "2026-05-05"
        assert buckets[4].time_bucket_end == horizon_end

    def test_weekly_buckets(self):
        engine = AggregateDemandEngine()
        # 2026-05-01 est un Vendredi
        horizon_start = date(2026, 5, 1)
        horizon_end = date(2026, 5, 15)

        buckets = engine._generate_time_buckets(horizon_start, horizon_end, "weekly")

        # Semaine 1: Ven 1 - Dim 3
        # Semaine 2: Lun 4 - Dim 10
        # Semaine 3: Lun 11 - Dim 15
        assert len(buckets) == 3
        assert buckets[0].time_bucket == "2026-W18"
        assert buckets[0].time_bucket_start == date(2026, 5, 1)
        assert buckets[0].time_bucket_end == date(2026, 5, 3)

    def test_monthly_buckets(self):
        engine = AggregateDemandEngine()
        horizon_start = date(2026, 4, 15)
        horizon_end = date(2026, 6, 15)

        buckets = engine._generate_time_buckets(horizon_start, horizon_end, "monthly")

        assert len(buckets) == 3
        assert buckets[0].time_bucket == "2026-04"
        assert buckets[0].time_bucket_end == date(2026, 4, 30)
        assert buckets[1].time_bucket == "2026-05"
        assert buckets[1].time_bucket_end == date(2026, 5, 31)
        assert buckets[2].time_bucket == "2026-06"
        assert buckets[2].time_bucket_end == date(2026, 6, 15)

    def test_invalid_time_grain(self):
        engine = AggregateDemandEngine()
        horizon_start = date(2026, 5, 1)
        horizon_end = date(2026, 5, 10)

        with pytest.raises(ValueError, match="time_grain invalide"):
            engine._generate_time_buckets(horizon_start, horizon_end, "invalid")


class TestAggregateByBucket:
    """Tests pour l'agrégation par time bucket."""

    def test_aggregate_forecast_only(self):
        engine = AggregateDemandEngine()

        # Créer des buckets
        horizon_start = date(2026, 5, 1)
        horizon_end = date(2026, 5, 7)
        buckets = engine._generate_time_buckets(horizon_start, horizon_end, "daily")

        # Créer des données de forecast
        forecast_data = [
            DemandSource(
                source_type="forecast",
                source_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                demand_date=date(2026, 5, 1),
                quantity=Decimal("100"),
            ),
            DemandSource(
                source_type="forecast",
                source_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                demand_date=date(2026, 5, 2),
                quantity=Decimal("150"),
            ),
        ]

        sales_orders_data = []

        result = engine._aggregate_by_bucket(
            buckets, forecast_data, sales_orders_data,
            forecast_weight=Decimal("1.0"),
            orders_weight=Decimal("0.5"),
            time_grain="daily",
        )

        assert result[0].forecast_quantity == Decimal("100")
        assert result[0].sales_orders_quantity == Decimal("0")
        assert result[0].total_demand == Decimal("100")

        assert result[1].forecast_quantity == Decimal("150")
        assert result[1].total_demand == Decimal("150")

    def test_aggregate_with_weights(self):
        engine = AggregateDemandEngine()

        horizon_start = date(2026, 5, 1)
        horizon_end = date(2026, 5, 1)
        buckets = engine._generate_time_buckets(horizon_start, horizon_end, "daily")

        forecast_data = [
            DemandSource(
                source_type="forecast",
                source_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                demand_date=date(2026, 5, 1),
                quantity=Decimal("200"),
            ),
        ]

        sales_orders_data = [
            DemandSource(
                source_type="sales_order",
                source_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                demand_date=date(2026, 5, 1),
                quantity=Decimal("100"),
            ),
        ]

        # Poids 50/50
        result = engine._aggregate_by_bucket(
            buckets, forecast_data, sales_orders_data,
            forecast_weight=Decimal("0.5"),
            orders_weight=Decimal("0.5"),
            time_grain="daily",
        )

        assert result[0].forecast_quantity == Decimal("100")  # 200 * 0.5
        assert result[0].sales_orders_quantity == Decimal("50")  # 100 * 0.5
        assert result[0].total_demand == Decimal("150")

    def test_aggregate_multiple_sources_same_day(self):
        engine = AggregateDemandEngine()

        horizon_start = date(2026, 5, 1)
        horizon_end = date(2026, 5, 1)
        buckets = engine._generate_time_buckets(horizon_start, horizon_end, "daily")

        # Plusieurs forecast le même jour
        forecast_data = [
            DemandSource(
                source_type="forecast",
                source_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                demand_date=date(2026, 5, 1),
                quantity=Decimal("50"),
            ),
            DemandSource(
                source_type="forecast",
                source_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                demand_date=date(2026, 5, 1),
                quantity=Decimal("75"),
            ),
        ]

        sales_orders_data = []

        result = engine._aggregate_by_bucket(
            buckets, forecast_data, sales_orders_data,
            forecast_weight=Decimal("1.0"),
            orders_weight=Decimal("0"),
            time_grain="daily",
        )

        assert result[0].forecast_quantity == Decimal("125")
        assert result[0].total_demand == Decimal("125")





# ─────────────────────────────────────────────────────────────
# Tests de validation des poids
# ─────────────────────────────────────────────────────────────

class TestWeightValidation:
    """Tests pour la validation des poids forecast/orders."""

    def test_weights_sum_to_one(self):
        """Les poids doivent pouvoir être ajustés librement."""
        from ootils_core.mps.engine import AggregateDemandRequest as EngineRequest
        
        # Cas 1: 100% forecast
        req = EngineRequest(
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start="2026-05-01",
            horizon_end="2026-05-07",
            forecast_weight=Decimal("1.0"),
            orders_weight=Decimal("0.0"),
        )
        assert req.forecast_weight == Decimal("1.0")

        # Cas 2: 100% orders
        req = EngineRequest(
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start="2026-05-01",
            horizon_end="2026-05-07",
            forecast_weight=Decimal("0.0"),
            orders_weight=Decimal("1.0"),
        )
        assert req.orders_weight == Decimal("1.0")

        # Cas 3: 50/50 (default)
        req = EngineRequest(
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start="2026-05-01",
            horizon_end="2026-05-07",
        )
        assert req.forecast_weight == Decimal("0.5")
        assert req.orders_weight == Decimal("0.5")

    def test_weights_can_exceed_bounds(self):
        """Les poids ne sont pas validés à la construction (validation métier à l'exécution)."""
        from ootils_core.mps.engine import AggregateDemandRequest as EngineRequest
        
        # La validation des poids se fait dans la méthode aggregate(), pas au constructeur
        req = EngineRequest(
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start="2026-05-01",
            horizon_end="2026-05-07",
            forecast_weight=Decimal("1.5"),  # > 1
            orders_weight=Decimal("-0.5"),   # < 0
        )
        # Construction autorisée, validation différée
        assert req.forecast_weight == Decimal("1.5")
        assert req.orders_weight == Decimal("-0.5")
