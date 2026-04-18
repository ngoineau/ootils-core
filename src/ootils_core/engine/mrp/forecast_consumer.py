"""
Forecast Consumer — APICS-Compliant Forecast Consumption Engine.

Implements the 4 standard APICS CPIM Part 2 (Module 4 — Demand Management)
forecast consumption strategies:

  MAX           → net_demand = max(forecast, customer_orders)
  FORECAST_ONLY → net_demand = forecast (ignore customer orders entirely)
  ORDERS_ONLY   → net_demand = customer_orders (ignore forecast entirely)
  PRIORITY      → Consume forecast first; orders consume remaining forecast;
                  net_demand = max(remaining_forecast, customer_orders)

Critical design principle: **DOUBLE COUNTING PREVENTION**
Forecast and customer orders are NEVER summed. Within the demand fence,
the larger of the two drives net demand; beyond the fence, only forecast
counts (orders aren't confirmed enough yet).

Architecture: pure-logic core (no DB) + thin DB wrapper.
Unit tests hit the pure core exclusively; the DB layer is an integration concern.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Sequence, Tuple
from uuid import UUID

logger = logging.getLogger(__name__)


# ─── Strategy Enum ───────────────────────────────────────────────────────────

class ConsumptionStrategy(str, Enum):
    """APICS forecast consumption strategies.

    Values match the canonical names used in planning parameters:
      MAX, FORECAST_ONLY, ORDERS_ONLY, PRIORITY
    """
    MAX = "MAX"
    FORECAST_ONLY = "FORECAST_ONLY"
    ORDERS_ONLY = "ORDERS_ONLY"
    PRIORITY = "PRIORITY"

    # Backward-compatible aliases from the old DB enum values
    @classmethod
    def _missing_(cls, value: object) -> Optional["ConsumptionStrategy"]:
        alias_map = {
            "max_only": cls.MAX,
            "consume_forward": cls.PRIORITY,   # closest old analogue
            "consume_backward": cls.PRIORITY,
            "consume_both": cls.PRIORITY,
        }
        if isinstance(value, str) and value in alias_map:
            return alias_map[value]
        return None


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ConsumedBucket:
    """Immutable result of forecast consumption for one weekly period."""
    period_start: date
    period_end: date
    original_forecast: Decimal
    customer_orders: Decimal
    net_demand: Decimal
    consumed_forecast: Decimal       # how much forecast was consumed by orders
    remaining_forecast: Decimal      # forecast left after order consumption
    over_consumption: Decimal        # orders beyond available forecast
    strategy: str
    is_within_fence: bool


# ─── Pure-Logic Core ──────────────────────────────────────────────────────────

class ForecastConsumerCore:
    """
    DB-free, pure-logic forecast consumption engine.

    All inputs are plain dicts of {period_start: Decimal}.
    This class is the unit-test target; the DB-backed ForecastConsumer
    delegates to it after loading data.
    """

    @staticmethod
    def week_start(d: date) -> date:
        """Return the Monday of the ISO week containing *d*."""
        return d - timedelta(days=d.weekday())

    @staticmethod
    def week_end(week_start_date: date) -> date:
        """Return the Monday after the ISO week (exclusive end)."""
        return week_start_date + timedelta(days=7)

    @classmethod
    def align_to_weeks(
        cls,
        forecasts: Sequence[Tuple[date, Decimal]],
        customer_orders: Sequence[Tuple[date, Decimal]],
        horizon_weeks: int = 26,
        start_date: Optional[date] = None,
    ) -> Tuple[Dict[date, Decimal], Dict[date, Decimal]]:
        """
        Bucket raw (date, qty) tuples into weekly aggregates.

        Returns (forecast_map, orders_map) keyed by week-start Monday.
        """
        f_map: Dict[date, Decimal] = defaultdict(Decimal)
        o_map: Dict[date, Decimal] = defaultdict(Decimal)

        for d, qty in forecasts:
            ws = cls.week_start(d)
            f_map[ws] += qty

        for d, qty in customer_orders:
            ws = cls.week_start(d)
            o_map[ws] += qty

        # Ensure all weeks in horizon are present (even with zero)
        if start_date is None:
            start_date = date.today()
        ws = cls.week_start(start_date)
        for _ in range(horizon_weeks):
            f_map.setdefault(ws, Decimal("0"))
            o_map.setdefault(ws, Decimal("0"))
            ws += timedelta(days=7)

        return dict(f_map), dict(o_map)

    @classmethod
    def consume(
        cls,
        forecasts: Sequence[Tuple[date, Decimal]],
        customer_orders: Sequence[Tuple[date, Decimal]],
        strategy: ConsumptionStrategy = ConsumptionStrategy.MAX,
        demand_fence_weeks: int = 0,
        horizon_weeks: int = 26,
        start_date: Optional[date] = None,
    ) -> List[ConsumedBucket]:
        """
        Main entry point for pure-logic forecast consumption.

        Args:
            forecasts:  list of (period_date, qty)
            customer_orders: list of (period_date, qty)
            strategy: which APICS consumption strategy to apply
            demand_fence_weeks: weeks from start_date within which both
                forecast and orders are considered; beyond the fence,
                only forecast counts (orders are too uncertain).
            horizon_weeks: how many weekly buckets to produce
            start_date: anchor date (defaults to today)

        Returns:
            List[ConsumedBucket] sorted by period_start.
        """
        if start_date is None:
            start_date = date.today()

        f_map, o_map = cls.align_to_weeks(
            forecasts, customer_orders, horizon_weeks, start_date
        )

        # Compute fence boundary
        fence_end = cls.week_start(start_date) + timedelta(weeks=demand_fence_weeks)

        # Apply the chosen strategy
        dispatch = {
            ConsumptionStrategy.MAX: cls._strategy_max,
            ConsumptionStrategy.FORECAST_ONLY: cls._strategy_forecast_only,
            ConsumptionStrategy.ORDERS_ONLY: cls._strategy_orders_only,
            ConsumptionStrategy.PRIORITY: cls._strategy_priority,
        }
        handler = dispatch.get(strategy, cls._strategy_max)

        return handler(f_map, o_map, fence_end)

    # ── Strategy: MAX ────────────────────────────────────────────────────

    @classmethod
    def _strategy_max(
        cls,
        f_map: Dict[date, Decimal],
        o_map: Dict[date, Decimal],
        fence_end: date,
    ) -> List[ConsumedBucket]:
        """
        MAX strategy (APICS default).

        Within fence:  net_demand = max(forecast, orders)
        Beyond fence:  net_demand = forecast  (orders not confirmed enough)

        This is the #1 defence against double counting:
        forecast + orders are NEVER summed; we take the larger.
        """
        results: List[ConsumedBucket] = []

        for period_start in sorted(f_map.keys()):
            period_end = cls.week_end(period_start)
            fc = f_map.get(period_start, Decimal("0"))
            co = o_map.get(period_start, Decimal("0"))
            within_fence = period_start < fence_end

            if within_fence:
                net_demand = max(fc, co)
            else:
                net_demand = fc

            consumed_fc = min(fc, co) if within_fence else Decimal("0")
            remaining_fc = max(Decimal("0"), fc - consumed_fc)
            over_consumption = max(Decimal("0"), co - fc) if within_fence else Decimal("0")

            results.append(ConsumedBucket(
                period_start=period_start,
                period_end=period_end,
                original_forecast=fc,
                customer_orders=co,
                net_demand=net_demand,
                consumed_forecast=consumed_fc,
                remaining_forecast=remaining_fc,
                over_consumption=over_consumption,
                strategy=ConsumptionStrategy.MAX.value,
                is_within_fence=within_fence,
            ))

        return results

    # ── Strategy: FORECAST_ONLY ──────────────────────────────────────────

    @classmethod
    def _strategy_forecast_only(
        cls,
        f_map: Dict[date, Decimal],
        o_map: Dict[date, Decimal],
        fence_end: date,
    ) -> List[ConsumedBucket]:
        """
        FORECAST_ONLY strategy.

        Ignore customer orders entirely. net_demand = forecast always.
        Used when orders are too volatile or unreliable to factor in.
        """
        results: List[ConsumedBucket] = []

        for period_start in sorted(f_map.keys()):
            period_end = cls.week_end(period_start)
            fc = f_map.get(period_start, Decimal("0"))
            co = o_map.get(period_start, Decimal("0"))
            within_fence = period_start < fence_end

            results.append(ConsumedBucket(
                period_start=period_start,
                period_end=period_end,
                original_forecast=fc,
                customer_orders=co,
                net_demand=fc,
                consumed_forecast=Decimal("0"),
                remaining_forecast=fc,
                over_consumption=Decimal("0"),
                strategy=ConsumptionStrategy.FORECAST_ONLY.value,
                is_within_fence=within_fence,
            ))

        return results

    # ── Strategy: ORDERS_ONLY ────────────────────────────────────────────

    @classmethod
    def _strategy_orders_only(
        cls,
        f_map: Dict[date, Decimal],
        o_map: Dict[date, Decimal],
        fence_end: date,
    ) -> List[ConsumedBucket]:
        """
        ORDERS_ONLY strategy.

        Ignore forecast entirely. Within fence: net_demand = orders.
        Beyond fence: net_demand = 0 (no forecast, no confirmed orders).
        Used for make-to-order items where forecast is irrelevant.
        """
        results: List[ConsumedBucket] = []

        for period_start in sorted(f_map.keys()):
            period_end = cls.week_end(period_start)
            fc = f_map.get(period_start, Decimal("0"))
            co = o_map.get(period_start, Decimal("0"))
            within_fence = period_start < fence_end

            net_demand = co if within_fence else Decimal("0")

            results.append(ConsumedBucket(
                period_start=period_start,
                period_end=period_end,
                original_forecast=fc,
                customer_orders=co,
                net_demand=net_demand,
                consumed_forecast=Decimal("0"),
                remaining_forecast=Decimal("0"),
                over_consumption=Decimal("0"),
                strategy=ConsumptionStrategy.ORDERS_ONLY.value,
                is_within_fence=within_fence,
            ))

        return results

    # ── Strategy: PRIORITY ───────────────────────────────────────────────

    @classmethod
    def _strategy_priority(
        cls,
        f_map: Dict[date, Decimal],
        o_map: Dict[date, Decimal],
        fence_end: date,
    ) -> List[ConsumedBucket]:
        """
        PRIORITY strategy (APICS CPIM "forecast consumption" pattern).

        Within fence:
          1. Orders consume forecast: consumed = min(forecast, orders)
          2. remaining_forecast = max(0, forecast - orders)
          3. over_consumption = max(0, orders - forecast)
          4. net_demand = max(forecast, orders) — same magnitude as MAX,
             but PRIORITY tracks the *composition*: how much forecast was
             consumed and whether orders exceeded forecast.

        Beyond fence:
          net_demand = forecast only (orders too uncertain)

        The key distinction from MAX: PRIORITY tracks *how much* forecast was
        consumed by orders and *how much* over-consumption occurred — information
        that MAX discards. For planning purposes, net_demand equals MAX.
        """
        results: List[ConsumedBucket] = []

        for period_start in sorted(f_map.keys()):
            period_end = cls.week_end(period_start)
            fc = f_map.get(period_start, Decimal("0"))
            co = o_map.get(period_start, Decimal("0"))
            within_fence = period_start < fence_end

            if within_fence:
                # Orders consume forecast first
                consumed_fc = min(fc, co)
                remaining_fc = max(Decimal("0"), fc - co)
                over_consumption = max(Decimal("0"), co - fc)
                # Net demand = max(forecast, orders) — same result as MAX
                # The PRIORITY distinction is in *tracking* consumption,
                # not in the net_demand value. PRIORITY records how much
                # forecast was consumed and how much over-consumption occurred.
                net_demand = max(fc, co)
            else:
                # Beyond fence: only forecast counts
                consumed_fc = Decimal("0")
                remaining_fc = fc
                over_consumption = Decimal("0")
                net_demand = fc

            results.append(ConsumedBucket(
                period_start=period_start,
                period_end=period_end,
                original_forecast=fc,
                customer_orders=co,
                net_demand=net_demand,
                consumed_forecast=consumed_fc,
                remaining_forecast=remaining_fc,
                over_consumption=over_consumption,
                strategy=ConsumptionStrategy.PRIORITY.value,
                is_within_fence=within_fence,
            ))

        return results


# ─── DB-Backed Wrapper ────────────────────────────────────────────────────────

class ForecastConsumer:
    """
    DB-backed forecast consumption engine.

    Delegates all computation to ForecastConsumerCore (pure logic).
    This class handles:
      - Loading forecast/order data from the nodes table
      - Persisting results to forecast_consumption_log
      - Integration with MrpApicsEngine
    """

    def __init__(self, db, scenario_id: UUID):
        self.db = db
        self.scenario_id = scenario_id
        self._core = ForecastConsumerCore()

    def consume_all(
        self,
        location_id: Optional[UUID],
        horizon_days: int,
        strategy: str = "MAX",
        consumption_window_days: int = 7,
    ) -> Dict[UUID, Dict[date, Decimal]]:
        """
        Consume forecast for all items with forecast demand nodes.

        Returns:
            Dict[item_id, Dict[period_start, net_demand]]
        """
        # Resolve strategy (handles old DB enum values too)
        ConsumptionStrategy(strategy)

        items = self._get_items_with_forecast(location_id, horizon_days)
        results: Dict[UUID, Dict[date, Decimal]] = {}

        for item_id, item_location_id in items:
            params = self._get_consumption_params(item_id, item_location_id)
            item_strat = ConsumptionStrategy(params.get("strategy", strategy))
            demand_fence = params.get("demand_fence_weeks", 4)

            buckets = self.consume_item(
                item_id=item_id,
                location_id=item_location_id,
                horizon_days=horizon_days,
                strategy=item_strat,
                demand_fence_weeks=demand_fence,
            )

            results[item_id] = {
                b.period_start: b.net_demand for b in buckets
            }

        return results

    def consume_item(
        self,
        item_id: UUID,
        location_id: Optional[UUID],
        horizon_days: int,
        strategy: str = "MAX",
        demand_fence_weeks: int = 4,
        start_date: Optional[date] = None,
    ) -> List[ConsumedBucket]:
        """
        Consume forecast for a single item using the pure-logic core.

        Loads data from DB, delegates computation, returns results.
        """
        if start_date is None:
            start_date = date.today()

        strat = ConsumptionStrategy(strategy) if isinstance(strategy, str) else strategy
        horizon_weeks = (horizon_days + 6) // 7  # ceil division

        # Load from DB
        forecast_map = self._get_forecast_quantities(
            item_id, location_id, start_date,
            start_date + timedelta(days=horizon_days)
        )
        orders_map = self._get_customer_order_quantities(
            item_id, location_id, start_date,
            start_date + timedelta(days=horizon_days)
        )

        # Convert maps to sequences expected by core
        forecasts = sorted(forecast_map.items())
        orders = sorted(orders_map.items())

        # Delegate to pure core
        return self._core.consume(
            forecasts=forecasts,
            customer_orders=orders,
            strategy=strat,
            demand_fence_weeks=demand_fence_weeks,
            horizon_weeks=horizon_weeks,
            start_date=start_date,
        )

    # ── DB access methods ────────────────────────────────────────────────

    def _get_items_with_forecast(
        self,
        location_id: Optional[UUID],
        horizon_days: int,
    ) -> List[tuple]:
        """Get all (item_id, location_id) pairs that have forecast demand."""
        loc_filter = "AND location_id = %s" if location_id else ""
        params: list = [self.scenario_id]
        if location_id:
            params.append(location_id)

        rows = self.db.execute(f"""
            SELECT DISTINCT item_id, location_id
            FROM nodes
            WHERE node_type = 'ForecastDemand'
              AND scenario_id = %s
              {loc_filter}
              AND active = true
              AND quantity > 0
        """, params).fetchall()

        return [
            (UUID(str(r["item_id"])),
             UUID(str(r["location_id"])) if r["location_id"] else None)
            for r in rows
        ]

    def _get_forecast_quantities(
        self,
        item_id: UUID,
        location_id: Optional[UUID],
        start: date,
        end: date,
    ) -> Dict[date, Decimal]:
        """Get forecast quantities keyed by date."""
        loc_and = "AND location_id = %s" if location_id else ""
        params: list = [item_id, self.scenario_id]
        if location_id:
            params.append(location_id)
        params.extend([start, end])

        rows = self.db.execute(f"""
            SELECT time_span_start, COALESCE(SUM(quantity), 0) AS qty
            FROM nodes
            WHERE node_type = 'ForecastDemand'
              AND item_id = %s
              AND scenario_id = %s
              {loc_and}
              AND active = true
              AND time_span_start >= %s
              AND time_span_start < %s
            GROUP BY time_span_start
            ORDER BY time_span_start
        """, params).fetchall()

        return {row["time_span_start"]: Decimal(str(row["qty"])) for row in rows}

    def _get_customer_order_quantities(
        self,
        item_id: UUID,
        location_id: Optional[UUID],
        start: date,
        end: date,
    ) -> Dict[date, Decimal]:
        """Get customer order quantities keyed by date."""
        loc_and = "AND location_id = %s" if location_id else ""
        params: list = [item_id, self.scenario_id]
        if location_id:
            params.append(location_id)
        params.extend([start, end])

        rows = self.db.execute(f"""
            SELECT time_ref, COALESCE(SUM(quantity), 0) AS qty
            FROM nodes
            WHERE node_type = 'CustomerOrderDemand'
              AND item_id = %s
              AND scenario_id = %s
              {loc_and}
              AND active = true
              AND time_ref >= %s
              AND time_ref < %s
            GROUP BY time_ref
            ORDER BY time_ref
        """, params).fetchall()

        return {row["time_ref"]: Decimal(str(row["qty"])) for row in rows}

    def _get_consumption_params(
        self, item_id: UUID, location_id: Optional[UUID]
    ) -> dict:
        """Get forecast consumption parameters for an item/location."""
        loc_and = "AND location_id = %s" if location_id else ""
        params: list = [item_id]
        if location_id:
            params.append(location_id)

        row = self.db.execute(f"""
            SELECT forecast_consumption_strategy, consumption_window_days
            FROM item_planning_params
            WHERE item_id = %s
              {loc_and}
              AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
            ORDER BY effective_from DESC
            LIMIT 1
        """, params).fetchone()

        if row:
            return {
                "strategy": row["forecast_consumption_strategy"] or "MAX",
                "window_days": int(row["consumption_window_days"]) if row["consumption_window_days"] else 7,
            }
        return {"strategy": "MAX", "window_days": 7}

    def log_consumption(
        self,
        run_id: Optional[UUID],
        item_id: UUID,
        location_id: Optional[UUID],
        consumed_buckets: List[ConsumedBucket],
    ) -> int:
        """Log consumption results to forecast_consumption_log.

        Legacy DB compatibility:
        - older schemas require location_id NOT NULL
        - older check constraint accepts only legacy strategy names
        """
        if location_id is None:
            logger.warning(
                "Skipping forecast_consumption_log write for item %s because location_id is NULL",
                item_id,
            )
            return 0

        strategy_aliases = {
            "MAX": "max_only",
            "FORECAST_ONLY": "max_only",
            "ORDERS_ONLY": "consume_forward",
            "PRIORITY": "consume_forward",
        }

        logged = 0
        for bucket in consumed_buckets:
            db_strategy = strategy_aliases.get(bucket.strategy, bucket.strategy)
            self.db.execute("""
                INSERT INTO forecast_consumption_log (
                    run_id, item_id, location_id,
                    period_start, period_end,
                    original_forecast, customer_orders,
                    consumed_qty, remaining_forecast,
                    carry_forward, carry_backward,
                    strategy, net_demand
                ) VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s
                )
            """, (
                run_id,
                item_id,
                location_id,
                bucket.period_start,
                bucket.period_end,
                bucket.original_forecast,
                bucket.customer_orders,
                bucket.consumed_forecast,
                bucket.remaining_forecast,
                Decimal("0"),
                Decimal("0"),
                db_strategy,
                bucket.net_demand,
            ))
            logged += 1
        return logged
