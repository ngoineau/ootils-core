"""
forecasts.py — monthly ForecastDemand nodes for FG SKUs at DCs.

Each (FG, DC) gets 18 monthly buckets covering today..today+18mo. The
volume signal carries three realistic patterns:

  base_volume:   log-normal across FGs (Pareto 80/20). Drives the
                 absolute scale per item — A-class FGs get ~10x B-class,
                 B-class ~10x C-class.

  seasonality:   sinusoidal +/-25% with peak at Nov-Dec (consumer-style;
                 picked because it's the most common pattern in retail).

  trend:         per-item random walk +/- 5% per year, applied as a
                 monthly multiplicative drift. Lets the dataset exercise
                 trend-aware forecasting later.

Noise (white gaussian, sigma=8%) is layered on top so the series isn't
artificially smooth.

Volumes for profile M:
  500 FG x 3 DC x 18 months = 27 000 ForecastDemand nodes
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection
from ootils_core.seed.config import Profile
from ootils_core.seed.master.items import ItemSet
from ootils_core.seed.master.locations import LocationSet
from ootils_core.seed.transactional.nodes import BASELINE_SCENARIO_ID


# Forecast horizon — 18 months forward from today
_FORECAST_HORIZON_MONTHS = 18
# Seasonality amplitude (peak +25%, trough -25%)
_SEASON_AMPLITUDE = 0.25
# Seasonality peak month (1=Jan, 11=Nov for consumer pattern)
_SEASON_PEAK_MONTH = 11
# Trend range per year, applied per-item
_TREND_PER_YEAR_RANGE = (-0.05, 0.10)  # slight upward bias overall
# Noise (multiplicative gaussian std)
_NOISE_SIGMA = 0.08


@dataclass(frozen=True)
class ForecastNode:
    node_id: UUID
    item_id: UUID
    location_id: UUID
    quantity: Decimal
    time_span_start: date
    time_span_end: date


def _first_of_month(d: date) -> date:
    return d.replace(day=1)


def _next_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def _seasonality_factor(month: int) -> float:
    """+/-25% sine wave with peak at _SEASON_PEAK_MONTH."""
    # Shift so cosine peaks at the requested month
    phase = (month - _SEASON_PEAK_MONTH) / 12.0 * 2 * math.pi
    return 1.0 + _SEASON_AMPLITUDE * math.cos(phase)


def _draw_base_volume(rng: random.Random) -> Decimal:
    """Log-normal base volume — Pareto-like spread (some FGs huge, most modest)."""
    # mu, sigma chosen so 5th percentile ≈ 20, median ≈ 200, 95th ≈ 2000
    mu = math.log(200)
    sigma = 1.2
    raw = math.exp(rng.gauss(mu, sigma))
    # Clamp to a sane range; quantize to integer
    return Decimal(max(10, min(int(raw), 5000)))


def generate_forecasts(
    profile: Profile,
    item_set: ItemSet,
    loc_set: LocationSet,
) -> list[ForecastNode]:
    """Build monthly forecasts for every (FG, DC) pair over the horizon."""
    rng = random.Random(profile.seed + 6001)
    fgs = item_set.at_level(0)
    dcs = loc_set.dcs()
    today = date.today()
    horizon_start = _first_of_month(today)

    nodes: list[ForecastNode] = []
    for fg in fgs:
        # Some FGs are obsolete — skip forecasting them
        if fg.status == "obsolete":
            continue
        base_volume = _draw_base_volume(rng)
        # Per-item annual trend (multiplicative per month: (1+trend_annual)^(1/12))
        trend_annual = rng.uniform(*_TREND_PER_YEAR_RANGE)
        trend_monthly = (1.0 + trend_annual) ** (1.0 / 12.0) - 1.0

        for dc in dcs:
            # Per-DC scale: each DC gets 60-130% of base. Some DCs are bigger.
            dc_scale = rng.uniform(0.6, 1.3)

            month = horizon_start
            for m_idx in range(_FORECAST_HORIZON_MONTHS):
                season = _seasonality_factor(month.month)
                trend = (1.0 + trend_monthly) ** m_idx
                noise = max(0.1, rng.gauss(1.0, _NOISE_SIGMA))
                qty_float = float(base_volume) * dc_scale * season * trend * noise
                qty = Decimal(max(1, int(qty_float)))

                next_m = _next_month(month)
                nodes.append(ForecastNode(
                    node_id=uuid4(),
                    item_id=fg.item_id,
                    location_id=dc.location_id,
                    quantity=qty,
                    time_span_start=month,
                    time_span_end=next_m,
                ))
                month = next_m
    return nodes


def insert_forecasts(conn: DictRowConnection, nodes: list[ForecastNode]) -> int:
    """Bulk-insert forecasts as ForecastDemand nodes."""
    if not nodes:
        return 0
    ids = [n.node_id for n in nodes]
    item_ids = [n.item_id for n in nodes]
    loc_ids = [n.location_id for n in nodes]
    qtys = [n.quantity for n in nodes]
    starts = [n.time_span_start for n in nodes]
    ends = [n.time_span_end for n in nodes]
    cur = conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             quantity, qty_uom, time_grain, time_span_start, time_span_end,
             is_dirty, active)
        SELECT
            f.id, 'ForecastDemand', %s, f.item_id, f.loc_id,
            f.qty, 'EA', 'month', f.s, f.e, FALSE, TRUE
        FROM UNNEST(
            %s::uuid[], %s::uuid[], %s::uuid[],
            %s::numeric[], %s::date[], %s::date[]
        ) AS f(id, item_id, loc_id, qty, s, e)
        """,
        (BASELINE_SCENARIO_ID, ids, item_ids, loc_ids, qtys, starts, ends),
    )
    return cur.rowcount or 0
