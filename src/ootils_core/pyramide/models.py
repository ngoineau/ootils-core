from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID

from ootils_core.forecasting import ForecastMethod

METHOD_AUTO_SELECT = "AUTO_SELECT"
METHOD_ENSEMBLE_STAT = "ENSEMBLE_STAT"
METHOD_STAT_AUTOETS = "STAT_AUTOETS"
METHOD_STAT_AUTOARIMA = "STAT_AUTOARIMA"
METHOD_ML_LGBM = "ML_LGBM"
METHOD_FM_CHRONOS = "FM_CHRONOS"
METHOD_FM_MOIRAI = "FM_MOIRAI"

SUPPORTED_METHODS = frozenset(
    {
        ForecastMethod.MA,
        ForecastMethod.EXP_SMOOTHING,
        ForecastMethod.CROSTON,
        ForecastMethod.SEASONAL,
        METHOD_AUTO_SELECT,
        METHOD_ENSEMBLE_STAT,
        METHOD_STAT_AUTOETS,
        METHOD_STAT_AUTOARIMA,
        METHOD_ML_LGBM,
        METHOD_FM_CHRONOS,
        METHOD_FM_MOIRAI,
    }
)
SUPPORTED_GRANULARITIES = frozenset({"daily", "weekly", "monthly"})


@dataclass(frozen=True)
class PyramideRunConfig:
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_days: int
    granularity: str = "daily"
    method: str = METHOD_AUTO_SELECT
    method_params: Mapping[str, Any] = field(default_factory=dict)
    model_strategy: str = "stat"
    recon_method: str = "bottomup"
    random_seed: int = 0
    code_version: str = "local"

    def __post_init__(self) -> None:
        object.__setattr__(self, "method_params", MappingProxyType(dict(self.method_params or {})))

    @property
    def horizon_end(self) -> date:
        return self.horizon_start + timedelta(days=self.horizon_days - 1)


@dataclass(frozen=True)
class PyramideValue:
    bucket_index: int
    forecast_date: date
    quantity: Decimal
    method: str


@dataclass(frozen=True)
class PyramideRunResult:
    config: PyramideRunConfig
    values: tuple[PyramideValue, ...]
    source_history_count: int
    selected_model: str
    engine_backend: str
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    warnings: tuple[str, ...] = ()

    @property
    def total_quantity(self) -> Decimal:
        total = Decimal("0")
        for value in self.values:
            total += value.quantity
        return total
