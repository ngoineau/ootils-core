from __future__ import annotations

import calendar
from datetime import date, timedelta
from decimal import Decimal
from math import ceil
from typing import Sequence

from .engines import PyramideEngineError, PyramideForecastEngine
from .models import (
    SUPPORTED_GRANULARITIES,
    SUPPORTED_METHODS,
    PyramideRunConfig,
    PyramideRunResult,
    PyramideValue,
)


class PyramideError(ValueError):
    """Raised when a Pyramide run cannot produce a valid forecast snapshot."""


class PyramideRunner:
    """Build immutable forecast snapshots from historical demand series."""

    def __init__(self, forecast_engine: PyramideForecastEngine | None = None) -> None:
        self._forecast_engine = forecast_engine or PyramideForecastEngine()

    def run(self, config: PyramideRunConfig, history: Sequence[Decimal | float | int]) -> PyramideRunResult:
        self._validate_config(config)
        if not history:
            raise PyramideError("Historical demand is required for a Pyramide run")

        normalized_history = [self._to_decimal(value) for value in history]
        bucket_dates = self._bucket_dates(config)

        try:
            forecast = self._forecast_engine.forecast(
                history=normalized_history,
                periods=len(bucket_dates),
                method=config.method,
                method_params=config.method_params,
                model_strategy=config.model_strategy,
                granularity=config.granularity,
                horizon_start=config.horizon_start,
                random_seed=config.random_seed,
            )
        except PyramideEngineError as exc:
            raise PyramideError(str(exc)) from exc

        values = tuple(
            PyramideValue(
                bucket_index=index,
                forecast_date=bucket_date,
                quantity=max(forecast.values[index], Decimal("0")),
                method=forecast.value_method,
            )
            for index, bucket_date in enumerate(bucket_dates)
        )
        return PyramideRunResult(
            config=config,
            values=values,
            source_history_count=len(normalized_history),
            selected_model=forecast.selected_model,
            engine_backend=forecast.engine_backend,
            warnings=forecast.warnings,
        )

    @staticmethod
    def _validate_config(config: PyramideRunConfig) -> None:
        if config.horizon_days < 1:
            raise PyramideError("horizon_days must be >= 1")
        if config.granularity not in SUPPORTED_GRANULARITIES:
            raise PyramideError(f"Unsupported granularity: {config.granularity}")
        if config.method not in SUPPORTED_METHODS:
            raise PyramideError(f"Unsupported forecast method: {config.method}")

    @staticmethod
    def _to_decimal(value: Decimal | float | int) -> Decimal:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @classmethod
    def _bucket_dates(cls, config: PyramideRunConfig) -> tuple[date, ...]:
        if config.granularity == "daily":
            return tuple(config.horizon_start + timedelta(days=i) for i in range(config.horizon_days))

        if config.granularity == "weekly":
            periods = ceil(config.horizon_days / 7)
            return tuple(config.horizon_start + timedelta(days=i * 7) for i in range(periods))

        periods = cls._monthly_period_count(config.horizon_start, config.horizon_end)
        return tuple(cls._add_months(config.horizon_start, i) for i in range(periods))

    @classmethod
    def _monthly_period_count(cls, start: date, end: date) -> int:
        count = 1
        cursor = start
        while cls._add_months(cursor, 1) <= end:
            cursor = cls._add_months(cursor, 1)
            count += 1
        return count

    @staticmethod
    def _add_months(start: date, months: int) -> date:
        month_index = start.month - 1 + months
        year = start.year + month_index // 12
        month = month_index % 12 + 1
        day = min(start.day, calendar.monthrange(year, month)[1])
        return date(year, month, day)
