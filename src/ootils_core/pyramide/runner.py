from __future__ import annotations

import calendar
from datetime import date, timedelta
from decimal import Decimal
from math import ceil
from typing import Sequence

from .engines import PyramideEngineError, PyramideForecastEngine, conformal_bounds
from .models import (
    SUPPORTED_GRANULARITIES,
    SUPPORTED_METHODS,
    PyramideRunConfig,
    PyramideRunResult,
    PyramideValue,
)


class PyramideError(ValueError):
    """Raised when a Pyramide run cannot produce a valid forecast snapshot."""


def bucket_dates(horizon_start: date, horizon_days: int, granularity: str) -> tuple[date, ...]:
    """Forecast bucket start dates for a horizon — the single definition
    shared by the leaf runner (PyramideRunner) and the hierarchical
    runner (hierarchy/runner.py), so the two can never bucket a horizon
    differently."""
    if granularity == "daily":
        return tuple(horizon_start + timedelta(days=i) for i in range(horizon_days))

    if granularity == "weekly":
        periods = ceil(horizon_days / 7)
        return tuple(horizon_start + timedelta(days=i * 7) for i in range(periods))

    horizon_end = horizon_start + timedelta(days=horizon_days - 1)
    periods = _monthly_period_count(horizon_start, horizon_end)
    return tuple(_add_months(horizon_start, i) for i in range(periods))


def _monthly_period_count(start: date, end: date) -> int:
    count = 1
    cursor = start
    while _add_months(cursor, 1) <= end:
        cursor = _add_months(cursor, 1)
        count += 1
    return count


def _add_months(start: date, months: int) -> date:
    month_index = start.month - 1 + months
    year = start.year + month_index // 12
    month = month_index % 12 + 1
    day = min(start.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


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
            # Bornes conformal par bucket, dérivées des résidus du backtest
            # du modèle qui a PRODUIT les valeurs (accuracy_report). Pas de
            # rapport / trop peu de résidus → bornes None + provenance dans
            # les warnings (jamais des bornes inventées).
            lowers, uppers, conformal_warnings = conformal_bounds(
                report=forecast.accuracy_report,
                values=forecast.values[: len(bucket_dates)],
                method_params=config.method_params,
            )
        except PyramideEngineError as exc:
            raise PyramideError(str(exc)) from exc

        values = tuple(
            PyramideValue(
                bucket_index=index,
                forecast_date=bucket_date,
                quantity=max(forecast.values[index], Decimal("0")),
                method=forecast.value_method,
                confidence_lower=lowers[index],
                confidence_upper=uppers[index],
            )
            for index, bucket_date in enumerate(bucket_dates)
        )
        return PyramideRunResult(
            config=config,
            values=values,
            source_history_count=len(normalized_history),
            selected_model=forecast.selected_model,
            engine_backend=forecast.engine_backend,
            warnings=(*forecast.warnings, *conformal_warnings),
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
        return bucket_dates(config.horizon_start, config.horizon_days, config.granularity)
