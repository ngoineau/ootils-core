"""
Forecasting module for Ootils Core.

Provides statistical forecasting algorithms (MA, ES, Croston) via a unified
ForecastingEngine service interface.
"""

from .engine import ForecastingEngine, ForecastMethod, ForecastResult, AccuracyMetrics
from .algorithms import (
    Forecaster,
    MovingAverageForecaster,
    ExponentialSmoothingForecaster,
    CrostonForecaster,
    ForecastingError,
    create_forecaster,
)

__all__ = [
    "ForecastingEngine",
    "ForecastMethod",
    "ForecastResult",
    "AccuracyMetrics",
    "Forecaster",
    "MovingAverageForecaster",
    "ExponentialSmoothingForecaster",
    "CrostonForecaster",
    "ForecastingError",
    "create_forecaster",
]
