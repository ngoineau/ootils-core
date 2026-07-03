"""
Pyramide forecast layer.

Pyramide is the stochastic demand-forecast boundary for ootils-core. It keeps
forecast generation and metadata outside the deterministic engine and exposes
only frozen forecast values as deterministic artifacts.
"""

from .models import (
    PyramideRunConfig,
    PyramideRunResult,
    PyramideValue,
    SUPPORTED_GRANULARITIES,
    SUPPORTED_METHODS,
)
from .engines import PyramideForecastEngine
from .routing import (
    RoutingDecision,
    RoutingError,
    RoutingThresholds,
    SeriesFeatures,
    route,
    seasonal_strength,
)
from .runner import PyramideError, PyramideRunner

__all__ = [
    "PyramideError",
    "PyramideRunConfig",
    "PyramideRunResult",
    "PyramideForecastEngine",
    "PyramideRunner",
    "PyramideValue",
    "RoutingDecision",
    "RoutingError",
    "RoutingThresholds",
    "SeriesFeatures",
    "route",
    "seasonal_strength",
    "SUPPORTED_GRANULARITIES",
    "SUPPORTED_METHODS",
]
