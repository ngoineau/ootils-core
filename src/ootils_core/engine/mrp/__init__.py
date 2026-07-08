"""
MRP APICS-Compliant Engine for Ootils Core.

Multi-level MRP write path (POST /v1/mrp/run) built on APICS CPIM principles.
Since ADR-020 PAS 4 / #423 PR2 the MRP MATH lives in a single place — the
consolidated core (``engine/mrp/core.py`` + ``loader.py``); ``MrpApicsEngine``
delegates the calculation there and keeps only:
- Low-Level Code (LLC) maintenance for the core's cascade order
- Graph materialization (nodes, edges, events) of the core's plan

``ForecastConsumer`` and ``LotSizingEngine`` remain as standalone calculators
backing the /v1/mrp/consumption and /v1/mrp/lot-sizing endpoints; they are no
longer wired into the write-path cascade.
"""

from ootils_core.engine.mrp.mrp_apics_engine import MrpApicsEngine
from ootils_core.engine.mrp.llc_calculator import LLCCalculator
from ootils_core.engine.mrp.gross_to_net import BucketRecord, TimeBucket
from ootils_core.engine.mrp.forecast_consumer import ForecastConsumer, ForecastConsumerCore, ConsumptionStrategy
from ootils_core.engine.mrp.lot_sizing import LotSizingEngine, LotSizeRule
from ootils_core.engine.mrp.time_fences import TimeFenceChecker, TimeFenceZone
from ootils_core.engine.mrp.graph_integration import GraphIntegration

__all__ = [
    "MrpApicsEngine",
    "LLCCalculator",
    "BucketRecord",
    "TimeBucket",
    "ForecastConsumer",
    "ForecastConsumerCore",
    "ConsumptionStrategy",
    "LotSizingEngine",
    "LotSizeRule",
    "TimeFenceChecker",
    "TimeFenceZone",
    "GraphIntegration",
]
