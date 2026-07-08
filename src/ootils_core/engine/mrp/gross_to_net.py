"""
MRP materialization DTOs (ADR-020 PAS 4 / #423 PR2).

The APICS write-path engine (``mrp_apics_engine``) delegates ALL of its MRP math
to the consolidated core (``engine/mrp/core.py`` + ``loader.py``) and keeps only
graph materialization. What used to live here — ``GrossToNetCalculator``, a
SECOND implementation of the APICS projected-on-hand / netting chain — has been
removed: the core is the single source of MRP truth, and parity between the two
is now a hard CI guard (``scripts/parity_mrp_engines.py``, no longer xfail).

Only the two data-transfer objects survive, because they are the interchange
format between the core's planned-order tuples and the graph writer
(``graph_integration``): ``BucketRecord`` (one materializable time-phased row)
and ``TimeBucket`` (a weekly period descriptor). They carry no math.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID


@dataclass
class TimeBucket:
    """A time period in the planning horizon."""
    sequence: int
    start: date
    end: date
    grain: str = "week"  # day, week, month


@dataclass
class BucketRecord:
    """One materializable MRP row for an item/location.

    Fields follow the APICS grid layout the graph writer and the
    ``mrp_bucket_records`` table expect:
      Period | GR | SR | PAB | NR | POR | PORel | POH_after

    ``release_period_start`` (#423 PR2) lets the caller pass the core's OWN
    release-bucket date explicitly rather than re-deriving it from a lead-time
    offset: the core already accounts for past-due clamping (release clamped to
    bucket 0), so the materialized release node lands exactly on the core's
    release bucket. When ``None``, the writer falls back to the historical
    ``period_start − lead_time_days`` offset.
    """
    bucket_id: UUID
    item_id: UUID
    location_id: Optional[UUID]
    period_start: date
    period_end: date
    bucket_sequence: int

    # --- Inputs ---
    gross_requirements: Decimal = Decimal("0")
    scheduled_receipts: Decimal = Decimal("0")

    # --- Gross-to-net (PAB = POH(t-1) + SR - GR ; NR = max(0, SS - PAB)) ---
    projected_on_hand: Decimal = Decimal("0")
    net_requirements: Decimal = Decimal("0")

    # --- Lot-sizing / lead-time offset ---
    planned_order_receipts: Decimal = Decimal("0")
    planned_order_releases: Decimal = Decimal("0")

    # --- Post-lot-sizing ---
    projected_on_hand_after: Decimal = Decimal("0")

    # --- Flags & metadata ---
    has_shortage: bool = False
    shortage_qty: Decimal = Decimal("0")
    llc: int = 0
    time_fence_zone: Optional[str] = None
    lot_size_rule_applied: Optional[str] = None
    release_period_start: Optional[date] = None
