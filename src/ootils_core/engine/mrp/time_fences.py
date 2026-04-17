"""
Time Fence Enforcement for APICS-compliant MRP.

Time fences define planning boundaries:
- Frozen Zone: No new planned orders (inside frozen fence)
- Slashed Zone: Planner approval required for new orders
- Liquid Zone: Free planning (outside all fences)

Based on item_planning_params.frozen_time_fence_days and
item_planning_params.slashed_time_fence_days.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Optional
from uuid import UUID

logger = logging.getLogger(__name__)


class TimeFenceZone(str, Enum):
    """Time fence zones per APICS conventions."""
    FROZEN = "FROZEN"
    SLASHED = "SLASHED"
    LIQUID = "LIQUID"


@dataclass
class TimeFenceResult:
    """Result of time fence check."""
    zone: TimeFenceZone
    frozen_fence_days: int
    slashed_fence_days: int
    can_create_order: bool
    requires_approval: bool
    fence_date: Optional[date] = None


class TimeFenceChecker:
    """Check and enforce time fences for MRP order planning."""

    def __init__(self, frozen_fence_days: int = 7, slashed_fence_days: int = 30):
        """
        Args:
            frozen_fence_days: Days from today defining the frozen zone boundary
            slashed_fence_days: Days from today defining the slashed zone boundary
        """
        self.frozen_fence_days = frozen_fence_days
        self.slashed_fence_days = slashed_fence_days

    @classmethod
    def from_planning_params(cls, params: dict) -> "TimeFenceChecker":
        """Create a TimeFenceChecker from planning params dict."""
        return cls(
            frozen_fence_days=int(params.get("frozen_time_fence_days") or 7),
            slashed_fence_days=int(params.get("slashed_time_fence_days") or 30),
        )

    def check_zone(self, target_date: date, reference_date: Optional[date] = None) -> TimeFenceResult:
        """
        Determine the time fence zone for a given date.

        Args:
            target_date: The date to check
            reference_date: Reference date (defaults to today)

        Returns:
            TimeFenceResult with zone and flags
        """
        if reference_date is None:
            reference_date = date.today()

        frozen_boundary = reference_date + timedelta(days=self.frozen_fence_days)
        slashed_boundary = reference_date + timedelta(days=self.slashed_fence_days)

        if target_date <= frozen_boundary:
            return TimeFenceResult(
                zone=TimeFenceZone.FROZEN,
                frozen_fence_days=self.frozen_fence_days,
                slashed_fence_days=self.slashed_fence_days,
                can_create_order=False,
                requires_approval=True,
                fence_date=frozen_boundary,
            )
        elif target_date <= slashed_boundary:
            return TimeFenceResult(
                zone=TimeFenceZone.SLASHED,
                frozen_fence_days=self.frozen_fence_days,
                slashed_fence_days=self.slashed_fence_days,
                can_create_order=True,
                requires_approval=True,
                fence_date=slashed_boundary,
            )
        else:
            return TimeFenceResult(
                zone=TimeFenceZone.LIQUID,
                frozen_fence_days=self.frozen_fence_days,
                slashed_fence_days=self.slashed_fence_days,
                can_create_order=True,
                requires_approval=False,
                fence_date=None,
            )

    def adjust_order_date(
        self,
        requested_date: date,
        reference_date: Optional[date] = None,
    ) -> tuple:
        """
        Adjust a planned order date based on time fences.

        In the frozen zone, push the order to the frozen boundary.
        In the slashed zone, allow but flag for approval.

        Returns:
            (adjusted_date, zone, requires_approval)
        """
        result = self.check_zone(requested_date, reference_date)

        if result.zone == TimeFenceZone.FROZEN:
            # Push to frozen boundary
            adjusted = reference_date + timedelta(days=self.frozen_fence_days) if reference_date else date.today() + timedelta(days=self.frozen_fence_days)
            return adjusted, TimeFenceZone.FROZEN, True

        return requested_date, result.zone, result.requires_approval
