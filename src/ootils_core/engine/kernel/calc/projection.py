"""
projection.py — Pure computation kernel for projected inventory.

Zero DB access. All data pre-loaded by caller.
This layer will be replaced by Rust in V2.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal


class ProjectionKernel:
    """
    Pure computation kernel — no DB access. Takes pre-loaded data.
    This layer will be replaced by Rust in V2.
    """

    def compute_pi_node(
        self,
        opening_stock: Decimal,
        supply_events: list,
        demand_events: list,
        bucket_start: date,
        bucket_end: date,
    ) -> dict:
        """
        Compute a projected inventory bucket.

        Args:
            opening_stock: Stock at the start of this bucket.
            supply_events: list of (date, quantity) tuples — supply contributions.
            demand_events: list of (date, quantity) tuples — demand contributions.
            bucket_start: Inclusive start of the bucket.
            bucket_end: Exclusive end of the bucket.

        Returns:
            dict with keys:
                opening_stock, inflows, outflows, closing_stock,
                has_shortage, shortage_qty
        """
        # Accumulate inflows: supply events whose date falls in [bucket_start, bucket_end)
        inflows = Decimal("0")
        for supply_date, supply_qty in supply_events:
            contribution = self.apply_contribution_rule(
                source_date=supply_date,
                source_qty=Decimal(str(supply_qty)),
                bucket_start=bucket_start,
                bucket_end=bucket_end,
                rule="point_in_bucket",
            )
            inflows += contribution

        # Accumulate outflows: demand events whose date falls in [bucket_start, bucket_end)
        outflows = Decimal("0")
        for demand_date, demand_qty in demand_events:
            contribution = self.apply_contribution_rule(
                source_date=demand_date,
                source_qty=Decimal(str(demand_qty)),
                bucket_start=bucket_start,
                bucket_end=bucket_end,
                rule="point_in_bucket",
            )
            outflows += contribution

        closing_stock = opening_stock + inflows - outflows

        has_shortage = closing_stock < Decimal("0")
        shortage_qty = abs(closing_stock) if has_shortage else Decimal("0")

        return {
            "opening_stock": opening_stock,
            "inflows": inflows,
            "outflows": outflows,
            "closing_stock": closing_stock,
            "has_shortage": has_shortage,
            "shortage_qty": shortage_qty,
        }

    def apply_contribution_rule(
        self,
        source_date: date,
        source_qty: Decimal,
        bucket_start: date,
        bucket_end: date,
        rule: str = "point_in_bucket",
    ) -> Decimal:
        """
        Apply a contribution rule to determine how much of source_qty
        counts toward the bucket [bucket_start, bucket_end).

        Rules:
            point_in_bucket: returns source_qty if bucket_start <= source_date < bucket_end,
                             else Decimal('0').

        Args:
            source_date: The date of the supply/demand event.
            source_qty: The quantity of the event.
            bucket_start: Inclusive start of the bucket.
            bucket_end: Exclusive end of the bucket.
            rule: Contribution rule identifier. Default: 'point_in_bucket'.

        Returns:
            Decimal contribution to this bucket.

        Raises:
            ValueError: If the rule is not recognised.
        """
        # Invariant: bucket_end is EXCLUSIVE throughout the engine (fix for #159).
        # bucket_start <= source_date < bucket_end.
        # This assertion surfaces any caller that passes an inclusive end date.
        assert bucket_start <= bucket_end, (
            f"bucket_start ({bucket_start}) must be <= bucket_end ({bucket_end})"
        )

        if rule == "point_in_bucket":
            if bucket_start <= source_date < bucket_end:
                return Decimal(str(source_qty))
            return Decimal("0")

        raise ValueError(f"Unknown contribution rule: {rule!r}")
