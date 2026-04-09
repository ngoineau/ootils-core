"""
Comprehensive tests for ootils_core.engine.kernel.calc.projection.ProjectionKernel

Covers:
- compute_pi_node: happy path, empty events, shortage, no shortage, boundary dates
- apply_contribution_rule: point_in_bucket in/out/boundary, unknown rule
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ootils_core.engine.kernel.calc.projection import ProjectionKernel


@pytest.fixture
def kernel():
    return ProjectionKernel()


# ---------------------------------------------------------------------------
# apply_contribution_rule tests
# ---------------------------------------------------------------------------

class TestApplyContributionRule:
    def test_point_in_bucket_inside(self, kernel):
        result = kernel.apply_contribution_rule(
            source_date=date(2025, 1, 15),
            source_qty=Decimal("100"),
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result == Decimal("100")

    def test_point_in_bucket_on_start_boundary(self, kernel):
        """Bucket start is inclusive."""
        result = kernel.apply_contribution_rule(
            source_date=date(2025, 1, 1),
            source_qty=Decimal("50"),
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result == Decimal("50")

    def test_point_in_bucket_on_end_boundary_excluded(self, kernel):
        """Bucket end is exclusive."""
        result = kernel.apply_contribution_rule(
            source_date=date(2025, 2, 1),
            source_qty=Decimal("50"),
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result == Decimal("0")

    def test_point_in_bucket_before_start(self, kernel):
        result = kernel.apply_contribution_rule(
            source_date=date(2024, 12, 31),
            source_qty=Decimal("50"),
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result == Decimal("0")

    def test_point_in_bucket_after_end(self, kernel):
        result = kernel.apply_contribution_rule(
            source_date=date(2025, 3, 1),
            source_qty=Decimal("50"),
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result == Decimal("0")

    def test_zero_quantity(self, kernel):
        result = kernel.apply_contribution_rule(
            source_date=date(2025, 1, 15),
            source_qty=Decimal("0"),
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result == Decimal("0")

    def test_unknown_rule_raises_value_error(self, kernel):
        with pytest.raises(ValueError, match="Unknown contribution rule"):
            kernel.apply_contribution_rule(
                source_date=date(2025, 1, 15),
                source_qty=Decimal("100"),
                bucket_start=date(2025, 1, 1),
                bucket_end=date(2025, 2, 1),
                rule="spread_across_bucket",
            )

    def test_explicit_rule_parameter(self, kernel):
        """Explicitly passing 'point_in_bucket' works the same as default."""
        result = kernel.apply_contribution_rule(
            source_date=date(2025, 1, 15),
            source_qty=Decimal("75"),
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
            rule="point_in_bucket",
        )
        assert result == Decimal("75")


# ---------------------------------------------------------------------------
# compute_pi_node tests
# ---------------------------------------------------------------------------

class TestComputePiNode:
    def test_happy_path_no_shortage(self, kernel):
        result = kernel.compute_pi_node(
            opening_stock=Decimal("100"),
            supply_events=[(date(2025, 1, 10), Decimal("50"))],
            demand_events=[(date(2025, 1, 15), Decimal("30"))],
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result["opening_stock"] == Decimal("100")
        assert result["inflows"] == Decimal("50")
        assert result["outflows"] == Decimal("30")
        assert result["closing_stock"] == Decimal("120")
        assert result["has_shortage"] is False
        assert result["shortage_qty"] == Decimal("0")

    def test_shortage_detected(self, kernel):
        result = kernel.compute_pi_node(
            opening_stock=Decimal("10"),
            supply_events=[],
            demand_events=[(date(2025, 1, 5), Decimal("50"))],
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result["closing_stock"] == Decimal("-40")
        assert result["has_shortage"] is True
        assert result["shortage_qty"] == Decimal("40")

    def test_empty_supply_and_demand(self, kernel):
        result = kernel.compute_pi_node(
            opening_stock=Decimal("200"),
            supply_events=[],
            demand_events=[],
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result["inflows"] == Decimal("0")
        assert result["outflows"] == Decimal("0")
        assert result["closing_stock"] == Decimal("200")
        assert result["has_shortage"] is False

    def test_events_outside_bucket_ignored(self, kernel):
        result = kernel.compute_pi_node(
            opening_stock=Decimal("50"),
            supply_events=[(date(2024, 12, 15), Decimal("100"))],  # before bucket
            demand_events=[(date(2025, 3, 1), Decimal("200"))],    # after bucket
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result["inflows"] == Decimal("0")
        assert result["outflows"] == Decimal("0")
        assert result["closing_stock"] == Decimal("50")

    def test_multiple_supply_and_demand_events(self, kernel):
        result = kernel.compute_pi_node(
            opening_stock=Decimal("0"),
            supply_events=[
                (date(2025, 1, 5), Decimal("100")),
                (date(2025, 1, 20), Decimal("50")),
            ],
            demand_events=[
                (date(2025, 1, 10), Decimal("30")),
                (date(2025, 1, 25), Decimal("20")),
            ],
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result["inflows"] == Decimal("150")
        assert result["outflows"] == Decimal("50")
        assert result["closing_stock"] == Decimal("100")

    def test_zero_opening_stock(self, kernel):
        result = kernel.compute_pi_node(
            opening_stock=Decimal("0"),
            supply_events=[],
            demand_events=[],
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result["closing_stock"] == Decimal("0")
        assert result["has_shortage"] is False
        assert result["shortage_qty"] == Decimal("0")

    def test_closing_stock_exactly_zero_no_shortage(self, kernel):
        result = kernel.compute_pi_node(
            opening_stock=Decimal("50"),
            supply_events=[],
            demand_events=[(date(2025, 1, 15), Decimal("50"))],
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result["closing_stock"] == Decimal("0")
        assert result["has_shortage"] is False
        assert result["shortage_qty"] == Decimal("0")

    def test_supply_qty_as_int_string_converts(self, kernel):
        """Quantities are passed through Decimal(str(...)), so ints work."""
        result = kernel.compute_pi_node(
            opening_stock=Decimal("0"),
            supply_events=[(date(2025, 1, 5), 100)],  # int, not Decimal
            demand_events=[],
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result["inflows"] == Decimal("100")

    def test_mixed_in_and_out_of_bucket_events(self, kernel):
        """Some events inside bucket, some outside."""
        result = kernel.compute_pi_node(
            opening_stock=Decimal("10"),
            supply_events=[
                (date(2025, 1, 15), Decimal("20")),   # inside
                (date(2025, 2, 15), Decimal("999")),   # outside
            ],
            demand_events=[
                (date(2025, 1, 20), Decimal("5")),     # inside
                (date(2024, 12, 1), Decimal("888")),   # outside
            ],
            bucket_start=date(2025, 1, 1),
            bucket_end=date(2025, 2, 1),
        )
        assert result["inflows"] == Decimal("20")
        assert result["outflows"] == Decimal("5")
        assert result["closing_stock"] == Decimal("25")

    def test_single_day_bucket(self, kernel):
        """Bucket of exactly one day: [Jan 15, Jan 16)."""
        result = kernel.compute_pi_node(
            opening_stock=Decimal("10"),
            supply_events=[(date(2025, 1, 15), Decimal("5"))],
            demand_events=[(date(2025, 1, 15), Decimal("3"))],
            bucket_start=date(2025, 1, 15),
            bucket_end=date(2025, 1, 16),
        )
        assert result["closing_stock"] == Decimal("12")
