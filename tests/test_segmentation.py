"""
Unit tests for the PURE buy-program segmentation core
(pyramide/segmentation.py, DEM-2 PR1, issue #444, ADR-030 proof machine).
DB-free: taxonomy, dense-calendar builder, segmented backtest and the ΔFVA
aggregation are exercised on in-memory fixtures only. The real DB half
(reader SQL, alias resolution, harness end-to-end) lives in
tests/integration/test_segmentation_integration.py.

The load-bearing invariants under test:

- **Taxonomy None-honesty**: ``order_type`` NULL/blank -> ``UNKNOWN``,
  NEVER ``BASE`` (the POC bug this module exists to correct); a present but
  unrecognized value (e.g. "STANDARD VISTA") -> ``BASE`` (a real
  classification, not missing data). Every input maps into
  ``BUY_PROGRAM_BUCKETS`` — the partition is exhaustive.
- **GOLDEN partition sum**: for every bucket of every calendar,
  Σ(per-program series) == mixed total EXACTLY (Decimal equality, no
  tolerance) — several datasets including UNKNOWN, holes, year crossing,
  duplicates. ``verify_partition_exhaustive`` is the explicit witness.
- **Residual additivity**: the APRÈS (segmented) rolling-origin residuals
  of the reconstructed total equal the sum of the per-program residuals,
  origin by origin, horizon by horizon.
- **ΔFVA algebra and None-honesty**: delta = fva_segmented - fva_mixed =
  WAPE_mixed - WAPE_segmented when computable; ``None`` + ``basis_count=0``
  on insufficient data (never an invented delta, never a count alongside a
  None delta); a LINEAR forecast function yields delta == 0 (a real,
  comparable zero, distinct from None); a negative delta is legitimate and
  never clamped.
- **Fail-loudly**: daily granularity (and any non weekly/monthly) raises
  ``ValueError`` in both the builder and the reader — the reader BEFORE
  touching the connection.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from ootils_core.pyramide.accuracy import evaluate_rolling_origin
from ootils_core.pyramide.segmentation import (
    BUCKET_BASE,
    BUCKET_EARLY,
    BUCKET_FWD,
    BUCKET_SPRING,
    BUCKET_SUMMER,
    BUCKET_UNKNOWN,
    BUY_PROGRAM_BUCKETS,
    SEGMENTED_GRANULARITIES,
    SegmentationProofResult,
    SegmentationProofRow,
    aggregate_delta_fva_wape,
    build_program_demand_calendar,
    buy_program_bucket,
    get_historical_demand_by_program,
    run_segmented_fva_proof,
    verify_partition_exhaustive,
)

D = Decimal


def _row(bucket_date: date, order_type: str | None, qty) -> dict:
    """One raw grouped row, exactly the shape the DB reader produces."""
    return {"bucket_date": bucket_date, "order_type": order_type, "total_qty": qty}


# ---------------------------------------------------------------------------
# 1. Taxonomy — buy_program_bucket, the SOURCE UNIQUE
# ---------------------------------------------------------------------------


class TestTaxonomy:
    def test_null_order_type_is_unknown_never_base(self):
        # THE None-honesty correction over the POC (which folded None into
        # BASE silently): missing data gets its own explicit bucket.
        assert buy_program_bucket(None) == BUCKET_UNKNOWN
        assert buy_program_bucket(None) != BUCKET_BASE

    @pytest.mark.parametrize("blank", ["", " ", "  ", "\t", "\n  \t"])
    def test_blank_order_type_is_unknown(self, blank):
        assert buy_program_bucket(blank) == BUCKET_UNKNOWN

    @pytest.mark.parametrize(
        ("order_type", "expected"),
        [
            # Every POC marker (ex-forecast_program_poc.py, retire 2026-07-19 — historique git),
            # substring match, real ERP-shaped values included.
            ("SPRING BUY", BUCKET_SPRING),
            ("2025 SPRING BUY", BUCKET_SPRING),
            ("SUMMER BUY", BUCKET_SUMMER),
            ("CN SUMMER BUY", BUCKET_SUMMER),
            ("EARLY BUY", BUCKET_EARLY),
            ("CN EARLY BUY", BUCKET_EARLY),
            ("FWD BUY", BUCKET_FWD),
            ("LESLIES FWD BUY", BUCKET_FWD),
            ("FORWARD BUY", BUCKET_FWD),
            # Case-insensitive + surrounding whitespace stripped.
            ("spring buy", BUCKET_SPRING),
            ("Summer Buy", BUCKET_SUMMER),
            ("  early buy  ", BUCKET_EARLY),
            ("forward buy", BUCKET_FWD),
            # Present but not a program marker -> BASE: this IS a
            # classification (a real non-program order type), never UNKNOWN.
            ("STANDARD VISTA", BUCKET_BASE),
            ("STANDARD", BUCKET_BASE),
            ("VISTA", BUCKET_BASE),
            ("DROPSHIP", BUCKET_BASE),
            ("SOMETHING NEW", BUCKET_BASE),
        ],
    )
    def test_marker_classification(self, order_type, expected):
        assert buy_program_bucket(order_type) == expected

    @pytest.mark.parametrize(
        "order_type",
        [None, "", "   ", "SPRING BUY", "SUMMER BUY", "CN EARLY BUY",
         "LESLIES FWD BUY", "FORWARD BUY", "STANDARD VISTA", "junk", "42"],
    )
    def test_every_input_lands_in_the_fixed_bucket_set(self, order_type):
        # Exhaustive partition: buy_program_bucket never returns anything
        # outside BUY_PROGRAM_BUCKETS (the golden invariant's foundation).
        assert buy_program_bucket(order_type) in BUY_PROGRAM_BUCKETS

    def test_bucket_set_is_the_canonical_six(self):
        assert BUY_PROGRAM_BUCKETS == (
            BUCKET_SPRING, BUCKET_SUMMER, BUCKET_EARLY,
            BUCKET_FWD, BUCKET_BASE, BUCKET_UNKNOWN,
        )


# ---------------------------------------------------------------------------
# 2. GOLDEN partition sum — Σ(programs) == mixed total EXACTLY
# ---------------------------------------------------------------------------

# Dataset 1 — weekly, calendar hole (middle week has no demand at all),
# UNKNOWN (None AND blank) coexisting with real programs.
_W0 = date(2025, 3, 3)  # a Monday: a plausible date_trunc('week') output


def _weekly(k: int) -> date:
    return _W0 + timedelta(days=7 * k)


DATASET_WEEKLY_HOLE = [
    _row(_weekly(0), "2025 SPRING BUY", 10),
    _row(_weekly(0), None, 4),
    _row(_weekly(2), "STANDARD VISTA", 6),
    _row(_weekly(2), "LESLIES FWD BUY", 2),
    _row(_weekly(2), "  ", 1),
]

# Dataset 2 — monthly, YEAR CROSSING (Nov 2025 -> Jan 2026), all six buckets.
DATASET_MONTHLY_ALL_BUCKETS = [
    _row(date(2025, 11, 1), "SPRING BUY", 1),
    _row(date(2025, 11, 1), "SUMMER BUY", 2),
    _row(date(2025, 12, 1), "CN EARLY BUY", 3),
    _row(date(2025, 12, 1), "LESLIES FWD BUY", 4),
    _row(date(2026, 1, 1), "STANDARD VISTA", 5),
    _row(date(2026, 1, 1), None, 6),
    _row(date(2026, 1, 1), "", 7),
]

# Dataset 3 — monthly, duplicate (bucket_date, order_type) pairs summed,
# mixed numeric input types (int / float / str / Decimal) via Decimal(str()).
DATASET_MONTHLY_DUPES = [
    _row(date(2025, 5, 1), "SPRING BUY", 3),
    _row(date(2025, 5, 1), "SPRING BUY", 1.5),
    _row(date(2025, 5, 1), None, "2.25"),
    _row(date(2025, 6, 1), None, D("0.75")),
    _row(date(2025, 6, 1), "STANDARD VISTA", 4),
]


class TestGoldenPartitionSum:
    @pytest.mark.parametrize(
        ("rows", "granularity"),
        [
            (DATASET_WEEKLY_HOLE, "weekly"),
            (DATASET_MONTHLY_ALL_BUCKETS, "monthly"),
            (DATASET_MONTHLY_DUPES, "monthly"),
        ],
        ids=["weekly-hole-unknown", "monthly-year-crossing-all-buckets",
             "monthly-duplicates-mixed-types"],
    )
    def test_sum_of_programs_reconstructs_mixed_total_exactly(
        self, rows, granularity
    ):
        calendar = build_program_demand_calendar(rows, granularity)

        # The explicit witness.
        assert verify_partition_exhaustive(calendar)

        # Independent hand check, bucket by bucket: Decimal equality, no
        # tolerance — both against the per-program sum AND against the mixed
        # total recomputed from the raw rows.
        for index, bucket_date in enumerate(calendar.bucket_starts):
            program_sum = sum(
                (series[index] for series in calendar.series_by_program.values()),
                D("0"),
            )
            mixed = sum(
                (D(str(r["total_qty"])) for r in rows
                 if r["bucket_date"] == bucket_date),
                D("0"),
            )
            assert program_sum == calendar.total[index] == mixed

    def test_unknown_is_summed_into_the_total_not_dropped(self):
        # None AND blank rows land in UNKNOWN and COUNT in the total —
        # dropping them would silently understate mixed demand.
        calendar = build_program_demand_calendar(DATASET_WEEKLY_HOLE, "weekly")
        assert BUCKET_UNKNOWN in calendar.series_by_program
        assert calendar.series_by_program[BUCKET_UNKNOWN] == (D(4), D(0), D(1))
        assert calendar.total == (D(14), D(0), D(9))

    def test_empty_calendar_is_vacuously_exhaustive(self):
        assert verify_partition_exhaustive(
            build_program_demand_calendar([], "weekly")
        )


# ---------------------------------------------------------------------------
# 3. Dense calendar builder — zero-fill, holes, year crossing, ordering
# ---------------------------------------------------------------------------


class TestCalendarBuilder:
    def test_hole_bucket_appears_zero_filled(self):
        calendar = build_program_demand_calendar(DATASET_WEEKLY_HOLE, "weekly")
        assert calendar.bucket_starts == (_weekly(0), _weekly(1), _weekly(2))
        # The middle week carries NO row at all yet appears, all-zero, in
        # every program series AND the total (dense shared calendar).
        for series in calendar.series_by_program.values():
            assert series[1] == D(0)
        assert calendar.total[1] == D(0)

    def test_program_series_are_zero_filled_where_other_programs_book(self):
        calendar = build_program_demand_calendar(DATASET_WEEKLY_HOLE, "weekly")
        assert calendar.series_by_program[BUCKET_SPRING] == (D(10), D(0), D(0))
        assert calendar.series_by_program[BUCKET_BASE] == (D(0), D(0), D(6))
        assert calendar.series_by_program[BUCKET_FWD] == (D(0), D(0), D(2))

    def test_monthly_buckets_cross_the_year_boundary(self):
        calendar = build_program_demand_calendar(
            DATASET_MONTHLY_ALL_BUCKETS, "monthly"
        )
        assert calendar.bucket_starts == (
            date(2025, 11, 1), date(2025, 12, 1), date(2026, 1, 1),
        )
        assert set(calendar.series_by_program) == set(BUY_PROGRAM_BUCKETS)

    def test_programs_property_follows_canonical_order(self):
        # Insertion order of the rows is irrelevant: programs come out in
        # BUY_PROGRAM_BUCKETS order (SPRING..BASE, UNKNOWN last).
        calendar = build_program_demand_calendar(DATASET_WEEKLY_HOLE, "weekly")
        assert calendar.programs == (
            BUCKET_SPRING, BUCKET_FWD, BUCKET_BASE, BUCKET_UNKNOWN,
        )

    def test_duplicate_rows_are_summed_not_overwritten(self):
        calendar = build_program_demand_calendar(DATASET_MONTHLY_DUPES, "monthly")
        assert calendar.series_by_program[BUCKET_SPRING] == (D("4.5"), D(0))
        assert calendar.series_by_program[BUCKET_UNKNOWN] == (D("2.25"), D("0.75"))
        assert calendar.total == (D("6.75"), D("4.75"))

    def test_single_program_calendar(self):
        rows = [_row(date(2025, 4, 1), "SPRING BUY", 8)]
        calendar = build_program_demand_calendar(rows, "monthly")
        assert calendar.programs == (BUCKET_SPRING,)
        assert calendar.total == calendar.series_by_program[BUCKET_SPRING] == (D(8),)

    def test_empty_rows_yield_the_empty_calendar_never_a_fake_bucket(self):
        calendar = build_program_demand_calendar([], "monthly")
        assert calendar.bucket_starts == ()
        assert calendar.total == ()
        assert dict(calendar.series_by_program) == {}
        assert calendar.programs == ()

    @pytest.mark.parametrize("granularity", ["daily", "hourly", "WEEKLY", ""])
    def test_builder_rejects_non_segmented_granularities(self, granularity):
        with pytest.raises(ValueError):
            build_program_demand_calendar(DATASET_WEEKLY_HOLE, granularity)

    def test_daily_is_deliberately_absent_from_the_granularity_map(self):
        assert dict(SEGMENTED_GRANULARITIES) == {"weekly": "week", "monthly": "month"}

    def test_calendar_is_immutable(self):
        calendar = build_program_demand_calendar(DATASET_WEEKLY_HOLE, "weekly")
        with pytest.raises(Exception):
            calendar.total = ()  # type: ignore[misc]
        with pytest.raises(TypeError):
            calendar.series_by_program[BUCKET_SPRING] = ()  # type: ignore[index]


class TestReaderGranularityGuard:
    def test_reader_rejects_daily_before_touching_the_connection(self):
        # The ValueError must fire BEFORE any SQL: a sentinel that explodes
        # on ANY attribute access proves the validation-first ordering.
        class _NeverDB:
            def __getattr__(self, name):
                raise AssertionError(
                    "granularity must be validated before the DB is touched"
                )

        with pytest.raises(ValueError, match="daily is out of scope"):
            get_historical_demand_by_program(
                _NeverDB(), uuid4(), uuid4(), 90, "daily"
            )


# ---------------------------------------------------------------------------
# 4. Segmented backtest — residual additivity (the total reconstructed from
#    segmented residuals == residuals of the total)
# ---------------------------------------------------------------------------


def _clamped_trend(train, periods):
    """Deterministic NONLINEAR model: linear extrapolation clamped at zero.
    The clamp is the nonlinearity that makes segmented != mixed forecasts —
    a linear fn would make this test vacuous (see the linearity test)."""
    if len(train) >= 2:
        value = max(D(0), 2 * train[-1] - train[-2])
    else:
        value = train[-1]
    return [value] * periods


def _residual_calendar():
    """Weekly, 6 buckets, SPRING + UNKNOWN with interleaved zeros."""
    spring = [10, 0, 6, 0, 8, 2]
    unknown = [0, 4, 0, 5, 1, 0]
    rows = []
    for k, (s, u) in enumerate(zip(spring, unknown)):
        if s:
            rows.append(_row(_weekly(k), "SPRING BUY", s))
        if u:
            rows.append(_row(_weekly(k), None, u))
    return build_program_demand_calendar(rows, "weekly")


class TestResidualAdditivity:
    def test_segmented_residuals_equal_sum_of_per_program_residuals(self):
        calendar = _residual_calendar()
        result = run_segmented_fva_proof(
            calendar, _clamped_trend, min_train=3, horizon=2
        )
        assert result.segmented_report is not None

        # Independent per-program backtests at the SAME origins (the shared
        # dense calendar gives every program the same length, hence the same
        # cutoff set for the same min_train/step).
        per_program = [
            evaluate_rolling_origin(
                series=list(series), forecast_fn=_clamped_trend,
                horizon=2, min_train=3, step=1, m=1,
            )
            for series in calendar.series_by_program.values()
        ]
        for h, residuals in result.segmented_report.per_horizon_residuals.items():
            reconstructed = tuple(
                sum(values, D(0))
                for values in zip(
                    *(report.per_horizon_residuals[h] for report in per_program)
                )
            )
            assert residuals == reconstructed

    def test_nonlinear_fn_actually_diverges_mixed_vs_segmented(self):
        # Guard against vacuity: with the clamped-trend fn the segmented
        # forecast differs from the mixed one, so the additivity test above
        # exercises a REAL recombination, not an identity.
        calendar = _residual_calendar()
        result = run_segmented_fva_proof(
            calendar, _clamped_trend, min_train=3, horizon=2
        )
        assert result.mixed_report is not None
        assert result.segmented_report is not None
        assert (
            result.mixed_report.per_horizon_residuals
            != result.segmented_report.per_horizon_residuals
        )

    def test_mixed_report_is_the_plain_backtest_of_the_total(self):
        # AVANT is EXACTLY evaluate_rolling_origin on calendar.total with the
        # injected fn — no segmentation-specific behaviour on the mixed side.
        calendar = _residual_calendar()
        result = run_segmented_fva_proof(
            calendar, _clamped_trend, min_train=3, horizon=2
        )
        reference = evaluate_rolling_origin(
            series=list(calendar.total), forecast_fn=_clamped_trend,
            horizon=2, min_train=3, step=1, m=1,
        )
        assert result.mixed_report == reference

    def test_both_reports_share_cutoffs_and_observations(self):
        calendar = _residual_calendar()
        result = run_segmented_fva_proof(
            calendar, _clamped_trend, min_train=3, horizon=2
        )
        assert result.mixed_report is not None
        assert result.segmented_report is not None
        assert (
            result.mixed_report.n_cutoffs
            == result.segmented_report.n_cutoffs
        )
        assert (
            result.mixed_report.n_observations
            == result.segmented_report.n_observations
        )


# ---------------------------------------------------------------------------
# 5. ΔFVA — hand-golden algebra, sign, linearity zero, None-honesty
# ---------------------------------------------------------------------------


def _golden_monthly_calendar():
    """14 consecutive months (Jan 2024 .. Feb 2025, crossing the year),
    SPRING constant 5 + BASE constant 3 -> total constant 8. min_train=12
    equals the monthly season, so the seasonal-naive is computable and
    PERFECT (naive_wape == 0), making every FVA operand a hand literal."""
    rows = []
    months = [date(2024, m, 1) for m in range(1, 13)] + [
        date(2025, 1, 1), date(2025, 2, 1),
    ]
    for month in months:
        rows.append(_row(month, "SPRING BUY", 5))
        rows.append(_row(month, "STANDARD VISTA", 3))
    return build_program_demand_calendar(rows, "monthly")


def _lookup_fn(total_curve, spring_curve, base_curve):
    """Injected model with prescribed output per partition, keyed on the
    series head (total starts at 8, SPRING at 5, BASE at 3) — total control
    over both orchestrations for hand-derived literals."""
    def fn(train, periods):
        head = train[0]
        if head == D(8):
            return [D(total_curve)] * periods
        if head == D(5):
            return [D(spring_curve)] * periods
        if head == D(3):
            return [D(base_curve)] * periods
        raise AssertionError(f"unexpected training slice head: {head!r}")
    return fn


class TestDeltaFvaGolden:
    def test_positive_delta_hand_derived(self):
        # Mixed forecast deliberately wrong (6 vs actual 8, both cutoffs):
        #   WAPE_mixed = (2+2)/(8+8) = 0.25
        # Per-program forecasts exact (5 and 3, summing to 8):
        #   WAPE_segmented = 0
        # Seasonal-naive (season 12, min_train 12) repeats the constant 8
        # exactly: naive_wape = 0 on the same 2 cutoffs. Hence
        #   fva_mixed     = 0 - 0.25 = -0.25
        #   fva_segmented = 0 - 0    = 0
        #   delta         = 0 - (-0.25) = +0.25 = WAPE_mixed - WAPE_segmented
        calendar = _golden_monthly_calendar()
        result = run_segmented_fva_proof(
            calendar, _lookup_fn(6, 5, 3), min_train=12, horizon=1
        )

        assert result.mixed_report is not None
        assert result.segmented_report is not None
        assert result.mixed_report.wape == D("0.25")
        assert result.segmented_report.wape == D(0)

        assert result.fva_mixed is not None
        assert result.fva_segmented is not None
        assert result.fva_mixed.naive_wape == D(0)
        assert result.fva_mixed.fva_wape == D("-0.25")
        assert result.fva_segmented.fva_wape == D(0)

        assert result.delta_fva_wape == D("0.25")
        # The contract's stated algebra: the shared naive term cancels.
        assert result.delta_fva_wape == (
            result.mixed_report.wape - result.segmented_report.wape
        )
        assert result.basis_count == 2  # 14 buckets - min_train 12

        # Constant training slices leave MASE undefined on BOTH sides:
        # the mase axis is None-honest without polluting the wape axis.
        assert result.delta_fva_mase is None

    def test_negative_delta_when_segmentation_hurts_never_clamped(self):
        # Mixed forecast exact (8), per-program forecasts wrong (6 + 4 = 10):
        #   WAPE_mixed = 0 ; WAPE_segmented = (2+2)/16 = 0.25
        #   delta = -0.25 — segmentation LOSING is a legitimate, honest
        #   result and is never clamped to 0.
        calendar = _golden_monthly_calendar()
        result = run_segmented_fva_proof(
            calendar, _lookup_fn(8, 6, 4), min_train=12, horizon=1
        )
        assert result.delta_fva_wape == D("-0.25")
        assert result.delta_fva_wape < 0
        assert result.basis_count == 2

    def test_linear_fn_yields_exact_zero_delta_distinct_from_none(self):
        # For any LINEAR model, forecasting the sum == summing the per-
        # program forecasts (last-value naive here), so segmentation is a
        # structural no-op: delta == 0 EXACTLY — a real, comparable zero,
        # explicitly distinct from None ("not comparable").
        rows = []
        months = [date(2024, m, 1) for m in range(1, 13)] + [
            date(2025, 1, 1), date(2025, 2, 1),
        ]
        for k, month in enumerate(months):
            rows.append(_row(month, "SPRING BUY", k + 1))
            rows.append(_row(month, "STANDARD VISTA", 2))
        calendar = build_program_demand_calendar(rows, "monthly")

        def last_value(train, periods):
            return [train[-1]] * periods

        result = run_segmented_fva_proof(
            calendar, last_value, min_train=12, horizon=1
        )
        assert result.mixed_report is not None
        assert result.segmented_report is not None
        assert result.mixed_report.wape == result.segmented_report.wape
        assert result.delta_fva_wape == D(0)
        assert result.delta_fva_wape is not None
        assert result.basis_count == 2


class TestDeltaFvaNoneHonesty:
    def test_empty_calendar_all_none_basis_zero(self):
        calendar = build_program_demand_calendar([], "monthly")
        result = run_segmented_fva_proof(calendar, _clamped_trend, min_train=5)
        assert result.mixed_report is None
        assert result.segmented_report is None
        assert result.fva_mixed is None
        assert result.fva_segmented is None
        assert result.delta_fva_wape is None
        assert result.delta_fva_mase is None
        assert result.basis_count == 0
        assert result.n_buckets == 0
        assert result.programs == ()

    @pytest.mark.parametrize("min_train", [0, -1, 3, 4])
    def test_min_train_leaving_no_cutoff_short_circuits(self, min_train):
        # 3 buckets: min_train < 1 or >= n_buckets -> no cutoff can form ->
        # every report/FVA field None, basis 0 — never a fabricated report.
        calendar = build_program_demand_calendar(DATASET_WEEKLY_HOLE, "weekly")
        assert len(calendar.bucket_starts) == 3
        result = run_segmented_fva_proof(
            calendar, _clamped_trend, min_train=min_train
        )
        assert result.mixed_report is None
        assert result.delta_fva_wape is None
        assert result.basis_count == 0
        # Context fields still honest (the caller can report WHAT was short).
        assert result.n_buckets == 3
        assert result.min_train == min_train
        assert result.programs == (BUCKET_SPRING, BUCKET_FWD, BUCKET_BASE,
                                   BUCKET_UNKNOWN)

    def test_reports_exist_but_naive_uncomputable_delta_none_basis_zero(self):
        # Monthly season = 12 but min_train = 3 < 12: the rolling-origin
        # reports DO exist, yet the seasonal-naive has no value one season
        # ago at the first origin -> FVA None on both sides -> delta None and
        # basis_count 0 (never a count presented alongside a None delta).
        rows = [
            _row(date(2025, m, 1), "SPRING BUY", 5 + m) for m in range(1, 7)
        ] + [
            _row(date(2025, m, 1), None, 2) for m in range(1, 7)
        ]
        calendar = build_program_demand_calendar(rows, "monthly")
        result = run_segmented_fva_proof(
            calendar, _clamped_trend, min_train=3, horizon=1
        )
        assert result.mixed_report is not None
        assert result.segmented_report is not None
        assert result.fva_mixed is not None  # FvaResult with None fields
        assert result.fva_mixed.fva_wape is None
        assert result.delta_fva_wape is None
        assert result.delta_fva_mase is None
        assert result.basis_count == 0

    def test_weekly_season_52_unreachable_on_short_series_delta_none(self):
        # Weekly granularity resolves season 52: a 6-bucket weekly calendar
        # can never align the seasonal-naive -> None + basis 0, honestly.
        calendar = _residual_calendar()
        result = run_segmented_fva_proof(
            calendar, _clamped_trend, min_train=3, horizon=2
        )
        assert result.mixed_report is not None
        assert result.delta_fva_wape is None
        assert result.basis_count == 0

    def test_result_is_frozen(self):
        calendar = build_program_demand_calendar([], "monthly")
        result = run_segmented_fva_proof(calendar, _clamped_trend, min_train=1)
        with pytest.raises(Exception):
            result.delta_fva_wape = D(1)  # type: ignore[misc]


class TestForecastFnContract:
    def test_short_program_curve_fails_loudly_through_the_public_path(self):
        # The injected model returns a full curve for the mixed total but a
        # SHORT curve for the program partitions: the mixed pass succeeds,
        # the segmented wrapper must raise (a model bug, not a data
        # condition) with the program-partition marker in the message.
        calendar = _golden_monthly_calendar()

        def short_on_programs(train, periods):
            if train[0] == D(8):
                return [D(1)] * periods
            return []

        with pytest.raises(ValueError, match="program partition"):
            run_segmented_fva_proof(
                calendar, short_on_programs, min_train=12, horizon=1
            )


# ---------------------------------------------------------------------------
# 6. aggregate_delta_fva_wape — volume-weighted, None-honest
# ---------------------------------------------------------------------------


def _proof_row(delta: Decimal | None, volume: Decimal) -> SegmentationProofRow:
    basis = 2 if delta is not None else 0
    result = SegmentationProofResult(
        granularity="monthly",
        programs=(BUCKET_SPRING, BUCKET_BASE),
        n_buckets=14,
        min_train=12,
        horizon=1,
        mixed_report=None,
        segmented_report=None,
        fva_mixed=None,
        fva_segmented=None,
        delta_fva_wape=delta,
        delta_fva_mase=None,
        basis_count=basis,
    )
    return SegmentationProofRow(
        item_id=uuid4(), location_id=uuid4(), result=result, volume=volume,
    )


class TestAggregateDeltaFva:
    def test_empty_rows_none_and_zero(self):
        assert aggregate_delta_fva_wape([]) == (None, 0)

    def test_all_non_contributing_rows_none_and_zero(self):
        rows = [
            _proof_row(None, D(100)),      # no computable delta
            _proof_row(D("0.2"), D(0)),    # zero volume carries no weight
            _proof_row(D("0.3"), D(-5)),   # negative volume never contributes
        ]
        assert aggregate_delta_fva_wape(rows) == (None, 0)

    def test_volume_weighted_mean_hand_derived(self):
        rows = [
            _proof_row(D("0.2"), D(3)),
            _proof_row(D("-0.1"), D(1)),
        ]
        weighted_mean, n_series = aggregate_delta_fva_wape(rows)
        # (0.2*3 + (-0.1)*1) / 4 = 0.5/4 = 0.125
        assert weighted_mean == D("0.125")
        assert n_series == 2

    def test_n_series_counts_contributors_only(self):
        rows = [
            _proof_row(D("0.2"), D(3)),
            _proof_row(None, D(10)),
            _proof_row(D("0.4"), D(0)),
        ]
        weighted_mean, n_series = aggregate_delta_fva_wape(rows)
        assert weighted_mean == D("0.2")
        assert n_series == 1  # 1 contributor out of 3 rows — never conflated
