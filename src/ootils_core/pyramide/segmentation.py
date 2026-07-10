"""
Pyramide axis C — buy-program segmentation, the DEM-2 PR1 proof core.

Ootils forecasts on a single blended (item, location) demand series today.
``demand_history`` (migration 048) carries ``order_type`` — the ERP field that
encodes the BUY PROGRAM (SPRING BUY / SUMMER BUY / EARLY BUY / FWD BUY, plus
plain STANDARD/VISTA orders) — but nothing reads it: it is the first
already-ingested causal variable Ootils has never used
(``docs/ROADMAP-AGENTS-2026-H2.md`` §5, DEM-2). This module answers, with the
proof machine (ADR-030) rather than a narrative: does forecasting each buy
program on its OWN calendar and summing beat forecasting the blended total?

PR1 scope, deliberately narrow (issue #444):

- **READ-ONLY, ZERO migration.** ``demand_history``/``locations``/
  ``location_aliases`` in, nothing written.
- **DB-free maximised.** Only :func:`get_historical_demand_by_program` and
  :func:`build_program_demand_calendar`'s DB half touch a connection; the
  taxonomy, the dense-calendar builder and the segmented backtest are pure
  in-memory functions (same discipline as ``accuracy.py`` / ``fva.py``).
- **No parameter added to** ``repository.get_historical_demand``. Its sparse,
  ungrouped, single-series contract is unrelated to segmentation: a per-
  program series needs its OWN dense, zero-filled calendar (programs run on
  disjoint, non-summable date sets), which is exactly the re-aggregation
  ``#433`` scoped out of the leaf reader. This module adds a NEW reader
  instead of overloading the existing one.
- **Granularity weekly/monthly only.** Daily buckets segmented by program
  would mostly be zero (a program books a handful of days a year) — the
  reader fails loudly rather than emit a degenerate all-zero comparison; see
  :func:`get_historical_demand_by_program`.
- **compute_fva is reused, never re-implemented** (``fva.py:166``). The
  segmented backtest produces two ``AccuracyReport``s (mixed / segmented) on
  the IDENTICAL series, cutoffs and season length, and calls
  :func:`fva.compute_fva` on each; the delta is derived algebraically from
  the two ``FvaResult``s (see :func:`run_segmented_fva_proof`).
- **Orthogonal to ADR-022 / MinT by construction**, not by a runtime check:
  this module never touches ``item_hierarchy`` or the reconciler — segments
  are buy programs of ONE (item, location) leaf series, not hierarchy nodes.
- **Confidence (ADR-023): nothing wired in PR1.** ``pyramide/confidence.py``
  is not in the Pyramide run path today (it composes backtest WAPE, history
  depth, freshness for a *served* forecast); a proof-harness backtest is not
  a served run and degrades naturally through its own WAPE channel (a
  segmented series with too little history yields ``None`` FVA, not a
  confidence score) — nothing to compose here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Mapping, Sequence
from uuid import UUID

from ootils_core.db.types import DictRowConnection

from .accuracy import AccuracyReport, ForecastFn, evaluate_rolling_origin
from .fva import FvaResult, compute_fva, resolve_season_length
from .repository import _DEMAND_HISTORY_BUSINESS_PREDICATES, _warehouse_codes_subquery

logger = logging.getLogger(__name__)

__all__ = [
    "BUCKET_SPRING",
    "BUCKET_SUMMER",
    "BUCKET_EARLY",
    "BUCKET_FWD",
    "BUCKET_BASE",
    "BUCKET_UNKNOWN",
    "BUY_PROGRAM_BUCKETS",
    "SEGMENTED_GRANULARITIES",
    "buy_program_bucket",
    "ProgramDemandCalendar",
    "build_program_demand_calendar",
    "get_historical_demand_by_program",
    "verify_partition_exhaustive",
    "SegmentationProofResult",
    "run_segmented_fva_proof",
    "SegmentationProofRow",
    "aggregate_delta_fva_wape",
]


# ---------------------------------------------------------------------------
# Taxonomy — SOURCE UNIQUE (buy_program_bucket). Every consumer (the reader
# below, the harness, a future watcher) MUST classify order_type through this
# function; a second copy would be exactly the kind of silent divergence
# ADR-021's "two truths" discipline exists to prevent.
# ---------------------------------------------------------------------------

BUCKET_SPRING = "SPRING"
BUCKET_SUMMER = "SUMMER"
BUCKET_EARLY = "EARLY"
BUCKET_FWD = "FWD"
BUCKET_BASE = "BASE"
BUCKET_UNKNOWN = "UNKNOWN"

# Canonical display/aggregation order — buy programs first (the signal this
# module exists to isolate), BASE (classified as "not a program"), UNKNOWN
# last (the honesty bucket). Fixed set: buy_program_bucket() never returns
# anything outside it.
BUY_PROGRAM_BUCKETS: tuple[str, ...] = (
    BUCKET_SPRING,
    BUCKET_SUMMER,
    BUCKET_EARLY,
    BUCKET_FWD,
    BUCKET_BASE,
    BUCKET_UNKNOWN,
)


def buy_program_bucket(order_type: str | None) -> str:
    """Classify one ``demand_history.order_type`` value into a buy-program
    bucket. Reprises ``scripts/forecast_program_poc.py:38-48`` (SPRING/
    SUMMER/EARLY/FWD BUY marker matching), corrected for a None-honesty bug:
    the POC folded a missing/blank ``order_type`` into ``BASE`` silently —
    indistinguishable from a genuinely classified "not a buy program"
    order. Missing data is not a fact about the order; it is the ABSENCE of
    one, so it gets its own explicit bucket:

    - ``order_type`` is ``None`` or blank (no classification signal at all)
      -> :data:`BUCKET_UNKNOWN`.
    - ``order_type`` matches a known buy-program marker (substring match,
      case-insensitive, same markers as the POC) -> SPRING / SUMMER / EARLY
      / FWD (FWD also matches "FORWARD BUY", e.g. "LESLIES FWD BUY").
    - ``order_type`` is present but matches none of the markers (e.g.
      "STANDARD", "VISTA") -> :data:`BUCKET_BASE`: this IS a classification
      (a real, known, non-program order type), not missing data.

    The partition is EXHAUSTIVE over ``BUY_PROGRAM_BUCKETS`` by
    construction: every input maps to exactly one of the six buckets, so
    summing the per-bucket series always reconstructs the mixed total
    exactly (the golden invariant checked by
    :func:`verify_partition_exhaustive`).
    """
    if order_type is None:
        return BUCKET_UNKNOWN
    upper = order_type.strip().upper()
    if not upper:
        return BUCKET_UNKNOWN
    if "SPRING BUY" in upper:
        return BUCKET_SPRING
    if "SUMMER BUY" in upper:
        return BUCKET_SUMMER
    if "EARLY BUY" in upper:
        return BUCKET_EARLY
    if "FWD BUY" in upper or "FORWARD BUY" in upper:
        return BUCKET_FWD
    return BUCKET_BASE


def _program_sort_key(program: str) -> tuple[int, str]:
    try:
        return (BUY_PROGRAM_BUCKETS.index(program), program)
    except ValueError:
        # Defensive only: buy_program_bucket() cannot produce anything
        # outside BUY_PROGRAM_BUCKETS, but a future taxonomy edit that adds
        # a bucket here without updating callers should sort deterministically
        # rather than raise mid-report.
        return (len(BUY_PROGRAM_BUCKETS), program)


# ---------------------------------------------------------------------------
# Dense, zero-filled, per-program calendar.
# ---------------------------------------------------------------------------

SEGMENTED_GRANULARITIES: Mapping[str, str] = MappingProxyType(
    {"weekly": "week", "monthly": "month"}
)
"""Granularities :func:`get_historical_demand_by_program` accepts, mapped to
the PostgreSQL ``date_trunc`` unit. Daily is deliberately absent: buy
programs run on disjoint, sparse date sets (a program books a handful of
calendar DAYS a year), so a daily per-program series would be almost all
zero — not a meaningful comparison, and exactly the re-aggregation ``#433``
scoped out of the leaf reader. Weekly/monthly buckets give each program a
dense, comparable calendar."""


@dataclass(frozen=True)
class ProgramDemandCalendar:
    """A dense, zero-filled demand calendar shared by every buy-program
    bucket present in the source window, plus their reconstructed total.

    ``bucket_starts``: the shared calendar, ascending, one entry per
    granularity bucket from the first to the last bucket that carries ANY
    demand (any program) — a bucket with zero demand across every program
    still appears (zero-filled), unlike the sparse contract of
    ``repository.get_historical_demand``.

    ``series_by_program``: ``{bucket: tuple(values...)}`` keyed by
    :data:`BUY_PROGRAM_BUCKETS` name, each series aligned index-for-index
    with ``bucket_starts`` (index i of every program series is the SAME
    calendar bucket) — this alignment is what makes the segmented backtest
    (:func:`run_segmented_fva_proof`) able to slice every program's history
    at the SAME rolling origin.

    ``total``: index-for-index sum of every program series — this is NOT an
    independently queried column, it is derived by construction in
    :func:`build_program_demand_calendar`, so it equals the mixed
    (unsegmented) series EXACTLY, by construction rather than by a runtime
    check (the golden invariant; :func:`verify_partition_exhaustive` is the
    explicit, testable witness of it).
    """

    granularity: str
    bucket_starts: tuple[date, ...]
    series_by_program: Mapping[str, tuple[Decimal, ...]]
    total: tuple[Decimal, ...]

    @property
    def programs(self) -> tuple[str, ...]:
        return tuple(
            program
            for program in BUY_PROGRAM_BUCKETS
            if program in self.series_by_program
        )


def _empty_calendar(granularity: str) -> ProgramDemandCalendar:
    return ProgramDemandCalendar(
        granularity=granularity,
        bucket_starts=(),
        series_by_program=MappingProxyType({}),
        total=(),
    )


def _next_bucket_start(bucket_date: date, granularity: str) -> date:
    """Start of the granularity bucket immediately after ``bucket_date``
    (itself assumed to already be a bucket start, e.g. a Postgres
    ``date_trunc('week'|'month', ...)`` result)."""
    if granularity == "weekly":
        return bucket_date + timedelta(days=7)
    if granularity == "monthly":
        if bucket_date.month == 12:
            return date(bucket_date.year + 1, 1, 1)
        return date(bucket_date.year, bucket_date.month + 1, 1)
    raise ValueError(
        f"_next_bucket_start supports {sorted(SEGMENTED_GRANULARITIES)}, got "
        f"{granularity!r}"
    )


def build_program_demand_calendar(
    rows: Sequence[Mapping[str, Any]],
    granularity: str,
) -> ProgramDemandCalendar:
    """Pure, DB-free: turn raw grouped rows into a dense per-program
    calendar. Each row must carry ``bucket_date`` (a ``date``, already
    truncated to the granularity boundary), ``order_type`` (``str | None``,
    RAW ERP value — bucketed here via :func:`buy_program_bucket`, the single
    source of the taxonomy) and ``total_qty`` (numeric, summable via
    ``Decimal(str(...))``). This is exactly the shape
    :func:`get_historical_demand_by_program` groups its SQL by (bucket_date,
    order_type) — see that function for the DB half.

    Rows for the SAME ``(bucket_date, order_type)`` pair are NOT expected
    (the DB reader groups by both), but if a caller passes duplicates they
    are summed rather than overwritten, keeping this function total and
    honest for hand-built test fixtures.

    Empty ``rows`` -> the empty calendar (no bucket, no program, empty
    total) — an item/location/window with no qualifying demand_history at
    all, never a fabricated single zero bucket.

    Raises:
        ValueError: ``granularity`` not in :data:`SEGMENTED_GRANULARITIES`
            (structural misuse — fail loudly, like ``accuracy.py``).
    """
    if granularity not in SEGMENTED_GRANULARITIES:
        raise ValueError(
            f"build_program_demand_calendar requires granularity in "
            f"{sorted(SEGMENTED_GRANULARITIES)}; got {granularity!r} "
            "(daily buy-program calendars are disjoint/non-summable per "
            "program -- see module docstring / #433)"
        )
    if not rows:
        return _empty_calendar(granularity)

    per_bucket_program: dict[date, dict[str, Decimal]] = {}
    for row in rows:
        bucket_date = row["bucket_date"]
        program = buy_program_bucket(row["order_type"])
        qty = Decimal(str(row["total_qty"]))
        bucket = per_bucket_program.setdefault(bucket_date, {})
        bucket[program] = bucket.get(program, Decimal("0")) + qty

    min_bucket = min(per_bucket_program)
    max_bucket = max(per_bucket_program)
    bucket_starts: list[date] = []
    cursor = min_bucket
    while cursor <= max_bucket:
        bucket_starts.append(cursor)
        cursor = _next_bucket_start(cursor, granularity)

    programs = sorted(
        {program for bucket in per_bucket_program.values() for program in bucket},
        key=_program_sort_key,
    )
    series_by_program: dict[str, list[Decimal]] = {program: [] for program in programs}
    total: list[Decimal] = []
    for bucket_date in bucket_starts:
        bucket_values = per_bucket_program.get(bucket_date, {})
        bucket_total = Decimal("0")
        for program in programs:
            value = bucket_values.get(program, Decimal("0"))
            series_by_program[program].append(value)
            bucket_total += value
        total.append(bucket_total)

    return ProgramDemandCalendar(
        granularity=granularity,
        bucket_starts=tuple(bucket_starts),
        series_by_program=MappingProxyType(
            {program: tuple(values) for program, values in series_by_program.items()}
        ),
        total=tuple(total),
    )


def verify_partition_exhaustive(calendar: ProgramDemandCalendar) -> bool:
    """``True`` iff, for every bucket, summing the per-program series
    reconstructs ``calendar.total`` EXACTLY (``Decimal`` equality, no
    tolerance) — the golden invariant of the taxonomy: ``buy_program_bucket``
    maps every ``order_type`` (including ``None``) to exactly one of
    :data:`BUY_PROGRAM_BUCKETS`, so the partition is exhaustive by
    construction. This function is the explicit, testable witness of that
    invariant (rather than a bare assertion buried in the builder) — the
    proof harness calls it once per series as a belt-and-braces guard before
    trusting a ΔFVA number.
    """
    for index, expected in enumerate(calendar.total):
        actual = sum(
            (series[index] for series in calendar.series_by_program.values()),
            Decimal("0"),
        )
        if actual != expected:
            return False
    return True


def get_historical_demand_by_program(
    db: DictRowConnection,
    item_id: UUID,
    location_id: UUID,
    lookback_days: int,
    granularity: str,
) -> ProgramDemandCalendar:
    """Dense, per-buy-program demand calendar for one (item, location), read
    from ``demand_history`` only (migration 048's ``order_type``).

    SELECT-only, single grouped query: ``date_trunc(granularity, booked_date)``
    x ``order_type`` -> ``SUM(ordered_quantity)``. The taxonomy mapping
    (``order_type`` -> program bucket) happens in Python, in
    :func:`build_program_demand_calendar`, which also builds the dense
    zero-filled calendar shared by every program — see that function.

    Business predicates: the SAME shared
    ``repository._DEMAND_HISTORY_BUSINESS_PREDICATES`` as every other reader
    (``stream='regular'``, inter-entity excluded, ``booked_date`` present,
    strict past, bounded by ``lookback_days``) — never a re-derived copy.

    Site resolution: the SAME single-source
    ``repository._warehouse_codes_subquery`` as every other reader
    (``location.external_id`` UNION its ``location_aliases``, ADR-031) —
    never a hand-written ``warehouse_id = external_id`` equality.

    ``org_id`` is OUT OF SCOPE for PR1 (the roadmap's "extensible key" note):
    rows from every operating company are pooled into the same series here,
    exactly like the leaf reader ``repository.get_historical_demand`` today.

    No degraded CustomerOrderDemand fallback (unlike the leaf reader): a
    graph ``CustomerOrderDemand`` node carries no ``order_type`` to segment
    by, so a fallback here would silently collapse every program into a
    single bucket on an install whose demand_history is empty/not yet
    ingested — worse than the honest empty calendar this function returns
    instead (``build_program_demand_calendar`` on zero rows).

    ``demand_history`` carries no scenario_id (actuals are scenario-
    invariant, like every other demand_history reader in this package) —
    there is deliberately no ``scenario_id`` parameter.

    Raises:
        ValueError: ``granularity`` not in ``{"weekly", "monthly"}`` — daily
            per-program calendars are disjoint/non-summable (see module
            docstring and ``build_program_demand_calendar``); fail loudly
            rather than silently degrade to a near-all-zero comparison.
    """
    if granularity not in SEGMENTED_GRANULARITIES:
        raise ValueError(
            f"get_historical_demand_by_program requires granularity in "
            f"{sorted(SEGMENTED_GRANULARITIES)} (daily is out of scope -- "
            "buy-program calendars are disjoint/non-summable at daily "
            f"granularity, see #433); got {granularity!r}"
        )
    trunc_unit = SEGMENTED_GRANULARITIES[granularity]
    rows = db.execute(
        f"""
        SELECT date_trunc(%(trunc_unit)s, dh.booked_date)::date AS bucket_date,
               dh.order_type AS order_type,
               COALESCE(SUM(dh.ordered_quantity), 0) AS total_qty
        FROM demand_history dh
        WHERE dh.item_id = %(item_id)s
          AND dh.warehouse_id IN ({_warehouse_codes_subquery()})
          AND {_DEMAND_HISTORY_BUSINESS_PREDICATES}
        GROUP BY 1, 2
        ORDER BY 1 ASC
        """,
        {
            "trunc_unit": trunc_unit,
            "item_id": item_id,
            "location_id": location_id,
            "lookback_days": lookback_days,
        },
    ).fetchall()
    return build_program_demand_calendar(rows, granularity)


# ---------------------------------------------------------------------------
# Segmented backtest -> compute_fva -> DeltaFVA. DB-free, model-free: the
# forecast model is injected (ForecastFn, same inversion as accuracy.py), so
# this module never imports an engine — the caller (the proof harness) wires
# the stat engine the SAME way the existing backtest path does.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SegmentationProofResult:
    """ΔFVA proof of buy-program segmentation for one series (ADR-030).

    AVANT = ``mixed_report``: the SAME rolling-origin orchestration
    (``accuracy.evaluate_rolling_origin``) run with ONE partition — the
    calendar's blended ``total``, forecast by the injected ``forecast_fn``
    directly.

    APRÈS = ``segmented_report``: the IDENTICAL orchestration (same series,
    same ``min_train``/``horizon``/``step``, so same cutoffs by
    construction) with N partitions — at every origin, EVERY buy program is
    forecast on its OWN train slice (the shared dense calendar makes the
    slice indices line up) by the SAME injected ``forecast_fn``, then the
    per-program curves are summed. Residuals are additive by construction
    (``actual_total - sum(forecast_i) == sum(actual_i - forecast_i)``), so
    the reconstructed total's rolling-origin report is exactly what "N
    independent forecasts, recombined" would score.

    ``fva_mixed`` / ``fva_segmented``: ``fva.compute_fva`` called on
    ``mixed_report`` / ``segmented_report`` respectively — the EXISTING FVA
    function, never re-implemented (``fva.py:166``). Both share the same
    ``history`` (``calendar.total``) and ``season_length``, so both naive
    baselines are computed on the identical window.

    ``delta_fva_wape`` / ``delta_fva_mase`` = ``fva_segmented.fva_* -
    fva_mixed.fva_*`` (positive = segmentation adds value over the blended
    forecast). Algebraically this equals ``WAPE_mixed - WAPE_segmented``
    (the shared seasonal-naive term cancels), which is the contract's stated
    ΔFVA — derived here from two calls to the real ``compute_fva``, not a
    parallel formula. ``None`` whenever EITHER underlying FVA is ``None``
    (mismatched/unalignable naive cutoffs, non-comparable WAPE/MASE) —
    None-honest, never an invented delta.

    ``basis_count``: the shared rolling-origin cutoff count backing the
    delta (``mixed_report.n_cutoffs`` == ``segmented_report.n_cutoffs`` by
    construction) when a delta was computed, else ``0`` — never a count
    presented alongside a ``None`` delta.
    """

    granularity: str
    programs: tuple[str, ...]
    n_buckets: int
    min_train: int
    horizon: int
    mixed_report: AccuracyReport | None
    segmented_report: AccuracyReport | None
    fva_mixed: FvaResult | None
    fva_segmented: FvaResult | None
    delta_fva_wape: Decimal | None
    delta_fva_mase: Decimal | None
    basis_count: int


def _delta(after: Decimal | None, before: Decimal | None) -> Decimal | None:
    """``after - before``, ``None`` if either operand is ``None`` (a delta
    over a non-comparable FVA is itself non-comparable)."""
    if after is None or before is None:
        return None
    return after - before


def _sum_program_forecasts(
    forecast_fn: ForecastFn,
    series_by_program: Mapping[str, Sequence[Decimal]],
) -> ForecastFn:
    """Wrap ``forecast_fn`` into the APRÈS (N-partition) forecast function
    consumed by ``evaluate_rolling_origin``: at a rolling origin whose
    training slice has length ``len(train)``, forecast EVERY program on its
    OWN training slice of that SAME length (the shared dense calendar makes
    the index the same origin for every program) with ``forecast_fn``, then
    sum the resulting curves step-by-step. The origin index is recovered
    from ``len(train)`` rather than threaded explicitly, matching
    ``evaluate_rolling_origin``'s own ``forecast_fn`` contract
    (``forecast_fn(train, horizon) -> curve``) exactly — no orchestration
    change needed to go from AVANT (1 partition) to APRÈS (N partitions).
    """

    def _segmented(train: Sequence[Decimal], periods: int) -> list[Decimal]:
        origin = len(train)
        summed = [Decimal("0")] * periods
        for program_series in series_by_program.values():
            curve = forecast_fn(program_series[:origin], periods)
            if len(curve) < periods:
                raise ValueError(
                    f"forecast_fn returned {len(curve)} values for a "
                    f"{periods}-step window (program partition, origin={origin})"
                )
            summed = [total + Decimal(str(value)) for total, value in zip(summed, curve)]
        return summed

    return _segmented


def run_segmented_fva_proof(
    calendar: ProgramDemandCalendar,
    forecast_fn: ForecastFn,
    *,
    min_train: int,
    horizon: int = 1,
    step: int = 1,
) -> SegmentationProofResult:
    """Run the AVANT (1-partition) / APRÈS (N-partition) backtest pair on
    ``calendar`` and derive ΔFVA via the existing ``fva.compute_fva`` — see
    :class:`SegmentationProofResult` for the exact algebra and None-honest
    contract.

    ``forecast_fn`` is the SAME injected stat model for both orchestrations
    (AVANT forecasts the blended total directly; APRÈS forecasts each
    program with it and sums) — the proof isolates the effect of
    segmentation, not a change of forecasting method. Callers wire the SAME
    stat engine the existing Pyramide backtest path uses (see
    ``scripts/prove_segmentation_fva.py``).

    ``min_train`` / ``horizon`` / ``step`` are the caller's rolling-origin
    policy (this module carries no embedded default, like ``accuracy.py`` /
    ``fva.py`` — thresholds and windows belong to the consumer).

    None-honest short-circuit: when the calendar has no buckets, or
    ``min_train`` cannot form at least one rolling-origin cutoff
    (``min_train < 1`` or ``min_train >= n_buckets``), returns a result with
    every report/FVA field ``None`` and ``basis_count=0`` — never a
    fabricated ``AccuracyReport``/``FvaResult`` on data that cannot support
    one.
    """
    n_buckets = len(calendar.bucket_starts)
    programs = calendar.programs
    if n_buckets == 0 or min_train < 1 or min_train >= n_buckets:
        return SegmentationProofResult(
            granularity=calendar.granularity,
            programs=programs,
            n_buckets=n_buckets,
            min_train=min_train,
            horizon=horizon,
            mixed_report=None,
            segmented_report=None,
            fva_mixed=None,
            fva_segmented=None,
            delta_fva_wape=None,
            delta_fva_mase=None,
            basis_count=0,
        )

    total = list(calendar.total)
    season_length = resolve_season_length(calendar.granularity, {})

    mixed_report = evaluate_rolling_origin(
        series=total,
        forecast_fn=forecast_fn,
        horizon=horizon,
        min_train=min_train,
        step=step,
        m=1,
    )
    segmented_fn = _sum_program_forecasts(forecast_fn, calendar.series_by_program)
    segmented_report = evaluate_rolling_origin(
        series=total,
        forecast_fn=segmented_fn,
        horizon=horizon,
        min_train=min_train,
        step=step,
        m=1,
    )
    # Both reports were built from the SAME series/min_train/step -- only
    # forecast_fn differs -- so the cutoffs are identical by construction.
    # A mismatch would be a structural bug in this function, not a data
    # condition, hence an assertion rather than a None-honest branch.
    assert mixed_report.n_cutoffs == segmented_report.n_cutoffs, (
        "mixed/segmented rolling-origin diverged in cutoff count despite "
        "sharing series/min_train/step -- structural bug"
    )

    fva_mixed = compute_fva(total, season_length, stat_report=mixed_report)
    fva_segmented = compute_fva(total, season_length, stat_report=segmented_report)

    delta_wape = _delta(fva_segmented.fva_wape, fva_mixed.fva_wape)
    delta_mase = _delta(fva_segmented.fva_mase, fva_mixed.fva_mase)
    basis_count = mixed_report.n_cutoffs if delta_wape is not None else 0

    return SegmentationProofResult(
        granularity=calendar.granularity,
        programs=programs,
        n_buckets=n_buckets,
        min_train=min_train,
        horizon=horizon,
        mixed_report=mixed_report,
        segmented_report=segmented_report,
        fva_mixed=fva_mixed,
        fva_segmented=fva_segmented,
        delta_fva_wape=delta_wape,
        delta_fva_mase=delta_mase,
        basis_count=basis_count,
    )


@dataclass(frozen=True)
class SegmentationProofRow:
    """One pilot series' proof outcome — the harness table's unit of
    output/aggregation. ``volume`` is the aggregate weight (sum of
    ``calendar.total`` over the evaluated window), NOT a business ASP-priced
    value — a $-weighted variant belongs to a consumer with item_asp access,
    out of scope for this DB-free module."""

    item_id: UUID
    location_id: UUID
    result: SegmentationProofResult
    volume: Decimal


def aggregate_delta_fva_wape(
    rows: Sequence[SegmentationProofRow],
) -> tuple[Decimal | None, int]:
    """Volume-weighted mean of ``delta_fva_wape`` across proof rows that
    carry a computable delta (``basis_count > 0`` and positive ``volume``).

    Returns ``(weighted_mean, n_series)``: None-honest — ``(None, 0)`` when
    no row contributes (every series had insufficient data), never an
    invented aggregate. ``n_series`` counts only the CONTRIBUTING rows, not
    every row passed in — the harness reports the two counts separately so
    "0 series proved" is never silently indistinguishable from "5 series
    averaged to a small number".
    """
    weighted_sum = Decimal("0")
    weight_total = Decimal("0")
    n_series = 0
    for row in rows:
        if row.result.delta_fva_wape is None or row.volume <= 0:
            continue
        weighted_sum += row.result.delta_fva_wape * row.volume
        weight_total += row.volume
        n_series += 1
    if weight_total == 0:
        return None, 0
    return weighted_sum / weight_total, n_series
