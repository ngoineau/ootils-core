"""
Reconciliation bench (Pyramide axis A — design §8): which reconciliation
method wins, per block, on real held-out demand.

The pilot decides on numbers, not vibes: for each summing block the bench
replays a forecast **as of a past cutoff** and scores every candidate
reconciliation method against the demand that actually booked afterwards.

Temporal split — EXPLICIT, never CURRENT_DATE
---------------------------------------------
``cutoff = today - holdout_days``. TRAIN is ``demand_history`` STRICTLY
before the cutoff (bounded by ``lookback_days``); EVAL is
``[cutoff, cutoff + horizon)``. The SQL here composes the shared
``_DEMAND_HISTORY_STREAM_PREDICATES`` of ``pyramide/repository.py`` (the
pure business rules: stream='regular', inter-entity excluded, booked_date
present) with an explicit parameterized window — reusing the runtime
readers' CURRENT_DATE-bounded predicates would leak holdout days into
training, which is exactly the bug this split exists to prevent.

What is compared
----------------
Per block and per requested method (``reconcile()`` does the work — the
bench adds ZERO reconciliation logic), plus the ``'base'`` comparator:

* ``'base'`` = the un-reconciled recon-level forecasts. Leaf-level base
  curves are a documented NAIVE UNIFORM disaggregation (each leaf under a
  recon node receives ``curve / n_leaves``): the engine only forecasts at
  the reconciliation level, so no per-leaf base forecast exists — uniform
  split is the honest "no reconciliation intelligence" floor.

Scores come from ``ootils_core.pyramide.accuracy`` (wape / mase / bias) —
zero local metric formulas — at three levels: leaves (pooled), the
reconciliation level (pooled over its nodes) and the block root. MASE is
the per-series mean of the defined values (None-safe: series whose train
history is constant/short contribute nothing, per the accuracy contract).

Verdict: per block, the method with the minimal LEAF-level WAPE (None
WAPEs excluded; ties broken by method name — deterministic).

Strategies
----------
``'exact'`` is the only implemented strategy (single item hierarchy).
``'two_stage'`` (item x geography) raises ``NotImplementedError``: it is
gated on the geo hierarchy leg, which does not exist yet. Strategy
validation happens BEFORE any DB access, so the gate is testable without
a database.

Genericity: hierarchy/domain/levels all come from the migration-047
registry — no business constant anywhere in this module.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Mapping, Sequence
from uuid import UUID

import psycopg

from ..accuracy import bias as accuracy_bias
from ..accuracy import mase as accuracy_mase
from ..accuracy import wape as accuracy_wape
from ..engines import PyramideEngineError, PyramideForecastEngine
from ..models import METHOD_AUTO_SELECT
from ..repository import _DEMAND_HISTORY_STREAM_PREDICATES
from .reconcile import (
    MINT_MIN_INSAMPLE,
    RECON_MINT_SHRINK,
    SUPPORTED_RECON_METHODS,
    MintInputs,
    ReconciliationError,
    reconcile,
)
from .summing import AGGREGATE, LEAF, SummingBlock, load_summing_blocks

logger = logging.getLogger(__name__)

__all__ = [
    "LEVEL_LEAF",
    "LEVEL_ROOT",
    "METHOD_BASE",
    "STRATEGY_EXACT",
    "STRATEGY_TWO_STAGE",
    "BenchReport",
    "BenchRow",
    "build_bench_report",
    "compute_bench_row",
    "run_reconciliation_bench",
]

_ZERO = Decimal("0")

METHOD_BASE = "base"

LEVEL_LEAF = "leaf"
LEVEL_ROOT = "root"
# Display/sort rank: root first, intermediate (recon) levels, leaves last.
_LEVEL_RANK = {LEVEL_ROOT: 0, LEVEL_LEAF: 2}

STRATEGY_EXACT = "exact"
STRATEGY_TWO_STAGE = "two_stage"


# ---------------------------------------------------------------------------
# Report structures (pure — unit-testable without a database)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BenchRow:
    """One scored (block, level, method) cell of the bench.

    ``wape``/``mase`` follow the accuracy module's None sentinel (metric
    undefined on this data — e.g. zero actual demand in the eval window);
    ``bias`` is None only when the row scored zero observations, which
    the bench never emits itself (defensive for hand-built rows).
    """

    block: str
    level: str
    method: str
    wape: Decimal | None
    mase: Decimal | None
    bias: Decimal | None
    n_series: int
    n_obs: int


@dataclass(frozen=True)
class BenchReport:
    """Deterministic bench result: sorted rows + None-safe verdicts."""

    domain: str
    cutoff: date
    horizon: int
    holdout_days: int
    lookback_days: int
    rows: tuple[BenchRow, ...]
    warnings: tuple[str, ...] = ()

    def verdicts(self) -> dict[str, str | None]:
        """Per block: the method with the minimal leaf-level WAPE.

        None-safe: rows whose WAPE is None (undefined metric) never win;
        a block with no defined leaf WAPE at all gets verdict ``None``
        ("cannot rank" — never a silent arbitrary winner). Ties break on
        the method name — deterministic.
        """
        out: dict[str, str | None] = {}
        best: dict[str, tuple[Decimal, str]] = {}
        for row in self.rows:
            out.setdefault(row.block, None)
            if row.level != LEVEL_LEAF or row.wape is None:
                continue
            key = (row.wape, row.method)
            if row.block not in best or key < best[row.block]:
                best[row.block] = key
        for block, (_, method) in best.items():
            out[block] = method
        return out

    def to_rows(self) -> list[tuple]:
        """Plain tuples for tabular display, in the report's sort order."""
        return [
            (
                row.block,
                row.level,
                row.method,
                row.wape,
                row.mase,
                row.bias,
                row.n_series,
                row.n_obs,
            )
            for row in self.rows
        ]


def build_bench_report(
    rows: Sequence[BenchRow],
    *,
    domain: str,
    cutoff: date,
    horizon: int,
    holdout_days: int,
    lookback_days: int,
    warnings: Sequence[str] = (),
) -> BenchReport:
    """Assemble a report with deterministic row order.

    Sort key: block, then level rank (root -> intermediate -> leaves),
    then level name, then method — same inputs, byte-identical report.
    """
    ordered = tuple(
        sorted(
            rows,
            key=lambda r: (r.block, _LEVEL_RANK.get(r.level, 1), r.level, r.method),
        )
    )
    return BenchReport(
        domain=domain,
        cutoff=cutoff,
        horizon=horizon,
        holdout_days=holdout_days,
        lookback_days=lookback_days,
        rows=ordered,
        warnings=tuple(warnings),
    )


def compute_bench_row(
    *,
    block: str,
    level: str,
    method: str,
    actual_curves: Sequence[Sequence[Decimal]],
    forecast_curves: Sequence[Sequence[Decimal]],
    insamples: Sequence[Sequence[Decimal]],
) -> BenchRow:
    """Score one (block, level, method) cell — pure, golden-testable.

    WAPE and bias are POOLED over every (series, period) pair of the
    level (volume-weighted, the business-facing aggregation). MASE is the
    mean of the per-series values (each series scaled by its OWN train
    history, per the accuracy contract); series whose MASE is undefined
    (constant/short insample) are excluded, and the row's MASE is None
    when no series qualifies. Every formula lives in
    ``ootils_core.pyramide.accuracy`` — nothing metric-shaped here.
    """
    if not actual_curves:
        raise ValueError("actual_curves must not be empty")
    if not (len(actual_curves) == len(forecast_curves) == len(insamples)):
        raise ValueError(
            "actual_curves, forecast_curves and insamples must align "
            f"({len(actual_curves)}, {len(forecast_curves)}, {len(insamples)})"
        )
    pooled_actuals: list[Decimal] = []
    pooled_forecasts: list[Decimal] = []
    mase_values: list[Decimal] = []
    for actuals, forecasts, insample in zip(
        actual_curves, forecast_curves, insamples
    ):
        if len(actuals) != len(forecasts):
            raise ValueError(
                f"series curves must have the same length "
                f"({len(actuals)} != {len(forecasts)})"
            )
        pooled_actuals.extend(actuals)
        pooled_forecasts.extend(forecasts)
        series_mase = accuracy_mase(actuals, forecasts, insample)
        if series_mase is not None:
            mase_values.append(series_mase)
    mase_value: Decimal | None = None
    if mase_values:
        mase_value = sum(mase_values, _ZERO) / Decimal(len(mase_values))
    return BenchRow(
        block=block,
        level=level,
        method=method,
        wape=accuracy_wape(pooled_actuals, pooled_forecasts),
        mase=mase_value,
        bias=accuracy_bias(pooled_actuals, pooled_forecasts),
        n_series=len(actual_curves),
        n_obs=len(pooled_actuals),
    )


# ---------------------------------------------------------------------------
# Bench runner (DB-backed)
# ---------------------------------------------------------------------------


def run_reconciliation_bench(
    db: psycopg.Connection,
    *,
    domain: str,
    block_level: str | None = None,
    recon_level: str | None = None,
    lookback_days: int = 365,
    horizon: int = 28,
    holdout_days: int = 28,
    methods: Sequence[str] = ("middleout",),
    strategy: str = STRATEGY_EXACT,
    block_codes: Sequence[str] | None = None,
    forecast_engine: PyramideForecastEngine | None = None,
    today: date | None = None,
) -> BenchReport:
    """Run the reconciliation bench on the domain's default hierarchy.

    For every block (optionally restricted to ``block_codes``): train
    strictly before ``cutoff = today - holdout_days`` (lookback-bounded),
    forecast+reconcile over ``horizon`` days from the cutoff with each
    requested method, score against the booked actuals of
    ``[cutoff, cutoff + horizon)`` — plus the ``'base'`` comparator (see
    module docstring for its naive-uniform leaf floor).

    ``today`` exists for deterministic replays (defaults to the wall
    clock — a bench is an operator tool, not a core calculation).

    A block that cannot be benched (no node at the recon level, no
    training history, engine failure) is SKIPPED with a warning — one
    cold block must not kill the whole scoreboard; the warning is the
    fail-loudly trace.

    Raises:
        NotImplementedError: ``strategy='two_stage'`` (gated on the geo
            hierarchy leg — validated before any DB access).
        ValueError: unknown strategy/method, non-positive windows, or
            ``block_codes`` naming blocks the hierarchy does not have.
    """
    _validate_bench_params(
        strategy=strategy,
        horizon=horizon,
        holdout_days=holdout_days,
        lookback_days=lookback_days,
        methods=methods,
    )
    engine = forecast_engine or PyramideForecastEngine()
    cutoff = (today or date.today()) - timedelta(days=holdout_days)

    blocks = load_summing_blocks(db, domain=domain, block_level=block_level)
    if block_codes is not None:
        wanted = set(block_codes)
        blocks = [b for b in blocks if b.block_code in wanted]
        missing = wanted - {b.block_code for b in blocks}
        if missing:
            raise ValueError(
                f"blocks {sorted(missing)} not found at level "
                f"'{block_level or 'root'}' of domain '{domain}'"
            )

    rows: list[BenchRow] = []
    warnings: list[str] = []
    for block in blocks:
        if not block.leaves:
            warnings.append(
                f"block '{block.block_code}': no leaf column — skipped"
            )
            continue
        block_rows, block_warnings = _bench_block(
            db,
            block,
            recon_level or block.block_level,
            engine,
            cutoff=cutoff,
            horizon=horizon,
            lookback_days=lookback_days,
            methods=tuple(methods),
        )
        rows.extend(block_rows)
        warnings.extend(block_warnings)

    return build_bench_report(
        rows,
        domain=domain,
        cutoff=cutoff,
        horizon=horizon,
        holdout_days=holdout_days,
        lookback_days=lookback_days,
        warnings=warnings,
    )


def _validate_bench_params(
    *,
    strategy: str,
    horizon: int,
    holdout_days: int,
    lookback_days: int,
    methods: Sequence[str],
) -> None:
    """Parameter gate — runs BEFORE any DB access (testable without one)."""
    if strategy == STRATEGY_TWO_STAGE:
        raise NotImplementedError("two_stage: gated on the geo hierarchy")
    if strategy != STRATEGY_EXACT:
        raise ValueError(
            f"unknown bench strategy '{strategy}' "
            f"(supported: ['{STRATEGY_EXACT}'; '{STRATEGY_TWO_STAGE}' is gated])"
        )
    if horizon < 1:
        raise ValueError(f"horizon must be >= 1 (got {horizon})")
    if holdout_days < 1:
        raise ValueError(f"holdout_days must be >= 1 (got {holdout_days})")
    if horizon > holdout_days:
        # The eval window [cutoff, cutoff+horizon) would overflow past the
        # held-out actuals: _dense_curve would fill those days with phantom
        # zeros and penalise EVERY method with fake misses (review PR4).
        # Fail loudly instead of benching against the void.
        raise ValueError(
            f"horizon ({horizon}) must be <= holdout_days ({holdout_days}) — "
            "the eval window cannot extend past the held-out actuals"
        )
    if lookback_days < 1:
        raise ValueError(f"lookback_days must be >= 1 (got {lookback_days})")
    if not methods:
        raise ValueError("methods must not be empty")
    unknown = set(methods) - SUPPORTED_RECON_METHODS
    if unknown:
        raise ValueError(
            f"unsupported reconciliation methods {sorted(unknown)} "
            f"(supported: {sorted(SUPPORTED_RECON_METHODS)}; "
            f"'{METHOD_BASE}' is always benched implicitly)"
        )


def _bench_block(
    db: psycopg.Connection,
    block: SummingBlock,
    recon_level: str,
    engine: PyramideForecastEngine,
    *,
    cutoff: date,
    horizon: int,
    lookback_days: int,
    methods: tuple[str, ...],
) -> tuple[list[BenchRow], list[str]]:
    """Score one block: train before cutoff, evaluate on the holdout."""
    warnings: list[str] = []
    recon_refs = [
        (i, ref)
        for i, ref in enumerate(block.series)
        if ref.kind == AGGREGATE and ref.level == recon_level
    ]
    if not recon_refs:
        return [], [
            f"block '{block.block_code}': no node at reconciliation level "
            f"'{recon_level}' — skipped"
        ]

    train = _load_daily_by_item(
        db,
        block.leaves,
        start=cutoff - timedelta(days=lookback_days),
        stop=cutoff,
    )
    holdout = _load_daily_by_item(
        db, block.leaves, start=cutoff, stop=cutoff + timedelta(days=horizon)
    )
    leaf_totals = {
        key: sum(by_date.values(), _ZERO) for key, by_date in train.items()
    }
    leaf_actuals = [
        _dense_curve(holdout.get(leaf, {}), cutoff, horizon)
        for leaf in block.leaves
    ]
    leaf_insamples = [
        _sparse_series(train.get(leaf, {})) for leaf in block.leaves
    ]

    # Base forecasts at the reconciliation level, on TRAIN data only.
    base_curves: dict[str, tuple[Decimal, ...]] = {}
    node_insample: dict[str, list[Decimal]] = {}
    for i, ref in recon_refs:
        if not block.rows[i]:
            continue  # node without leaves: nothing to forecast or score
        series = _sparse_sum(train, [block.leaves[j] for j in block.rows[i]])
        if not series:
            return [], [
                f"block '{block.block_code}': node '{ref.key}' has no "
                f"training history before {cutoff.isoformat()} — skipped"
            ]
        node_insample[ref.key] = series
        try:
            base_curves[ref.key] = _clamped_forecast(
                engine, series, horizon, cutoff
            )
        except PyramideEngineError as exc:
            return [], [
                f"block '{block.block_code}': base forecast failed for node "
                f"'{ref.key}': {exc} — skipped"
            ]

    mint_inputs: MintInputs | None = None
    if RECON_MINT_SHRINK in methods:
        mint_inputs, mint_warnings = _bench_mint_inputs(
            block, base_curves, train, node_insample, engine, cutoff, horizon
        )
        warnings.extend(
            f"block '{block.block_code}': {w}" for w in mint_warnings
        )

    root_index = next(
        i
        for i, ref in enumerate(block.series)
        if ref.kind == AGGREGATE and ref.key == block.block_code
    )
    recon_indices = {i for i, _ in recon_refs}
    scored_refs = [(i, ref) for i, ref in recon_refs if block.rows[i]]

    rows: list[BenchRow] = []

    def _score_levels(
        method: str,
        leaf_forecasts: Sequence[Sequence[Decimal]],
        node_forecasts: Mapping[str, Sequence[Decimal]],
        root_forecast: Sequence[Decimal],
    ) -> None:
        rows.append(compute_bench_row(
            block=block.block_code, level=LEVEL_LEAF, method=method,
            actual_curves=leaf_actuals, forecast_curves=leaf_forecasts,
            insamples=leaf_insamples,
        ))
        if scored_refs:
            rows.append(compute_bench_row(
                block=block.block_code, level=recon_level, method=method,
                actual_curves=[
                    _pointwise_sum(
                        [leaf_actuals[j] for j in block.rows[i]], horizon
                    )
                    for i, _ in scored_refs
                ],
                forecast_curves=[
                    node_forecasts[ref.key] for _, ref in scored_refs
                ],
                insamples=[node_insample[ref.key] for _, ref in scored_refs],
            ))
        if root_index not in recon_indices:
            # Root row only when the root is not itself the recon level
            # (otherwise it duplicates the recon-level row exactly).
            rows.append(compute_bench_row(
                block=block.block_code, level=LEVEL_ROOT, method=method,
                actual_curves=[_pointwise_sum(leaf_actuals, horizon)],
                forecast_curves=[root_forecast],
                insamples=[_sparse_sum(train, block.leaves)],
            ))

    for method in methods:
        try:
            recon = reconcile(
                block,
                recon_level,
                base_curves,
                leaf_totals,
                method=method,
                mint_inputs=(
                    mint_inputs if method == RECON_MINT_SHRINK else None
                ),
            )
        except ReconciliationError as exc:
            warnings.append(
                f"block '{block.block_code}': method '{method}' failed: "
                f"{exc} — no rows for this method"
            )
            continue
        if recon.recon_method != method:
            warnings.append(
                f"block '{block.block_code}': method '{method}' fell back "
                f"to '{recon.recon_method}' — its rows score the fallback"
            )
        leaf_forecasts = [
            recon.values[i]
            for i, ref in enumerate(block.series)
            if ref.kind == LEAF
        ]
        _score_levels(
            method,
            leaf_forecasts,
            {ref.key: recon.values[i] for i, ref in scored_refs},
            recon.values[root_index],
        )

    # 'base' comparator: un-reconciled node curves; leaf floor = naive
    # UNIFORM disaggregation (documented in the module docstring). Leaves
    # not under any populated recon node keep a zero curve.
    base_leaf: list[Sequence[Decimal]] = [
        tuple(_ZERO for _ in range(horizon)) for _ in block.leaves
    ]
    for i, ref in scored_refs:
        n = Decimal(len(block.rows[i]))
        for j in block.rows[i]:
            base_leaf[j] = tuple(v / n for v in base_curves[ref.key])
    _score_levels(
        METHOD_BASE,
        base_leaf,
        base_curves,
        _pointwise_sum(list(base_curves.values()), horizon),
    )
    return rows, warnings


def _bench_mint_inputs(
    block: SummingBlock,
    base_curves: Mapping[str, tuple[Decimal, ...]],
    train: Mapping[str, dict[date, Decimal]],
    node_insample: Mapping[str, list[Decimal]],
    engine: PyramideForecastEngine,
    cutoff: date,
    horizon: int,
) -> tuple[MintInputs | None, list[str]]:
    """In-memory MinT inputs from the TRAIN split (no extra DB reads).

    Same V1 approximations as the runner (ADR-022): positional alignment
    of sparse series tails, naive lag-1 fitted proxy. Any empty series or
    a tail shorter than MINT_MIN_INSAMPLE skips MinT (the dispatcher then
    falls back to middle-out, and the bench warns).
    """
    histories: list[tuple[object, list[Decimal]]] = []
    for i, ref in enumerate(block.series):
        if ref.kind == AGGREGATE:
            series = node_insample.get(ref.key) or _sparse_sum(
                train, [block.leaves[j] for j in block.rows[i]]
            )
        else:
            series = _sparse_series(train.get(ref.key, {}))
        if not series:
            return None, [
                f"{RECON_MINT_SHRINK} skipped: series '{ref.key}' has no "
                "training history (middle-out fallback)"
            ]
        histories.append((ref, series))

    k = min(len(series) for _, series in histories)
    if k < MINT_MIN_INSAMPLE:
        return None, [
            f"{RECON_MINT_SHRINK} skipped: aligned insample tail is {k} < "
            f"{MINT_MIN_INSAMPLE} points (middle-out fallback)"
        ]

    aggregate_curves: dict[str, Sequence[Decimal]] = {}
    leaf_curves: dict[str, Sequence[Decimal]] = {}
    aggregate_insample: dict[str, Sequence[Decimal]] = {}
    leaf_insample: dict[str, Sequence[Decimal]] = {}
    aggregate_fitted: dict[str, Sequence[Decimal]] = {}
    leaf_fitted: dict[str, Sequence[Decimal]] = {}
    for ref, series in histories:
        if ref.kind == AGGREGATE and ref.key in base_curves:
            curve: Sequence[Decimal] = base_curves[ref.key]
        else:
            try:
                curve = _clamped_forecast(engine, series, horizon, cutoff)
            except PyramideEngineError as exc:
                return None, [
                    f"{RECON_MINT_SHRINK} skipped: base forecast failed "
                    f"for series '{ref.key}': {exc} (middle-out fallback)"
                ]
        tail = tuple(series[-k:])
        fitted = (tail[0], *tail[:-1])  # naive lag-1 insample proxy
        if ref.kind == AGGREGATE:
            aggregate_curves[ref.key] = curve
            aggregate_insample[ref.key] = tail
            aggregate_fitted[ref.key] = fitted
        else:
            leaf_curves[ref.key] = curve
            leaf_insample[ref.key] = tail
            leaf_fitted[ref.key] = fitted
    return MintInputs(
        aggregate_curves=aggregate_curves,
        leaf_curves=leaf_curves,
        aggregate_insample=aggregate_insample,
        leaf_insample=leaf_insample,
        aggregate_fitted=aggregate_fitted,
        leaf_fitted=leaf_fitted,
    ), []


# ---------------------------------------------------------------------------
# Windowed history readers + series helpers
# ---------------------------------------------------------------------------


def _load_daily_by_item(
    db: psycopg.Connection,
    leaves: Sequence[str],
    *,
    start: date,
    stop: date,
) -> dict[str, dict[date, Decimal]]:
    """Daily booked sums per item over an EXPLICIT [start, stop) window.

    Composes the shared stream predicates (business rules) with the
    bench's parameterized window — this is the anti-leak read path: the
    same query serves the TRAIN split (stop = cutoff) and the EVAL split
    (start = cutoff), so the two can never overlap by construction.
    Cross-site sums, like every hierarchy reader (the site split is the
    DRP layer's job); demand_history is scenario-invariant (actuals).
    """
    rows = db.execute(
        f"""
        SELECT dh.item_id,
               dh.booked_date AS demand_date,
               COALESCE(SUM(dh.ordered_quantity), 0) AS total_qty
        FROM demand_history dh
        WHERE dh.item_id = ANY(%(item_ids)s)
          AND {_DEMAND_HISTORY_STREAM_PREDICATES}
          AND dh.booked_date >= %(start)s
          AND dh.booked_date < %(stop)s
        GROUP BY dh.item_id, dh.booked_date
        ORDER BY dh.item_id ASC, dh.booked_date ASC
        """,
        {"item_ids": [UUID(key) for key in leaves], "start": start, "stop": stop},
    ).fetchall()
    out: dict[str, dict[date, Decimal]] = {}
    for row in rows:
        out.setdefault(str(row["item_id"]), {})[row["demand_date"]] = Decimal(
            str(row["total_qty"])
        )
    return out


def _sparse_series(by_date: Mapping[date, Decimal]) -> list[Decimal]:
    """Sparse daily series, date ASC — the engines' history contract."""
    return [by_date[d] for d in sorted(by_date)]


def _sparse_sum(
    train: Mapping[str, dict[date, Decimal]], leaf_keys: Sequence[str]
) -> list[Decimal]:
    """Aggregate sparse series: per-date sums over the given leaves."""
    merged: dict[date, Decimal] = {}
    for key in leaf_keys:
        for day, qty in train.get(key, {}).items():
            merged[day] = merged.get(day, _ZERO) + qty
    return _sparse_series(merged)


def _dense_curve(
    by_date: Mapping[date, Decimal], start: date, horizon: int
) -> tuple[Decimal, ...]:
    """DENSE daily actuals over the eval window: a day without booking IS
    zero demand (unlike the sparse training contract — the forecast made
    a claim for every day of the horizon, so every day is scored)."""
    return tuple(
        by_date.get(start + timedelta(days=t), _ZERO) for t in range(horizon)
    )


def _pointwise_sum(
    curves: Sequence[Sequence[Decimal]], horizon: int
) -> tuple[Decimal, ...]:
    return tuple(
        sum((curve[t] for curve in curves), _ZERO) for t in range(horizon)
    )


def _clamped_forecast(
    engine: PyramideForecastEngine,
    history: Sequence[Decimal],
    horizon: int,
    cutoff: date,
) -> tuple[Decimal, ...]:
    """Base forecast on the TRAIN slice, clamped at 0 (same as runners)."""
    computation = engine.forecast(
        history=list(history),
        periods=horizon,
        method=METHOD_AUTO_SELECT,
        method_params={},
        model_strategy="stat",
        granularity="daily",
        horizon_start=cutoff,
        random_seed=0,
    )
    return tuple(max(value, _ZERO) for value in computation.values)
