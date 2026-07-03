"""
Hierarchical forecast reconciliation (Pyramide axis A — PR3).

Two paths, one coherence contract (docs/DESIGN-pyramide-forecasting.md
§3-4, docs/ADR-022-pyramide-reconciliation.md):

* ``middle_out()`` — **THE guaranteed deterministic path** (pure Python,
  Decimal arithmetic, golden-testable). Forecast at a configurable
  reconciliation level, disaggregate to the leaves by historical shares,
  aggregate every other level through the sparse summing matrix S.
* ``mint_shrink()`` — **optional improvement** via *hierarchicalforecast*
  (Nixtla, ``ootils-core[forecast]`` extra). Lazy import + fallback to
  middle-out (same pattern as ``engines.PyramideForecastEngine`` with
  statsforecast). MinT is float/numpy linear algebra: deterministic for a
  fixed environment but NOT bit-for-bit guaranteed across BLAS/numpy
  builds — it lives on the stochastic edge (seeded, versioned, logged),
  never in the deterministic core.

Coherence contract
------------------
Every result satisfies ``y_hat = S @ b_hat`` EXACTLY by construction:
leaf curves are computed first and every series value (all levels,
including the reconciliation level itself) is the sparse-row sum of the
leaf curves (``SummingBlock.rows``). The persisted reconciliation-level
curve is therefore re-derived from its leaves; it can differ from the
raw base forecast by Decimal-division dust (< 1e-25 relative) when the
shares are not exact decimals — never the other way around.

Proportions (design §4 — V1 answer)
-----------------------------------
* Shares = historical booked totals per leaf over a sliding lookback
  window, normalized within each reconciliation node. The sum of shares
  per node is 1 up to Decimal quantization: non-terminating divisions
  (e.g. thirds) leave dust in the last of the 28 significant digits —
  ~9 orders of magnitude below any business quantity. Coherence of the
  OUTPUT does not depend on it: every series, including the
  reconciliation level itself, is re-derived as the sparse sum of the
  disaggregated leaves (ŷ = S·b̂ exact by construction).
* Cold start (leaf with zero history): TWIN rule — the leaf inherits the
  arithmetic mean of the positive weights of its siblings (leaves
  attached to the SAME direct parent node), then shares are recomputed
  over the imputed weights. Documented extension point: forecasting the
  cold leaf directly with a foundation model then reconciling (design
  §4(c), implementation step 3 of §7) replaces this rule later.
* Natural zero: a leaf with zero history AND no positive-history twin
  keeps weight 0 (a structurally-unserved leaf must not receive demand);
  a warning documents each one.
* NON-GOAL of this PR (documented): season-phase proportion profiles
  (shares varying along the season per design §4) — V1 shares are a
  scalar per leaf over the lookback window.

Genericity: nothing here is domain-specific — blocks come from the
migration-047 registry via ``summing.py``; no business constant.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .summing import AGGREGATE, LEAF, SeriesRef, SummingBlock

logger = logging.getLogger(__name__)

RECON_MIDDLEOUT = "middleout"
RECON_MINT_SHRINK = "mintrace_wls_shrink"
SUPPORTED_RECON_METHODS = frozenset({RECON_MIDDLEOUT, RECON_MINT_SHRINK})

# MinT-shrink estimates a residual covariance: below this many aligned
# in-sample points the shrinkage target dominates and the result is
# noise — refuse and fall back (documented, deterministic rule).
MINT_MIN_INSAMPLE = 8

_ZERO = Decimal("0")


class ReconciliationError(ValueError):
    """The inputs cannot produce coherent reconciled curves."""


class ReconciliationUnavailable(RuntimeError):
    """The optional MinT backend (or its aligned inputs) is unavailable.

    Not a data error: the caller is expected to fall back to middle-out
    (the deterministic path) unless strict mode was requested.
    """


@dataclass(frozen=True)
class LeafShare:
    """Explainability record of one leaf's disaggregation share.

    ``weight`` is the historical total actually used (after twin
    imputation when ``cold_start`` is True); ``share`` = weight / sum of
    the weights of the same reconciliation node.
    """
    leaf: str
    recon_node: str
    weight: Decimal
    share: Decimal
    cold_start: bool


@dataclass(frozen=True)
class ReconciledBlock:
    """Coherent forecast curves for every series of one summing block.

    ``series`` mirrors ``SummingBlock.series`` (same order);
    ``values[i]`` is the horizon curve of ``series[i]``.
    ``recon_method`` is the method EFFECTIVELY applied ('middleout' or
    'mintrace_wls_shrink') — a MinT request that fell back reports
    'middleout' plus a warning, so provenance never lies.
    ``shares`` is empty on the MinT path (MinT derives its weights from
    the error covariance, not from explicit proportions — the
    explainability trade-off documented in design §4).
    """
    block_code: str
    recon_level: str
    recon_method: str
    series: tuple[SeriesRef, ...]
    values: tuple[tuple[Decimal, ...], ...]
    shares: tuple[LeafShare, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class MintInputs:
    """Aligned inputs for the optional MinT-shrink path.

    Curves: one base-forecast curve per series (aggregates keyed by
    node code, leaves by leaf column key), all of the same horizon
    length. Insample/fitted: aligned tails of the same length k
    (>= MINT_MIN_INSAMPLE) for EVERY series — MinT estimates a residual
    covariance, which requires a common observation grid.
    """
    aggregate_curves: Mapping[str, Sequence[Decimal]]
    leaf_curves: Mapping[str, Sequence[Decimal]]
    aggregate_insample: Mapping[str, Sequence[Decimal]]
    leaf_insample: Mapping[str, Sequence[Decimal]]
    aggregate_fitted: Mapping[str, Sequence[Decimal]]
    leaf_fitted: Mapping[str, Sequence[Decimal]]


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def reconcile(
    block: SummingBlock,
    recon_level: str,
    base_curves: Mapping[str, Sequence[Decimal]],
    leaf_history_totals: Mapping[str, Decimal],
    *,
    method: str = RECON_MIDDLEOUT,
    mint_inputs: MintInputs | None = None,
    strict: bool = False,
) -> ReconciledBlock:
    """
    Reconcile one block with the requested method.

    'middleout' runs the deterministic path directly. 'mintrace_wls_shrink'
    attempts the optional backend and FALLS BACK to middle-out on any
    failure (missing dependency, missing aligned inputs, backend error),
    reporting the effectively-applied method plus a warning — unless
    ``strict`` is True, in which case the failure is re-raised as a
    ReconciliationError (mirrors the engine's ``strict_backend``).
    """
    if method not in SUPPORTED_RECON_METHODS:
        raise ReconciliationError(
            f"unsupported reconciliation method '{method}' "
            f"(supported: {sorted(SUPPORTED_RECON_METHODS)})"
        )
    if method == RECON_MIDDLEOUT:
        return middle_out(block, recon_level, base_curves, leaf_history_totals)

    try:
        if mint_inputs is None:
            raise ReconciliationUnavailable(
                "MinT inputs not provided (base forecasts + aligned "
                "insample series for every series of the block)"
            )
        return mint_shrink(block, recon_level, mint_inputs)
    except ReconciliationError:
        raise  # data errors are never absorbed by the fallback
    except Exception as exc:
        if strict:
            raise ReconciliationError(
                f"{RECON_MINT_SHRINK} failed in strict mode: {exc}"
            ) from exc
        logger.warning(
            "reconcile: %s unavailable for block '%s'; falling back to "
            "middle-out: %s",
            RECON_MINT_SHRINK, block.block_code, exc,
        )
        fallback = middle_out(block, recon_level, base_curves, leaf_history_totals)
        return ReconciledBlock(
            block_code=fallback.block_code,
            recon_level=fallback.recon_level,
            recon_method=fallback.recon_method,  # 'middleout' — effective truth
            series=fallback.series,
            values=fallback.values,
            shares=fallback.shares,
            warnings=(
                f"{RECON_MINT_SHRINK} unavailable; fell back to "
                f"{RECON_MIDDLEOUT}: {exc}",
                *fallback.warnings,
            ),
        )


# ---------------------------------------------------------------------------
# Middle-out (deterministic core)
# ---------------------------------------------------------------------------


def middle_out(
    block: SummingBlock,
    recon_level: str,
    base_curves: Mapping[str, Sequence[Decimal]],
    leaf_history_totals: Mapping[str, Decimal],
) -> ReconciledBlock:
    """
    Deterministic middle-out reconciliation by historical proportions.

    Args:
        block: sparse summing matrix of the block.
        recon_level: hierarchy level the base forecasts were made at.
            Every leaf column must be covered by exactly one node of
            that level (fails loudly otherwise — pick a higher level).
        base_curves: node_code -> base forecast curve, one per
            reconciliation-level node that has leaves; all the same
            horizon length, values >= 0.
        leaf_history_totals: leaf column key -> historical booked total
            over the lookback window (the share numerators). Missing
            keys count as 0 (a leaf without history — twin rule / natural
            zero apply, see module docstring).
    """
    if not block.leaves:
        raise ReconciliationError(
            f"block '{block.block_code}' has no leaf columns — nothing to reconcile"
        )
    recon_nodes = _recon_nodes(block, recon_level)
    node_codes = {ref.key for _, ref in recon_nodes}
    unknown = set(base_curves) - node_codes
    if unknown:
        raise ReconciliationError(
            f"base curves provided for series {sorted(unknown)} which are "
            f"not '{recon_level}' nodes of block '{block.block_code}'"
        )

    horizon = _validated_horizon(base_curves)
    leaf_code_of = {
        ref.key: ref.leaf_code for ref in block.series if ref.kind == LEAF
    }

    leaf_curves: list[tuple[Decimal, ...] | None] = [None] * len(block.leaves)
    shares: list[LeafShare] = []
    warnings: list[str] = []

    for i, ref in recon_nodes:
        cols = block.rows[i]
        raw_curve = base_curves.get(ref.key)
        if not cols:
            if raw_curve is not None and any(_as_decimal(v) != 0 for v in raw_curve):
                raise ReconciliationError(
                    f"reconciliation node '{ref.key}' has a non-zero base "
                    f"forecast but no leaf column to disaggregate to"
                )
            continue
        if raw_curve is None:
            raise ReconciliationError(
                f"missing base curve for reconciliation node '{ref.key}' "
                f"of block '{block.block_code}'"
            )
        curve = tuple(_as_decimal(v) for v in raw_curve)
        if any(v < 0 for v in curve):
            raise ReconciliationError(
                f"base curve of node '{ref.key}' contains negative values "
                "(the engine clamps at 0 — negative input is a caller bug)"
            )

        weights, cold_cols, node_warnings = _node_weights(
            ref.key, cols, block, leaf_history_totals, leaf_code_of
        )
        total = sum(weights.values(), _ZERO)
        if total == 0:
            if any(v != 0 for v in curve):
                warnings.extend(node_warnings)
                raise ReconciliationError(
                    f"node '{ref.key}' has a non-zero base forecast but zero "
                    "historical weight on every leaf — cannot disaggregate. "
                    "Cold-start block: forecast the leaves directly (FM "
                    "extension point, design §4) or extend the lookback."
                )
            # All-zero node with an all-zero curve: legitimate quiet branch —
            # do NOT surface the per-leaf 'natural zero' warnings here (review
            # PR3): there is nothing to disaggregate, silence is honest.
            for j in cols:
                leaf_curves[j] = tuple(_ZERO for _ in range(horizon))
                shares.append(LeafShare(
                    leaf=block.leaves[j], recon_node=ref.key,
                    weight=_ZERO, share=_ZERO, cold_start=False,
                ))
            continue
        warnings.extend(node_warnings)

        for j in cols:
            share = weights[j] / total
            leaf_curves[j] = tuple(value * share for value in curve)
            shares.append(LeafShare(
                leaf=block.leaves[j], recon_node=ref.key,
                weight=weights[j], share=share, cold_start=j in cold_cols,
            ))

    completed = [
        curve if curve is not None else tuple(_ZERO for _ in range(horizon))
        for curve in leaf_curves
    ]
    values = _series_values_from_leaves(block, completed, horizon)
    return ReconciledBlock(
        block_code=block.block_code,
        recon_level=recon_level,
        recon_method=RECON_MIDDLEOUT,
        series=block.series,
        values=values,
        shares=tuple(shares),
        warnings=tuple(warnings),
    )


def _recon_nodes(
    block: SummingBlock, recon_level: str
) -> list[tuple[int, SeriesRef]]:
    """Reconciliation-level nodes + coverage check (fail loudly)."""
    nodes = [
        (i, ref)
        for i, ref in enumerate(block.series)
        if ref.kind == AGGREGATE and ref.level == recon_level
    ]
    if not nodes:
        raise ReconciliationError(
            f"block '{block.block_code}' has no aggregate node at level "
            f"'{recon_level}'"
        )
    covered: list[int] = []
    for i, _ in nodes:
        covered.extend(block.rows[i])
    if len(covered) != len(set(covered)):
        # Impossible in a tree; defensive against a corrupted block.
        raise ReconciliationError(
            f"level '{recon_level}' nodes of block '{block.block_code}' "
            "overlap on leaf columns — corrupted summing block"
        )
    missing = set(range(len(block.leaves))) - set(covered)
    if missing:
        names = sorted(block.leaves[j] for j in missing)
        raise ReconciliationError(
            f"leaves {names} of block '{block.block_code}' are not covered "
            f"by any '{recon_level}' node (attached above the "
            "reconciliation level) — pick a higher reconciliation level"
        )
    return nodes


def _node_weights(
    node_code: str,
    cols: Sequence[int],
    block: SummingBlock,
    leaf_history_totals: Mapping[str, Decimal],
    leaf_code_of: Mapping[str, str | None],
) -> tuple[dict[int, Decimal], set[int], list[str]]:
    """Historical weights of one node's leaves, with twin imputation."""
    raw: dict[int, Decimal] = {}
    for j in cols:
        weight = _as_decimal(leaf_history_totals.get(block.leaves[j], _ZERO))
        if weight < 0:
            raise ReconciliationError(
                f"leaf '{block.leaves[j]}' has a negative history total "
                f"({weight}) — shares are only defined on non-negative demand"
            )
        raw[j] = weight

    by_parent: dict[str, list[int]] = {}
    for j in cols:
        parent = leaf_code_of.get(block.leaves[j]) or ""
        by_parent.setdefault(parent, []).append(j)

    weights = dict(raw)
    cold: set[int] = set()
    warnings: list[str] = []
    for parent in sorted(by_parent):
        siblings = by_parent[parent]
        positive = [raw[j] for j in siblings if raw[j] > 0]
        for j in siblings:
            if raw[j] > 0:
                continue
            if positive:
                # TWIN rule: mean of the positive weights of the leaves
                # sharing the same direct parent node.
                weights[j] = sum(positive, _ZERO) / Decimal(len(positive))
                cold.add(j)
            else:
                warnings.append(
                    f"leaf '{block.leaves[j]}' under node '{node_code}' has "
                    f"no history and no positive-history twin (parent "
                    f"'{parent}') — natural zero share"
                )
    return weights, cold, warnings


def _series_values_from_leaves(
    block: SummingBlock,
    leaf_curves: Sequence[tuple[Decimal, ...]],
    horizon: int,
) -> tuple[tuple[Decimal, ...], ...]:
    """y_hat = S @ b_hat, exactly, for every series of the block."""
    return tuple(
        tuple(
            sum((leaf_curves[j][t] for j in row), _ZERO)
            for t in range(horizon)
        )
        for row in block.rows
    )


def _validated_horizon(base_curves: Mapping[str, Sequence[Decimal]]) -> int:
    lengths = {code: len(curve) for code, curve in base_curves.items()}
    if not lengths:
        raise ReconciliationError("no base curve provided")
    distinct = set(lengths.values())
    if len(distinct) > 1:
        raise ReconciliationError(
            f"base curves have inconsistent horizon lengths: {lengths}"
        )
    horizon = distinct.pop()
    if horizon < 1:
        raise ReconciliationError("base curves must have at least one period")
    return horizon


def _as_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


# ---------------------------------------------------------------------------
# MinT-shrink (optional stochastic-edge improvement)
# ---------------------------------------------------------------------------


def mint_shrink(
    block: SummingBlock,
    recon_level: str,
    inputs: MintInputs,
) -> ReconciledBlock:
    """
    MinT (Minimum Trace) reconciliation with Ledoit-Wolf-style shrinkage,
    via *hierarchicalforecast* (Nixtla) — lazy import, optional
    ``[forecast]`` extra.

    Raises ReconciliationUnavailable when the backend is not installed,
    the inputs are too short/misaligned, or the backend API rejects the
    call — the dispatcher falls back to middle-out.

    Reproducibility note (ADR-022): the computation is numpy/BLAS float
    algebra — stable for a fixed environment (and logged with the run's
    code_version), but NOT bit-for-bit reproducible across BLAS builds.
    The output is re-based on the reconciled LEAF values (clamped at 0)
    and every level is re-derived through S, so the coherence contract
    ``y_hat = S @ b_hat`` holds exactly whatever the backend returns.
    """
    try:
        import numpy as np
        import pandas as pd
        from hierarchicalforecast.core import HierarchicalReconciliation
        from hierarchicalforecast.methods import MinTrace
    except ImportError as exc:
        raise ReconciliationUnavailable(
            "hierarchicalforecast backend not installed "
            "(pip install 'ootils-core[forecast]')"
        ) from exc

    # never used as data, only to satisfy the coverage contract early
    _recon_nodes(block, recon_level)

    series_ids: list[str] = []
    curves: list[Sequence[Decimal]] = []
    insample: list[Sequence[Decimal]] = []
    fitted: list[Sequence[Decimal]] = []
    for ref in block.series:
        if ref.kind == AGGREGATE:
            sid = f"agg::{ref.key}"
            curve = inputs.aggregate_curves.get(ref.key)
            hist = inputs.aggregate_insample.get(ref.key)
            fit = inputs.aggregate_fitted.get(ref.key)
        else:
            sid = f"leaf::{ref.key}"
            curve = inputs.leaf_curves.get(ref.key)
            hist = inputs.leaf_insample.get(ref.key)
            fit = inputs.leaf_fitted.get(ref.key)
        if curve is None or hist is None or fit is None:
            raise ReconciliationUnavailable(
                f"MinT inputs incomplete for series '{ref.key}' "
                "(base curve + insample + fitted are all required)"
            )
        series_ids.append(sid)
        curves.append(curve)
        insample.append(hist)
        fitted.append(fit)

    horizons = {len(c) for c in curves}
    if len(horizons) != 1:
        raise ReconciliationUnavailable(
            f"MinT base curves have inconsistent horizons: {sorted(horizons)}"
        )
    horizon = horizons.pop()
    ks = {len(h) for h in insample} | {len(f) for f in fitted}
    if len(ks) != 1:
        raise ReconciliationUnavailable(
            "MinT insample/fitted series are not aligned to a single length"
        )
    k = ks.pop()
    if k < MINT_MIN_INSAMPLE:
        raise ReconciliationUnavailable(
            f"MinT needs >= {MINT_MIN_INSAMPLE} aligned insample points, got {k}"
        )

    n_series, n_leaves = len(block.series), len(block.leaves)
    s_matrix = np.zeros((n_series, n_leaves))
    for i, row in enumerate(block.rows):
        for j in row:
            s_matrix[i, j] = 1.0
    leaf_ids = [f"leaf::{key}" for key in block.leaves]
    s_df = pd.DataFrame(s_matrix, index=series_ids, columns=leaf_ids)

    # integer ds grid: 1..k insample, k+1..k+horizon forecast
    y_df = pd.DataFrame({
        "unique_id": np.repeat(series_ids, k),
        "ds": np.tile(np.arange(1, k + 1), n_series),
        "y": [float(v) for hist in insample for v in hist],
        "base": [float(v) for fit in fitted for v in fit],
    })
    y_hat_df = pd.DataFrame({
        "unique_id": np.repeat(series_ids, horizon),
        "ds": np.tile(np.arange(k + 1, k + 1 + horizon), n_series),
        "base": [float(v) for curve in curves for v in curve],
    })

    hrec = HierarchicalReconciliation(
        reconcilers=[MinTrace(method="mint_shrink")]
    )
    try:
        try:
            rec = hrec.reconcile(
                Y_hat_df=y_hat_df, Y_df=y_df, S=s_df, tags=_tags(block, np)
            )
        except TypeError:
            # older/newer keyword spelling of the S matrix argument
            rec = hrec.reconcile(
                Y_hat_df=y_hat_df, Y_df=y_df, S_df=s_df, tags=_tags(block, np)
            )
    except Exception as exc:
        raise ReconciliationUnavailable(
            f"hierarchicalforecast MinTrace(mint_shrink) failed: {exc}"
        ) from exc

    rec_columns = [c for c in rec.columns if "MinTrace" in str(c)]
    if not rec_columns:
        raise ReconciliationUnavailable(
            f"no MinTrace column in the reconciled frame (columns: "
            f"{list(rec.columns)})"
        )
    rec_column = rec_columns[0]
    if "unique_id" not in rec.columns:
        rec = rec.reset_index()

    # Re-base on the reconciled LEAF values (clamped at 0) and re-derive
    # every level through S — coherence exact by construction.
    leaf_curves: list[tuple[Decimal, ...]] = []
    for leaf_id in leaf_ids:
        chunk = rec[rec["unique_id"] == leaf_id].sort_values("ds")
        if len(chunk) != horizon:
            raise ReconciliationUnavailable(
                f"reconciled frame misses periods for '{leaf_id}' "
                f"({len(chunk)} != {horizon})"
            )
        leaf_curves.append(tuple(
            max(Decimal(str(v)), _ZERO) for v in chunk[rec_column].tolist()
        ))

    values = _series_values_from_leaves(block, leaf_curves, horizon)
    return ReconciledBlock(
        block_code=block.block_code,
        recon_level=recon_level,
        recon_method=RECON_MINT_SHRINK,
        series=block.series,
        values=values,
        shares=(),  # MinT has no explicit proportions (design §4 trade-off)
        warnings=(
            "mintrace_wls_shrink: float linear algebra — deterministic for "
            "a fixed environment, not bit-for-bit across numpy/BLAS builds "
            "(ADR-022); leaf values clamped at 0 then re-aggregated "
            "through S",
        ),
    )


def _tags(block: SummingBlock, np) -> dict:
    """hierarchicalforecast tags: series ids grouped by hierarchy level."""
    tags: dict[str, list[str]] = {}
    for ref in block.series:
        if ref.kind == AGGREGATE:
            tags.setdefault(f"level::{ref.level}", []).append(f"agg::{ref.key}")
        else:
            tags.setdefault("level::__leaf__", []).append(f"leaf::{ref.key}")
    return {name: np.array(ids) for name, ids in tags.items()}
