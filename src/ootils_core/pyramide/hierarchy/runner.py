"""
Hierarchical multi-series orchestration (Pyramide axis A — PR3).

For ONE summing block (a root node of the block level, summing.py):
load S -> read per-series history (reconciliation nodes via
``get_historical_demand_by_node``, leaf weights via
``get_historical_demand_totals_by_items``) -> seasonal base forecasts at
the reconciliation level (PyramideForecastEngine, PR1) -> reconcile
(reconcile.py: middle-out deterministic core, MinT-shrink optional edge)
-> persist EVERY level in the forecast tables (migration 053: aggregates
carry hierarchy_id/level/node_code, leaves carry item/location) with
full provenance (base method + selected model label, which embeds
season_length when a seasonal model won + recon_method effectively
applied + forecast level), seeded/versioned through pyramide_runs.

Graph boundary: aggregates are queryable in the tables and NEVER
committed to the graph (repository.commit_run guards it); only leaf
(item, location) runs get pyramide_snapshots and can be materialized as
ForecastDemand nodes.

Leaf addressing: summing-block leaves are ITEMS (item_hierarchy —
demand summed across all sites, like every hierarchy reader). The
persisted leaf forecasts therefore need a demand location:
``leaf_location_id`` is the location the leaf series are addressed to
(typically the network/central site whose graph consumes the demand
plan; the per-site split is the DRP layer's job, ADR-020). Generic —
callers choose the location, nothing is hardcoded.

Scenario-aware: the run's ``scenario_id`` stamps every persisted row, so
a fork can hold its own hierarchical forecasts. History readers are
scenario-invariant by design (actuals do not fork).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Mapping, Sequence
from uuid import UUID

import psycopg

from ..engines import (
    ForecastComputation,
    PyramideEngineError,
    PyramideForecastEngine,
    conformal_bounds,
    resolve_conformal_alpha,
)
from ..models import METHOD_AUTO_SELECT, SUPPORTED_GRANULARITIES, SUPPORTED_METHODS
from ..repository import (
    PyramidePersistedRun,
    get_historical_demand_by_item,
    get_historical_demand_by_node,
    get_historical_demand_totals_by_items,
    persist_series_run,
)
from ..routing import RoutingDecision
from ..runner import PyramideError, bucket_dates
from .reconcile import (
    MINT_MIN_INSAMPLE,
    RECON_MIDDLEOUT,
    RECON_MINT_SHRINK,
    SUPPORTED_RECON_METHODS,
    LeafShare,
    MintInputs,
    ReconciledBlock,
    ReconciliationError,
    reconcile,
)
from .summing import AGGREGATE, SeriesRef, SummingBlock, load_summing_blocks

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")


@dataclass(frozen=True)
class HierarchicalRunConfig:
    """Configuration of one hierarchical (block-level) forecast run.

    ``recon_level`` defaults to the block level itself (one base forecast
    at the block root, disaggregated to the leaves); pass a deeper level
    for a classic middle-out at an intermediate level.

    ``routing_decisions`` (PR-B1, opt-in provenance): the head/tail
    router's decision per series, keyed by the series key (item UUID
    string for leaves, hierarchy_node code for aggregates). Each matched
    series persists its decision as routed_method / routed_level /
    routing_reason (migration 058). In B1 this is PROVENANCE ONLY — it
    does not change which engine runs (full routed execution is B2);
    empty (default) = nothing persisted, historical behaviour unchanged.
    """
    hierarchy_id: str
    block_code: str
    leaf_location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_days: int
    block_level: str | None = None
    recon_level: str | None = None
    granularity: str = "daily"
    method: str = METHOD_AUTO_SELECT
    method_params: Mapping[str, Any] = field(default_factory=dict)
    model_strategy: str = "stat"
    recon_method: str = RECON_MIDDLEOUT
    lookback_days: int = 365
    random_seed: int = 0
    code_version: str = "local"
    routing_decisions: Mapping[str, RoutingDecision] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "method_params", MappingProxyType(dict(self.method_params or {}))
        )
        object.__setattr__(
            self,
            "routing_decisions",
            MappingProxyType(dict(self.routing_decisions or {})),
        )

    @property
    def horizon_end(self) -> date:
        return self.horizon_start + timedelta(days=self.horizon_days - 1)


@dataclass(frozen=True)
class HierarchicalPersistedSeries:
    """One persisted series of a hierarchical run (kind: aggregate|leaf).

    snapshot_id is None for aggregates (leaf-only snapshot contract)."""
    kind: str
    key: str
    level: str | None
    run_id: UUID
    forecast_id: UUID
    snapshot_id: UUID | None


@dataclass(frozen=True)
class HierarchicalRunResult:
    hierarchy_id: str
    block_code: str
    block_level: str
    recon_level: str
    recon_method: str  # effectively applied (never the rejected request)
    horizon_start: date
    horizon_end: date
    granularity: str
    method: str
    shares: tuple[LeafShare, ...]
    persisted: tuple[HierarchicalPersistedSeries, ...]
    warnings: tuple[str, ...]


class HierarchicalRunner:
    """Run a coherent multi-level forecast for one hierarchy block."""

    def __init__(self, forecast_engine: PyramideForecastEngine | None = None) -> None:
        self._engine = forecast_engine or PyramideForecastEngine()

    def run(
        self, db: psycopg.Connection, config: HierarchicalRunConfig
    ) -> HierarchicalRunResult:
        method = self._validate_config(config)
        # Fail loudly BEFORE any forecast/persist work: an invalid
        # conformal_alpha is a configuration error of the published bounds.
        try:
            resolve_conformal_alpha(config.method_params)
        except PyramideEngineError as exc:
            raise PyramideError(str(exc)) from exc
        block = self._load_block(db, config)
        recon_level = config.recon_level or block.block_level
        dates = bucket_dates(
            config.horizon_start, config.horizon_days, config.granularity
        )
        periods = len(dates)
        warnings: list[str] = []

        recon_refs = [
            (i, ref)
            for i, ref in enumerate(block.series)
            if ref.kind == AGGREGATE and ref.level == recon_level
        ]
        if not recon_refs:
            raise PyramideError(
                f"block '{block.block_code}' has no node at reconciliation "
                f"level '{recon_level}'"
            )

        base_curves: dict[str, tuple[Decimal, ...]] = {}
        base_computations: dict[str, ForecastComputation] = {}
        node_histories: dict[str, list[Decimal]] = {}
        for i, ref in recon_refs:
            if not block.rows[i]:
                continue  # node without leaves: nothing to forecast/split
            history = get_historical_demand_by_node(
                db, config.hierarchy_id, ref.key, config.lookback_days
            )
            if not history:
                raise PyramideError(
                    f"no demand history for reconciliation node '{ref.key}' "
                    f"(hierarchy '{config.hierarchy_id}') in the "
                    f"{config.lookback_days}-day lookback window"
                )
            computation = self._base_forecast(history, periods, method, config)
            node_histories[ref.key] = history
            base_computations[ref.key] = computation
            base_curves[ref.key] = computation.values
            warnings.extend(f"{ref.key}: {w}" for w in computation.warnings)

        leaf_totals = get_historical_demand_totals_by_items(
            db, [UUID(key) for key in block.leaves], config.lookback_days
        )

        mint_inputs: MintInputs | None = None
        if config.recon_method == RECON_MINT_SHRINK:
            mint_inputs, mint_warnings = self._build_mint_inputs(
                db, block, config, method, periods, base_curves, node_histories
            )
            warnings.extend(mint_warnings)

        strict = bool(config.method_params.get("strict_recon"))
        try:
            recon = reconcile(
                block,
                recon_level,
                base_curves,
                leaf_totals,
                method=config.recon_method,
                mint_inputs=mint_inputs,
                strict=strict,
            )
        except ReconciliationError as exc:
            raise PyramideError(str(exc)) from exc
        warnings.extend(recon.warnings)

        persisted, persist_warnings = self._persist(
            db, block, recon, recon_refs, base_computations,
            node_histories, config, method, dates,
        )
        warnings.extend(persist_warnings)
        return HierarchicalRunResult(
            hierarchy_id=config.hierarchy_id,
            block_code=block.block_code,
            block_level=block.block_level,
            recon_level=recon_level,
            recon_method=recon.recon_method,
            horizon_start=config.horizon_start,
            horizon_end=config.horizon_end,
            granularity=config.granularity,
            method=method,
            shares=recon.shares,
            persisted=tuple(persisted),
            warnings=tuple(warnings),
        )

    # ------------------------------------------------------------------
    # Steps
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_config(config: HierarchicalRunConfig) -> str:
        if config.horizon_days < 1:
            raise PyramideError("horizon_days must be >= 1")
        if config.lookback_days < 1:
            raise PyramideError("lookback_days must be >= 1")
        if config.granularity not in SUPPORTED_GRANULARITIES:
            raise PyramideError(f"Unsupported granularity: {config.granularity}")
        method = config.method.upper()
        if method not in SUPPORTED_METHODS:
            raise PyramideError(f"Unsupported forecast method: {config.method}")
        if config.recon_method not in SUPPORTED_RECON_METHODS:
            raise PyramideError(
                f"Unsupported reconciliation method: {config.recon_method} "
                f"(supported: {sorted(SUPPORTED_RECON_METHODS)})"
            )
        return method

    @staticmethod
    def _load_block(
        db: psycopg.Connection, config: HierarchicalRunConfig
    ) -> SummingBlock:
        blocks = load_summing_blocks(
            db,
            hierarchy_id=config.hierarchy_id,
            block_level=config.block_level,
        )
        for block in blocks:
            if block.block_code == config.block_code:
                return block
        raise PyramideError(
            f"block '{config.block_code}' not found at level "
            f"'{config.block_level or 'root'}' of hierarchy "
            f"'{config.hierarchy_id}' (blocks: {[b.block_code for b in blocks]})"
        )

    def _base_forecast(
        self,
        history: Sequence[Decimal],
        periods: int,
        method: str,
        config: HierarchicalRunConfig,
    ) -> ForecastComputation:
        try:
            computation = self._engine.forecast(
                history=list(history),
                periods=periods,
                method=method,
                method_params=config.method_params,
                model_strategy=config.model_strategy,
                granularity=config.granularity,
                horizon_start=config.horizon_start,
                random_seed=config.random_seed,
            )
        except PyramideEngineError as exc:
            raise PyramideError(str(exc)) from exc
        # Same clamp as the leaf runner: demand forecasts are >= 0.
        # replace() keeps accuracy_report: the backtest residuals of the
        # node's selected model feed the leaves' conformal bounds.
        return replace(
            computation,
            values=tuple(max(value, _ZERO) for value in computation.values),
        )

    def _build_mint_inputs(
        self,
        db: psycopg.Connection,
        block: SummingBlock,
        config: HierarchicalRunConfig,
        method: str,
        periods: int,
        recon_curves: Mapping[str, tuple[Decimal, ...]],
        node_histories: Mapping[str, list[Decimal]],
    ) -> tuple[MintInputs | None, list[str]]:
        """
        Best-effort aligned inputs for the optional MinT path: every
        series (all aggregate levels + leaves) gets its own history and
        base forecast; insample tails are aligned to the shortest series.

        Known V1 approximations (documented, ADR-022): histories are
        sparse date-less series, so alignment is positional (last k
        points), and the insample "fitted" values are a naive lag-1
        proxy. If any series is empty or the aligned tail is shorter
        than MINT_MIN_INSAMPLE, MinT is skipped (None + warning) and the
        dispatcher falls back to deterministic middle-out.
        """
        histories: dict[str, list[Decimal]] = {}
        for ref in block.series:
            if ref.kind == AGGREGATE:
                history = node_histories.get(ref.key)
                if history is None:
                    history = get_historical_demand_by_node(
                        db, config.hierarchy_id, ref.key, config.lookback_days
                    )
            else:
                history = get_historical_demand_by_item(
                    db, UUID(ref.key), config.lookback_days
                )
            if not history:
                return None, [
                    f"{RECON_MINT_SHRINK} skipped: series "
                    f"'{ref.key}' has no history in the lookback window "
                    "(middle-out fallback)"
                ]
            histories[_series_key(ref)] = history

        k = min(len(h) for h in histories.values())
        if k < MINT_MIN_INSAMPLE:
            return None, [
                f"{RECON_MINT_SHRINK} skipped: aligned insample tail is "
                f"{k} < {MINT_MIN_INSAMPLE} points (middle-out fallback)"
            ]

        aggregate_curves: dict[str, Sequence[Decimal]] = {}
        leaf_curves: dict[str, Sequence[Decimal]] = {}
        aggregate_insample: dict[str, Sequence[Decimal]] = {}
        leaf_insample: dict[str, Sequence[Decimal]] = {}
        aggregate_fitted: dict[str, Sequence[Decimal]] = {}
        leaf_fitted: dict[str, Sequence[Decimal]] = {}
        for ref in block.series:
            history = histories[_series_key(ref)]
            if ref.kind == AGGREGATE and ref.key in recon_curves:
                curve: Sequence[Decimal] = recon_curves[ref.key]
            else:
                curve = self._base_forecast(history, periods, method, config).values
            tail = tuple(history[-k:])
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

    def _persist(
        self,
        db: psycopg.Connection,
        block: SummingBlock,
        recon: ReconciledBlock,
        recon_refs: Sequence[tuple[int, SeriesRef]],
        base_computations: Mapping[str, ForecastComputation],
        node_histories: Mapping[str, list[Decimal]],
        config: HierarchicalRunConfig,
        method: str,
        dates: Sequence[date],
    ) -> tuple[list[HierarchicalPersistedSeries], list[str]]:
        parent_of: dict[str, str] = {}
        for i, ref in recon_refs:
            for j in block.rows[i]:
                parent_of[block.leaves[j]] = ref.key

        # Part de désagrégation par feuille (middle-out) : le facteur
        # d'échelle des offsets conformal du nœud vers la feuille
        # (feuille = part × nœud, donc résidu feuille implicite =
        # part × résidu nœud). Vide sur le chemin MinT (pas de parts
        # explicites) → bornes NULL pour les feuilles, documenté.
        share_of = {ls.leaf: ls.share for ls in recon.shares}

        recon_backend = f"internal:reconciliation:{recon.recon_method}"
        persisted: list[HierarchicalPersistedSeries] = []
        warnings: list[str] = []
        leaves_without_bounds = 0
        for index, ref in enumerate(block.series):
            quantities = recon.values[index]
            if ref.kind == AGGREGATE:
                computation = base_computations.get(ref.key)
                if computation is not None:
                    selected_model = computation.selected_model
                    engine_backend = computation.engine_backend
                    value_method = computation.value_method
                    history_count = len(node_histories[ref.key])
                else:
                    # non-reconciliation level: values are S-sums of leaves
                    selected_model = f"RECON({recon.recon_method}@{recon.recon_level})"
                    engine_backend = recon_backend
                    value_method = method
                    history_count = 0
                # Bornes NULL pour TOUS les agrégats : la réconciliation
                # d'intervalles hiérarchiques (bornes cohérentes entre
                # niveaux) est frontier — non-objectif V1 (spec §2.D).
                # Métriques de backtest (migration 055) : uniquement pour
                # les nœuds du niveau de réconciliation, qui possèdent
                # leur propre computation (les niveaux S-sommés n'ont pas
                # été backtestés → zéro ligne, documenté).
                record = persist_series_run(
                    db,
                    scenario_id=config.scenario_id,
                    horizon_start=config.horizon_start,
                    horizon_end=config.horizon_end,
                    granularity=config.granularity,
                    method=method,
                    model_strategy=config.model_strategy,
                    recon_method=recon.recon_method,
                    random_seed=config.random_seed,
                    code_version=config.code_version,
                    selected_model=selected_model,
                    engine_backend=engine_backend,
                    source_history_count=history_count,
                    bucket_dates=dates,
                    quantities=quantities,
                    value_method=value_method,
                    hierarchy_id=config.hierarchy_id,
                    level=ref.level,
                    node_code=ref.key,
                    accuracy_report=(
                        computation.accuracy_report if computation else None
                    ),
                    # PR-B1 provenance only (migration 058): the caller's
                    # routing decision for this node, if any — does not
                    # change which engine ran (routed execution is B2).
                    routing=config.routing_decisions.get(ref.key),
                )
            else:
                recon_node = parent_of.get(ref.key)
                computation = (
                    base_computations.get(recon_node) if recon_node else None
                )
                base_label = (
                    computation.selected_model if computation else method
                )
                value_method = (
                    computation.value_method if computation else method
                )
                # Bornes conformal de la FEUILLE : résidus de backtest du
                # modèle du nœud de réconciliation, offsets transportés à
                # l'échelle de la feuille par sa part de désagrégation
                # (approximation V1 documentée dans conformal_bounds).
                # Sans rapport de backtest (ex. base ENSEMBLE_STAT) ou sans
                # part explicite (chemin MinT) → NULL + warning agrégé.
                lowers: Sequence[Decimal | None] | None = None
                uppers: Sequence[Decimal | None] | None = None
                share = share_of.get(ref.key)
                report = computation.accuracy_report if computation else None
                if report is not None and share is not None:
                    lowers, uppers, bound_warnings = conformal_bounds(
                        report=report,
                        values=quantities,
                        method_params=config.method_params,
                        scale=share,
                    )
                    warnings.extend(f"{ref.key}: {w}" for w in bound_warnings)
                else:
                    leaves_without_bounds += 1
                record = persist_series_run(
                    db,
                    scenario_id=config.scenario_id,
                    horizon_start=config.horizon_start,
                    horizon_end=config.horizon_end,
                    granularity=config.granularity,
                    method=method,
                    model_strategy=config.model_strategy,
                    recon_method=recon.recon_method,
                    random_seed=config.random_seed,
                    code_version=config.code_version,
                    selected_model=f"{base_label}@{recon_node or recon.recon_level}",
                    engine_backend=recon_backend,
                    source_history_count=0,
                    bucket_dates=dates,
                    quantities=quantities,
                    value_method=value_method,
                    item_id=UUID(ref.key),
                    location_id=config.leaf_location_id,
                    lowers=lowers,
                    uppers=uppers,
                    # Métriques de backtest de la FEUILLE (migration 055) :
                    # mêmes provenance et garde-fou que les bornes — le
                    # rapport du modèle du nœud de réconciliation, avec le
                    # biais transporté par la part de désagrégation (les
                    # métriques scale-free passent inchangées, approximation
                    # V1 middle-out documentée dans accuracy_metric_rows).
                    # Sans rapport ou sans part explicite (MinT) → None,
                    # zéro ligne.
                    accuracy_report=(
                        report if (report is not None and share is not None) else None
                    ),
                    accuracy_bias_scale=(
                        share if share is not None else Decimal("1")
                    ),
                    # PR-B1 provenance only (migration 058), keyed by the
                    # leaf's item UUID string — see routing_decisions doc.
                    routing=config.routing_decisions.get(ref.key),
                )
            persisted.append(_persisted_series(ref, record))
        if leaves_without_bounds:
            warnings.append(
                f"conformal intervals: {leaves_without_bounds} leaf/leaves "
                "persisted with NULL bounds (no base-model backtest report "
                "or no explicit disaggregation share)"
            )
        return persisted, warnings


def _series_key(ref: SeriesRef) -> str:
    return f"{ref.kind}::{ref.key}"


def _persisted_series(
    ref: SeriesRef, record: PyramidePersistedRun
) -> HierarchicalPersistedSeries:
    return HierarchicalPersistedSeries(
        kind=ref.kind,
        key=ref.key,
        level=ref.level,
        run_id=record.run_id,
        forecast_id=record.forecast_id,
        snapshot_id=record.snapshot_id,
    )
