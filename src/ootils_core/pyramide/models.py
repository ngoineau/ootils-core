from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID

from ootils_core.forecasting import ForecastMethod

from .accuracy import AccuracyReport

METHOD_AUTO_SELECT = "AUTO_SELECT"
METHOD_ENSEMBLE_STAT = "ENSEMBLE_STAT"
METHOD_STAT_AUTOETS = "STAT_AUTOETS"
METHOD_STAT_AUTOARIMA = "STAT_AUTOARIMA"
METHOD_ML_LGBM = "ML_LGBM"
METHOD_FM_CHRONOS = "FM_CHRONOS"
# FM_MOIRAI (Salesforce Moirai) est EXCLU de l'application : licence
# cc-by-nc-4.0, incompatible avec un usage commercial (décision verrouillée
# 2026-05-31). La valeur reste tolérée dans les CHECK DB pour l'historique
# (migration 057) mais toute requête API la reçoit en 422 comme n'importe
# quelle méthode inconnue.

SUPPORTED_METHODS = frozenset(
    {
        ForecastMethod.MA,
        ForecastMethod.EXP_SMOOTHING,
        ForecastMethod.CROSTON,
        ForecastMethod.SEASONAL,
        METHOD_AUTO_SELECT,
        METHOD_ENSEMBLE_STAT,
        METHOD_STAT_AUTOETS,
        METHOD_STAT_AUTOARIMA,
        METHOD_ML_LGBM,
        METHOD_FM_CHRONOS,
    }
)
SUPPORTED_GRANULARITIES = frozenset({"daily", "weekly", "monthly"})


@dataclass(frozen=True)
class PyramideRunConfig:
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_days: int
    granularity: str = "daily"
    method: str = METHOD_AUTO_SELECT
    method_params: Mapping[str, Any] = field(default_factory=dict)
    model_strategy: str = "stat"
    # recon_method carries the reconciliation EFFECTIVELY applied
    # (migration 054). A standalone single-series leaf run reconciles
    # nothing — 'none' is the honest default. Hierarchical runs are
    # persisted by pyramide/hierarchy/runner.py with the method the
    # reconciler actually used ('middleout' / 'mintrace_wls_shrink').
    recon_method: str = "none"
    random_seed: int = 0
    code_version: str = "local"

    def __post_init__(self) -> None:
        object.__setattr__(self, "method_params", MappingProxyType(dict(self.method_params or {})))

    @property
    def horizon_end(self) -> date:
        return self.horizon_start + timedelta(days=self.horizon_days - 1)


@dataclass(frozen=True)
class PyramideValue:
    bucket_index: int
    forecast_date: date
    quantity: Decimal
    method: str
    # Bornes conformal du bucket, persistées dans
    # forecast_values.confidence_interval_lower/upper (migration 026).
    # None = pas de calibration honnête disponible (pas de backtest
    # déterministe pour les valeurs servies, ou trop peu de résidus pour
    # la garantie finite-sample) → colonnes NULL, jamais des bornes
    # inventées. Voir engines.conformal_bounds.
    confidence_lower: Decimal | None = None
    confidence_upper: Decimal | None = None


@dataclass(frozen=True)
class PyramideRunResult:
    config: PyramideRunConfig
    values: tuple[PyramideValue, ...]
    source_history_count: int
    selected_model: str
    engine_backend: str
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    warnings: tuple[str, ...] = ()
    # Rapport de backtest rolling-origin du modèle qui a PRODUIT les
    # valeurs (ForecastComputation.accuracy_report, remonté par le
    # runner). Persisté dans pyramide_accuracy_metrics (migration 055)
    # par persist_run. None = pas de backtest déterministe disponible
    # (ENSEMBLE_STAT, backend externe, historique trop court) → AUCUNE
    # ligne de métriques, jamais des métriques inventées.
    accuracy_report: AccuracyReport | None = None
    # Scellé des poids du modèle de fondation qui a produit les valeurs
    # (ForecastComputation.model_revision, remonté par le runner).
    # Persisté dans pyramide_runs.model_revision (migration 059).
    # None = méthode non-FM (ou fallback déterministe servi à la place
    # du FM) → colonne NULL.
    model_revision: str | None = None

    @property
    def total_quantity(self) -> Decimal:
        total = Decimal("0")
        for value in self.values:
            total += value.quantity
        return total
