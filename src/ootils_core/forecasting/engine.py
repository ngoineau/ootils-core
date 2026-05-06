"""
ForecastingEngine — Service unifié pour la génération de prévisions.

Fournit une interface unique pour tous les algorithmes de forecasting
avec calcul automatique des métriques d'accuracy (MAPE, Bias, Tracking Signal).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from .algorithms import (
    CrostonForecaster,
    ExponentialSmoothingForecaster,
    ForecastingError,
    Forecaster,
    MovingAverageForecaster,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Types et constantes
# ─────────────────────────────────────────────────────────────

class ForecastMethod:
    """Constantes pour les méthodes de forecasting."""
    MA = "MA"  # Moving Average
    EXP_SMOOTHING = "EXP_SMOOTHING"  # Exponential Smoothing
    CROSTON = "CROSTON"  # Croston (demande intermittente)
    SEASONAL = "SEASONAL"  # Seasonal (optionnel v1)


@dataclass
class ForecastResult:
    """Résultat d'une exécution de forecasting."""
    method: str
    forecast_value: Decimal
    metrics: Dict[str, Any] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    historical_count: int = 0
    warnings: List[str] = field(default_factory=list)


@dataclass
class AccuracyMetrics:
    """Métriques d'accuracy calculées sur les prévisions."""
    mape: Optional[Decimal] = None  # Mean Absolute Percentage Error
    bias: Optional[Decimal] = None  # Bias (sur/sous-prévision)
    tracking_signal: Optional[Decimal] = None  # Tracking Signal
    mad: Optional[Decimal] = None  # Mean Absolute Deviation
    mse: Optional[Decimal] = None  # Mean Squared Error


# ─────────────────────────────────────────────────────────────
# ForecastingEngine Service
# ─────────────────────────────────────────────────────────────

class ForecastingEngine:
    """
    Service principal pour la génération de prévisions de demande.
    
    Interface unifiée supportant multiples algorithmes avec:
    - Génération de forecast via méthode `generate`
    - Calcul automatique des métriques d'accuracy
    - Auto-calibration des paramètres (optionnel)
    - Support de la demande intermittente (Croston)
    
    Exemple:
        engine = ForecastingEngine()
        result = engine.generate(
            item_history=[100, 120, 110, 130, 125],
            method=ForecastMethod.MA,
            params={"window": 3}
        )
        print(f"Forecast: {result.forecast_value}")
        print(f"MAPE: {result.metrics.get('mape')}")
    """
    
    def __init__(self, auto_calibrate: bool = False):
        """
        Initialiser le moteur de forecasting.
        
        Args:
            auto_calibrate: Si True, calibre automatiquement les paramètres
                           (alpha pour ES, window pour MA) sur les données historiques.
        """
        self.auto_calibrate = auto_calibrate
        logger.info(f"ForecastingEngine initialisé (auto_calibrate={auto_calibrate})")
    
    def generate(
        self,
        item_history: List[Union[Decimal, float, int]],
        method: str,
        params: Optional[Dict[str, Any]] = None,
        actuals: Optional[List[Union[Decimal, float, int]]] = None,
    ) -> ForecastResult:
        """
        Générer une prévision pour un item donné.
        
        Args:
            item_history: Historique des quantités (chronologique, plus récent en dernier).
            method: Méthode de forecasting (MA, EXP_SMOOTHING, CROSTON).
            params: Paramètres spécifiques à la méthode:
                    - MA: {"window": int}
                    - EXP_SMOOTHING: {"alpha": float, "auto_calibrate": bool}
                    - CROSTON: {"min_demand_threshold": float}
            actuals: Valeurs réelles pour calcul des métriques d'accuracy (optionnel).
                    Si fourni, doit être de même longueur que les prévisions testées.
        
        Returns:
            ForecastResult avec forecast_value, metrics, parameters.
        
        Raises:
            ForecastingError: Si les données sont insuffisantes ou méthode inconnue.
        
        Exemple:
            >>> engine = ForecastingEngine()
            >>> result = engine.generate(
            ...     item_history=[100, 120, 110, 130, 125],
            ...     method=ForecastMethod.MA,
            ...     params={"window": 3}
            ... )
            >>> result.forecast_value
            Decimal('121.67')
        """
        params = params or {}
        warnings = []
        
        # Validation minimale des données
        if not item_history or len(item_history) == 0:
            raise ForecastingError("item_history ne peut pas être vide")
        
        # Auto-calibration si activée
        if self.auto_calibrate or params.get("auto_calibrate"):
            if method == ForecastMethod.EXP_SMOOTHING:
                params["alpha"] = self._calibrate_alpha(item_history)
                warnings.append(f"Alpha auto-calibré: {params['alpha']:.3f}")
            elif method == ForecastMethod.MA:
                params["window"] = self._calibrate_window(item_history)
                warnings.append(f"Window auto-calibré: {params['window']}")
        
        # Créer le forecaster approprié
        forecaster = self._create_forecaster(method, params)
        
        # Générer la prévision
        forecast_value = forecaster.forecast(item_history)
        
        # Calculer les métriques d'accuracy si actuals fournis
        metrics = {}
        if actuals and len(actuals) > 0:
            accuracy = self.calculate_accuracy_metrics(
                forecasts=[forecast_value] * len(actuals),  # Simplifié pour one-step
                actuals=actuals
            )
            metrics = {
                "mape": accuracy.mape,
                "bias": accuracy.bias,
                "tracking_signal": accuracy.tracking_signal,
                "mad": accuracy.mad,
                "mse": accuracy.mse,
            }
        
        # Construire le résultat
        result = ForecastResult(
            method=method,
            forecast_value=forecast_value,
            metrics=metrics,
            parameters=params.copy(),
            historical_count=len(item_history),
            warnings=warnings,
        )
        
        logger.info(
            f"Forecast généré: method={method}, value={forecast_value}, "
            f"historical_count={len(item_history)}"
        )
        
        return result
    
    def _create_forecaster(self, method: str, params: Dict[str, Any]) -> Forecaster:
        """Créer un forecaster selon la méthode demandée."""
        if method == ForecastMethod.MA:
            window = params.get("window", 3)
            return MovingAverageForecaster(window_size=window)
        
        elif method == ForecastMethod.EXP_SMOOTHING:
            alpha = params.get("alpha", 0.3)
            return ExponentialSmoothingForecaster(alpha=alpha)
        
        elif method == ForecastMethod.CROSTON:
            threshold = params.get("min_demand_threshold", 0.0)
            return CrostonForecaster(min_demand_threshold=threshold)
        
        else:
            raise ForecastingError(
                f"Méthode de forecasting inconnue: '{method}'. "
                f"Disponible: {ForecastMethod.MA}, {ForecastMethod.EXP_SMOOTHING}, {ForecastMethod.CROSTON}"
            )
    
    def _calibrate_alpha(self, historical_data: List[Union[Decimal, float, int]]) -> float:
        """
        Calibrer automatiquement le paramètre alpha pour Exponential Smoothing.
        
        Stratégie: tester plusieurs valeurs d'alpha et choisir celle qui minimise
        l'erreur quadratique moyenne (MSE) sur les données historiques.
        
        Args:
            historical_data: Données historiques pour la calibration.
        
        Returns:
            float: Valeur optimale d'alpha (entre 0.1 et 0.9).
        """
        if len(historical_data) < 3:
            return 0.3  # Valeur par défaut si pas assez de données
        
        # Convertir en float
        data = [float(v) if not isinstance(v, float) else v for v in historical_data]
        
        best_alpha = 0.3
        best_mse = float('inf')
        
        # Tester plusieurs valeurs d'alpha
        for alpha in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
            mse = self._compute_mse_for_alpha(data, alpha)
            if mse < best_mse:
                best_mse = mse
                best_alpha = alpha
        
        return best_alpha
    
    def _compute_mse_for_alpha(
        self,
        data: List[float],
        alpha: float
    ) -> float:
        """Calculer le MSE pour une valeur d'alpha donnée."""
        if len(data) < 2:
            return float('inf')
        
        # Initialiser avec la première valeur
        forecast = data[0]
        squared_errors = []
        
        # Calculer les erreurs one-step-ahead
        for i in range(1, len(data)):
            # Erreur à la période i
            error = data[i] - forecast
            squared_errors.append(error ** 2)
            
            # Mettre à jour la prévision pour la période suivante
            forecast = alpha * data[i] + (1 - alpha) * forecast
        
        if not squared_errors:
            return float('inf')
        
        return sum(squared_errors) / len(squared_errors)
    
    def _calibrate_window(self, historical_data: List[Union[Decimal, float, int]]) -> int:
        """
        Calibrer automatiquement la fenêtre pour Moving Average.
        
        Stratégie: tester plusieurs tailles de fenêtre et choisir celle
        qui minimise le MSE sur les données historiques.
        
        Args:
            historical_data: Données historiques pour la calibration.
        
        Returns:
            int: Taille optimale de fenêtre (entre 2 et len(data)-1).
        """
        if len(historical_data) < 4:
            return 3  # Valeur par défaut si pas assez de données
        
        # Convertir en float
        data = [float(v) if not isinstance(v, float) else v for v in historical_data]
        
        best_window = 3
        best_mse = float('inf')
        
        # Tester plusieurs tailles de fenêtre
        for window in range(2, min(len(data), 10)):
            mse = self._compute_mse_for_window(data, window)
            if mse < best_mse:
                best_mse = mse
                best_window = window
        
        return best_window
    
    def _compute_mse_for_window(self, data: List[float], window: int) -> float:
        """Calculer le MSE pour une taille de fenêtre donnée."""
        if len(data) < window + 1:
            return float('inf')
        
        squared_errors = []
        
        # Calculer les erreurs one-step-ahead
        for i in range(window, len(data)):
            # Prévision = moyenne des window périodes précédentes
            forecast = sum(data[i-window:i]) / window
            
            # Erreur à la période i
            error = data[i] - forecast
            squared_errors.append(error ** 2)
        
        if not squared_errors:
            return float('inf')
        
        return sum(squared_errors) / len(squared_errors)
    
    def calculate_accuracy_metrics(
        self,
        forecasts: List[Union[Decimal, float]],
        actuals: List[Union[Decimal, float]],
    ) -> AccuracyMetrics:
        """
        Calculer les métriques d'accuracy complètes.
        
        Métriques calculées:
        - MAPE (Mean Absolute Percentage Error): erreur en pourcentage
        - Bias: tendance à sur/sous-prévoir
        - Tracking Signal: ratio Bias/MAD (alerte si > 4 ou < -4)
        - MAD (Mean Absolute Deviation): erreur absolue moyenne
        - MSE (Mean Squared Error): erreur quadratique moyenne
        
        Args:
            forecasts: Liste des prévisions.
            actuals: Liste des valeurs réelles correspondantes.
        
        Returns:
            AccuracyMetrics avec toutes les métriques.
        
        Raises:
            ForecastingError: Si longueurs différentes ou données vides.
        """
        if len(forecasts) != len(actuals):
            raise ForecastingError(
                f"Longueurs différentes: forecasts={len(forecasts)}, actuals={len(actuals)}"
            )
        
        if len(forecasts) == 0:
            raise ForecastingError("Les listes ne peuvent pas être vides")
        
        # Convertir en float pour les calculs
        f = [float(x) if not isinstance(x, float) else x for x in forecasts]
        a = [float(x) if not isinstance(x, float) else x for x in actuals]
        
        # Calculer les erreurs
        errors = [a[i] - f[i] for i in range(len(f))]
        abs_errors = [abs(e) for e in errors]
        
        # MAPE (Mean Absolute Percentage Error)
        try:
            percentage_errors = [abs_errors[i] / abs(a[i]) * 100 if a[i] != 0 else 0 for i in range(len(a))]
            mape = Decimal(str(sum(percentage_errors) / len(percentage_errors)))
        except ZeroDivisionError:
            mape = None
        
        # Bias (somme des erreurs)
        bias = Decimal(str(sum(errors)))
        
        # MAD (Mean Absolute Deviation)
        mad = Decimal(str(sum(abs_errors) / len(abs_errors)))
        
        # Tracking Signal = Bias / MAD
        tracking_signal = bias / mad if mad and mad != 0 else None
        
        # MSE (Mean Squared Error)
        squared_errors = [e ** 2 for e in errors]
        mse = Decimal(str(sum(squared_errors) / len(squared_errors)))
        
        return AccuracyMetrics(
            mape=mape,
            bias=bias,
            tracking_signal=tracking_signal,
            mad=mad,
            mse=mse,
        )
    
    def forecast_series(
        self,
        item_history: List[Union[Decimal, float, int]],
        method: str,
        params: Optional[Dict[str, Any]] = None,
        periods: int = 1,
    ) -> List[Decimal]:
        """
        Générer une série de prévisions sur plusieurs périodes.
        
        Args:
            item_history: Historique des quantités.
            method: Méthode de forecasting.
            params: Paramètres de la méthode.
            periods: Nombre de périodes à prévoir (défaut: 1).
        
        Returns:
            Liste des prévisions pour chaque période future.
        
        Note:
            Pour MA et ES, la prévision est constante sur toutes les périodes.
            Pour Croston, idem (modèle sans tendance).
        """
        # Générer la prévision de base
        result = self.generate(item_history, method, params)
        
        # Répéter pour le nombre de périodes demandé
        return [result.forecast_value] * periods
