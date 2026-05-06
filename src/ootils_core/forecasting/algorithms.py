"""
Statistical forecasting algorithms for demand planning.

Implements three classical forecasting algorithms:
1. Moving Average (moyenne mobile)
2. Exponential Smoothing (lissage exponentiel)
3. Croston (pour demande intermittente)

Conforme aux spécifications APICS CPIM/CSCP pour le Demand Planning.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import List, Union


logger = logging.getLogger(__name__)


class ForecastingError(Exception):
    """Exception spécifique aux erreurs de forecasting."""
    pass


class Forecaster(ABC):
    """
    Interface commune pour tous les algorithmes de prévision.
    
    Tous les forecasters doivent implémenter la méthode `forecast`
    qui prend des données historiques et retourne la prévision.
    """
    
    @abstractmethod
    def forecast(self, historical_data: List[Union[Decimal, float, int]]) -> Decimal:
        """
        Générer une prévision à partir des données historiques.
        
        Args:
            historical_data: Liste des valeurs historiques (quantités vendues).
                            Peut être des Decimal, float ou int.
        
        Returns:
            Decimal: Prévision pour la période suivante.
        
        Raises:
            ForecastingError: Si les données sont insuffisantes ou invalides.
        """
        pass
    
    def validate_historical_data(self, data: List[Union[Decimal, float, int]], 
                                min_length: int = 1) -> None:
        """
        Valider les données historiques avant traitement.
        
        Args:
            data: Données à valider
            min_length: Longueur minimale requise (défaut: dépend de l'algorithme)
        
        Raises:
            ForecastingError: Si validation échoue
        """
        if len(data) < min_length:
            raise ForecastingError(
                f"Données historiques insuffisantes: {len(data)} valeurs, "
                f"minimum requis: {min_length}"
            )
        
        # Convertir en Decimal pour uniformité
        for i, val in enumerate(data):
            if isinstance(val, (float, int)):
                # Conversion safe
                pass
            elif not isinstance(val, Decimal):
                raise ForecastingError(
                    f"Type de donnée invalide à l'index {i}: {type(val)}. "
                    f"Attendu: Decimal, float ou int"
                )


class MovingAverageForecaster(Forecaster):
    """
    Moyenne Mobile (Moving Average) - modèle statistique simple.
    
    La prévision est la moyenne des N dernières périodes.
    Utilisé pour les séries temporelles sans tendance ni saisonnalité marquée.
    
    Args:
        window_size: Taille de la fenêtre de calcul (nombre de périodes à inclure)
    
    Performance: O(window_size) en temps, O(1) en mémoire avec deque.
    """
    
    def __init__(self, window_size: int = 3):
        """
        Initialiser le forecaster avec une taille de fenêtre.
        
        Args:
            window_size: Nombre de périodes à inclure dans la moyenne (>= 1)
        
        Raises:
            ValueError: Si window_size < 1
        """
        if window_size < 1:
            raise ValueError(f"window_size doit être >= 1, reçu: {window_size}")
        
        self.window_size = window_size
        logger.debug(f"MovingAverageForecaster initialisé avec window_size={window_size}")
    
    def forecast(self, historical_data: List[Union[Decimal, float, int]]) -> Decimal:
        """
        Calculer la prévision par moyenne mobile.
        
        Algorithm:
        1. Prendre les N dernières valeurs (window_size)
        2. Calculer la moyenne arithmétique
        3. Retourner comme prévision
        
        Exemple:
            historical_data = [100,20,130,40,150]
            window_size = 3
            → derniers 3 valeurs = [130,40,150]
            → moyenne = (130+40+150)/3 = 106.67
        
        Args:
            historical_data: Données historiques (plus récent en dernier)
        
        Returns:
            Decimal: Prévision pour la période suivante
        """
        # Validation
        self.validate_historical_data(historical_data, min_length=self.window_size)
        
        # Prendre les N dernières valeurs
        recent_data = historical_data[-self.window_size:]
        
        # Convertir en float pour calcul, puis retourner en Decimal
        total = 0.0
        count = 0
        
        for val in recent_data:
            if isinstance(val, Decimal):
                total += float(val)
            else:
                total += float(val)
            count += 1
        
        # Calcul de la moyenne
        forecast_value = Decimal(str(total / count)) if count > 0 else Decimal("0")
        
        logger.debug(
            f"MovingAverageForecaster: {len(historical_data)} valeurs historiques, "
            f"window_size={self.window_size}, forecast={forecast_value}"
        )
        
        return forecast_value
    
    def __repr__(self) -> str:
        return f"MovingAverageForecaster(window_size={self.window_size})"


class ExponentialSmoothingForecaster(Forecaster):
    """
    Lissage Exponentiel Simple (Simple Exponential Smoothing).
    
    La prévision est une moyenne pondérée où les poids décroissent
    exponentiellement avec l'âge des observations.
    
    Formule: F_t+1 = α * Y_t + (1-α) * F_t
    où α est le smoothing factor (0 < α ≤ 1)
    
    Args:
        alpha: Facteur de lissage (smoothing factor), contrôle la réactivité.
               α élevé = plus de poids aux observations récentes.
               α faible = plus lisse, moins réactif.
    
    Performance: O(n) en temps, O(1) en mémoire.
    """
    
    def __init__(self, alpha: float = 0.3):
        """
        Initialiser le forecaster avec un facteur de lissage.
        
        Args:
            alpha: Facteur de lissage (0 < α ≤ 1)
        
        Raises:
            ValueError: Si alpha hors de ]0,1]
        """
        if not (0 < alpha <= 1):
            raise ValueError(f"alpha doit être dans ]0,1], reçu: {alpha}")
        
        self.alpha = alpha
        logger.debug(f"ExponentialSmoothingForecaster initialisé avec alpha={alpha}")
    
    def forecast(self, historical_data: List[Union[Decimal, float, int]]) -> Decimal:
        """
        Calculer la prévision par lissage exponentiel.
        
        Algorithm:
        1. Initialiser la prévision avec la première valeur
        2. Pour chaque valeur suivante:
           F = α * Y + (1-α) * F
        3. Retourner la dernière prévision calculée
        
        Exemple avec α=0.3:
            Y = [100, 120, 110]
            F1 = 100 (initialisation)
            F2 = 0.3*120 + 0.7*100 = 106
            F3 = 0.3*110 + 0.7*106 = 107.2
            → forecast = 107.2
        
        Args:
            historical_data: Données historiques (chronologiques)
        
        Returns:
            Decimal: Prévision pour la période suivante
        """
        # Validation
        self.validate_historical_data(historical_data, min_length=1)
        
        # Initialiser avec la première valeur
        if isinstance(historical_data[0], Decimal):
            forecast = float(historical_data[0])
        else:
            forecast = float(historical_data[0])
        
        # Appliquer le lissage exponentiel sur les valeurs suivantes
        for i in range(1, len(historical_data)):
            if isinstance(historical_data[i], Decimal):
                current_val = float(historical_data[i])
            else:
                current_val = float(historical_data[i])
            
            forecast = self.alpha * current_val + (1 - self.alpha) * forecast
        
        # Retourner en Decimal
        forecast_decimal = Decimal(str(forecast))
        
        logger.debug(
            f"ExponentialSmoothingForecaster: {len(historical_data)} valeurs historiques, "
            f"alpha={self.alpha}, forecast={forecast_decimal}"
        )
        
        return forecast_decimal
    
    def __repr__(self) -> str:
        return f"ExponentialSmoothingForecaster(alpha={self.alpha})"


class CrostonForecaster(Forecaster):
    """
    Méthode de Croston pour la demande intermittente.
    
    Spécialement conçu pour les items avec demande sporadique
    (beaucoup de zéros dans l'historique).
    
    Algorithm:
    1. Séparer les périodes avec demande > 0
    2. Calculer la taille moyenne des demandes (Z)
    3. Calculer l'intervalle moyen entre demandes (P)
    4. Forecast = Z / P
    
    Référence: Croston, J.D. (1972) "Forecasting and Stock Control for Intermittent Demands"
    
    Performance: O(n) en temps, O(1) en mémoire.
    """
    
    def __init__(self, min_demand_threshold: float = 0.0):
        """
        Initialiser le forecaster Croston.
        
        Args:
            min_demand_threshold: Seuil minimum pour considérer une demande comme positive.
                                  Utile pour filtrer le bruit (défaut: 0.0).
        """
        self.min_demand_threshold = min_demand_threshold
        logger.debug(f"CrostonForecaster initialisé avec threshold={min_demand_threshold}")
    
    def forecast(self, historical_data: List[Union[Decimal, float, int]]) -> Decimal:
        """
        Calculer la prévision par méthode Croston.
        
        Algorithm détaillé:
        1. Identifier les périodes avec demande ( > threshold)
        2. Calculer Z = moyenne des demandes positives
        3. Calculer P = intervalle moyen entre demandes positives
        4. Si P > 0, forecast = Z / P
        5. Sinon, forecast = 0 (aucune demande détectée)
        
        Exemple:
            historical_data = [0,0,100,0,0,150,0,0,0,200]
            → demandes positives = [100,150,200] à indices 2,5,9
            → Z = (100+150+200)/3 = 150
            → P = ((5-2)+(9-5))/2 = (3+4)/2 = 3.5
            → forecast = 150 / 3.5 = 42.86
        
        Args:
            historical_data: Données historiques (peut contenir des zéros)
        
        Returns:
            Decimal: Prévision pour la période suivante (demande par période)
        """
        # Validation
        self.validate_historical_data(historical_data, min_length=1)
        
        # Convertir en float pour les calculs
        data_float = []
        for val in historical_data:
            if isinstance(val, Decimal):
                data_float.append(float(val))
            else:
                data_float.append(float(val))
        
        # Identifier les périodes avec demande positive
        positive_demands = []
        positive_indices = []
        
        for i, demand in enumerate(data_float):
            if demand > self.min_demand_threshold:
                positive_demands.append(demand)
                positive_indices.append(i)
        
        # Cas 1: Aucune demande positive
        if len(positive_demands) == 0:
            logger.debug("CrostonForecaster: aucune demande positive détectée, forecast=0")
            return Decimal("0")
        
        # Cas 2: Une seule demande positive
        if len(positive_demands) == 1:
            Z = positive_demands[0]
            # Pas assez de données pour calculer P, on retourne la demande moyenne
            # (qui est juste cette unique demande)
            forecast_decimal = Decimal(str(Z))
            logger.debug(f"CrostonForecaster: une seule demande positive, forecast={forecast_decimal}")
            return forecast_decimal
        
        # Cas 3: Plusieurs demandes positives
        # Calculer Z (taille moyenne des demandes)
        Z = sum(positive_demands) / len(positive_demands)
        
        # Calculer P (intervalle moyen entre demandes)
        intervals = []
        for i in range(1, len(positive_indices)):
            interval = positive_indices[i] - positive_indices[i-1]
            intervals.append(interval)
        
        P = sum(intervals) / len(intervals)
        
        # Calculer le forecast
        forecast = Z / P if P > 0 else 0.0
        
        forecast_decimal = Decimal(str(forecast))
        
        logger.debug(
            f"CrostonForecaster: {len(historical_data)} valeurs, "
            f"{len(positive_demands)} demandes positives, "
            f"Z={Z:.2f}, P={P:.2f}, forecast={forecast_decimal}"
        )
        
        return forecast_decimal
    
    def __repr__(self) -> str:
        return f"CrostonForecaster(min_demand_threshold={self.min_demand_threshold})"


# ─────────────────────────────────────────────────────────────
# Fonctions utilitaires
# ─────────────────────────────────────────────────────────────

def create_forecaster(model_type: str, **kwargs) -> Forecaster:
    """
    Factory function pour créer un forecaster par type.
    
    Args:
        model_type: Type de modèle ('moving_average', 'exponential_smoothing', 'croston')
        **kwargs: Paramètres spécifiques au modèle
    
    Returns:
        Instance du forecaster approprié
    
    Raises:
        ValueError: Si model_type inconnu
    """
    if model_type == 'moving_average':
        window_size = kwargs.get('window_size', 3)
        return MovingAverageForecaster(window_size=window_size)
    
    elif model_type == 'exponential_smoothing':
        alpha = kwargs.get('alpha', 0.3)
        return ExponentialSmoothingForecaster(alpha=alpha)
    
    elif model_type == 'croston':
        threshold = kwargs.get('min_demand_threshold', 0.0)
        return CrostonForecaster(min_demand_threshold=threshold)
    
    else:
        raise ValueError(
            f"Type de modèle inconnu: '{model_type}'. "
            f"Disponible: 'moving_average', 'exponential_smoothing', 'croston'"
        )
