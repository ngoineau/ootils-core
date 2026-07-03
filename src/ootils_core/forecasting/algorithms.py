"""
Statistical forecasting algorithms for demand planning.

Implements four classical forecasting algorithms:
1. Moving Average (moyenne mobile)
2. Exponential Smoothing (lissage exponentiel)
3. Croston (pour demande intermittente)
4. Seasonal (décomposition par indices saisonniers, courbe multi-périodes)

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


class SeasonalForecaster(Forecaster):
    """
    Décomposition saisonnière multiplicative classique (indices saisonniers).

    Déterministe, pur Python, sans dépendance. La longueur de cycle est
    paramétrable (ex. 7 quotidien, 12 mensuel, 52 hebdomadaire).

    Algorithm:
    1. Conserver les k derniers cycles COMPLETS (k = n // season_length),
       alignés sur la FIN de l'historique — la phase du prochain point est
       ainsi connue même si l'historique commence en milieu de cycle.
    2. Indice de la position p = moyenne des valeurs observées à la position p
       / moyenne globale des cycles complets. Les indices moyennent 1.0 par
       construction. Série constante → indices tous égaux à 1.0.
    3. Niveau = moyenne du dernier cycle complet désaisonnalisé
       (valeur / indice de sa position).
    4. Prévision de la période future h = niveau × indice[(h-1) % season_length].

    Requiert >= 2 cycles complets d'historique (sinon ForecastingError) : avec
    un seul cycle, indices et bruit sont indiscernables. Le fallback plat est
    de la responsabilité de l'appelant (cf. ForecastingEngine.generate, qui le
    documente dans la provenance).

    ⚠️ Ne pas utiliser sur demande intermittente (beaucoup de zéros) : les
    indices y capturent la position aléatoire des transactions, pas une
    saison — Croston reste le bon modèle (et reste plat par design).

    Args:
        season_length: Nombre de périodes par cycle saisonnier (>= 2).

    Performance: O(n) en temps, O(season_length) en mémoire.
    """

    def __init__(self, season_length: int):
        """
        Initialiser le forecaster saisonnier.

        Args:
            season_length: Longueur du cycle saisonnier (>= 2)

        Raises:
            ValueError: Si season_length < 2
        """
        if not isinstance(season_length, int) or season_length < 2:
            raise ValueError(f"season_length doit être un entier >= 2, reçu: {season_length}")

        self.season_length = season_length
        logger.debug(f"SeasonalForecaster initialisé avec season_length={season_length}")

    def _fit(self, historical_data: List[Union[Decimal, float, int]]) -> tuple:
        """
        Ajuster (niveau, indices) sur les cycles complets de l'historique.

        Returns:
            tuple[float, list[float]]: (niveau désaisonnalisé, indices par position)

        Raises:
            ForecastingError: Si l'historique couvre moins de 2 cycles complets.
        """
        self.validate_historical_data(historical_data, min_length=2 * self.season_length)

        data = [float(value) for value in historical_data]
        cycle_count = len(data) // self.season_length
        # Cycles complets alignés sur la fin : la position 0 est la position
        # du premier point conservé, et le point suivant l'historique tombe
        # exactement en position 0 (longueur conservée = multiple du cycle).
        trimmed = data[len(data) - cycle_count * self.season_length:]

        grand_mean = sum(trimmed) / len(trimmed)
        if grand_mean <= 0:
            # Série nulle (ou de somme <= 0) : aucun profil exploitable,
            # indices neutres — la prévision retombe sur le niveau seul.
            indices = [1.0] * self.season_length
        else:
            indices = []
            for position in range(self.season_length):
                position_values = [trimmed[j] for j in range(position, len(trimmed), self.season_length)]
                indices.append((sum(position_values) / len(position_values)) / grand_mean)

        # Niveau = moyenne du dernier cycle désaisonnalisé. Les positions
        # d'indice nul (demande toujours nulle à cette position) sont exclues
        # de la moyenne : leur prévision est structurellement 0.
        deseasonalized = [
            trimmed[j] / indices[j % self.season_length]
            for j in range(len(trimmed) - self.season_length, len(trimmed))
            if indices[j % self.season_length] > 0
        ]
        level = sum(deseasonalized) / len(deseasonalized) if deseasonalized else 0.0

        return level, indices

    def seasonal_indices(self, historical_data: List[Union[Decimal, float, int]]) -> List[Decimal]:
        """
        Retourner les indices saisonniers ajustés (un par position du cycle).

        Args:
            historical_data: Données historiques (>= 2 cycles complets)

        Returns:
            List[Decimal]: season_length indices, de moyenne 1.0
        """
        _, indices = self._fit(historical_data)
        return [Decimal(str(index)) for index in indices]

    def forecast(self, historical_data: List[Union[Decimal, float, int]]) -> Decimal:
        """
        Calculer la prévision de la période suivante (niveau × indice).

        Exemple (season_length=4):
            historical_data = [200, 50, 100, 50, 200, 50, 100, 50]
            → moyenne globale = 100 ; indices = [2.0, 0.5, 1.0, 0.5]
            → niveau (dernier cycle désaisonnalisé) = 100
            → prochaine période en position 0 → forecast = 100 × 2.0 = 200

        Args:
            historical_data: Données historiques (>= 2 cycles complets)

        Returns:
            Decimal: Prévision pour la période suivante
        """
        return self.forecast_curve(historical_data, periods=1)[0]

    def forecast_curve(
        self,
        historical_data: List[Union[Decimal, float, int]],
        periods: int,
    ) -> List[Decimal]:
        """
        Calculer la COURBE de prévision sur plusieurs périodes.

        Chaque période future h (1-indexée) reçoit
        niveau × indice[(h-1) % season_length] — le profil saisonnier se
        répète sur l'horizon.

        Args:
            historical_data: Données historiques (>= 2 cycles complets)
            periods: Nombre de périodes à prévoir (>= 1)

        Returns:
            List[Decimal]: periods prévisions (la courbe saisonnière)

        Raises:
            ValueError: Si periods < 1
            ForecastingError: Si l'historique couvre moins de 2 cycles complets
        """
        if periods < 1:
            raise ValueError(f"periods doit être >= 1, reçu: {periods}")

        level, indices = self._fit(historical_data)
        curve = [
            Decimal(str(level * indices[(h - 1) % self.season_length]))
            for h in range(1, periods + 1)
        ]

        logger.debug(
            f"SeasonalForecaster: {len(historical_data)} valeurs historiques, "
            f"season_length={self.season_length}, level={level}, "
            f"curve[0]={curve[0]}"
        )

        return curve

    def __repr__(self) -> str:
        return f"SeasonalForecaster(season_length={self.season_length})"


# ─────────────────────────────────────────────────────────────
# Fonctions utilitaires
# ─────────────────────────────────────────────────────────────

def create_forecaster(model_type: str, **kwargs) -> Forecaster:
    """
    Factory function pour créer un forecaster par type.
    
    Args:
        model_type: Type de modèle ('moving_average', 'exponential_smoothing', 'croston', 'seasonal')
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

    elif model_type == 'seasonal':
        # Pas de défaut : la longueur du cycle dépend de la granularité des
        # données (7 quotidien, 12 mensuel, 52 hebdomadaire) — l'appelant choisit.
        season_length = kwargs.get('season_length')
        if season_length is None:
            raise ValueError("Le modèle 'seasonal' requiert le paramètre season_length (entier >= 2)")
        return SeasonalForecaster(season_length=season_length)

    else:
        raise ValueError(
            f"Type de modèle inconnu: '{model_type}'. "
            f"Disponible: 'moving_average', 'exponential_smoothing', 'croston', 'seasonal'"
        )
