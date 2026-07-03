from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import date
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from ootils_core.forecasting import ForecastMethod, ForecastingEngine, ForecastingError

from .accuracy import AccuracyReport, conformal_intervals, evaluate_rolling_origin
from .models import (
    METHOD_AUTO_SELECT,
    METHOD_ENSEMBLE_STAT,
    METHOD_FM_CHRONOS,
    METHOD_FM_MOIRAI,
    METHOD_ML_LGBM,
    METHOD_STAT_AUTOARIMA,
    METHOD_STAT_AUTOETS,
)


logger = logging.getLogger(__name__)


BASE_METHODS = frozenset(
    {ForecastMethod.MA, ForecastMethod.EXP_SMOOTHING, ForecastMethod.CROSTON, ForecastMethod.SEASONAL}
)
EXTERNAL_METHODS = frozenset(
    {METHOD_STAT_AUTOETS, METHOD_STAT_AUTOARIMA, METHOD_ML_LGBM, METHOD_FM_CHRONOS, METHOD_FM_MOIRAI}
)


class PyramideEngineError(ValueError):
    """Raised when no forecast engine can produce values for a run."""


@dataclass(frozen=True)
class ForecastComputation:
    values: tuple[Decimal, ...]
    selected_model: str
    value_method: str
    engine_backend: str
    warnings: tuple[str, ...] = ()
    # Rapport de backtest rolling-origin du modèle qui a PRODUIT `values`
    # (accuracy.evaluate_rolling_origin) : la matière première des bornes
    # conformal persistées par le runner. None = pas de backtest
    # déterministe disponible pour ces valeurs — mélange ENSEMBLE_STAT
    # (les résidus d'un candidat ne décrivent pas le blend), backend
    # externe servi directement (backtester statsforecast/LGBM à chaque
    # run est hors périmètre V1), ou historique trop court pour une seule
    # origine. L'appelant ne doit JAMAIS inventer d'intervalles quand ce
    # champ est None (colonnes NULL + provenance).
    accuracy_report: AccuracyReport | None = None


@dataclass(frozen=True)
class _Candidate:
    method: str
    params: Mapping[str, Any]
    label: str


class PyramideForecastEngine:
    """Forecast model router for Pyramide.

    The internal engines are deterministic and dependency-free. Heavy backends
    are loaded lazily and can fall back to AUTO_SELECT unless strict_backend is
    requested in method_params.
    """

    def __init__(self, forecasting_engine: ForecastingEngine | None = None) -> None:
        self._forecasting_engine = forecasting_engine or ForecastingEngine()

    def forecast(
        self,
        *,
        history: Sequence[Decimal],
        periods: int,
        method: str,
        method_params: Mapping[str, Any],
        model_strategy: str,
        granularity: str,
        horizon_start: date,
        random_seed: int,
    ) -> ForecastComputation:
        if periods < 1:
            raise PyramideEngineError("periods must be >= 1")

        method = method.upper()
        params = dict(method_params)
        # Longueur de cycle saisonnier : paramétrable, sinon dérivée de la
        # granularité (7 quotidien, 52 hebdo, 12 mensuel). Résolue une fois ici
        # pour que SEASONAL direct, les candidats AUTO_SELECT/ENSEMBLE et les
        # backends externes partagent la même valeur.
        params.setdefault("season_length", _default_season_length(granularity))
        if method in BASE_METHODS:
            computed = self._classical(history, periods, method, params)
            # Rapport d'accuracy du modèle demandé, attaché ICI (et pas dans
            # _classical) pour ne pas re-backtester à chaque recomputation
            # interne d'AUTO_SELECT / ENSEMBLE_STAT.
            report = self._backtest_report(
                history,
                _Candidate(method=method, params=params, label=self._label(method, params)),
                horizon=periods,
            )
            return replace(computed, accuracy_report=report)
        if method == METHOD_AUTO_SELECT:
            return self._auto_select(history, periods, params)
        if method == METHOD_ENSEMBLE_STAT:
            return self._ensemble_stat(history, periods, params)
        if method in {METHOD_STAT_AUTOETS, METHOD_STAT_AUTOARIMA}:
            return self._with_fallback(
                lambda: self._statsforecast(history, periods, method, params, granularity, horizon_start),
                method,
                history,
                periods,
                params,
            )
        if method == METHOD_ML_LGBM:
            return self._with_fallback(
                lambda: self._mlforecast_lgbm(history, periods, params, granularity, horizon_start, random_seed),
                method,
                history,
                periods,
                params,
            )
        if method in {METHOD_FM_CHRONOS, METHOD_FM_MOIRAI}:
            return self._foundation_model_fallback(method, history, periods, params, model_strategy)

        raise PyramideEngineError(f"Unsupported forecast method: {method}")

    def _classical(
        self,
        history: Sequence[Decimal],
        periods: int,
        method: str,
        params: Mapping[str, Any],
    ) -> ForecastComputation:
        try:
            result = self._forecasting_engine.generate(
                item_history=list(history),
                method=method,
                params=dict(params),
            )
        except ForecastingError as exc:
            raise PyramideEngineError(str(exc)) from exc

        if method == ForecastMethod.SEASONAL and result.parameters.get("seasonal_applied"):
            # Courbe saisonnière déterministe (niveau × indices). Si l'historique
            # couvre < 2 cycles complets, generate() est déjà retombé sur un
            # niveau plat et l'a documenté dans result.warnings.
            series = self._forecasting_engine.forecast_series(
                item_history=list(history),
                method=method,
                params=dict(params),
                periods=periods,
            )
            values = tuple(max(value, Decimal("0")) for value in series)
        else:
            value = max(result.forecast_value, Decimal("0"))
            values = tuple(value for _ in range(periods))

        return ForecastComputation(
            values=values,
            selected_model=self._label(method, params),
            value_method=method,
            engine_backend="internal:classical",
            warnings=tuple(result.warnings),
        )

    def _auto_select(
        self,
        history: Sequence[Decimal],
        periods: int,
        params: Mapping[str, Any],
    ) -> ForecastComputation:
        candidates = self._candidate_specs(history, params)
        # Le rapport complet est conservé par candidat : celui du gagnant
        # devient l'accuracy_report du résultat (résidus par horizon →
        # bornes conformal côté runner), sans re-backtester.
        scored: list[tuple[float, _Candidate, AccuracyReport]] = []
        for candidate in candidates:
            report = self._backtest_report(history, candidate, horizon=periods)
            # report None (le candidat ne peut tourner sur aucune origine)
            # ou WAPE None (demande totale nulle sur les fenêtres évaluées :
            # métrique indéfinie, cf. accuracy.wape) = candidat non
            # scorable, exclu du tri — même sémantique que l'ancien
            # candidat sans score.
            if report is not None and report.wape is not None:
                scored.append((float(report.wape), candidate, report))

        best_report: AccuracyReport | None = None
        if scored:
            scored.sort(key=lambda item: (item[0], item[1].label))
            best = scored[0][1]
            best_report = scored[0][2]
        elif candidates:
            best = candidates[0]
        else:
            raise PyramideEngineError("No candidate model can run on this history")

        computed = self._classical(history, periods, best.method, best.params)
        warning = f"AUTO_SELECT chose {best.label}"
        return ForecastComputation(
            values=computed.values,
            selected_model=best.label,
            value_method=best.method,
            engine_backend="internal:auto_select",
            warnings=(warning, *computed.warnings),
            accuracy_report=best_report,
        )

    def _ensemble_stat(
        self,
        history: Sequence[Decimal],
        periods: int,
        params: Mapping[str, Any],
    ) -> ForecastComputation:
        candidates = self._candidate_specs(history, params)
        forecasts: list[tuple[tuple[Decimal, ...], float, _Candidate]] = []
        for candidate in candidates:
            try:
                computed = self._classical(history, periods, candidate.method, candidate.params)
            except PyramideEngineError:
                continue
            score = self._backtest_score(history, candidate, horizon=periods)
            # score is None = pas de backtest possible (ou WAPE indéfini sur
            # demande nulle) → poids neutre 1.0.
            # Un score PARFAIT de 0.0 est falsy mais doit recevoir le poids
            # plafond (1/0.0001), pas le poids neutre : ne pas utiliser `or`.
            weight = 1.0 if score is None else 1.0 / max(score, 0.0001)
            forecasts.append((computed.values, weight, candidate))

        if not forecasts:
            return self._auto_select(history, periods, params)

        # Mélange pondéré pas-à-pas : les candidats plats contribuent une
        # constante, un candidat saisonnier contribue sa courbe.
        total_weight = sum(weight for _, weight, _ in forecasts)
        values = tuple(
            max(
                Decimal(str(sum(float(curve[step]) * weight for curve, weight, _ in forecasts) / total_weight)),
                Decimal("0"),
            )
            for step in range(periods)
        )
        model_names = ",".join(candidate.label for _, _, candidate in forecasts)
        # accuracy_report reste None : les résidus de backtest d'UN candidat
        # ne décrivent pas le mélange pondéré, et backtester le blend
        # lui-même (poids recalculés à chaque origine) est hors périmètre
        # V1 — bornes NULL + provenance côté runner, jamais des bornes
        # empruntées à un autre modèle.
        return ForecastComputation(
            values=values,
            selected_model=METHOD_ENSEMBLE_STAT,
            value_method=METHOD_ENSEMBLE_STAT,
            engine_backend="internal:ensemble_stat",
            warnings=(f"ENSEMBLE_STAT blended {model_names}",),
        )

    def _with_fallback(
        self,
        provider,
        requested_method: str,
        history: Sequence[Decimal],
        periods: int,
        params: Mapping[str, Any],
    ) -> ForecastComputation:
        try:
            return provider()
        except Exception as exc:
            if params.get("strict_backend"):
                raise PyramideEngineError(f"{requested_method} failed: {exc}") from exc
            fallback = self._auto_select(history, periods, params)
            # Les valeurs servies SONT celles d'AUTO_SELECT : son rapport de
            # backtest reste honnête pour elles et voyage avec.
            return replace(
                fallback,
                warnings=(f"{requested_method} unavailable; fell back to AUTO_SELECT: {exc}", *fallback.warnings),
            )

    def _statsforecast(
        self,
        history: Sequence[Decimal],
        periods: int,
        method: str,
        params: Mapping[str, Any],
        granularity: str,
        horizon_start: date,
    ) -> ForecastComputation:
        import pandas as pd
        from statsforecast import StatsForecast
        from statsforecast.models import AutoARIMA, AutoETS

        freq = _freq_for_granularity(granularity)
        season_length = int(params.get("season_length", _default_season_length(granularity)))
        model = AutoETS(season_length=season_length) if method == METHOD_STAT_AUTOETS else AutoARIMA(season_length=season_length)
        dates = pd.date_range(end=pd.Timestamp(horizon_start), periods=len(history) + 1, freq=freq)[:-1]
        frame = pd.DataFrame(
            {
                "unique_id": "series",
                "ds": dates,
                "y": [float(value) for value in history],
            }
        )
        stats_forecast = StatsForecast(models=[model], freq=freq, n_jobs=1)
        forecast = stats_forecast.forecast(df=frame, h=periods)
        value_column = next(column for column in forecast.columns if column not in {"unique_id", "ds"})
        values = tuple(max(Decimal(str(value)), Decimal("0")) for value in forecast[value_column].tolist())
        return ForecastComputation(
            values=values,
            selected_model=method,
            value_method=method,
            engine_backend="statsforecast",
            warnings=(),
        )

    def _mlforecast_lgbm(
        self,
        history: Sequence[Decimal],
        periods: int,
        params: Mapping[str, Any],
        granularity: str,
        horizon_start: date,
        random_seed: int,
    ) -> ForecastComputation:
        import pandas as pd
        from lightgbm import LGBMRegressor
        from mlforecast import MLForecast

        freq = _freq_for_granularity(granularity)
        lags = list(params.get("lags", [1, 2, 3, 7]))
        dates = pd.date_range(end=pd.Timestamp(horizon_start), periods=len(history) + 1, freq=freq)[:-1]
        frame = pd.DataFrame(
            {
                "unique_id": "series",
                "ds": dates,
                "y": [float(value) for value in history],
            }
        )
        model = LGBMRegressor(
            random_state=random_seed,
            n_estimators=int(params.get("n_estimators", 100)),
            verbosity=-1,
        )
        forecast = MLForecast(models=[model], freq=freq, lags=lags)
        forecast.fit(frame)
        prediction = forecast.predict(h=periods)
        value_column = next(column for column in prediction.columns if column not in {"unique_id", "ds"})
        values = tuple(max(Decimal(str(value)), Decimal("0")) for value in prediction[value_column].tolist())
        return ForecastComputation(
            values=values,
            selected_model=METHOD_ML_LGBM,
            value_method=METHOD_ML_LGBM,
            engine_backend="mlforecast:lightgbm",
            warnings=(),
        )

    def _foundation_model_fallback(
        self,
        method: str,
        history: Sequence[Decimal],
        periods: int,
        params: Mapping[str, Any],
        model_strategy: str,
    ) -> ForecastComputation:
        if params.get("strict_backend"):
            raise PyramideEngineError(f"{method} backend is not installed in this environment")
        fallback = self._auto_select(history, periods, params)
        return ForecastComputation(
            values=fallback.values,
            selected_model=fallback.selected_model,
            value_method=fallback.value_method,
            engine_backend=fallback.engine_backend,
            warnings=(f"{method} requested under {model_strategy}; fell back to AUTO_SELECT", *fallback.warnings),
        )

    def _candidate_specs(self, history: Sequence[Decimal], params: Mapping[str, Any]) -> list[_Candidate]:
        # method_params est un dict libre côté router : toute valeur illisible
        # écarte simplement le candidat concerné (ou retombe sur le défaut),
        # tracée en debug — jamais d'exception depuis cette méthode.
        windows = params.get("ma_windows", [3, 6, 12])
        if not isinstance(windows, (list, tuple)):
            logger.debug("ma_windows illisible (%r); défaut [3, 6, 12]", windows)
            windows = [3, 6, 12]
        alphas = params.get("exp_alphas", [0.2, 0.5, 0.8])
        if not isinstance(alphas, (list, tuple)):
            logger.debug("exp_alphas illisible (%r); défaut [0.2, 0.5, 0.8]", alphas)
            alphas = [0.2, 0.5, 0.8]
        candidates: list[_Candidate] = []
        for window in windows:
            window_int = _as_int(window)
            if window_int is None or window_int < 1:
                logger.debug("ma_windows: fenêtre illisible (%r); candidat MA ignoré", window)
                continue
            if len(history) >= window_int:
                candidates.append(
                    _Candidate(
                        method=ForecastMethod.MA,
                        params={"window": window_int},
                        label=f"MA(window={window_int})",
                    )
                )
        for alpha in alphas:
            alpha_float = _as_float(alpha)
            if alpha_float is None or not 0 < alpha_float <= 1:
                logger.debug("exp_alphas: alpha illisible (%r); candidat EXP_SMOOTHING ignoré", alpha)
                continue
            candidates.append(
                _Candidate(
                    method=ForecastMethod.EXP_SMOOTHING,
                    params={"alpha": alpha_float},
                    label=f"EXP_SMOOTHING(alpha={alpha_float:.2f})",
                )
            )
        # Candidat saisonnier : uniquement si l'historique couvre >= 2 cycles
        # complets ET que la série n'est pas intermittente — projeter des
        # indices saisonniers sur une demande intermittente extrapole la
        # position aléatoire des transactions, pas une saison (Croston reste
        # le modèle plat adapté, par design).
        season_length = _as_int(params.get("season_length", 0))
        if season_length is None:
            logger.debug(
                "season_length illisible (%r); candidat SEASONAL non proposé",
                params.get("season_length"),
            )
            season_length = 0
        zero_ratio_threshold = _as_float(params.get("croston_zero_ratio", 0.2))
        if zero_ratio_threshold is None:
            logger.debug(
                "croston_zero_ratio illisible (%r); défaut 0.2",
                params.get("croston_zero_ratio"),
            )
            zero_ratio_threshold = 0.2
        intermittent = self._zero_ratio(history) >= zero_ratio_threshold
        if season_length >= 2 and len(history) >= 2 * season_length and not intermittent:
            candidates.append(
                _Candidate(
                    method=ForecastMethod.SEASONAL,
                    params={"season_length": season_length},
                    label=f"SEASONAL(season_length={season_length})",
                )
            )
        if intermittent:
            threshold = _as_float(params.get("min_demand_threshold", 0.0))
            if threshold is None:
                logger.debug(
                    "min_demand_threshold illisible (%r); défaut 0.0",
                    params.get("min_demand_threshold"),
                )
                threshold = 0.0
            candidates.append(
                _Candidate(
                    method=ForecastMethod.CROSTON,
                    params={"min_demand_threshold": threshold},
                    label="CROSTON",
                )
            )
        return candidates

    def _backtest_report(
        self, history: Sequence[Decimal], candidate: _Candidate, horizon: int = 1
    ) -> AccuracyReport | None:
        """Backtest rolling-origin multi-horizon d'UN candidat, DÉLÉGUÉ à
        accuracy.evaluate_rolling_origin (PR-D2) — le rapport complet
        (WAPE/MASE/sMAPE/biais + résidus par horizon) est la matière
        première des bornes conformal et du scalaire de tri.

        Sémantique conservée de l'ancienne boucle locale :
        - fenêtre d'évaluation = queue des 52 dernières origines,
        - courbes multi-pas (fenêtres partielles en fin de série incluses,
          même convention qu'evaluate_rolling_origin),
        - candidat SEASONAL scoré uniquement sur les origines où il tourne
          vraiment (>= 2 cycles complets d'entraînement) : sinon le score
          mesurerait le fallback plat, pas le modèle saisonnier,
        - forecast clampé à 0 comme les valeurs servies (_classical /
          runner) : le backtest mesure ce qui serait réellement livré.

        Origine de départ : les modèles internes sont monotones en longueur
        d'entraînement (MA(window) exige window points, EXP/CROSTON >= 1,
        SEASONAL géré ci-dessus), donc on avance l'origine jusqu'à la
        première où le candidat tourne au lieu d'ignorer silencieusement
        des origines au milieu de l'évaluation.

        MASE : m=1 uniformément (naive lag-1) pour que le MASE du rapport
        reste comparable ENTRE candidats d'un même AUTO_SELECT ; le tri
        lui-même se fait sur le WAPE (cf. _backtest_score).

        None = candidat non scorable : série trop courte (< 3 points),
        aucune origine exploitable, ou échec du modèle en cours
        d'évaluation (non-monotonie inattendue, tracée en debug).
        """
        if len(history) < 3:
            return None

        horizon = max(1, horizon)
        min_train = max(1, len(history) - 52)
        if candidate.method == ForecastMethod.SEASONAL:
            min_train = max(min_train, 2 * int(candidate.params.get("season_length", 0)))

        def forecast_fn(train: Sequence[Decimal], periods: int) -> Sequence[Decimal]:
            series = self._forecasting_engine.forecast_series(
                item_history=list(train),
                method=candidate.method,
                params=dict(candidate.params),
                periods=periods,
            )
            return [max(value, Decimal("0")) for value in series]

        while min_train < len(history):
            try:
                forecast_fn(history[:min_train], 1)
                break
            except (ForecastingError, ValueError):
                min_train += 1
        if min_train >= len(history):
            return None

        try:
            return evaluate_rolling_origin(
                series=list(history),
                forecast_fn=forecast_fn,
                horizon=horizon,
                min_train=min_train,
                step=1,
                m=1,
            )
        except (ForecastingError, ValueError) as exc:
            logger.debug("backtest impossible pour %s: %s", candidate.label, exc)
            return None

    def _backtest_score(self, history: Sequence[Decimal], candidate: _Candidate, horizon: int = 1) -> float | None:
        """Scalaire de tri des candidats (AUTO_SELECT / ENSEMBLE_STAT) : le
        WAPE du rapport rolling-origin délégué (_backtest_report).

        CHANGEMENT ASSUMÉ (validé pilote) vs l'ancienne boucle locale : le
        dénominateur piégé max(|actual|, 1) a disparu — sur des fenêtres à
        demande totale nulle le WAPE est indéfini (None, cf. accuracy.wape)
        et le candidat est « non scorable » : exclu du tri côté AUTO_SELECT,
        poids neutre côté ENSEMBLE_STAT — même sémantique que l'ancien
        candidat sans score."""
        report = self._backtest_report(history, candidate, horizon=horizon)
        if report is None or report.wape is None:
            return None
        return float(report.wape)

    @staticmethod
    def _zero_ratio(history: Sequence[Decimal]) -> float:
        if not history:
            return 0.0
        zeros = sum(1 for value in history if value <= 0)
        return zeros / len(history)

    @staticmethod
    def _label(method: str, params: Mapping[str, Any]) -> str:
        if method == ForecastMethod.MA:
            return f"MA(window={int(params.get('window', 3))})"
        if method == ForecastMethod.EXP_SMOOTHING:
            return f"EXP_SMOOTHING(alpha={float(params.get('alpha', 0.3)):.2f})"
        if method == ForecastMethod.CROSTON:
            return "CROSTON"
        if method == ForecastMethod.SEASONAL:
            return f"SEASONAL(season_length={int(params.get('season_length', 0))})"
        return method


# ─────────────────────────────────────────────────────────────
# Bornes conformal persistables (migration 026) — PR-D2
# ─────────────────────────────────────────────────────────────

DEFAULT_CONFORMAL_ALPHA = Decimal("0.2")
_ZERO = Decimal("0")
_ONE = Decimal("1")


def resolve_conformal_alpha(method_params: Mapping[str, Any]) -> Decimal:
    """Taux de non-couverture alpha des intervalles conformal.

    Défaut 0.2 (couverture nominale 80 %), paramétrable via
    ``method_params["conformal_alpha"]``. Contrairement aux paramètres de
    candidats (_candidate_specs, tolérants par design), un alpha illisible
    ou hors (0, 1) est une erreur de CONFIGURATION des bornes publiées :
    fail loudly (PyramideEngineError → 422 côté routeur), jamais un défaut
    silencieux qui publierait des bornes à une couverture différente de
    celle demandée."""
    raw = method_params.get("conformal_alpha", DEFAULT_CONFORMAL_ALPHA)
    try:
        alpha = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):
        raise PyramideEngineError(
            f"method_params.conformal_alpha must be a number in (0, 1) (got {raw!r})"
        )
    if not (_ZERO < alpha < _ONE):
        raise PyramideEngineError(
            f"method_params.conformal_alpha must be in (0, 1) (got {raw!r})"
        )
    return alpha


def conformal_min_residuals(alpha: Decimal) -> int:
    """Nombre minimal de résidus de calibration par horizon pour que la
    garantie finite-sample du split conformal tienne : ceil(2/alpha) - 1
    (cf. accuracy.conformal_intervals, « small-sample clamp »). En dessous,
    accuracy.py clamperait aux statistiques d'ordre extrêmes (best-effort) ;
    la couche persistance REFUSE ce mode dégradé — bornes NULL plutôt que
    des bornes sans garantie (jamais de bornes inventées)."""
    return int((Decimal(2) / alpha).to_integral_value(rounding=ROUND_CEILING)) - 1


def conformal_bounds(
    *,
    report: AccuracyReport | None,
    values: Sequence[Decimal],
    method_params: Mapping[str, Any],
    scale: Decimal = _ONE,
) -> tuple[tuple[Decimal | None, ...], tuple[Decimal | None, ...], tuple[str, ...]]:
    """Bornes conformal par bucket pour une courbe servie — la matière des
    colonnes ``forecast_values.confidence_interval_lower/upper``.

    ``report`` est le backtest rolling-origin du modèle qui a PRODUIT les
    valeurs (ForecastComputation.accuracy_report). Pour chaque bucket i
    (horizon h = i + 1) :

        lower[i] = max(0, max(0, values[i]) + scale * lower_offset[h])
        upper[i] = max(0, max(0, values[i]) + scale * upper_offset[h])

    - Clamp à 0 de la borne inférieure (et supérieure, par cohérence) :
      une demande négative n'existe pas — un offset très négatif sur un
      point proche de zéro donne une borne 0, pas une borne absurde.
    - Horizon sans assez de résidus (< conformal_min_residuals(alpha)) →
      bornes None pour ce bucket + warning : la garantie finite-sample
      n'existe plus en dessous du clamp small-sample d'accuracy.py.
    - ``report`` absent (None) ou sans résidus → tout None + warning :
      pas de backtest déterministe pour ces valeurs, on ne publie rien.
    - ``scale`` : facteur appliqué aux offsets — 1 pour une série prévue
      directement ; la part de désagrégation middle-out pour une FEUILLE
      de bloc hiérarchique (feuille = part × nœud, donc l'erreur du nœud
      se transporte multipliée par la part). Approximation V1 documentée :
      l'erreur idiosyncratique de la feuille n'est pas observée au niveau
      du nœud, la garantie de couverture est celle du nœud, pas de la
      feuille.

    Retour : (lowers, uppers, warnings), lowers/uppers alignés sur
    ``values``. Lève PyramideEngineError si conformal_alpha est invalide.
    """
    alpha = resolve_conformal_alpha(method_params)
    periods = len(values)
    none_bounds: tuple[Decimal | None, ...] = (None,) * periods
    if report is None or not report.per_horizon_residuals:
        return none_bounds, none_bounds, (
            "conformal intervals unavailable: no deterministic backtest "
            "residuals for the served values (bounds NULL)",
        )

    n_min = conformal_min_residuals(alpha)
    eligible = {
        h: residuals
        for h, residuals in report.per_horizon_residuals.items()
        if len(residuals) >= n_min
    }
    lower_offsets, upper_offsets = conformal_intervals(eligible, alpha)

    lowers: list[Decimal | None] = []
    uppers: list[Decimal | None] = []
    missing = 0
    for index, value in enumerate(values):
        h = index + 1
        if h not in lower_offsets:
            missing += 1
            lowers.append(None)
            uppers.append(None)
            continue
        point = max(value, _ZERO)
        lowers.append(max(point + scale * lower_offsets[h], _ZERO))
        uppers.append(max(point + scale * upper_offsets[h], _ZERO))

    warnings: tuple[str, ...] = ()
    if missing:
        warnings = (
            f"conformal intervals: {missing}/{periods} bucket(s) without "
            f"enough backtest residuals (need >= {n_min} per horizon for "
            f"alpha={alpha}); their bounds stay NULL",
        )
    return tuple(lowers), tuple(uppers), warnings


def _as_int(value: Any) -> int | None:
    """Coercition tolérante d'un paramètre libre : None si illisible."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    """Coercition tolérante d'un paramètre libre : None si illisible."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _freq_for_granularity(granularity: str) -> str:
    return {"daily": "D", "weekly": "W", "monthly": "MS"}[granularity]


def _default_season_length(granularity: str) -> int:
    return {"daily": 7, "weekly": 52, "monthly": 12}[granularity]
