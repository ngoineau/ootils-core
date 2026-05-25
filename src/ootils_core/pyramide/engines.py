from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Mapping, Sequence

from ootils_core.forecasting import ForecastMethod, ForecastingEngine, ForecastingError

from .models import (
    METHOD_AUTO_SELECT,
    METHOD_ENSEMBLE_STAT,
    METHOD_FM_CHRONOS,
    METHOD_FM_MOIRAI,
    METHOD_ML_LGBM,
    METHOD_STAT_AUTOARIMA,
    METHOD_STAT_AUTOETS,
)


BASE_METHODS = frozenset({ForecastMethod.MA, ForecastMethod.EXP_SMOOTHING, ForecastMethod.CROSTON})
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
        if method in BASE_METHODS:
            return self._classical(history, periods, method, params)
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

        value = max(result.forecast_value, Decimal("0"))
        return ForecastComputation(
            values=tuple(value for _ in range(periods)),
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
        scored = []
        for candidate in candidates:
            score = self._backtest_score(history, candidate)
            if score is not None:
                scored.append((score, candidate))

        if scored:
            scored.sort(key=lambda item: (item[0], item[1].label))
            best = scored[0][1]
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
        )

    def _ensemble_stat(
        self,
        history: Sequence[Decimal],
        periods: int,
        params: Mapping[str, Any],
    ) -> ForecastComputation:
        candidates = self._candidate_specs(history, params)
        forecasts: list[tuple[Decimal, float, _Candidate]] = []
        for candidate in candidates:
            try:
                computed = self._classical(history, 1, candidate.method, candidate.params)
            except PyramideEngineError:
                continue
            score = self._backtest_score(history, candidate)
            weight = 1.0 / max(score or 1.0, 0.0001)
            forecasts.append((computed.values[0], weight, candidate))

        if not forecasts:
            return self._auto_select(history, periods, params)

        total_weight = sum(weight for _, weight, _ in forecasts)
        blended = sum(float(value) * weight for value, weight, _ in forecasts) / total_weight
        value = max(Decimal(str(blended)), Decimal("0"))
        model_names = ",".join(candidate.label for _, _, candidate in forecasts)
        return ForecastComputation(
            values=tuple(value for _ in range(periods)),
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
            return ForecastComputation(
                values=fallback.values,
                selected_model=fallback.selected_model,
                value_method=fallback.value_method,
                engine_backend=fallback.engine_backend,
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
        windows = params.get("ma_windows", [3, 6, 12])
        alphas = params.get("exp_alphas", [0.2, 0.5, 0.8])
        candidates: list[_Candidate] = []
        for window in windows:
            window_int = int(window)
            if len(history) >= window_int:
                candidates.append(
                    _Candidate(
                        method=ForecastMethod.MA,
                        params={"window": window_int},
                        label=f"MA(window={window_int})",
                    )
                )
        for alpha in alphas:
            candidates.append(
                _Candidate(
                    method=ForecastMethod.EXP_SMOOTHING,
                    params={"alpha": float(alpha)},
                    label=f"EXP_SMOOTHING(alpha={float(alpha):.2f})",
                )
            )
        if self._zero_ratio(history) >= float(params.get("croston_zero_ratio", 0.2)):
            candidates.append(
                _Candidate(
                    method=ForecastMethod.CROSTON,
                    params={"min_demand_threshold": float(params.get("min_demand_threshold", 0.0))},
                    label="CROSTON",
                )
            )
        return candidates

    def _backtest_score(self, history: Sequence[Decimal], candidate: _Candidate) -> float | None:
        if len(history) < 3:
            return None

        errors = []
        tail_start = max(1, len(history) - 52)
        for index in range(tail_start, len(history)):
            train = list(history[:index])
            actual = history[index]
            if not train:
                continue
            try:
                result = self._forecasting_engine.generate(
                    item_history=train,
                    method=candidate.method,
                    params=dict(candidate.params),
                )
            except (ForecastingError, ValueError):
                continue
            forecast = max(result.forecast_value, Decimal("0"))
            denom = max(abs(actual), Decimal("1"))
            errors.append(float(abs(actual - forecast) / denom))

        if not errors:
            return None
        return sum(errors) / len(errors)

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
        return method


def _freq_for_granularity(granularity: str) -> str:
    return {"daily": "D", "weekly": "W", "monthly": "MS"}[granularity]


def _default_season_length(granularity: str) -> int:
    return {"daily": 7, "weekly": 52, "monthly": 12}[granularity]
