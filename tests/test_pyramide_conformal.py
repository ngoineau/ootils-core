"""
Pyramide axis D — PR-D2 unit tests (pure, no DB): the engine backtest is
DELEGATED to accuracy.evaluate_rolling_origin, and the runner derives
persistable conformal bounds from the selected model's backtest residuals.

Covered:
  - _backtest_score is the WAPE of the delegated rolling-origin report
    (hand-computed golden), replacing the old max(|actual|, 1) trap;
  - zero-demand evaluation windows -> WAPE None -> candidate "non
    scorable" (excluded from AUTO_SELECT ranking, no invented score);
  - the perfect candidate still wins AUTO_SELECT (behavioral contract,
    complements tests/test_pyramide_runner.py);
  - PyramideRunner attaches conformal bounds per bucket: point + offset,
    lower clamped at 0, honest NULL (+ warning) when there is no
    deterministic backtest or too few residuals for the finite-sample
    guarantee;
  - alpha is configurable via method_params.conformal_alpha and fails
    loudly when invalid;
  - conformal_bounds scale= transports node offsets to a hierarchy leaf
    (share x offset), the documented V1 approximation.
"""
from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest

from ootils_core.forecasting import ForecastMethod
from ootils_core.pyramide import PyramideError, PyramideRunConfig, PyramideRunner
from ootils_core.pyramide.accuracy import AccuracyReport
from ootils_core.pyramide.engines import (
    PyramideEngineError,
    PyramideForecastEngine,
    _Candidate,
    conformal_bounds,
    conformal_min_residuals,
    resolve_conformal_alpha,
)


ITEM_ID = UUID("10000000-0000-0000-0000-000000000001")
LOCATION_ID = UUID("20000000-0000-0000-0000-000000000001")
SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


def _config(**overrides) -> PyramideRunConfig:
    values = {
        "item_id": ITEM_ID,
        "location_id": LOCATION_ID,
        "scenario_id": SCENARIO_ID,
        "horizon_start": date(2026, 7, 1),
        "horizon_days": 3,
        "granularity": "daily",
        "method": "MA",
        "method_params": {"window": 2},
    }
    values.update(overrides)
    return PyramideRunConfig(**values)


def _candidate(method: str, params: dict, label: str) -> _Candidate:
    return _Candidate(method=method, params=params, label=label)


# ---------------------------------------------------------------------------
# Delegated backtest — golden WAPE, None semantics
# ---------------------------------------------------------------------------


def test_backtest_score_is_wape_of_delegated_report():
    """Hand-computed golden. history=[10,10,10,10,20], MA(window=2):
    origins 2..4 (MA needs 2 points), horizon 1 ->
      o=2: f=10 a=10 e=0 ; o=3: f=10 a=10 e=0 ; o=4: f=10 a=20 e=10
    WAPE = (0+0+10) / (10+10+20) = 0.25.
    The OLD local loop would have scored mean(|e|/max(|a|,1)) = 1/6 —
    the score scale changed by design (CHANGEMENT ASSUMÉ, validated)."""
    engine = PyramideForecastEngine()
    candidate = _candidate(ForecastMethod.MA, {"window": 2}, "MA(window=2)")
    history = [Decimal(v) for v in (10, 10, 10, 10, 20)]

    score = engine._backtest_score(history, candidate, horizon=1)
    report = engine._backtest_report(history, candidate, horizon=1)

    assert score == 0.25
    assert report is not None
    assert report.wape == Decimal("0.25")
    assert report.n_cutoffs == 3
    # Residuals are actual - forecast, in cutoff order (conformal input).
    assert report.per_horizon_residuals[1] == (
        Decimal("0.0"), Decimal("0.0"), Decimal("10.0"),
    )


def test_backtest_score_none_on_zero_demand_windows():
    """Every evaluated window has zero total demand -> WAPE is undefined
    (accuracy.wape returns None) -> the candidate is 'non scorable' (None),
    NOT scored against the old masked max(|actual|, 1) denominator."""
    engine = PyramideForecastEngine()
    candidate = _candidate(ForecastMethod.MA, {"window": 1}, "MA(window=1)")
    history = [Decimal(v) for v in (5, 0, 0, 0)]

    assert engine._backtest_score(history, candidate, horizon=1) is None


def test_auto_select_on_zero_demand_windows_has_no_accuracy_report():
    """When no candidate is scorable (all evaluated windows are zero
    demand), AUTO_SELECT still serves values (first candidate fallback)
    but attaches NO accuracy report — the caller must not invent bounds."""
    engine = PyramideForecastEngine()
    computation = engine.forecast(
        history=[Decimal(v) for v in (5, 0, 0, 0)],
        periods=2,
        method="AUTO_SELECT",
        method_params={},
        model_strategy="stat",
        granularity="daily",
        horizon_start=date(2026, 7, 1),
        random_seed=0,
    )
    assert computation.values  # still forecastable
    assert computation.accuracy_report is None


def test_auto_select_winner_report_travels_with_the_computation():
    """The winner's FULL report (multi-horizon residuals) is attached to
    the AUTO_SELECT result without re-backtesting — the raw material of
    the conformal bounds."""
    engine = PyramideForecastEngine()
    history = [Decimal(8) if i % 2 == 0 else Decimal(12) for i in range(20)]
    computation = engine.forecast(
        history=history,
        periods=3,
        method="AUTO_SELECT",
        method_params={},
        model_strategy="stat",
        granularity="daily",
        horizon_start=date(2026, 7, 1),
        random_seed=0,
    )
    report = computation.accuracy_report
    assert isinstance(report, AccuracyReport)
    assert report.wape is not None
    # Multi-horizon: horizons 1..3 all evaluated (partial windows included).
    assert set(report.per_horizon_residuals) == {1, 2, 3}


def test_backtest_report_none_when_no_origin_can_run():
    """A candidate that cannot run on any origin (window larger than the
    whole history) is non-scorable, not an exception."""
    engine = PyramideForecastEngine()
    candidate = _candidate(ForecastMethod.MA, {"window": 99}, "MA(window=99)")
    history = [Decimal(10), Decimal(11), Decimal(12)]

    assert engine._backtest_report(history, candidate, horizon=1) is None
    assert engine._backtest_score(history, candidate, horizon=1) is None


# ---------------------------------------------------------------------------
# Runner — persisted conformal bounds
# ---------------------------------------------------------------------------


def test_runner_attaches_conformal_bounds_around_the_point():
    """Alternating 8/12 series, MA(window=2): the point forecast is 10 and
    every backtest residual is exactly +/-2, so at alpha=0.2 the bounds are
    the observed extremes [8, 12] at every horizon (order statistics ARE
    observed residuals — explainability)."""
    history = [Decimal(8) if i % 2 == 0 else Decimal(12) for i in range(40)]
    result = PyramideRunner().run(_config(), history)

    assert len(result.values) == 3
    for value in result.values:
        assert value.quantity == Decimal("10.0")
        assert value.confidence_lower == Decimal("8.0")
        assert value.confidence_upper == Decimal("12.0")
        assert value.confidence_lower <= value.quantity <= value.confidence_upper
    assert not [w for w in result.warnings if "conformal" in w]


def test_runner_lower_bound_is_clamped_at_zero():
    """Declining-to-zero series: the MA(2) point forecast ends at 0 while
    the calibration residuals are strongly negative — without the clamp the
    lower bound would be negative demand, which does not exist."""
    history = [Decimal(v) for v in (50, 40, 30, 20, 10, 0, 0, 0, 0, 0, 0, 0)]
    result = PyramideRunner().run(_config(horizon_days=1), history)

    value = result.values[0]
    assert value.quantity == Decimal("0")
    assert value.confidence_lower == Decimal("0")
    assert value.confidence_upper is not None
    assert value.confidence_upper >= Decimal("0")


def test_runner_short_series_yields_null_bounds_and_provenance():
    """Too few backtest residuals for the finite-sample guarantee
    (n < ceil(2/alpha) - 1 = 9 at alpha 0.2): bounds are None and the
    provenance says so — never invented bounds."""
    result = PyramideRunner().run(
        _config(),
        [Decimal("10"), Decimal("12"), Decimal("14"), Decimal("16")],
    )

    for value in result.values:
        assert value.confidence_lower is None
        assert value.confidence_upper is None
    assert any("conformal" in w and "NULL" in w for w in result.warnings)


def test_runner_alpha_is_configurable_via_method_params():
    """alpha=0.5 needs only ceil(2/0.5)-1 = 3 residuals per horizon: the
    same short series that yields NULL at the default alpha=0.2 gets bounds
    at alpha=0.5 (wider miscoverage, smaller calibration requirement)."""
    history = [Decimal(8) if i % 2 == 0 else Decimal(12) for i in range(6)]

    default_run = PyramideRunner().run(_config(horizon_days=1), history)
    assert default_run.values[0].confidence_lower is None

    loose_run = PyramideRunner().run(
        _config(horizon_days=1, method_params={"window": 2, "conformal_alpha": 0.5}),
        history,
    )
    value = loose_run.values[0]
    assert value.confidence_lower is not None
    assert value.confidence_upper is not None
    assert value.confidence_lower <= value.quantity <= value.confidence_upper


@pytest.mark.parametrize("alpha", ["n/a", 0, 1, -0.1, 2, None])
def test_runner_rejects_invalid_conformal_alpha(alpha):
    """An unreadable or out-of-range alpha is a configuration error of the
    PUBLISHED bounds: fail loudly (422 at the router), no silent default."""
    with pytest.raises(PyramideError):
        PyramideRunner().run(
            _config(method_params={"window": 2, "conformal_alpha": alpha}),
            [Decimal(8) if i % 2 == 0 else Decimal(12) for i in range(40)],
        )


def test_runner_ensemble_stat_has_null_bounds():
    """ENSEMBLE_STAT serves a weighted blend: no single candidate's
    residuals describe it, so accuracy_report is None and the bounds stay
    NULL with explicit provenance (never bounds borrowed from another
    model)."""
    history = [Decimal(8) if i % 2 == 0 else Decimal(12) for i in range(20)]
    result = PyramideRunner().run(
        _config(method="ENSEMBLE_STAT", method_params={}),
        history,
    )

    assert result.engine_backend == "internal:ensemble_stat"
    for value in result.values:
        assert value.confidence_lower is None
        assert value.confidence_upper is None
    assert any("conformal" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# conformal_bounds helper — scale (hierarchy leaves) and alpha resolution
# ---------------------------------------------------------------------------


def _report_with_residuals(residuals: tuple[Decimal, ...]) -> AccuracyReport:
    return AccuracyReport(
        mase=None,
        wape=Decimal("0.1"),
        smape=Decimal("0.1"),
        bias=Decimal("0"),
        coverage=None,
        per_horizon_residuals={1: residuals},
        n_cutoffs=len(residuals),
        n_observations=len(residuals),
    )


def test_conformal_bounds_scale_transports_offsets_to_leaf_share():
    """Hierarchy leaf = share x node: the node's residual offsets are
    scaled by the share. 10 residuals +/-4 at alpha 0.2 -> node offsets
    [-4, +4]; at share 0.5 the leaf bounds are point -/+ 2."""
    residuals = tuple(
        Decimal(-4) if i % 2 == 0 else Decimal(4) for i in range(10)
    )
    report = _report_with_residuals(residuals)

    node_lowers, node_uppers, _ = conformal_bounds(
        report=report, values=[Decimal(100)], method_params={},
    )
    assert node_lowers == (Decimal(96),)
    assert node_uppers == (Decimal(104),)

    leaf_lowers, leaf_uppers, _ = conformal_bounds(
        report=report, values=[Decimal(50)], method_params={},
        scale=Decimal("0.5"),
    )
    assert leaf_lowers == (Decimal(48),)
    assert leaf_uppers == (Decimal(52),)


def test_conformal_bounds_without_report_is_all_null_with_provenance():
    lowers, uppers, warnings = conformal_bounds(
        report=None, values=[Decimal(1), Decimal(2)], method_params={},
    )
    assert lowers == (None, None)
    assert uppers == (None, None)
    assert warnings and "no deterministic backtest" in warnings[0]


def test_conformal_min_residuals_matches_finite_sample_requirement():
    assert conformal_min_residuals(Decimal("0.2")) == 9
    assert conformal_min_residuals(Decimal("0.5")) == 3
    assert conformal_min_residuals(Decimal("0.1")) == 19


def test_resolve_conformal_alpha_default_and_override():
    assert resolve_conformal_alpha({}) == Decimal("0.2")
    assert resolve_conformal_alpha({"conformal_alpha": "0.1"}) == Decimal("0.1")
    with pytest.raises(PyramideEngineError):
        resolve_conformal_alpha({"conformal_alpha": "not-a-number"})
