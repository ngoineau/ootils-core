from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest

from ootils_core.pyramide import PyramideError, PyramideRunConfig, PyramideRunner


ITEM_ID = UUID("10000000-0000-0000-0000-000000000001")
LOCATION_ID = UUID("20000000-0000-0000-0000-000000000001")
SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


def _config(**overrides) -> PyramideRunConfig:
    values = {
        "item_id": ITEM_ID,
        "location_id": LOCATION_ID,
        "scenario_id": SCENARIO_ID,
        "horizon_start": date(2026, 6, 1),
        "horizon_days": 3,
        "granularity": "daily",
        "method": "MA",
        "method_params": {"window": 2},
    }
    values.update(overrides)
    return PyramideRunConfig(**values)


def test_pyramide_runner_builds_daily_snapshot_values():
    result = PyramideRunner().run(
        _config(),
        [Decimal("10"), Decimal("20"), Decimal("30")],
    )

    assert len(result.values) == 3
    assert [value.forecast_date for value in result.values] == [
        date(2026, 6, 1),
        date(2026, 6, 2),
        date(2026, 6, 3),
    ]
    assert [value.quantity for value in result.values] == [Decimal("25")] * 3
    assert result.total_quantity == Decimal("75")
    assert result.source_history_count == 3


def test_pyramide_runner_builds_weekly_buckets():
    result = PyramideRunner().run(
        _config(horizon_days=15, granularity="weekly"),
        [Decimal("10"), Decimal("20"), Decimal("30")],
    )

    assert [value.forecast_date for value in result.values] == [
        date(2026, 6, 1),
        date(2026, 6, 8),
        date(2026, 6, 15),
    ]


def test_pyramide_runner_rejects_unknown_method():
    # SEASONAL is a real method now — use a genuinely unknown name.
    with pytest.raises(PyramideError):
        PyramideRunner().run(
            _config(method="NOT_A_METHOD"),
            [Decimal("10"), Decimal("20"), Decimal("30")],
        )


def test_pyramide_runner_auto_select_chooses_candidate_model():
    result = PyramideRunner().run(
        _config(method="AUTO_SELECT", method_params={}),
        [Decimal("10"), Decimal("12"), Decimal("14"), Decimal("16"), Decimal("18"), Decimal("20")],
    )

    assert len(result.values) == 3
    assert result.selected_model
    assert result.engine_backend == "internal:auto_select"
    assert all(value.method in {"MA", "EXP_SMOOTHING", "CROSTON"} for value in result.values)


def test_pyramide_runner_external_backend_falls_back_without_optional_dependency():
    result = PyramideRunner().run(
        _config(method="FM_CHRONOS", method_params={}),
        [Decimal("10"), Decimal("12"), Decimal("14"), Decimal("16"), Decimal("18"), Decimal("20")],
    )

    assert result.engine_backend == "internal:auto_select"
    assert any("FM_CHRONOS" in warning for warning in result.warnings)


# Two full cycles of a season_length=4 profile: grand mean 100,
# hand-computed indices [2.0, 0.5, 1.0, 0.5], deseasonalized level 100
# (full derivation in tests/test_seasonal_forecaster_golden.py).
SEASONAL_HISTORY = [Decimal(v) for v in (200, 50, 100, 50, 200, 50, 100, 50)]


def test_pyramide_runner_seasonal_builds_a_curve():
    """SEASONAL emits a CURVE (level x indices), not a repeated value."""
    result = PyramideRunner().run(
        _config(method="SEASONAL", method_params={"season_length": 4}, horizon_days=6),
        SEASONAL_HISTORY,
    )

    assert [value.quantity for value in result.values] == [
        Decimal("200"), Decimal("50"), Decimal("100"), Decimal("50"), Decimal("200"), Decimal("50"),
    ]
    assert result.selected_model == "SEASONAL(season_length=4)"
    assert result.engine_backend == "internal:classical"
    assert all(value.method == "SEASONAL" for value in result.values)


def test_pyramide_runner_seasonal_falls_back_flat_below_two_cycles():
    """< 2 complete cycles: flat values + documented fallback in provenance."""
    result = PyramideRunner().run(
        _config(method="SEASONAL", method_params={"season_length": 4}, horizon_days=4),
        SEASONAL_HISTORY[:6],
    )

    quantities = [value.quantity for value in result.values]
    assert all(quantity == quantities[0] for quantity in quantities)
    assert any("fallback" in warning for warning in result.warnings)


def test_pyramide_runner_auto_select_seasonal_candidate_wins_on_seasonal_series():
    """On a strongly seasonal series (3 perfect cycles), the SEASONAL candidate
    backtests at zero error over the h-step curves and beats the flat models."""
    history = SEASONAL_HISTORY + SEASONAL_HISTORY[:4]  # 3 full cycles
    result = PyramideRunner().run(
        _config(method="AUTO_SELECT", method_params={"season_length": 4}, horizon_days=4),
        history,
    )

    assert result.selected_model == "SEASONAL(season_length=4)"
    assert result.engine_backend == "internal:auto_select"
    assert [value.quantity for value in result.values] == [
        Decimal("200"), Decimal("50"), Decimal("100"), Decimal("50"),
    ]


def test_pyramide_runner_auto_select_tolerates_non_numeric_season_length():
    """method_params is a free-form dict on the router side: a non-numeric
    season_length must never raise — the SEASONAL candidate is simply not
    proposed and the run completes on the flat models."""
    result = PyramideRunner().run(
        _config(method="AUTO_SELECT", method_params={"season_length": "n/a"}),
        [Decimal("10"), Decimal("12"), Decimal("14"), Decimal("16"), Decimal("18"), Decimal("20")],
    )

    assert not result.selected_model.startswith("SEASONAL")
    assert result.engine_backend == "internal:auto_select"


def test_pyramide_runner_ensemble_perfect_candidate_dominates():
    """A candidate with a PERFECT backtest score of 0.0 (falsy!) must get the
    capped weight 1/0.0001, not the neutral weight 1.0: on 3 perfect seasonal
    cycles the SEASONAL candidate dominates the blend and the ensemble curve
    lands (numerically) on the seasonal curve."""
    history = SEASONAL_HISTORY + SEASONAL_HISTORY[:4]  # 3 full cycles
    result = PyramideRunner().run(
        _config(method="ENSEMBLE_STAT", method_params={"season_length": 4}, horizon_days=4),
        history,
    )

    assert result.engine_backend == "internal:ensemble_stat"
    assert any("SEASONAL(season_length=4)" in warning for warning in result.warnings)
    expected = [Decimal("200"), Decimal("50"), Decimal("100"), Decimal("50")]
    for value, target in zip(result.values, expected):
        assert abs(value.quantity - target) < Decimal("1")


def test_pyramide_runner_auto_select_skips_seasonal_on_intermittent_series():
    """Intermittent demand never gets a SEASONAL candidate (indices would
    capture the random position of transactions): the forecast stays flat."""
    history = [Decimal(v) for v in (0, 0, 100, 0, 0, 0, 90, 0, 0, 0, 110, 0)]
    result = PyramideRunner().run(
        _config(method="AUTO_SELECT", method_params={"season_length": 4}, horizon_days=4),
        history,
    )

    assert not result.selected_model.startswith("SEASONAL")
    quantities = [value.quantity for value in result.values]
    assert all(quantity == quantities[0] for quantity in quantities)
