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
    with pytest.raises(PyramideError):
        PyramideRunner().run(
            _config(method="SEASONAL"),
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
