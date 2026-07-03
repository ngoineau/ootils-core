"""
Unit tests for the PURE mapping AccuracyReport -> pyramide_accuracy_metrics
rows (repository.accuracy_metric_rows, PR-D3). No DB: the round-trip and
the UNIQUE / re-persist semantics live in
tests/integration/test_pyramide_accuracy_metrics_integration.py.
"""
from __future__ import annotations

from decimal import Decimal

from ootils_core.pyramide.accuracy import AccuracyReport
from ootils_core.pyramide.repository import accuracy_metric_rows

D = Decimal


def _report(**overrides) -> AccuracyReport:
    base = dict(
        mase=D("0.8"),
        wape=D("0.15"),
        smape=D("0.2"),
        bias=D("-2.5"),
        coverage=None,
        per_horizon_residuals={
            1: (D("1"), D("3"), D("2")),
            2: (D("-4"), D("2")),
        },
        n_cutoffs=3,
        n_observations=5,
    )
    base.update(overrides)
    return AccuracyReport(**base)


def test_aggregate_row_first_carries_report_metrics_verbatim():
    rows = accuracy_metric_rows(_report())
    horizon, mase, wape, smape, bias, coverage, n_cutoffs, n_obs = rows[0]
    assert horizon is None
    assert (mase, wape, smape, bias) == (D("0.8"), D("0.15"), D("0.2"), D("-2.5"))
    assert coverage is None  # rolling-origin: no intervals evaluated
    assert (n_cutoffs, n_obs) == (3, 5)


def test_per_horizon_rows_bias_sign_flip_and_counts():
    # Residuals are actual - forecast; bias contract is forecast - actual
    # (positive = over-forecast) -> bias_h = -mean(residuals_h).
    rows = accuracy_metric_rows(_report())
    assert [row[0] for row in rows] == [None, 1, 2]

    h1 = rows[1]
    assert h1[4] == -(D("1") + D("3") + D("2")) / D("3")  # -mean = -2
    assert (h1[6], h1[7]) == (3, 3)  # one residual per cutoff reaching h

    h2 = rows[2]
    assert h2[4] == -(D("-4") + D("2")) / D("2")  # -mean = 1
    assert (h2[6], h2[7]) == (2, 2)

    # Metrics needing the per-horizon actuals stay None — the report only
    # carries residuals; nothing is invented.
    for row in rows[1:]:
        assert row[1] is None and row[2] is None and row[3] is None  # mase/wape/smape
        assert row[5] is None  # coverage


def test_none_metrics_pass_through_none_honest():
    rows = accuracy_metric_rows(_report(mase=None, wape=None))
    assert rows[0][1] is None and rows[0][2] is None


def test_bias_scale_transports_only_scale_dependent_metrics():
    # Middle-out leaf transport: leaf = share x node -> bias scales by the
    # share; mase/wape/smape are scale-free and pass through unchanged.
    rows = accuracy_metric_rows(_report(), bias_scale=D("0.25"))
    assert rows[0][4] == D("-2.5") * D("0.25")
    assert (rows[0][1], rows[0][2], rows[0][3]) == (D("0.8"), D("0.15"), D("0.2"))
    assert rows[1][4] == D("-2") * D("0.25")


def test_empty_residuals_yield_aggregate_row_only():
    rows = accuracy_metric_rows(_report(per_horizon_residuals={}))
    assert len(rows) == 1 and rows[0][0] is None
