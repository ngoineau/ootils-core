"""
Pyramide axis B — PR-B2 unit tests: Chronos-2 wrapper WITHOUT weights.

Everything here runs in the plain unit environment (no [foundation]
extra, no HuggingFace download, no DB):

- ``foundation.forecast_batch`` is exercised through DEPENDENCY
  INJECTION (a fake pipeline object) — no library mock: the function
  takes the pipeline as a parameter by design.
- The engine's FM path uses the injectable ``foundation_loader`` for
  the same reason; the fallback tests inject a loader that raises, so
  they are deterministic whether or not chronos happens to be installed.
- The single test that REQUIRES the real weights is at the bottom,
  marked ``foundation`` + importorskip: it only runs in the dedicated
  CI job (and self-skips everywhere else).
"""
from __future__ import annotations

import importlib.util
from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest

from ootils_core.pyramide import PyramideRunConfig, PyramideRunner
from ootils_core.pyramide.engines import (
    PyramideEngineError,
    PyramideForecastEngine,
)
from ootils_core.pyramide.foundation import (
    DEFAULT_MODEL_ID,
    FoundationUnavailable,
    LoadedPipeline,
    forecast_batch,
    load_pipeline,
)
from ootils_core.pyramide.hierarchy.reconcile import LeafShare, middle_out
from ootils_core.pyramide.hierarchy.runner import (
    _leaf_routing_warnings,
    _routed_node_method,
)
from ootils_core.pyramide.hierarchy.summing import (
    AGGREGATE,
    LEAF,
    SeriesRef,
    SummingBlock,
)
from ootils_core.pyramide.models import METHOD_FM_CHRONOS
from ootils_core.pyramide.routing import METHOD_TWIN, RoutingDecision

CHRONOS_INSTALLED = importlib.util.find_spec("chronos") is not None

HISTORY = [Decimal(v) for v in (10, 12, 14, 16, 18, 20)]


class FakePipeline:
    """Minimal predict_quantiles double — injected, not a library mock.

    Median at step t for series i = last value of series i + t + 1
    (deterministic, distinguishable per series). Records every call so
    tests can assert BATCH inference (one call for N series).
    """

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def predict_quantiles(self, context, prediction_length, quantile_levels):
        self.calls.append(
            {
                "batch_size": len(context),
                "prediction_length": prediction_length,
                "quantile_levels": list(quantile_levels),
            }
        )
        quantiles = []
        for series in context:
            values = [float(v) for v in series]
            quantiles.append(
                [
                    [values[-1] + step + 1 for _ in quantile_levels]
                    for step in range(prediction_length)
                ]
            )
        mean = [[row[0] for row in series_q] for series_q in quantiles]
        return quantiles, mean


def _fake_loaded(pipeline=None) -> LoadedPipeline:
    return LoadedPipeline(
        pipeline=pipeline or FakePipeline(),
        model_id="fake/model",
        revision="abc123def",
        revision_source="hf_commit_sha",
    )


# ---------------------------------------------------------------------------
# foundation.load_pipeline — clean unavailability
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    CHRONOS_INSTALLED, reason="chronos installed: the real backend would load"
)
def test_load_pipeline_raises_foundation_unavailable_without_backend():
    with pytest.raises(FoundationUnavailable) as exc_info:
        load_pipeline()
    assert "ootils-core[foundation]" in str(exc_info.value)


def test_default_model_id_is_chronos2():
    # The license decision of spec §2.B (Apache-2.0; Moirai stays excluded).
    assert DEFAULT_MODEL_ID == "amazon/chronos-2"


# ---------------------------------------------------------------------------
# foundation.forecast_batch — injected pipeline (pure)
# ---------------------------------------------------------------------------


def test_forecast_batch_is_one_call_for_many_series():
    pipeline = FakePipeline()
    curves = forecast_batch(
        pipeline,
        [[1.0, 2.0, 3.0], [10.0, 20.0], [5.0]],
        4,
        seed=42,
    )
    # ONE backend call for the whole batch — never series-by-series.
    assert len(pipeline.calls) == 1
    assert pipeline.calls[0]["batch_size"] == 3
    assert pipeline.calls[0]["prediction_length"] == 4
    # Median (quantile 0.5) only: the point forecast.
    assert pipeline.calls[0]["quantile_levels"] == [0.5]
    # Caller order preserved; fake median = last value + step + 1.
    assert curves == [
        [4.0, 5.0, 6.0, 7.0],
        [21.0, 22.0, 23.0, 24.0],
        [6.0, 7.0, 8.0, 9.0],
    ]


def test_forecast_batch_empty_batch_is_empty():
    pipeline = FakePipeline()
    assert forecast_batch(pipeline, [], 4, seed=0) == []
    assert pipeline.calls == []


def test_forecast_batch_rejects_bad_inputs():
    pipeline = FakePipeline()
    with pytest.raises(ValueError):
        forecast_batch(pipeline, [[1.0]], 0, seed=0)
    with pytest.raises(ValueError):
        forecast_batch(pipeline, [[1.0], []], 4, seed=0)


def test_forecast_batch_wraps_backend_failure():
    class BrokenPipeline:
        def predict_quantiles(self, *args, **kwargs):
            raise RuntimeError("boom")

    with pytest.raises(FoundationUnavailable):
        forecast_batch(BrokenPipeline(), [[1.0, 2.0]], 2, seed=0)


# ---------------------------------------------------------------------------
# Engine FM path — injected loader (pure)
# ---------------------------------------------------------------------------


def _engine_kwargs(**overrides):
    kwargs = dict(
        periods=3,
        method_params={},
        model_strategy="fm",
        granularity="daily",
        random_seed=7,
    )
    kwargs.update(overrides)
    return kwargs


def test_engine_batches_fm_series_with_provenance_seal():
    pipeline = FakePipeline()
    engine = PyramideForecastEngine(
        foundation_loader=lambda params: _fake_loaded(pipeline)
    )
    results = engine.forecast_foundation_batch(
        series=[("node_a", HISTORY), ("node_b", [Decimal("5"), Decimal("6")])],
        **_engine_kwargs(),
    )
    assert len(pipeline.calls) == 1  # one inference call for the run
    assert pipeline.calls[0]["batch_size"] == 2
    for computation in results.values():
        assert computation.selected_model == "FM_CHRONOS(fake/model@abc123def)"
        assert computation.value_method == METHOD_FM_CHRONOS
        assert computation.engine_backend == "chronos:hf_commit_sha"
        assert computation.model_revision == "abc123def"
        # No deterministic backtest for a zero-shot FM: the conformal
        # layer must publish NULL bounds, never the model's quantiles.
        assert computation.accuracy_report is None
    assert results["node_a"].values == (
        Decimal("21"), Decimal("22"), Decimal("23"),
    )
    assert results["node_b"].values == (
        Decimal("7"), Decimal("8"), Decimal("9"),
    )


def test_engine_fm_curves_are_clamped_at_zero():
    class NegativePipeline(FakePipeline):
        def predict_quantiles(self, context, prediction_length, quantile_levels):
            super().predict_quantiles(context, prediction_length, quantile_levels)
            quantiles = [
                [[-5.0 for _ in quantile_levels] for _ in range(prediction_length)]
                for _ in context
            ]
            return quantiles, None

    engine = PyramideForecastEngine(
        foundation_loader=lambda params: _fake_loaded(NegativePipeline())
    )
    results = engine.forecast_foundation_batch(
        series=[("n", HISTORY)], **_engine_kwargs()
    )
    assert results["n"].values == (Decimal("0"),) * 3


def test_engine_single_series_fm_goes_through_the_batch_path():
    pipeline = FakePipeline()
    engine = PyramideForecastEngine(
        foundation_loader=lambda params: _fake_loaded(pipeline)
    )
    computation = engine.forecast(
        history=HISTORY,
        periods=2,
        method=METHOD_FM_CHRONOS,
        method_params={},
        model_strategy="fm",
        granularity="daily",
        horizon_start=date(2026, 7, 1),
        random_seed=0,
    )
    assert len(pipeline.calls) == 1
    assert computation.selected_model == "FM_CHRONOS(fake/model@abc123def)"
    assert computation.model_revision == "abc123def"
    assert computation.values == (Decimal("21"), Decimal("22"))


def test_engine_rejects_duplicate_batch_keys():
    engine = PyramideForecastEngine(
        foundation_loader=lambda params: _fake_loaded()
    )
    with pytest.raises(PyramideEngineError):
        engine.forecast_foundation_batch(
            series=[("dup", HISTORY), ("dup", HISTORY)], **_engine_kwargs()
        )


# ---------------------------------------------------------------------------
# Fallback golden — deterministic whether or not chronos is installed
# ---------------------------------------------------------------------------


def _unavailable_loader(params):
    raise FoundationUnavailable("weights not reachable (test)")


def test_fm_fallback_serves_auto_select_values_unchanged():
    """Golden contract: FM unavailable => EXACTLY the AUTO_SELECT result
    (deterministic fallback preserved), with the cause in the warnings
    and NO model_revision (no weights ran)."""
    engine = PyramideForecastEngine(foundation_loader=_unavailable_loader)
    common = dict(
        history=HISTORY,
        periods=3,
        method_params={},
        model_strategy="stat",
        granularity="daily",
        horizon_start=date(2026, 7, 1),
        random_seed=0,
    )
    fm = engine.forecast(method=METHOD_FM_CHRONOS, **common)
    auto = engine.forecast(method="AUTO_SELECT", **common)

    assert fm.values == auto.values
    assert fm.engine_backend == "internal:auto_select"
    assert fm.model_revision is None
    assert fm.warnings[0].startswith("FM_CHRONOS backend unavailable:")
    assert any(
        "fell back to AUTO_SELECT" in warning for warning in fm.warnings
    )


def test_fm_strict_backend_fails_loudly_when_unavailable():
    engine = PyramideForecastEngine(foundation_loader=_unavailable_loader)
    with pytest.raises(PyramideEngineError) as exc_info:
        engine.forecast(
            history=HISTORY,
            periods=3,
            method=METHOD_FM_CHRONOS,
            method_params={"strict_backend": True},
            model_strategy="fm",
            granularity="daily",
            horizon_start=date(2026, 7, 1),
            random_seed=0,
        )
    assert "FM_CHRONOS failed" in str(exc_info.value)


def test_non_fm_run_has_no_model_revision():
    """Provenance: model_revision stays None (=> DB NULL) for non-FM."""
    result = PyramideRunner().run(
        PyramideRunConfig(
            item_id=UUID("10000000-0000-0000-0000-000000000001"),
            location_id=UUID("20000000-0000-0000-0000-000000000001"),
            scenario_id=UUID("00000000-0000-0000-0000-000000000001"),
            horizon_start=date(2026, 7, 1),
            horizon_days=3,
            method="MA",
            method_params={"window": 2},
        ),
        [Decimal("10"), Decimal("20"), Decimal("30")],
    )
    assert result.model_revision is None


# ---------------------------------------------------------------------------
# Routed execution helpers (hierarchy runner) — pure
# ---------------------------------------------------------------------------


def _decision(method: str, level: str = "leaf") -> RoutingDecision:
    return RoutingDecision(method=method, level=level, reason="test reason")


class TestRoutedNodeMethod:
    def test_no_decision_keeps_the_run_method(self):
        assert _routed_node_method(None, "node", "AUTO_SELECT") == "AUTO_SELECT"

    def test_executable_routed_method_wins(self):
        decision = _decision("CROSTON", level="leaf")
        assert _routed_node_method(decision, "node", "AUTO_SELECT") == "CROSTON"

    def test_fm_routes_to_the_batch(self):
        decision = _decision(METHOD_FM_CHRONOS, level="aggregate")
        assert (
            _routed_node_method(decision, "node", "AUTO_SELECT")
            == METHOD_FM_CHRONOS
        )

    def test_twin_at_a_node_fails_loudly(self):
        from ootils_core.pyramide.runner import PyramideError

        with pytest.raises(PyramideError):
            _routed_node_method(_decision(METHOD_TWIN), "node", "AUTO_SELECT")

    def test_unknown_routed_method_fails_loudly(self):
        from ootils_core.pyramide.runner import PyramideError

        with pytest.raises(PyramideError):
            _routed_node_method(_decision("TSB"), "node", "AUTO_SELECT")


class TestLeafRoutingWarnings:
    LEAVES = frozenset({"leaf-1", "leaf-2"})
    NODES = frozenset({"NODE-A"})

    def _warnings(self, decisions, shares):
        return _leaf_routing_warnings(
            decisions,
            leaf_keys=self.LEAVES,
            recon_node_keys=self.NODES,
            shares=shares,
            block_code="BLOCK",
        )

    def test_twin_on_a_cold_start_leaf_is_silently_executed(self):
        shares = (
            LeafShare(
                leaf="leaf-1", recon_node="NODE-A",
                weight=Decimal("5"), share=Decimal("0.5"), cold_start=True,
            ),
        )
        assert self._warnings({"leaf-1": _decision(METHOD_TWIN)}, shares) == []

    def test_twin_on_a_leaf_with_history_warns(self):
        shares = (
            LeafShare(
                leaf="leaf-1", recon_node="NODE-A",
                weight=Decimal("9"), share=Decimal("0.9"), cold_start=False,
            ),
        )
        warnings = self._warnings({"leaf-1": _decision(METHOD_TWIN)}, shares)
        assert len(warnings) == 1
        assert "not applied" in warnings[0]

    def test_twin_without_explicit_share_warns(self):
        warnings = self._warnings({"leaf-1": _decision(METHOD_TWIN)}, ())
        assert len(warnings) == 1
        assert "MinT" in warnings[0]

    def test_non_twin_leaf_decision_is_provenance_only(self):
        warnings = self._warnings(
            {"leaf-2": _decision(METHOD_FM_CHRONOS)}, ()
        )
        assert len(warnings) == 1
        assert "provenance-only" in warnings[0]

    def test_recon_node_decision_is_not_rewarned(self):
        assert self._warnings(
            {"NODE-A": _decision("AUTO_SELECT", level="aggregate")}, ()
        ) == []

    def test_unmatched_key_warns(self):
        warnings = self._warnings(
            {"OTHER-LEVEL-NODE": _decision("AUTO_SELECT", "aggregate")}, ()
        )
        assert len(warnings) == 1
        assert "provenance only" in warnings[0]


# ---------------------------------------------------------------------------
# TWIN executor golden (pure): the routed twin IS the reconciler's shares
# ---------------------------------------------------------------------------


def test_twin_routing_executes_via_the_existing_twin_shares():
    """Golden: a TWIN-routed cold-start leaf is served the MEAN of its
    positive-history siblings' weights (reconcile._node_weights) — the
    routed twin-transfer reuses the disaggregation shares, no separate
    engine (B2 mapping documented in hierarchy/runner.py)."""
    block = SummingBlock(
        hierarchy_id="h1",
        block_code="ROOT",
        block_level="root",
        series=(
            SeriesRef(kind=AGGREGATE, key="ROOT", level="root"),
            SeriesRef(kind=LEAF, key="twin-a", leaf_code="ROOT"),
            SeriesRef(kind=LEAF, key="twin-b", leaf_code="ROOT"),
            SeriesRef(kind=LEAF, key="cold", leaf_code="ROOT"),
        ),
        leaves=("twin-a", "twin-b", "cold"),
        rows=((0, 1, 2), (0,), (1,), (2,)),
    )
    recon = middle_out(
        block,
        "root",
        {"ROOT": (Decimal("100"), Decimal("200"))},
        {"twin-a": Decimal("30"), "twin-b": Decimal("10")},  # cold: no history
    )
    shares = {ls.leaf: ls for ls in recon.shares}
    # Twin rule: cold weight = mean(30, 10) = 20 -> share 20/60 = 1/3.
    assert shares["cold"].cold_start is True
    assert shares["cold"].weight == Decimal("20")
    cold_curve = recon.values[3]
    third = Decimal("20") / Decimal("60")
    assert cold_curve == (Decimal("100") * third, Decimal("200") * third)
    # And the routing layer stays SILENT for it: decision executed.
    assert (
        _leaf_routing_warnings(
            {"cold": _decision(METHOD_TWIN)},
            leaf_keys=frozenset(block.leaves),
            recon_node_keys=frozenset({"ROOT"}),
            shares=recon.shares,
            block_code=block.block_code,
        )
        == []
    )


# ---------------------------------------------------------------------------
# REAL weights (dedicated CI job only) — marker 'foundation'
# ---------------------------------------------------------------------------


@pytest.mark.foundation
def test_chronos2_real_batch_shape_and_seeded_determinism():
    """Requires the [foundation] extra + downloadable/pre-downloaded
    weights. Auto-skips when chronos is not importable (unit CI). The
    dedicated job runs: python -m pytest -m foundation -q"""
    pytest.importorskip("chronos")
    loaded = load_pipeline()
    histories = [[float(v) for v in range(1, 13)], [3.0, 5.0, 7.0, 9.0]]
    first = forecast_batch(loaded.pipeline, histories, 4, seed=42)
    second = forecast_batch(loaded.pipeline, histories, 4, seed=42)

    assert len(first) == 2
    assert all(len(curve) == 4 for curve in first)
    assert all(isinstance(v, float) for curve in first for v in curve)
    # Best-effort seeded determinism: same process, same device, same
    # seed => same medians (GPU/BLAS caveats in foundation.py docstring).
    assert first == second
    # The seal must exist and never be fabricated.
    assert loaded.revision
    assert loaded.revision_source in {
        "hf_commit_sha", "requested_revision", "package_version",
    }
