"""
Golden tests for the pure confidence-score module (ADR-023) —
pyramide/confidence.py. No DB: every expected value is hand-computed
from the documented formula so the contract is locked, not mirrored.

Formula under test (components each in [0, 1], weighted sum, weights
normalized): accuracy = 1/(1+wape); depth = min(1, d/saturation);
freshness = 1 within SLA else sla/age (stale=True past SLA); missing
input -> prudent 0.25 default, traced. Default weights 0.5/0.25/0.25.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from ootils_core.pyramide.confidence import (
    DEFAULT_SLA_DAYS,
    MISSING_COMPONENT_DEFAULT,
    ConfidenceScore,
    compute_confidence,
)


def test_golden_all_signals_known():
    # accuracy = 1/(1+0.25) = 0.8 ; depth = 365/365 = 1 ; freshness = 1
    # score = 0.5*0.8 + 0.25*1 + 0.25*1 = 0.9
    result = compute_confidence(Decimal("0.25"), 365, 3)
    assert result.score == Decimal("0.9000")
    assert result.components["accuracy"] == Decimal("0.8000")
    assert result.components["depth"] == Decimal("1.0000")
    assert result.components["freshness"] == Decimal("1.0000")
    assert result.stale is False
    assert "wape=0.25" in result.explanation


def test_golden_stale_decay_past_sla():
    # wape=1 -> accuracy 0.5 ; depth 73/365 = 0.2 ; age 14 > SLA 7 ->
    # freshness 7/14 = 0.5, stale.
    # score = 0.5*0.5 + 0.25*0.2 + 0.25*0.5 = 0.425
    result = compute_confidence(Decimal("1"), 73, 14, sla_days=7)
    assert result.score == Decimal("0.4250")
    assert result.components["freshness"] == Decimal("0.5000")
    assert result.stale is True
    assert "STALE" in result.explanation


def test_age_exactly_at_sla_is_fresh():
    result = compute_confidence(Decimal("0"), 365, DEFAULT_SLA_DAYS)
    assert result.stale is False
    assert result.components["freshness"] == Decimal("1")


def test_all_missing_components_use_prudent_default():
    # Nothing is known -> every component 0.25 -> score 0.25 (weights
    # sum to 1). Never an optimistic 1.0; the trace names each default.
    result = compute_confidence(None, None, None)
    assert result.score == Decimal("0.2500")
    assert all(
        component == MISSING_COMPONENT_DEFAULT
        for component in result.components.values()
    )
    # An UNKNOWN freshness never invents a stale flag (proof-only).
    assert result.stale is False
    assert result.explanation.count("prudent default") >= 3


def test_single_missing_component_is_traced_not_invented():
    # accuracy missing only: 0.5*0.25 + 0.25*1 + 0.25*1 = 0.625
    result = compute_confidence(None, 365, 0)
    assert result.score == Decimal("0.6250")
    assert result.components["accuracy"] == MISSING_COMPONENT_DEFAULT
    assert "accuracy unknown" in result.explanation


def test_depth_saturation_is_a_parameter():
    # depth 100 with saturation 100 -> full credit; with the default 365
    # the same depth is partial. No business constant baked in.
    saturated = compute_confidence(Decimal("0"), 100, 0, depth_saturation_days=100)
    assert saturated.components["depth"] == Decimal("1.0000")
    partial = compute_confidence(Decimal("0"), 100, 0)
    assert partial.components["depth"] == Decimal("0.2740")  # 100/365 quantized


def test_custom_weights_are_normalized():
    # weights 1/1/2 normalize to 0.25/0.25/0.5:
    # score = 0.25*0.5 + 0.25*1 + 0.5*1 = 0.875 (wape=1, full depth, fresh)
    result = compute_confidence(
        Decimal("1"), 365, 0,
        weights={
            "accuracy": Decimal("1"),
            "depth": Decimal("1"),
            "freshness": Decimal("2"),
        },
    )
    assert result.score == Decimal("0.8750")


def test_negative_ingest_age_clamped_to_zero_not_crash():
    # Sub-day clock skew between app and DB server: clamp, stay fresh.
    result = compute_confidence(Decimal("0"), 365, -1)
    assert result.components["freshness"] == Decimal("1")
    assert result.stale is False


def test_determinism_same_inputs_same_output():
    a = compute_confidence(Decimal("0.37"), 210, 11, sla_days=5)
    b = compute_confidence(Decimal("0.37"), 210, 11, sla_days=5)
    assert isinstance(a, ConfidenceScore)
    assert a.score == b.score
    assert dict(a.components) == dict(b.components)
    assert a.stale == b.stale
    assert a.explanation == b.explanation


def test_score_reproducible_by_hand_from_trace():
    # Explainability contract: score == sum(component * weight) for the
    # default weights, re-derived from the returned trace itself.
    result = compute_confidence(Decimal("0.42"), 123, 9, sla_days=7)
    recomposed = (
        result.components["accuracy"] * Decimal("0.5")
        + result.components["depth"] * Decimal("0.25")
        + result.components["freshness"] * Decimal("0.25")
    ).quantize(Decimal("0.0001"))
    assert result.score == recomposed


@pytest.mark.parametrize(
    "kwargs",
    [
        {"sla_days": 0},
        {"depth_saturation_days": 0},
        {"weights": {"accuracy": Decimal("1")}},  # missing keys
        {"weights": {"accuracy": Decimal("-1"), "depth": Decimal("1"), "freshness": Decimal("1")}},
        {"weights": {"accuracy": Decimal("0"), "depth": Decimal("0"), "freshness": Decimal("0")}},
    ],
)
def test_invalid_parameters_fail_loudly(kwargs):
    with pytest.raises(ValueError):
        compute_confidence(Decimal("0.2"), 30, 1, **kwargs)


def test_negative_wape_and_depth_are_caller_bugs():
    with pytest.raises(ValueError):
        compute_confidence(Decimal("-0.1"), 30, 1)
    with pytest.raises(ValueError):
        compute_confidence(Decimal("0.1"), -1, 1)
