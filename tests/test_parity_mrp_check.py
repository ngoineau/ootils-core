"""
Unit tests for the CI drift guard of scripts/parity_mrp_engines.py (#332,
ADR-020 step 1). Pure-Python — no DB: covers check_thresholds() (the band
that gates CI) and the metrics contract returned by diff().

The band values mirror the documented pilot measurements (ADR-020
"Validation du fix de netting"): median residual 4.1% -> cap 5%,
netting-bug ratio ~48x / post-fix ~1.02x -> symmetric 1.5x band.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import parity_mrp_engines as parity  # noqa: E402

MAX_MEDIAN = 0.05
MAX_RATIO = 1.5


def _metrics(**overrides) -> dict:
    """Healthy post-fix baseline (pilot-like): median 4.1%, ratio ~1.02x."""
    m = {
        "common": 42,
        "only_b": 0,
        "total_a_common": 960_000.0,
        "total_b_common": 979_200.0,  # ratio 1.02
        "median_rel": 0.041,
        "max_rel": 0.30,
    }
    m.update(overrides)
    return m


def test_pilot_residual_passes():
    assert parity.check_thresholds(_metrics(), MAX_MEDIAN, MAX_RATIO) == []


def test_median_at_cap_passes_strictly_above_fails():
    assert parity.check_thresholds(_metrics(median_rel=0.05), MAX_MEDIAN, MAX_RATIO) == []
    failures = parity.check_thresholds(_metrics(median_rel=0.051), MAX_MEDIAN, MAX_RATIO)
    assert len(failures) == 1 and "median" in failures[0]


def test_netting_explosion_fails_ratio_guard():
    # The original bug: B over-plans ~48x (total B = 46.2M vs A = 0.96M).
    failures = parity.check_thresholds(
        _metrics(total_b_common=46_200_000.0, median_rel=0.96),
        MAX_MEDIAN, MAX_RATIO,
    )
    assert any("ratio" in f for f in failures)
    assert any("median" in f for f in failures)


def test_under_planning_fails_symmetric_band():
    # ratio 0.5 < 1/1.5 — a silent under-planner is just as wrong.
    failures = parity.check_thresholds(
        _metrics(total_b_common=480_000.0), MAX_MEDIAN, MAX_RATIO,
    )
    assert len(failures) == 1 and "ratio" in failures[0]


def test_no_common_items_is_unmeasurable_not_a_pass():
    failures = parity.check_thresholds(
        _metrics(common=0, median_rel=None, max_rel=None,
                 total_a_common=0.0, total_b_common=0.0),
        MAX_MEDIAN, MAX_RATIO,
    )
    assert len(failures) == 1 and "unmeasurable" in failures[0]


def test_degenerate_total_fails_instead_of_dividing():
    failures = parity.check_thresholds(
        _metrics(total_a_common=0.0), MAX_MEDIAN, MAX_RATIO,
    )
    assert any("degenerate" in f for f in failures)


def test_diff_returns_metrics_contract():
    a = {"item-1": 100.0, "item-2": 200.0, "item-3": 50.0}
    b = {"item-1": 102.0, "item-2": 200.0, "item-4": 10.0}
    m = parity.diff(a, b, sampled=True)
    assert m["common"] == 2
    assert m["only_b"] == 1
    assert m["total_a_common"] == 300.0
    assert m["total_b_common"] == 302.0
    # rels: item-1 -> 2/102, item-2 -> 0 ; median of the pair
    assert m["max_rel"] == 2.0 / 102.0
    assert m["median_rel"] == (0.0 + 2.0 / 102.0) / 2
    # the healthy tiny diff passes the CI band end-to-end
    assert parity.check_thresholds(m, MAX_MEDIAN, MAX_RATIO) == []


def test_diff_empty_common_yields_none_medians():
    m = parity.diff({"a": 1.0}, {"b": 2.0}, sampled=True)
    assert m["common"] == 0
    assert m["median_rel"] is None and m["max_rel"] is None
