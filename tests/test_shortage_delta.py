"""Unit tests for the counter-factual shortage-delta matching (levier 1a).

The fix keys shortages by their BUSINESS coordinate (item, location, date)
instead of the raw pi_node_id — which a fork deep-copy regenerates, making the
old id-difference over-report every list. Pure, no DB.
"""
from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from uuid import uuid4

from ootils_core.engine.kernel.shortage import match_shortage_delta
from ootils_core.engine.kernel.shortage.delta import shortage_key
from ootils_core.models import ShortageRecord

_ITEM_A = uuid4()
_ITEM_B = uuid4()
_LOC = uuid4()
_D1 = _dt.date(2026, 8, 1)
_D2 = _dt.date(2026, 8, 8)


def _sr(*, item=None, location=None, date=None, node_id=None) -> ShortageRecord:
    """A ShortageRecord with a FRESH pi_node_id by default (mimicking a fork's
    regenerated node ids) and the given business coordinates."""
    return ShortageRecord(
        shortage_id=uuid4(),
        scenario_id=uuid4(),
        pi_node_id=node_id or uuid4(),
        item_id=item,
        location_id=location,
        shortage_date=date,
        shortage_qty=Decimal("10"),
        severity_score=Decimal("100"),
        explanation_id=None,
        calc_run_id=uuid4(),
    )


def test_fork_identical_to_baseline_yields_no_new_no_resolved():
    """THE core fix: fork shortages carry FRESH pi_node_ids but the SAME
    (item, location, date) as baseline -> matched -> 0 new, 0 resolved. The old
    raw-node-id keying returned all-new + all-resolved here."""
    baseline = [_sr(item=_ITEM_A, location=_LOC, date=_D1),
                _sr(item=_ITEM_B, location=_LOC, date=_D2)]
    fork = [_sr(item=_ITEM_A, location=_LOC, date=_D1),
            _sr(item=_ITEM_B, location=_LOC, date=_D2)]
    # The trap: node ids are genuinely disjoint across the two scenarios.
    assert {s.pi_node_id for s in baseline}.isdisjoint({s.pi_node_id for s in fork})

    new, resolved = match_shortage_delta(baseline, fork)
    assert new == []
    assert resolved == []


def test_override_that_trips_a_new_shortage_is_reported_new_only():
    baseline = [_sr(item=_ITEM_A, location=_LOC, date=_D1)]
    fork = [_sr(item=_ITEM_A, location=_LOC, date=_D1),   # persisted
            _sr(item=_ITEM_B, location=_LOC, date=_D2)]   # NEW
    new, resolved = match_shortage_delta(baseline, fork)
    assert len(new) == 1 and new[0].item_id == _ITEM_B
    assert resolved == []


def test_override_that_resolves_a_baseline_shortage_is_reported_resolved_only():
    baseline = [_sr(item=_ITEM_A, location=_LOC, date=_D1),
                _sr(item=_ITEM_B, location=_LOC, date=_D2)]
    fork = [_sr(item=_ITEM_A, location=_LOC, date=_D1)]   # B resolved
    new, resolved = match_shortage_delta(baseline, fork)
    assert new == []
    assert len(resolved) == 1 and resolved[0].item_id == _ITEM_B


def test_net_change_invariant_preserved():
    """len(new) - len(resolved) must equal len(fork) - len(baseline) — the same
    count callers surface as net_shortage_change (the net was always correct)."""
    baseline = [_sr(item=_ITEM_A, location=_LOC, date=_D1)]
    fork = [_sr(item=_ITEM_A, location=_LOC, date=_D1),
            _sr(item=_ITEM_B, location=_LOC, date=_D2),
            _sr(item=_ITEM_B, location=_LOC, date=_D1)]
    new, resolved = match_shortage_delta(baseline, fork)
    assert len(new) - len(resolved) == len(fork) - len(baseline)


def test_none_date_does_not_collide_with_a_real_date():
    baseline = [_sr(item=_ITEM_A, location=_LOC, date=None)]
    fork = [_sr(item=_ITEM_A, location=_LOC, date=_D1)]   # different key
    new, resolved = match_shortage_delta(baseline, fork)
    assert len(new) == 1 and new[0].shortage_date == _D1
    assert len(resolved) == 1 and resolved[0].shortage_date is None


def test_none_item_and_location_match_when_equal():
    baseline = [_sr(item=None, location=None, date=_D1)]
    fork = [_sr(item=None, location=None, date=_D1)]
    new, resolved = match_shortage_delta(baseline, fork)
    assert new == [] and resolved == []


def test_multiset_collision_reports_only_the_surplus():
    """Same key K times in baseline, M times in fork -> min(K,M) matched, only
    the surplus reported (multiset, not overwrite)."""
    baseline = [_sr(item=_ITEM_A, location=_LOC, date=_D1),
                _sr(item=_ITEM_A, location=_LOC, date=_D1)]      # 2
    fork = [_sr(item=_ITEM_A, location=_LOC, date=_D1),
            _sr(item=_ITEM_A, location=_LOC, date=_D1),
            _sr(item=_ITEM_A, location=_LOC, date=_D1)]          # 3
    new, resolved = match_shortage_delta(baseline, fork)
    assert len(new) == 1        # surplus 3 - 2
    assert resolved == []


def test_empty_inputs():
    assert match_shortage_delta([], []) == ([], [])
    new, resolved = match_shortage_delta([], [_sr(item=_ITEM_A, location=_LOC, date=_D1)])
    assert len(new) == 1 and resolved == []


def test_shortage_key_ignores_node_id_and_separates_items():
    a = _sr(item=_ITEM_A, location=_LOC, date=_D1)
    b = _sr(item=_ITEM_A, location=_LOC, date=_D1, node_id=uuid4())
    assert shortage_key(a) == shortage_key(b)          # same coord, diff node id
    c = _sr(item=_ITEM_B, location=_LOC, date=_D1)
    assert shortage_key(a) != shortage_key(c)
