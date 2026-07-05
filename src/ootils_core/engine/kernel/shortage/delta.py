"""
delta.py — pure counter-factual shortage-delta matching (no DB).

Shared by the two fork->propagate->delta callers:
  - ``ootils_core.tools.agent_tools._fork_propagate_delta`` (the in-process
    /v1/simulate path used by the watcher fleet, #340/#347),
  - ``ootils_core.api.routers.simulate`` (the HTTP /v1/simulate endpoint).

Both compute "which shortages are NEW in the fork vs the baseline, and which
baseline shortages were RESOLVED". The naive implementation set-differenced by
raw ``pi_node_id``, but ``ScenarioManager.create_scenario`` deep-copies nodes
with FRESH ``node_id`` values (``gen_random_uuid``) while preserving the
business coordinates. A fork PI node therefore NEVER shares an id with its
baseline counterpart, so the raw-id difference reported 100% of the fork's
shortages as "new" and 100% of the baseline's as "resolved" the moment the
baseline had any — over-reporting both lists (the ``net_shortage_change``
count stayed correct).

The fix keys a shortage by its BUSINESS coordinate
``(item_id, location_id, shortage_date)`` — the same cross-scenario matching
principle ``ScenarioManager.diff`` / ``promote`` already use for nodes (they
key on ``(node_type, item_id, location_id, time_span_start, bucket_sequence)``;
for a ProjectedInventory shortage ``node_type`` is constant and
``shortage_date`` is the bucket's ``time_span_start``).

Collision policy — MULTISET, not overwrite. Within one scenario+calc_run,
``(item, location, shortage_date)`` is expected to be unique (projection
buckets of one series are contiguous ``[start, end)`` intervals, so two
buckets cannot share ``time_span_start``; the series itself is unique per
``(item, location, scenario)``). But a PI node with no time coordinate carries
``shortage_date = None``, and we refuse to silently drop or overwrite a
collision: keys are counted with multiplicity so that if K baseline and M fork
shortages share one key, ``min(K, M)`` are treated as unchanged and only the
surplus on each side is reported. This degenerates to plain set difference in
the unique-key case and stays correct under collisions.
"""
from __future__ import annotations

from collections import Counter
from typing import Optional

from ootils_core.models import ShortageRecord

# Sentinel for a None component of the business key. Distinct, non-UUID,
# non-date literals so a genuine None never collides with a real value's str().
_NULL_ITEM = "\x00item"
_NULL_LOCATION = "\x00location"
_NULL_DATE = "\x00date"

ShortageKey = tuple[str, str, str]


def shortage_key(shortage: ShortageRecord) -> ShortageKey:
    """Business coordinate of a shortage: (item_id, location_id, shortage_date)
    as stable strings. Each component may be None on the record (all three are
    Optional on ShortageRecord); a None maps to a private sentinel so it never
    collides with a real value and remains hashable/deterministic."""
    item = str(shortage.item_id) if shortage.item_id is not None else _NULL_ITEM
    location = (
        str(shortage.location_id) if shortage.location_id is not None else _NULL_LOCATION
    )
    date = (
        shortage.shortage_date.isoformat()
        if shortage.shortage_date is not None
        else _NULL_DATE
    )
    return (item, location, date)


def match_shortage_delta(
    baseline: list[ShortageRecord],
    scenario: list[ShortageRecord],
) -> tuple[list[ShortageRecord], list[ShortageRecord]]:
    """Compute (new_shortages, resolved_shortages) by business key.

    A scenario shortage is NEW when its ``(item, location, date)`` key has more
    occurrences in the scenario than in the baseline; a baseline shortage is
    RESOLVED when its key has more occurrences in the baseline than in the
    scenario. Matched pairs (same key present on both sides) are neither new nor
    resolved — the shortage persisted across the fork.

    Multiset semantics (see module docstring): with K baseline and M scenario
    shortages on one key, ``max(M - K, 0)`` scenario rows are new and
    ``max(K - M, 0)`` baseline rows are resolved. Returned lists preserve input
    order and never fabricate or drop a record.

    ``len(new) - len(resolved)`` equals ``len(scenario) - len(baseline)`` — the
    same count delta the callers already surface as ``net_shortage_change`` —
    so the fix cannot change the net count, only make the two LISTS honest.
    """
    baseline_counts = Counter(shortage_key(s) for s in baseline)
    scenario_counts = Counter(shortage_key(s) for s in scenario)

    # Number of scenario rows to report as NEW per key = surplus over baseline.
    new_budget: dict[ShortageKey, int] = {}
    for key, n in scenario_counts.items():
        surplus = n - baseline_counts.get(key, 0)
        if surplus > 0:
            new_budget[key] = surplus

    # Number of baseline rows to report as RESOLVED per key = surplus over scenario.
    resolved_budget: dict[ShortageKey, int] = {}
    for key, n in baseline_counts.items():
        surplus = n - scenario_counts.get(key, 0)
        if surplus > 0:
            resolved_budget[key] = surplus

    new_shortages = _take_by_budget(scenario, new_budget)
    resolved_shortages = _take_by_budget(baseline, resolved_budget)
    return new_shortages, resolved_shortages


def _take_by_budget(
    records: list[ShortageRecord],
    budget: dict[ShortageKey, int],
) -> list[ShortageRecord]:
    """Take the first ``budget[key]`` records for each key, in input order.

    Which specific record is picked among same-key duplicates is arbitrary (any
    of them carries that business coordinate); input order makes the choice
    deterministic. Records whose key is not in ``budget`` are skipped.
    """
    if not budget:
        return []
    remaining: dict[ShortageKey, int] = dict(budget)
    picked: list[ShortageRecord] = []
    for record in records:
        key = shortage_key(record)
        left: Optional[int] = remaining.get(key)
        if left:
            picked.append(record)
            remaining[key] = left - 1
    return picked
