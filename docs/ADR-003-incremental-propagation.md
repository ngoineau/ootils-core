# ADR-003: Deterministic Incremental Propagation

**Status:** Accepted  
**Date:** 2026-03-29  
**Author:** Nicolas GOINEAU

---

## Context

Supply chain planning requires recalculating projected inventory, shortages, and allocations whenever something changes. The critical question: **how much do we recalculate?**

### The batch recalculation problem

Every existing APS system uses a variant of the same approach:
1. Collect all changes since the last run
2. Recalculate the entire plan (or a large pre-defined segment)
3. Produce a new snapshot
4. Repeat on schedule (nightly, weekly, or on-demand)

This approach has three fundamental problems:

**Problem 1: Latency**
Changes take hours or a full batch cycle to propagate. By the time a planner sees the impact of a supplier delay, the window for action may have passed.

**Problem 2: Over-computation**
A single PO date change triggers recalculation of millions of rows that are completely unaffected. This is wasteful and creates unnecessary load.

**Problem 3: Opacity**
When everything recalculates together, it becomes impossible to attribute changes to specific causes. "The plan changed overnight" — but why, and from what?

**Problem 4: AI agents can't operate on stale snapshots**
An agent taking a decision needs the current state, not the state from last night's batch run.

---

## Decision

**The engine uses deterministic incremental propagation: only the affected subgraph is recalculated, in topological order, triggered by explicit events.**

### Core algorithm

```
on_event(e):
  # 1. Identify affected subgraph
  affected_nodes = expand_subgraph(e.trigger_node)
  windows = compute_time_windows(affected_nodes, e)
  
  # 2. Mark dirty
  mark_dirty(affected_nodes, windows)
  
  # 3. Recalculate in topological order
  ordered = topological_sort(affected_nodes)
  
  for node in ordered:
    if is_dirty(node):
      old_value = snapshot(node)
      new_value = compute(node, windows[node])
      
      if changed(old_value, new_value):
        persist(node, new_value)
        propagate_dirty(node)  # push dirty flag downstream
      else:
        # No change — stop propagation on this branch
        clear_dirty(node)
```

### Subgraph expansion rules

Given a trigger node, the affected subgraph is determined by traversing edges:

| Trigger | Expands to |
|---------|-----------|
| `supply_date_changed` | All downstream `ProjectedInventory` + `Shortage` nodes in the affected time window |
| `demand_qty_changed` | Upstream `ProjectedInventory` that this demand consumes + downstream `Shortage` |
| `onhand_changed` | All `ProjectedInventory` for that item/location from the change date forward |
| `policy_changed` | All nodes governed by this policy |
| `structure_changed` | Full subgraph recompute for affected item/location |

### Time window computation

Propagation is bounded by a time window:

```
window_start = max(trigger_date - lookback, horizon_start)
window_end   = min(trigger_date + max_lead_time, horizon_end)
```

This prevents unnecessary propagation into the distant future when a near-term change is unlikely to have impact beyond a certain horizon.

### Determinism guarantee

The same set of inputs, applied in the same order, always produces the same outputs.

This is enforced by:
1. Topological sort — nodes are always computed in the same dependency order
2. No randomness anywhere in the core engine
3. Explicit event timestamps — event ordering is deterministic
4. Scenario isolation — changes in one scenario never bleed into another

---

## Calculation sequence (per item/location/window)

For each `(item_id, location_id, time_window)`:

1. **Load availability**
   - `on_hand(t)`
   - `incoming_supply(t)` — POs, WOs, Transfers due in window
   - `committed_demand(t)` — confirmed orders

2. **Net demand**
   - Apply forecast consumption rule: `net_forecast(t) = max(0, forecast(t) - confirmed_orders(t))`

3. **Project inventory**
   - `proj_inv(t) = proj_inv(t-1) + supply(t) - demand(t)`

4. **Allocate (priority order)**
   - Sort demands by priority, then due date
   - Consume available supply/inventory in order
   - Create `pegged_to` edges for audit

5. **Apply constraints**
   - Check capacity bounds
   - Check material constraints (quotas)
   - Apply MOQ/rounding rules

6. **Detect shortages**
   - `if proj_inv(t) < 0: create or update Shortage node`

7. **Generate explanation**
   - Build causal path: demand → supply → constraint → root cause

8. **Check delta**
   - If results unchanged from previous computation: stop propagation
   - If changed: persist + propagate dirty downstream

---

## Consequences

**Positive:**
- Near-real-time response to supply chain events (seconds, not hours)
- Minimal computation — only what actually changed is recalculated
- Full auditability — every change is traceable to a trigger event
- AI agents get current state on every query, not stale snapshots
- Propagation naturally stops at unaffected branches — no false positives

**Negative / Trade-offs:**
- More complex state management than batch recalculation
- Requires careful dirty-flag tracking and subgraph expansion
- Initial load (first computation) is still a full pass — incremental only applies to changes
- Concurrent events require careful ordering (event queue with timestamps)

**Mitigations:**
- Event queue ensures ordered processing
- Dirty flag system is isolated as a core engine component, heavily tested
- Full recompute is available as a fallback (e.g., after schema migration)

---

## Alternatives Considered

### Option A: Full batch recalculation (rejected)
Simple to implement. Used by all existing APS. Fails on latency, over-computation, and AI-native requirements.

### Option B: Change data capture (CDC) with streaming (deferred)
Use database CDC to trigger recalculation. Elegant but adds significant infrastructure complexity (Kafka/Kinesis). Appropriate for V2 when operating at scale.

### Option C: Reactive programming model (RxPy / ReactiveX) (considered)
Elegant for expressing data flow. But supply chain planning requires explicit control over computation order (topological sort), which doesn't map cleanly to reactive streams.

---

## References
- [Incremental execution of rule-based model transformation — Boronat (2020)](BIBLIOGRAPHY.md)
- [VIATRA framework — Varró et al. (2016)](BIBLIOGRAPHY.md)
- [Implicit incremental model analyses and transformations — Hinkel (2021)](BIBLIOGRAPHY.md)
