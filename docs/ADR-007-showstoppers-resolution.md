# ADR-007: Conceptual Showstoppers Resolution

**Status:** Accepted  
**Date:** 2026-04-04  
**Author:** Architecture Review + Nicolas GOINEAU  
**Context:** Three showstoppers identified by independent conceptual review (REVIEW-conceptual-validation.md). All resolved before Sprint 1.

---

## S1 — Zone Boundary Migration Protocol

### Problem
As PLANNING_START advances daily, zone boundaries shift. A PO at day+91 today is in the weekly zone. In 2 days it is at day+89 — in the daily zone. Without an explicit migration protocol, PI node topology becomes inconsistent with zone policy.

### Resolution

**The calendar-boundary transition jobs already solve this — if execution order is guaranteed.**

Zone transitions fire at natural calendar boundaries (Monday for weekly→daily, 1st of month for monthly→weekly). Since PI weekly buckets have natural Monday→Sunday boundaries, no PO is ever stranded mid-week between two zones. When a week enters the daily zone, it enters as a complete week — the Monday job converts the entire week's bucket at once.

**The critical invariant: zone transition jobs must always complete before the propagation engine processes the day's first events.**

```python
async def daily_engine_startup(today: date):
    # Step 1: zone transitions — BLOCKING, must complete first
    await run_zone_transition_jobs(today)

    # Step 2: only then process pending events
    await process_pending_events()
```

This ordering guarantee eliminates the zone migration problem entirely. No additional migration mechanism is needed.

### Guarantees
- PI node topology is always consistent with zone policy at the start of each planning day
- No PO can be stranded in a wrong-grain PI bucket
- Zone transitions are bounded and predictable (calendar-driven, not data-driven)

---

## S2 — Who Populates PlannedSupply?

### Problem
`PlannedSupply` nodes exist in the graph model but nothing was specified to populate them. Without this, Ootils detects shortages but does not generate planned orders — making it an analysis engine, not a planning engine.

### Resolution

**Hybrid architecture: policy engine as baseline, AI agents as the primary planning layer.**

```
Level 1 — Policy Engine (automatic, in-engine)
  After each calc_run that generates a Shortage, the engine runs a
  policy_engine_pass. For each unresolved shortage, it consults the
  Policy node governing that item/location. If the policy has a simple
  replenishment rule (EOQ, min/max, DDMRP buffer), the engine creates
  a PlannedSupply automatically.
  → Deterministic, traceable, MRP-equivalent baseline behavior.

Level 2 — Agent Override (via API)
  An AI agent can create, modify, or cancel a PlannedSupply via:
  POST /events { type: "planned_supply_created", ... }
  Agent actions take precedence over policy engine output.
  Every agent action is recorded in the events table (full audit trail).

Level 3 — Human Override (via API)
  A planner can approve, modify, or reject any PlannedSupply.
  Approved orders transition to status='firmed' and are no longer
  modifiable by the policy engine.
```

### PlannedSupply attribution schema

```python
@dataclass
class PlannedSupply:
    id:            UUID
    scenario_id:   UUID
    item_id:       UUID
    location_id:   UUID
    supply_type:   str      # 'purchase' | 'manufacture' | 'transfer'
    qty:           Decimal
    due_date:      date
    status:        str      # 'proposed' | 'firmed' | 'released' | 'cancelled'

    # Attribution — who created this?
    source:        str      # 'policy_engine' | 'agent:{agent_id}' | 'planner:{user_id}'
    policy_ref:    UUID     # which Policy node governs this (if source=policy_engine)
    agent_run_ref: UUID     # which agent run created this (if source=agent:*)
    event_ref:     UUID     # the events table row that created/last modified this
```

### Architectural significance

This decision defines what Ootils IS:
- The policy engine layer makes Ootils a complete planning engine (not just analysis)
- The agent layer is the primary differentiator — AI agents replace the human planner for routine decisions
- The human override layer ensures planners retain final authority
- Full attribution on every PlannedSupply makes the system auditable and explainable at every level

---

## S3 — Cycle Detection

### Problem
The propagation engine assumes a DAG. `graphlib.TopologicalSorter` raises `CycleError` at computation time — not at data ingestion. A bad edge created by a user, agent, or data import can silently introduce a cycle, discovered only at the next calc_run in production.

### Resolution

**Two levels of protection: inline validation at edge creation + startup assertion.**

**Level 1 — Synchronous validation at every edge insert**

Before inserting any edge `(from_node_id → to_node_id)`, the engine checks whether a path already exists from `to_node_id` back to `from_node_id`. If yes, the insert is rejected with an explicit `CycleDetectedError`.

```python
def validate_no_cycle(from_id: UUID, to_id: UUID, scenario_id: UUID, db: Session):
    """
    DFS from to_id. If from_id is reachable, adding this edge creates a cycle.
    At PoA scale (< 50K nodes), inline DFS is fast enough.
    """
    visited = set()
    stack = [to_id]
    while stack:
        current = stack.pop()
        if current == from_id:
            raise CycleDetectedError(
                f"Edge {from_id} → {to_id} would create a cycle"
            )
        if current in visited:
            continue
        visited.add(current)
        neighbors = db.execute(
            "SELECT to_node_id FROM edges WHERE from_node_id = :id AND scenario_id = :sid",
            {"id": current, "sid": str(scenario_id)}
        ).scalars()
        stack.extend(neighbors)
```

**Level 2 — Startup assertion before any computation**

On engine startup, a full topological sort is attempted. If `CycleError` is raised, the engine refuses to start and logs the offending nodes.

```python
def engine_startup_checks(db: Session):
    sorter = graphlib.TopologicalSorter()
    for from_id, to_id in db.query(Edge.from_node_id, Edge.to_node_id).all():
        sorter.add(to_id, from_id)
    try:
        list(sorter.static_order())
    except graphlib.CycleError as e:
        raise EngineStartupError(f"Cycle detected in planning graph: {e}")
```

### Note: structural cycles vs. logical cycles

- **Structural cycle** (this ADR): an edge creates a graph cycle (A→B→C→A). This is a data integrity error. Prevented by inline validation + startup check.
- **Logical cycle** (separate concern): two valid edges create a planning contradiction (Transfer A→B + Transfer B→A for the same item/period). This is a business error. Detected during propagation, surfaced as a warning, requires planner resolution.

### Guarantees
- A structural cycle can never enter the graph silently
- The error is explicit, localized, and fires at the source (edge creation)
- The startup check is a safety net for direct DB imports or migration scripts
- At PoA scale, inline DFS latency is negligible (< 5ms for graphs < 50K nodes)

---

## Summary

| Showstopper | Resolution | Key mechanism |
|-------------|-----------|---------------|
| S1 — Zone boundary migration | Execution order guarantee: zone transitions before event processing | `daily_engine_startup()` ordering |
| S2 — PlannedSupply ownership | Hybrid: policy engine (baseline) + agent layer (primary) + human override | `PlannedSupply.source` attribution |
| S3 — Cycle detection | Inline DFS at edge insert + startup assertion | `validate_no_cycle()` + `engine_startup_checks()` |
