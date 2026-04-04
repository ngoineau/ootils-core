# Engine Execution Model — Foundation Proposal
**Ootils V1 Proof-of-Architecture: 100 SKUs × 5 Locations × 5–6 Scenarios**

*Status: Proposal for review*  
*Author: Architecture review — April 2026*

---

## Framing

The ADRs establish the what: graph-native, incremental, explainable, scenario-lightweight, time-elastic.  
This document establishes the **how** for the first executable proof: the execution model that makes all five properties work together correctly at PoA scale (100 SKUs × 5 locations × 5–6 scenarios).

The goal is not to build a production system. It is to **prove the architectural thesis holds under real supply chain logic** — shortages propagate correctly, explanations are causal, scenarios are cheap, and an AI agent can drive the whole thing via API.

---

## 1. Recommended Execution / Propagation Model

### 1.1 Core recommendation: Layered Push-Pull with Topological Ordering

The engine should implement a **two-phase, event-driven propagation model**:

```
Phase 1 — Event ingestion & dirty marking (Push)
Phase 2 — Demand-driven recomputation in topological order (Pull)
```

This is the right model because:
- Pure push (reactive) systems lose control over computation order, making determinism hard to guarantee
- Pure pull (on-demand) systems recompute too much when asked for aggregate views
- The combination gives determinism (topo order) + efficiency (only dirty nodes) + latency (event-triggered)

### 1.2 Node computation graph topology

At PoA scale the DAG is shallow and predictable. The natural layers are:

```
Layer 0 (Sources):    Item, Location, Supplier, Policy
Layer 1 (Raw events): OnHandSupply, PurchaseOrderSupply, WorkOrderSupply,
                      TransferSupply, ForecastDemand, CustomerOrderDemand
Layer 2 (Bridge):     TemporalBridge outputs (daily-bucketed net demand per item/loc)
Layer 3 (Core calc):  ProjectedInventory timeline per (item, location, scenario)
Layer 4 (Results):    Shortage nodes, pegged_to allocation edges
Layer 5 (Explain):    Explanation records (causal path, root cause ref)
Layer 6 (Impacts):    impacts edges → downstream demand notifications
```

Computation always flows L0 → L6. Dirty marking flows in both directions:
- **Forward** (L1 → L6): a PO date change dirtifies its downstream ProjectedInventory nodes
- **Backward** (not recompute — just invalidation): a demand quantity change dirtifies the allocation layer

### 1.3 Event queue

All mutations enter through a single ordered event queue:

```python
@dataclass
class PlanningEvent:
    event_id: str          # uuid
    event_type: str        # "supply_date_changed", "demand_qty_changed", etc.
    trigger_node_id: str
    scenario_id: str
    effective_date: date
    payload: dict
    created_at: datetime
```

At PoA scale (100 SKUs × 5 locations = 500 item/loc combos max), the event queue is an **in-process priority queue** (Python `heapq`, keyed by `created_at`). No Kafka, no Redis — those are V2 concerns. One process, one queue, deterministic processing order.

### 1.4 Dirty flag mechanics

Each `(node_id, scenario_id, time_window)` triple carries a dirty bit. The dirty set is a Python `dict[tuple, bool]`:

```python
dirty: dict[tuple[str, str, date, date], bool]
# key: (node_id, scenario_id, window_start, window_end)
```

When a node is recomputed and the result **does not change** (delta check), propagation stops on that branch. This is the critical efficiency invariant — it converts worst-case full-graph recalculation into best-case O(changed subgraph).

### 1.5 Topological sort

At PoA scale the DAG has at most ~3,000 nodes (500 item/loc × 6 scenarios across the 5 supply/result node types that need ordering). A single `graphlib.TopologicalSorter` (Python stdlib, 3.9+) is sufficient — no external graph library needed.

The sort is computed once at graph initialization and updated incrementally only when structure changes (new node/edge added). At PoA scale, structure changes are rare.

**Critical implementation note:** Sort across layers, not just within item/loc. A TransferSupply at DC-East that feeds ProjectedInventory at DC-West must sequence the source location's projection *before* the destination's — otherwise you get a stale read. This is the most common correctness bug in supply chain propagation engines.

### 1.6 Calculation sequence per (item, location, scenario, window)

```
1. Load net supply timeline:
   on_hand(t0) + Σ confirmed_supply(t) for t in window

2. Load net demand timeline (via TemporalBridge):
   net_demand(t) = max(0, disaggregated_forecast(t) - confirmed_orders(t))

3. Run forward inventory projection:
   proj_inv(t) = proj_inv(t-1) + supply(t) - net_demand(t)
   Starting condition: proj_inv(t0-1) = on_hand

4. Run allocation pass (priority-ordered):
   Sort demands by (priority ASC, due_date ASC)
   For each demand, consume available supply greedily
   Create/update pegged_to edges with allocated qty

5. Detect shortages:
   For each t where proj_inv(t) < 0:
     shortage_qty = -proj_inv(t)
     Create/update Shortage node

6. Generate explanation inline (see Section 2)

7. Delta check:
   If Shortage(t) unchanged AND proj_inv(t) unchanged → stop propagation
   Else → mark downstream nodes dirty
```

---

## 2. Shortage Detection and Explanation Computation

### 2.1 Shortage detection — the right semantics

A shortage in Ootils is not a negative inventory balance. It is a **named, attributed, causally-linked event** with:
- An identity (`shortage_id` — stable across recalculations for the same event)
- A quantity, date range, and severity
- A link to the causal explanation
- Links to impacted demand nodes (via `impacts` edges)

**Stable identity rule:** A shortage on (PUMP-01, DC-ATL, Apr 8–18) gets the same `shortage_id` if it recomputes to the same or different magnitude. Updates overwrite; the ID survives. This lets agents track shortages across scenario comparisons without identity loss.

```python
shortage_id = f"shortage-{item_id}-{location_id}-{need_date}-{scenario_id}"
```

### 2.2 Shortage severity scoring

At PoA scale, three-dimensional severity is tractable:

```
severity = shortage_qty × unit_margin × days_at_risk × priority_weight
```

Where:
- `unit_margin` = item-level margin or unit cost proxy
- `days_at_risk` = length of shortage window (when does next supply arrive?)
- `priority_weight` = max priority of impacted demands (customer A vs. B vs. C)

This makes shortages **sortable and comparable across scenarios**, which is what the AI agent needs to rank its recommendations.

### 2.3 Explanation computation — inline, causal, typed

Explanations are built **during the allocation pass**, not post-hoc. The causal path is assembled step by step as the engine processes each demand:

```python
def build_explanation(demand, supply_state, policy_state) -> Explanation:
    path = []

    # Step 1: what demand triggered this?
    path.append(CausalStep(
        node_id=demand.id,
        node_type=demand.type,
        edge_type="consumes",
        fact=f"{demand.type} {demand.id} requires {demand.qty}u due {demand.due_date}"
    ))

    # Step 2: what supply was available?
    for supply in supply_state.consumed:
        path.append(CausalStep(
            node_id=supply.id,
            node_type=supply.type,
            edge_type="replenishes",
            fact=f"{supply.id} provides {supply.allocated_qty}u (of {supply.total_qty}u)"
        ))

    # Step 3: why is supply insufficient?
    if supply_state.is_delayed:
        root = supply_state.blocking_supply
        path.append(CausalStep(
            node_id=root.id,
            node_type=root.type,
            edge_type="depends_on",
            fact=f"{root.id} delayed from {root.original_date} to {root.current_date} (+{root.delay_days}d)"
        ))

    # Step 4: policy check
    if not policy_state.has_substitute:
        path.append(CausalStep(
            node_id=None,
            node_type="PolicyCheck",
            edge_type="governed_by",
            fact="No active substitution rule at this item/location"
        ))

    return Explanation(
        target_node_id=shortage.id,
        root_cause_node_id=path[-1].node_id,  # deepest causal node
        causal_path=path,
        summary=_summarize(path),
    )
```

**Root cause classification** — at PoA scale, enumerate the five root cause archetypes explicitly:

| Root Cause Class | Detection Rule |
|-----------------|----------------|
| `supply_delay` | Blocking supply has current_date > original_date |
| `supply_gap` | No supply exists in the shortage window at all |
| `demand_spike` | Demand qty increased vs. prior computation |
| `allocation_conflict` | Higher-priority demand consumed supply needed by this demand |
| `capacity_bound` | Supply is bounded by a CapacityBucket or MaterialConstraint |

Tagging each explanation with its root cause class makes agent reasoning dramatically simpler: "find all `supply_delay` shortages in the next 30 days" is a clean filter, not a graph traversal.

---

## 3. Scenario Representation

### 3.1 The delta model — copy-on-write, not copy-everything

The ADRs are right: copying the full dataset per scenario is prohibitively expensive even at PoA scale. At 100 SKUs × 5 locations × 6 scenarios × 90 days = 270,000 daily inventory positions — manageable, but the pattern must be right from the start.

**Recommended model: layered delta overlay**

```
Baseline scenario → full graph, fully computed
Scenario S2      → delta set Δ(S2) over baseline
Scenario S3      → delta set Δ(S3) over baseline
```

A delta is a small `dict` of node overrides:

```python
@dataclass
class ScenarioDelta:
    scenario_id: str
    parent_scenario_id: str   # "baseline" or another scenario_id
    overrides: dict[str, NodeOverride]  # node_id → {field: new_value}
    created_at: datetime
    label: str
```

At PoA scale, a typical scenario delta is 1–20 node overrides (e.g., "PO-991 delayed by 8 days" = 1 override; "demand +20% across all 100 SKUs" = 100 overrides).

### 3.2 Scenario resolution — transparent to the engine

The engine never sees raw scenarios. It sees a **resolved view** for a given `(node_id, scenario_id)`:

```python
def resolve_node(node_id: str, scenario_id: str) -> NodeData:
    delta = scenario_store.get_delta(scenario_id)
    if node_id in delta.overrides:
        return apply_override(baseline_node(node_id), delta.overrides[node_id])
    return baseline_node(node_id)
```

This indirection costs one dict lookup per node access — negligible at PoA scale, correct by construction.

### 3.3 Scenario computation scope

When a scenario is created or its delta changes, **only the affected subgraph is recomputed**:

```python
def recompute_scenario(scenario_id: str):
    delta = scenario_store.get_delta(scenario_id)
    affected_nodes = set()
    for node_id in delta.overrides:
        affected_nodes |= expand_subgraph(node_id, scenario_id)
    
    ordered = topo_sort(affected_nodes)
    for node in ordered:
        compute(node, scenario_id)
```

At PoA scale with 5–6 scenarios and typical deltas of 1–20 overrides, total scenario computation is O(100s of nodes) per scenario per event — completing in milliseconds, not minutes.

### 3.4 Scenario comparison

The primary agent use case is ranking scenarios:

```python
def compare_scenarios(baseline_id: str, scenario_ids: list[str]) -> ScenarioComparison:
    return ScenarioComparison(
        baseline=summarize(baseline_id),
        alternatives=[
            ScenarioSummary(
                scenario_id=s,
                delta_shortages=shortage_diff(baseline_id, s),
                delta_inventory_risk=inventory_risk_diff(baseline_id, s),
                delta_cost=cost_diff(baseline_id, s),
            )
            for s in scenario_ids
        ]
    )
```

This comparison is a pure read over already-computed results. At PoA scale it's a table scan over ~600 shortage records per scenario — microseconds.

---

## 4. Correctness Risks and Invariants

### 4.1 The five correctness invariants — must hold at all times

**Invariant 1: Topological ordering is respected**  
No node is computed before all its upstream dependencies (in the same scenario) are current.  
*Violation symptom:* stale reads, where a downstream node uses an old value from a node that was already updated in this propagation pass.  
*Enforcement:* topological sort must include cross-location transfer edges. Test: insert a deliberate delay in a transfer node and verify the destination inventory reflects it before the source completes.

**Invariant 2: Scenario isolation**  
A mutation in scenario S never modifies the baseline or any other scenario's nodes.  
*Violation symptom:* scenario bleed — a "what-if demand spike" in S2 appears in baseline results.  
*Enforcement:* all writes are namespaced by `scenario_id`. The delta store must enforce immutability of the baseline record.  
*Test:* modify S2, assert baseline inventory unchanged, assert S3 unchanged.

**Invariant 3: Explanation consistency**  
Every Shortage node has exactly one Explanation. Every Explanation's causal path terminates at a real node or a classified root cause.  
*Violation symptom:* dangling explanation references; explanations that blame a node that no longer exists.  
*Enforcement:* create/update Shortage and Explanation in the same transaction. FK constraint in SQLite enforces existence.

**Invariant 4: Delta termination**  
Propagation always terminates (no cycles in the DAG).  
*Violation symptom:* infinite recomputation loop.  
*Enforcement:* the graph must be a DAG — validated at graph construction time. Cycles are a schema error, not an engine error. Assert no cycles after every structural change.

**Invariant 5: Determinism**  
Given the same initial state and the same sequence of events, the engine always produces the same result.  
*Violation symptom:* flaky tests; results differ between runs.  
*Enforcement:* no randomness, no `datetime.now()` inside core calculation functions (pass timestamps as arguments), deterministic sort order for equal-priority demands (use `demand_id` as tiebreaker), deterministic iteration over dicts (Python 3.7+ guarantees insertion order — use it).

### 4.2 High-risk zones

**Risk 1: Forecast consumption race condition**  
`net_demand(t) = max(0, forecast(t) - confirmed_orders(t))` must be computed *before* the allocation pass reads it. If a new CustomerOrder arrives mid-propagation, it must either wait for the current pass to finish or trigger a new event. At PoA scale: single-threaded processing eliminates this entirely.

**Risk 2: Transfer loop**  
A transfer from Location A to B and a separate transfer from B to A in the same planning horizon creates a logical (not structural) cycle that inflates projected inventory at both ends. Detection: flag any `(from_loc, to_loc, item, window)` pair that has a corresponding reverse transfer, emit a warning, and force manual resolution.

**Risk 3: Allocation priority instability**  
If two demands have identical `(priority, due_date)`, the allocation result is non-deterministic without a stable tiebreaker. Enforce: always sort by `(priority ASC, due_date ASC, demand_id ASC)`. The `demand_id` tiebreaker must be stable across recalculations (UUIDs are fine; auto-increment integers are not if nodes are deleted and reinserted).

**Risk 4: Stale on-hand snapshot**  
`OnHandSupply` is a snapshot as of `as_of_date`. If the engine recomputes a past window using a stale on-hand value, it produces wrong results. Enforce: prohibit projections before `as_of_date`. The planning horizon always starts at `max(today, as_of_date)`.

**Risk 5: Scenario delta coherence**  
Overriding a PO's date without also updating its `replenishes` edge's `effective_start` creates an inconsistent state: the node says April 18 but the edge says April 10. Enforce: node overrides must include edge attribute updates. The delta schema should express overrides as node+edge pairs, not just node fields.

---

## 5. Smallest Architecture That Proves the Thesis

### 5.1 The minimal execution stack

At PoA scale (100 SKUs, 5 locations, 5–6 scenarios, 90-day horizon), the engine needs:

| Component | Implementation | Rationale |
|-----------|---------------|-----------|
| Graph store | SQLite (single file) | ACID, zero infra, sufficient at PoA scale, swap to Postgres in V2 |
| Graph traversal | Python `graphlib.TopologicalSorter` | Stdlib, no deps, handles PoA scale trivially |
| Event queue | Python `heapq` in-process | Single-threaded, ordered, zero infra |
| Dirty set | Python `dict` in-memory | Fits entirely in RAM at PoA scale (< 10KB) |
| Temporal bridge | Pure Python functions | No pandas needed; daily disaggregation for 90 days × 500 combos = 45K rows, trivial |
| Scenario store | Python `dict` of deltas | 5–6 scenarios × 20 overrides max = ~120 override records in RAM |
| Explanation store | SQLite table | Append-only, queryable, no special tech |
| API layer | FastAPI | Single file, async, OpenAPI autodoc, AI agent compatible |
| Test harness | pytest | Already present in repo |

**Total external dependencies added for the core engine:** zero beyond what already exists.  
**Lines of code estimate:** ~800–1,200 lines for the graph engine, not counting the existing policy/supplier code.

### 5.2 File structure for the PoA engine

```
src/ootils_core/
  engine/
    graph/
      __init__.py
      store.py          # SQLite-backed node/edge persistence
      traversal.py      # TopologicalSorter wrapper + subgraph expansion
      dirty.py          # Dirty flag set management
    temporal/
      bridge.py         # Disaggregation, aggregation, forecast consumption
    scenarios/
      store.py          # Delta store + resolution
    calc/
      projection.py     # Forward inventory projection (core loop)
      allocation.py     # Priority-ordered demand allocation
      shortage.py       # Shortage detection + severity scoring
      explanation.py    # Causal path builder (inline)
    propagator.py       # Main engine: event → dirty → topo → compute
  api/
    routes.py           # FastAPI routes: /events, /projection, /issues, /explain, /simulate
    schemas.py          # Pydantic models for API I/O
```

The existing `engine/decision_engine.py` and `engine/policies.py` are **retained as-is**. They handle per-SKU replenishment policy calculations (EOQ, ROP, safety stock) and slot naturally into Layer 3 of the new graph engine — `projection.py` calls them for planned supply quantity suggestions.

### 5.3 The PoA demonstration loop

The minimal proof is a single executable scenario that an AI agent can drive:

```
1. Load baseline:
   - 100 SKUs, 5 locations
   - On-hand inventory per (item, loc)
   - Open POs, WOs, customer orders (synthetic but realistic)
   - Demand forecast (monthly, disaggregated to daily)

2. Full initial computation:
   - Run forward projection for all (item, loc) pairs
   - Detect shortages
   - Generate explanations
   - Persist all results

3. Agent query loop:
   GET /issues?severity=high&horizon_days=30
   → returns top-N shortages with severity scores

   GET /explain?node_id=shortage-PUMP01-DCATL-20260415
   → returns causal path, root cause class, impacted demands

4. Agent scenario comparison:
   POST /simulate { scenario: "expedite PO-991 by 8 days" }
   → creates delta, recomputes affected subgraph
   → returns shortage diff vs baseline

   POST /simulate { scenario: "demand +20% next 30 days" }
   → creates delta, recomputes affected subgraph
   → returns shortage diff vs baseline

5. Agent recommendation:
   Based on /issues + /explain + /simulate results:
   → "Expedite PO-991. Resolves 3 shortages (2.1M severity score). Cost: +$8,000 air freight."

6. Human review → approve → POST /events { type: "supply_date_changed", ... }
   → propagation runs, results update in <2s
```

This loop is the proof. It demonstrates:
- Graph-native propagation (Step 2, 4)
- Incremental updates (Step 6)
- Explainability (Step 3)
- Lightweight scenarios (Step 4 — delta only, not full copy)
- AI agent API (Step 3, 4, 5)
- Determinism (run Step 2 twice, get identical results)

### 5.4 What to skip in PoA (and why)

| Feature | Skip because |
|---------|-------------|
| Multi-echelon BOM explosion | Adds DependentDemand complexity; not needed to prove core thesis |
| Capacity constraints | CapacityBucket/uses_capacity adds capacity netting; save for V2 |
| Substitution rules | substitutes_for edge defined in schema but logic deferred to V2 |
| WebSocket streaming | REST polling is sufficient for agent loop at PoA scale |
| Auth | Basic API key is enough; production auth is V2 |
| Multi-threaded event processing | Single-threaded eliminates race conditions; add concurrency in V2 when needed |
| Dolt/version control for scenarios | Python dict is sufficient at 5–6 scenarios |

The right question for each skipped feature is: "Does omitting this prevent me from proving the thesis?" For all of the above, the answer is no.

---

## Sequencing Recommendation

Given the existing codebase and the M1–M7 milestone structure in ROADMAP.md, here is the recommended sequencing for the new engine components:

**Sprint 1 (M2 foundation):**  
`graph/store.py` + `graph/traversal.py` + `graph/dirty.py` + `calc/projection.py`  
Goal: forward projection for a single (item, location) pair in the baseline scenario, with dirty propagation on PO date change.  
Test: change PO date → verify downstream ProjectedInventory recalculates correctly.

**Sprint 2 (M2 completion + M4):**  
`temporal/bridge.py` + `calc/allocation.py` + `calc/shortage.py`  
Goal: full baseline computation across all 100 SKUs × 5 locations. Shortage detection working.  
Test: introduce a deliberate supply gap → verify Shortage created with correct qty and dates.

**Sprint 3 (M3):**  
`calc/explanation.py`  
Goal: every Shortage has a causal explanation. Root cause classified.  
Test: each of the five root cause archetypes produces a correctly typed explanation.

**Sprint 4 (M5):**  
`scenarios/store.py` + delta resolution in `propagator.py`  
Goal: create a scenario delta → recompute affected subgraph only → compare with baseline.  
Test: scenario with 1 override triggers recomputation of exactly the affected nodes (verify with propagation trace).

**Sprint 5 (M6 + M7):**  
`api/routes.py` + agent demo  
Goal: all endpoints working. Agent drives the PoA demonstration loop from Step 3 above.

---

## Summary

| Question | Answer |
|----------|--------|
| Execution model | Layered push-pull: event queue → dirty marking → topological sort → forward projection |
| Propagation unit | `(node_id, scenario_id, time_window)` triple with delta termination on no-change |
| Shortage semantics | Named, stable-identity, severity-scored, causally-linked events — not negative balances |
| Explanation approach | Inline causal path assembly during allocation pass; root cause classification into 5 archetypes |
| Scenario model | Layered delta overlays over baseline; copy-on-write; O(delta) recomputation |
| Critical invariants | Topo ordering, scenario isolation, explanation consistency, DAG (no cycles), determinism |
| PoA stack | SQLite + Python stdlib + FastAPI; zero new heavy dependencies |
| What to skip | BOM explosion, capacity netting, substitution, streaming, concurrent event processing |
| Proof milestone | Agent query loop: issues → explain → simulate → recommend → approve → propagate |
