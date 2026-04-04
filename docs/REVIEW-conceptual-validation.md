# Ootils Planning Engine — Conceptual Architecture Validation

**Reviewer:** Supply Chain Planning Systems Architect (independent expert review)
**Date:** 2026-04-04
**Scope:** Conceptual architecture review — not code review, not performance benchmark
**Stance:** Adversarial. No diplomacy.

---

## Executive Summary

Ootils is architecturally coherent and differentiated in several important ways. The graph-native, incremental, event-sourced thesis is sound and maps well onto how supply chain planning actually works. The elastic time model (ADR-002d) is the most innovative piece and also the most dangerous. The dirty-flag propagation is correct in principle but will face concrete failure modes at scale. The scenario model is good. The PoA scope is responsible.

**Verdict: CONDITIONAL YES** — this is a credible, buildable, differentiated engine. Three specific risks need to be addressed before any production deployment claim is defensible.

---

## 1. Conceptual Soundness

### What's correct

The core thesis is coherent. Supply chain planning **is** a graph problem. Every major APS internally operates on a network of supply/demand relationships, even if the UI hides it. Making the graph explicit as a first-class data model — not a hidden implementation detail — is architecturally correct and creates real value for explainability and AI agent traversal.

The node taxonomy is well-chosen. Nodes map cleanly to business objects a planner actually thinks about. Edges (`consumes`, `replenishes`, `pegged_to`, `governed_by`) are semantically meaningful and cover the primary planning relationships. This is not an academic graph — it's grounded in how planners reason.

Event-sourcing is the right persistence model for a planning engine. Planning decisions are temporal. You need to know what the state was at t, not just what it is now. An insert-only events table is the correct implementation of this principle.

The 2-layer Python architecture with a clean kernel interface is pragmatic. The Rust replacement path is credible if you maintain interface discipline. The kernel boundary needs to be explicit and contract-tested from day one — not as an afterthought when Python becomes the bottleneck.

### What's missing or underdefined

**"AI-native" is asserted, not architected.** The architecture document describes a graph that AI agents can traverse, but does not specify:
- How agents access the graph (read API? graph query language?)
- How agents propose changes (events? direct writes?)
- How agent actions are attributed, audited, and reversible
- What prevents an agent from creating an inconsistent state

AI-native means the system is designed for autonomous agent operation, not just accessible to agents. The current design is *agent-accessible*, which is a lower bar. This is not a showstopper for PoA, but it needs to be addressed before claiming "AI-native."

**Explanation nodes are conceptually correct but mechanically undefined.** An `Explanation` node in the graph is a genuinely good idea. But: what populates it? When? How is an explanation scoped to a specific calculation run vs. a stable artifact? If a planner changes a policy, do old explanations become stale, or do they represent the historical reasoning? The lifecycle of Explanation nodes is unspecified and will cause confusion in production.

**The `governed_by` edge semantics are underspecified.** A Policy node connected to many nodes via `governed_by` creates a fan-out propagation risk. Change one lead time policy for 500 SKUs and you've dirtied 500+ subgraphs simultaneously. The architecture needs to address this: batch processing? Priority queuing? Or is this acceptable at PoA scale?

---

## 2. Elastic Time Model (ADR-002d)

### What's brilliant

The core principle — that time granularity is a property of the node type, not a global calendar policy — is genuinely innovative and correct. Every major APS I have worked with treats time buckets as a global setting. This forces planners to choose: daily (accurate near-term, massive data volume) or weekly/monthly (manageable data, terrible near-term resolution). This is a false trade-off that Ootils correctly refuses to accept.

The zone transition at natural calendar boundaries (Monday for weekly→daily, 1st of month for monthly→weekly) is the right call. Fractional-week transitions are a source of endless reconciliation bugs. Every vendor who has tried mid-week zone transitions has regretted it.

PLANNING_START = max(today, as_of_date) is correct. The engine should never re-plan the past. Simple, explicit, defensible.

The `node_type_policies` table making zone breakpoints configurable per node type is excellent. This allows tuning without architectural change.

### What's risky

**Point-in-bucket contribution is the right default but breaks in specific real-world cases.** Consider:

1. A CustomerOrder created on day 85 (daily zone) with a requested delivery date on day 95 (still daily zone) — contribution is clear. Fine.
2. A CustomerOrder with requested delivery date on day 150 (monthly zone). The order contributes to the monthly bucket containing day 150. But what if that monthly bucket also contains capacity constraints that were computed assuming uniform distribution? The sub-bucket shortage flag partially addresses this, but the architecture assumes the planner understands that day 150 order visibility is month-level only. In practice, planners will ask "what day in that month?" and the answer is "we don't know" — which is a correct answer that will feel wrong to them.
3. **The real edge case:** A CustomerOrder arrives for day 92 (weekly zone). A new CustomerOrder arrives for day 91 (also weekly zone, same bucket). The PI node for that weekly bucket must be recomputed. But day 91 is 1 day away from the daily zone boundary (day 90). If the zone boundary shifts (because today advances to tomorrow), day 91 **moves into the daily zone** and now needs a daily PI node, not the weekly one it previously contributed to. This is the zone boundary migration problem. The architecture needs an explicit migration protocol for orders/supply records near zone transition points. "Natural calendar boundaries" partially addresses this, but the PLANNING_START advancing daily is the trigger that moves the zone boundaries forward, and this creates a daily cascade of migrations at the boundary.

**Forecast consumption logic (net_demand = max(0, forecast - confirmed_orders)) at bucket grain has a well-known pathology:** It works correctly when a CustomerOrder and a ForecastDemand node are in the same grain. When a daily CustomerOrder contributes to a monthly forecast bucket, the consumption logic at monthly grain is:
- Monthly forecast: 1000 units
- CustomerOrders in month: 3 orders totaling 400 units
- Net demand: 600 units forecast + 400 confirmed = 1000 total demand

This is correct. But if the planner has separate daily visibility requirements and the planning engine disaggregates the 600 net forecast demand as FLAT across 20 working days = 30 units/day, this means a planner looking at day 5 sees 30 units demand from forecast and 0 from orders, when in reality the orders might cluster on day 5 and the net forecast should be zero for that day. The architecture correctly marks disaggregated values as `approximated=true` — this is the right call, but the presentation layer must aggressively surface this approximation flag to planners, or they will make decisions on bad data and blame the system.

**Disaggregation is marked as presentation-only.** Good. But the TemporalBridge must never be in any path that influences planned orders or alerts. If it is ever called during propagation (even read-only), it creates implicit dependencies that break the incremental propagation determinism. This must be enforced architecturally, not by convention.

**Horizon Extension running on 1st of month + startup + API on-demand** — this is the right approach, but horizon extension generates new PI nodes, which triggers propagation on the new nodes. On the 1st of the month, this means a full recompute of the newly-created monthly bucket for every (item, location) pair. At 10K pairs, this is 10K new PI nodes, all dirty simultaneously. The architecture should treat Horizon Extension as a special bulk operation, not as N individual dirty-flag events.

---

## 3. Incremental Propagation

### What's correct

Dirty-flag + topological sort is the standard-of-the-art approach for incremental computation in DAG-structured systems. React, Excel, Apache Spark's lineage tracking — all variants of this pattern. For a supply chain planning DAG, it is correct.

The authoritative state in `dirty_nodes` table + fast-read `is_dirty` cache is a reasonable dual-layer approach. The in-memory Python set during propagation is the right performance optimization. Batch flush at level boundaries (not per-node) is correct.

Crash recovery via calc_runs state machine is necessary and the design is sound. The `pending → running → failed → pending` cycle is the right model.

Per-node clearing immediately after successful recompute is correct — do not clear in bulk at the end of a run, because a mid-run crash would lose the record of what was already computed.

Delta check stopping propagation when result unchanged is critical and correctly included. Without it, a policy change that doesn't actually affect inventory for a specific SKU-location would still propagate through the entire downstream graph.

### Known failure modes

**Cycle detection is not mentioned.** The architecture describes a DAG and relies on topological sort. But supply chain networks are not always DAGs. Co-products, by-products, circular material flows (rare but real), and certain multi-echelon substitution patterns can create cycles. The propagation engine must detect and reject cycles with explicit error messages, not silently loop. If this is not implemented, a bad data entry will cause the engine to hang or stack-overflow.

**The fan-out problem.** A single Policy node change can dirty thousands of downstream nodes. The topological sort will queue all of them. With the in-memory Python set, this means potentially millions of dirty node IDs in memory simultaneously. At PoA scale (1.15M PI nodes × scenarios), the in-memory set becomes a concern. The design needs an explicit memory budget and a fallback to paginated processing when the dirty set exceeds it.

**Concurrent scenario propagation.** The architecture is silent on whether multiple scenarios can be computed in parallel. If scenario A and scenario B both depend on the baseline, and the baseline is being recomputed, what is the locking model? The `stale` lifecycle state addresses detection, but not concurrent execution. If scenario propagation is single-threaded (one at a time), this must be stated explicitly and the queue management must prevent starvation.

**The `is_dirty` boolean cache coherence.** If the `dirty_nodes` table is the authoritative source and `is_dirty` on the node is a cache, there is a window where they disagree. The design says dirty marking happens in the same DB transaction as event insert — good. But what about cache invalidation? If the Python process is killed mid-propagation and restarted, the `dirty_nodes` table is authoritative (correct), but the in-memory state is gone (expected). The restart recovery must rebuild the in-memory state from `dirty_nodes` before resuming propagation. Is this explicitly handled? It should be.

**Topological sort stability.** The topological sort must be deterministic across runs. If two nodes at the same level can be processed in any order, and processing order affects numerical results (e.g., due to rounding or tie-breaking), then identical inputs can produce different outputs on restart. This violates the stated determinism property. The sort key must include node_id as a tiebreaker.

---

## 4. Scenario Model

### What's correct

Delta overlays are the right approach. Full-copy scenarios are a disaster that every vendor who implemented them regrets. At 10K SKU-locations, a full copy doubles your storage and compute budget per scenario. The `scenario_overrides` table approach is correct.

The baseline snapshot ID for stale detection is conceptually right. When the baseline is recomputed, variant scenarios that were computed against an older baseline are stale. This is the correct semantic.

The scenario lifecycle (draft → computing → computed → stale → approved → archived) covers the necessary states. `stale` is particularly important — it must be surfaced prominently to planners.

Merge via `scenario_merge` event processed by the normal propagation engine is elegant. It reuses existing machinery and makes merges auditable. This is a good design choice.

### What's risky

**Stale detection by snapshot ID comparison only catches baseline recomputes.** What about changes to reference data (items, locations, suppliers) that aren't represented in the baseline computation? If a supplier lead time is changed and the baseline is recomputed, all variants correctly become stale. But what if a new item is added and the baseline expands? The snapshot ID mechanism handles this only if baseline recompute always increments the snapshot ID — which it should, but this must be explicit.

**Branch depth cap of 2 is pragmatically correct for PoA** but will be the first constraint real planners hit. "What if I branch off this scenario to explore two sub-options?" is a natural planning workflow. The architecture should not present this as an architectural decision — it's a PoA constraint. Be honest about it.

**Merge conflict resolution is unaddressed.** If scenario A modifies lead time for item X, and scenario B modifies safety stock for item X, merging A and B has no conflicts. But if both modify lead time for item X differently, what happens? The `scenario_merge` event presumably takes one or the other, but the semantics are undefined. This is not a showstopper but it needs to be specified before any real use.

**Delta overlay reads have a fan-out read problem.** When computing a variant scenario, every node access must check: does this scenario have an override for this node? This is a `scenario_overrides` table lookup per node access. At 1.15M PI nodes × 5 scenarios, the read amplification is significant. The design needs a clear answer: are overrides cached in memory during propagation? Or is this acceptable at PoA scale and deferred to production hardening?

---

## 5. Comparison with Existing Systems

### vs. Kinaxis RapidResponse

RapidResponse's workbook model uses a proprietary columnar in-memory store (RR's "working memory") with a concurrent multi-user scenario model. Its key innovation is concurrent read/write with scenario isolation via copy-on-write at the column level.

**Ootils advantages over RapidResponse:**
- Explicit graph model makes pegging and explanation first-class, not a report feature
- Event sourcing creates a native audit trail that RR lacks without add-ons
- Open stack (Postgres + Python) vs. RR's black-box proprietary engine
- The elastic time model is more sophisticated than RR's global time bucket setting

**Ootils gaps vs. RapidResponse:**
- RR has 20+ years of production-hardened edge cases in supply chain math. Ootils has zero.
- RR's in-memory columnar store is dramatically faster for large-scale simultaneous scenario analysis. PostgreSQL is not a columnar store.
- RR has native multi-user concurrent planning support. Ootils design does not address multi-user write conflicts.
- RR has built-in optimization solvers (constrained ordering, capacity leveling). Ootils has PlannedSupply nodes but no visible solver infrastructure.

### vs. SAP IBP

SAP IBP uses a HANA in-memory key figure model. Planning is essentially a multi-dimensional pivot on time × product × location × key figure. It is fast for aggregated views but fundamentally not a graph engine.

**Ootils advantages over SAP IBP:**
- IBP's key figure model makes pegging and traceability difficult (it's a famous customer complaint). Ootils' graph model solves this natively.
- IBP requires heavy configuration to express supply chain constraints as key figure relationships. Ootils models them as graph edges, which is more natural.
- IBP's time series orientation makes incremental computation awkward. IBP generally recomputes all key figures on demand. Ootils' dirty-flag model is more efficient.

**Ootils gaps vs. SAP IBP:**
- IBP has native integration with S/4HANA. Ootils has no integration story at all.
- IBP has demand sensing, demand shaping, and ML-based forecasting built in. Ootils has a ForecastDemand node but no forecasting capability.
- IBP's HANA backend handles 100M+ SKU-location combinations. Ootils' PoA scope is 500 SKU-locations.

### Net assessment

Ootils is positioned correctly: not a RapidResponse replacement, not an IBP replacement, but a ground-up rethink of what a planning engine looks like when designed for explainability, AI autonomy, and developer-first architecture. The differentiation is real. The gap in production hardening is also real and should not be minimized.

---

## 6. Critical Gaps and Risks

### Showstopper candidates

**1. Zone boundary migration (HIGH RISK)**
The elastic time model creates daily shifts in zone boundaries as PLANNING_START advances. A PO at day 91 today is in the weekly zone. Tomorrow it's in the daily zone. The migration protocol for this daily cascade is not described. Without it, the PI node topology changes daily and the dirty-flag system must handle topology changes gracefully. This is non-trivial. If not handled, the engine will either produce wrong results silently or require a full recompute every day (defeating the incremental propagation entirely).

**2. No optimization solver (STRUCTURAL GAP)**
Ootils computes projected inventory given inputs. It does not optimize. PlannedSupply nodes exist, but what fills them? A separate solver? A policy-driven rule? An AI agent? Traditional APS systems have explicit planning algorithms (MRP, DDMRP, constrained optimization). Without this, Ootils is an *analysis engine*, not a *planning engine*. This is the most dangerous architectural gap. It may be intentional (AI agents provide the optimization layer) but if so, this must be stated explicitly.

**3. Multi-user write conflicts (UNADDRESSED)**
The design assumes events are serialized. In a real planning environment, multiple planners may simultaneously modify different parts of the graph. The event queue serializes them, which is correct, but: what if two planners modify the same node concurrently? Optimistic locking? Last-write-wins? This is not mentioned. For PoA (single user presumably), this is fine. For any multi-user deployment, this is a showstopper.

### High-risk assumptions

**Python performance at 1.15M nodes.** The kernel is in Python with a clean interface for Rust replacement. But "clean interface" is easier said than done. If the interface boundary is not established under performance testing before the Rust port is needed, the refactor cost will be much higher than anticipated. Python's GIL and memory overhead are material constraints for in-memory graph traversal at million-node scale.

**PostgreSQL as the event queue.** Using Postgres as an event queue is a well-known pattern (Transactional Outbox). It works at moderate event rates. At high event rates (automated AI agent actions, bulk imports, real-time integrations), Postgres table bloat and vacuum overhead become operational burdens. This is manageable but must be planned for.

**The DAG assumption.** As noted above — supply chain networks can have cycles. If the data model doesn't enforce DAG structure, a bad user input can break the propagation engine.

---

## 7. Verdict

**CONDITIONAL YES.**

This is a credible, buildable, differentiated supply chain planning engine. The architectural foundations are sound. The elastic time model is genuinely innovative. The graph-native explainability story is a real differentiator. The event-sourced, incremental propagation approach is correct in principle.

The conditions:

1. **Zone boundary migration protocol must be designed and implemented.** This is not a deferred optimization — it affects correctness. If PLANNING_START advances daily and zone boundaries shift, the PI node topology changes. The engine must handle this deterministically and without silent errors.

2. **The optimization/planning layer must be defined.** What fills PlannedSupply nodes? If the answer is "AI agents," that's a valid architectural choice, but it must be stated, and the agent→graph write interface must be specified. A planning engine that cannot generate planned orders is an analysis engine.

3. **Cycle detection must be implemented in the propagation engine** before any real data is loaded. It is a basic correctness guarantee, not a nice-to-have.

The PoA scope (100 SKUs × 5 locations × 5 scenarios) is responsible and achievable. The architecture does not over-promise on scale. This is appropriate intellectual honesty.

The comparison with RapidResponse and SAP IBP is favorable on design clarity and explainability. It is unfavorable on scale, solver capability, and integration maturity. These are expected gaps for a new engine and do not disqualify the design — but they define the correct positioning: Ootils is a next-generation engine for AI-augmented planning workflows, not a drop-in replacement for enterprise APS incumbents.

If the three conditions above are addressed, this architecture deserves to be built.

---

## Appendix: Specific Questions for the Architecture Team

1. What populates `PlannedSupply` nodes? Policy-driven MRP logic? Solver? AI agent action? The answer fundamentally changes what this system is.
2. How are `Explanation` nodes created, by whom, and when do they become stale?
3. What is the migration protocol when a PI node transitions from weekly to daily zone as PLANNING_START advances?
4. Is Horizon Extension treated as a bulk operation (single dirty-flush for all new nodes) or N individual events?
5. What is the locking model for concurrent scenario propagation?
6. How are `scenario_overrides` cached during propagation?
7. Is cycle detection implemented? Where?
8. What is the multi-user write conflict resolution strategy?

---

*Review completed: 2026-04-04. This document represents an independent architectural assessment based on the design documents provided. It is not a code review.*
