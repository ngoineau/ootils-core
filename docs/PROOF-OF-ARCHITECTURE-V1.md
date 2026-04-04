# Ootils V1 — Proof-of-Architecture Proposal

**Document type:** Foundation proposal — domain architecture  
**Scale target:** 100 SKUs · 5 locations · 5–6 scenarios  
**Author:** Senior SC Systems Architect review  
**Date:** 2026-04-03  
**Status:** Proposed

---

## Purpose

This document defines a rigorous, bounded scope for the Ootils V1 engine proof. The goal is not to build a full APS. The goal is to prove — at modest but operationally meaningful scale — that:

> A graph-native, incremental, explainable planning engine can detect shortages, allocate supply across locations, and rank competing scenarios in near-real-time, consumable by an AI agent via API.

A skeptical supply chain operator should be able to run this proof, watch the engine process realistic planning events, and come away convinced that the architectural foundations are sound — not that all features are built.

---

## 1. Exact V1 Proof Scope

### 1.1 Scale Envelope

| Dimension | Target | Rationale |
|-----------|--------|-----------|
| Active SKUs | 100 | Large enough to stress propagation logic; small enough to load by hand if needed |
| SKU families | 3–5 | Mix of types: finished good, semi-finished, purchased component |
| Locations | 5 | 1 plant, 2 DCs, 1 virtual supplier node, 1 virtual customer node |
| Planning horizon | 13 weeks (rolling) | Covers meaningful lead times without infinite data |
| Scenarios | 6 | Baseline + 5 named what-if variants (see §1.3) |
| Open POs | ~300 | ~3 per SKU on average |
| Customer orders | ~500 | Mix of priorities, due dates spread across horizon |
| Forecast rows | ~1,300 | Weekly grain, 100 SKUs × 13 weeks |
| Concurrent AI agents | 1 (demo) | No concurrency proof at V1 — that is V2 |

### 1.2 Network Topology

```
[SUPPLIER-VIRTUAL]
       |
    (PO flows)
       ↓
  [PLANT-MFG]   ← manufactures finished goods from purchased components
       |
  (transfer flows)
     /   \
[DC-EAST] [DC-WEST]
     \   /
   (fulfillment)
       ↓
[CUSTOMER-VIRTUAL]
```

- **SUPPLIER-VIRTUAL**: upstream source node for all PurchaseOrderSupply
- **PLANT-MFG**: single manufacturing location, produces 2 finished good families
- **DC-EAST / DC-WEST**: distribution centers, replenished by transfer from PLANT-MFG
- **CUSTOMER-VIRTUAL**: demand sink node; all CustomerOrderDemand originates here, allocated to DC-EAST or DC-WEST based on region

### 1.3 Six Scenario Set

| ID | Scenario Name | What Changes | Business Question |
|----|--------------|--------------|-------------------|
| S0 | `baseline` | Nothing — current plan as-is | What is the current state? |
| S1 | `po_delay` | PO-991 (top replenishment PO) delayed +10 days | What breaks if our critical PO slips? |
| S2 | `demand_spike` | Customer orders for SKU family A +30% in weeks 5–7 | Can we absorb a demand surge? |
| S3 | `expedite_transfer` | Transfer from PLANT-MFG to DC-EAST pulled 5 days earlier | What does expediting buy us? |
| S4 | `dc_west_stockout` | DC-WEST on-hand for top 10 SKUs set to zero | Simulate a warehouse disruption |
| S5 | `combined_stress` | S1 + S2 simultaneously | Worst-case compound event |

Each scenario is a **delta against baseline** — not a full copy of the plan. This is the core scenario primitive the engine must prove.

---

## 2. Required Node & Edge Entities

### 2.1 Active Node Types for V1 Proof

From the full 18-type dictionary, V1 activates **13 node types**. Five are deferred (marked below).

#### Reference Nodes (all 4 active)

| Node Type | V1 Role | Count at Proof Scale |
|-----------|---------|---------------------|
| `Item` | 100 active SKUs, typed as finished_good / component / raw_material | 100 |
| `Location` | 5 (PLANT-MFG, DC-EAST, DC-WEST, SUPPLIER-VIRTUAL, CUSTOMER-VIRTUAL) | 5 |
| `Supplier` | 2–3 suppliers covering all PO flows | 3 |
| `Policy` | Safety stock policies per SKU family; 1 allocation priority policy | ~10 |

> `Resource` node type: **deferred** — no capacity constraints in V1 proof.

#### Demand Nodes (3 of 4 active)

| Node Type | V1 Role | Count |
|-----------|---------|-------|
| `CustomerOrderDemand` | 500 customer order lines across horizon | ~500 |
| `ForecastDemand` | 1,300 weekly forecast rows (100 SKUs × 13 wks) | ~1,300 |
| `TransferDemand` | Replenishment pulls from DCs to PLANT-MFG | ~100 |

> `DependentDemand` node type: **deferred** — requires BOM explosion (V2). The V1 proof uses single-level supply; no multi-level netting.

#### Supply Nodes (4 of 5 active)

| Node Type | V1 Role | Count |
|-----------|---------|-------|
| `OnHandSupply` | 1 per (SKU × location), as_of today | ~300 |
| `PurchaseOrderSupply` | ~300 open POs against SUPPLIER-VIRTUAL | ~300 |
| `TransferSupply` | In-transit or planned inter-site movements | ~100 |
| `PlannedSupply` | Engine-generated proposals when shortage detected | Dynamic |

> `WorkOrderSupply` node type: **deferred** — no production modeling in V1.

#### Constraint Nodes (1 of 2 active)

| Node Type | V1 Role | Count |
|-----------|---------|-------|
| `MaterialConstraint` | Supplier allocation cap on 5 high-demand SKUs | ~10 |

> `CapacityBucket` node type: **deferred** — no capacity constraints in V1.

#### Result Nodes (both active)

| Node Type | V1 Role |
|-----------|---------|
| `ProjectedInventory` | Computed per (SKU × location × day) within horizon |
| `Shortage` | Generated when projected_qty < 0 at any (SKU × location × date) |

### 2.2 Active Edge Types for V1 Proof

From the 14-type edge dictionary, V1 requires **10 edge types**. Four are deferred.

| # | Edge Type | V1 Status | Reason if Deferred |
|---|-----------|-----------|-------------------|
| 1 | `replenishes` | ✅ Active | Core supply→inventory link |
| 2 | `consumes` | ✅ Active | Core demand→inventory link |
| 3 | `depends_on` | ✅ Active | Calculation dependency graph |
| 4 | `requires_component` | ❌ Deferred | BOM explosion is V2 |
| 5 | `produces` | ❌ Deferred | No WorkOrderSupply in V1 |
| 6 | `uses_capacity` | ❌ Deferred | No CapacityBucket in V1 |
| 7 | `bounded_by` | ✅ Active | MaterialConstraint on POs |
| 8 | `governed_by` | ✅ Active | Policy links to items/locations |
| 9 | `transfers_to` | ✅ Active | DC replenishment flows |
| 10 | `originates_from` | ✅ Active | Audit trail for PlannedSupply |
| 11 | `pegged_to` | ✅ Active | Supply→demand allocation (core of engine proof) |
| 12 | `substitutes_for` | ❌ Deferred | Substitution rules are V2 |
| 13 | `prioritized_over` | ✅ Active | Allocation priority between demand nodes |
| 14 | `impacts` | ✅ Active | Shortage→CustomerOrder consequence chain |

### 2.3 Business Entities Summary

```
Active V1 Graph (at proof scale):
  Nodes:  ~2,400 total across all types
  Edges:  ~6,000–8,000 (high fan-out from pegging and propagation)
  Scenarios: 6 (baseline + 5 deltas)
  Horizon: 91 days (13 weeks)
```

This is a tractable proof-scale graph — large enough to catch architectural issues, small enough to inspect and validate by hand.

---

## 3. Core Planning Flows

The V1 engine must execute exactly these five flows, end-to-end, without shortcuts.

### Flow 1: Inventory Projection (Single Item/Location)

**What it does:** Given on-hand, all incoming supply in the horizon, and all demand, compute the projected inventory position for each future day.

**Steps:**
1. Load `OnHandSupply` for (item, location) as period 0
2. Load all `PurchaseOrderSupply` and `TransferSupply` due in horizon → sorted by date
3. Load all `CustomerOrderDemand` and `ForecastDemand` in horizon
4. Apply forecast consumption rule: `net_demand(t) = max(0, forecast(t) - confirmed_orders(t))`
5. Walk forward through time: `proj_inv(t) = proj_inv(t-1) + supply(t) - net_demand(t)`
6. Apply `MaterialConstraint` where `bounded_by` edges exist
7. Emit one `ProjectedInventory` node per day where value changes (delta encoding, not full daily snapshot)

**Time grain handling (Temporal Bridge):**
- Forecast is weekly → FLAT disaggregation to daily
- POs have exact dates → no disaggregation needed
- Transfer receipts have exact dates → no disaggregation needed

**Output:** Series of `ProjectedInventory` nodes with explanation references attached.

---

### Flow 2: Shortage Detection & Severity Scoring

**What it does:** Identify every (item, location, date) where projected inventory goes below zero (or below configured safety stock threshold), create a `Shortage` node, score it, and attach a causal explanation.

**Steps:**
1. After Flow 1 completes for a (item, location) pair, scan the projected inventory series
2. For each period where `proj_inv(t) < safety_stock_threshold`:
   - Create or update a `Shortage` node
   - Compute `shortage_qty = abs(proj_inv(t))`
   - Score severity: `severity = shortage_qty × daily_unit_revenue × days_exposed`
   - Build causal path inline (ADR-004 pattern)
   - Create `impacts` edges to all `CustomerOrderDemand` nodes whose due_date falls in the shortage window
3. Store `Shortage` with `root_cause_ref` → `Explanation` node

**Severity tiers (for operator display):**
- `CRITICAL`: shortage_qty > 20% of monthly demand OR impacts priority-1 customer
- `HIGH`: shortage_qty > 10% OR impacts any customer with confirmed order
- `MEDIUM`: shortage_qty > safety stock floor but demand is forecast-only
- `LOW`: below safety stock but no actual demand impacted

**Output:** `Shortage` nodes with severity, explanation, and `impacts` edges to affected demands.

---

### Flow 3: Allocation Engine (Priority-Based, Deterministic)

**What it does:** When supply is insufficient to cover all demand in a period, allocate available supply to demand in deterministic priority order, creating `pegged_to` edges.

**Rules (in order):**
1. Priority from `prioritized_over` edges (defined by allocation Policy)
2. Tie-break: earlier due_date wins
3. Second tie-break: lower order_id (stable sort — critical for determinism)

**Steps:**
1. For each `(item, location, date)` with supply < total demand:
   - Sort demands by priority rules above
   - Walk demands in order: allocate as much available supply as possible
   - Create `pegged_to` edge with `weight_ratio = allocated_qty`
   - Reduce available supply by allocated amount
   - Any demand with unmet allocation → `shortage_qty = demand.qty - allocated_qty`
2. Repeat per time period, carrying forward residual inventory

**Determinism requirement:** Given identical inputs, allocation output is byte-identical. No random tie-breaking. This is testable.

**Output:** `pegged_to` edges linking supply to demand; updated `Shortage` nodes with `impacted_demand_ref`.

---

### Flow 4: Incremental Propagation (Event-Triggered)

**What it does:** When a planning event occurs (PO date changed, demand quantity changed, on-hand adjusted), recompute only the affected subgraph — not the full plan.

**Event types supported in V1:**
| Event | Trigger Node | Subgraph Expansion |
|-------|-------------|-------------------|
| `supply_date_changed` | `PurchaseOrderSupply` or `TransferSupply` | All downstream `ProjectedInventory` + `Shortage` for (item, location) from original_date to new_date |
| `supply_qty_changed` | `PurchaseOrderSupply` | Same as above, but bounded by shortage window |
| `demand_qty_changed` | `CustomerOrderDemand` | Upstream supply nodes that this demand is pegged to; downstream shortage |
| `onhand_adjusted` | `OnHandSupply` | Full (item, location) projection from today forward |
| `policy_changed` | `Policy` | All nodes with `governed_by` edge to this policy |
| `scenario_override` | Any node | Delta recompute within scenario only — baseline untouched |

**Algorithm (per ADR-003):**
1. Receive event → identify trigger node
2. Expand affected subgraph via edge traversal (bounded by horizon)
3. Mark dirty
4. Topological sort
5. Recompute in order, stopping branches where result is unchanged
6. Persist deltas; emit change notifications

**Performance target:** Any single-event propagation completes in <5 seconds at proof scale (100 SKUs, 5 locations). This is the key engine SLA.

---

### Flow 5: Scenario Simulation & Diff

**What it does:** Apply a named scenario override to the graph, compute the full impact against baseline, and return a structured diff that an AI agent can interpret.

**Scenario override mechanism:**
- A scenario is a named set of node-level overrides (not a full graph copy)
- Override: `{scenario_id, node_id, field, old_value, new_value}`
- Engine applies overrides in a scenario context; baseline graph is read-only
- All derived results (ProjectedInventory, Shortage, pegged_to) in the scenario are tagged `scenario_id`

**Diff output structure:**

```json
{
  "scenario_id": "po_delay",
  "vs_baseline": "baseline",
  "computed_at": "2026-04-03T14:00:00Z",
  "summary": {
    "new_shortages": 7,
    "resolved_shortages": 0,
    "severity_delta": "+$142,000",
    "items_affected": ["PUMP-01", "VALVE-03", "..."]
  },
  "shortage_diff": [
    {
      "item_id": "PUMP-01",
      "location_id": "DC-EAST",
      "baseline_qty": 0,
      "scenario_qty": 130,
      "impact": "CustomerOrder CO-778 unmet from Apr 8 to Apr 18",
      "explanation_ref": "exp-uuid-..."
    }
  ],
  "inventory_diff": [
    {
      "item_id": "PUMP-01",
      "location_id": "DC-EAST",
      "date": "2026-04-15",
      "baseline_proj": 45,
      "scenario_proj": -85
    }
  ]
}
```

**V1 requirement:** All 5 what-if scenarios (S1–S5) must compute and return a valid diff against S0 (baseline). S5 (combined stress = S1+S2) tests multi-override composition.

---

## 4. Explicit Exclusions from V1 Proof

The following are **hard out-of-scope** for V1. Attempting to include any of these will compromise the architectural proof by spreading implementation effort across unvalidated territory.

### 4.1 Domain Exclusions

| Excluded Feature | Why Excluded | When |
|-----------------|--------------|------|
| BOM explosion / multi-level netting | Requires `WorkOrderSupply` + `DependentDemand` + `requires_component` edges; doubles engine complexity | V2 |
| Capacity constraints | Requires `Resource` + `CapacityBucket` + `uses_capacity`; interacts with production scheduling in ways that are out of V1 scope | V2 |
| Substitution rules | `substitutes_for` edge logic requires alternate item netting; allocation engine complexity multiplies | V2 |
| Fair-share allocation | Complex rationing logic across customer segments; V1 proves priority-based allocation only | V2 |
| Full pegging (demand→supply→raw material) | Depends on BOM explosion | V2 |
| Optimization (MILP/LP) | Engine is heuristic + rule-based; optimization is a separate layer above the planning engine | V3 |
| Inter-company / contract manufacturing | Requires additional entity model not in scope | V2+ |
| Lot tracking / expiry | Adds significant complexity to inventory projection; requires lot-level pegging | V2+ |

### 4.2 Infrastructure Exclusions

| Excluded | Why | When |
|----------|-----|------|
| Any UI | Engine is API-first; UI is a consumer, not a deliverable | V2 |
| Production-grade auth | Not the architectural risk to prove | V2 |
| Cloud hosting / multi-tenant | Scale-out is not the V1 proof | V2 |
| Streaming (Kafka/SSE) | Incremental propagation proven via synchronous API; streaming deferred | V2 |
| Native graph DB (Neo4j) | SQL storage with app-layer graph semantics per ADR-001 | V2 option |
| Concurrent agents | Multi-agent orchestration primitives are V3 | V3 |
| LLM-generated explanations | Structured explanations proven first; LLM wrapper is presentation layer | V3 |

### 4.3 Data Exclusions

| Excluded | Scope Boundary |
|----------|---------------|
| Historical actuals / analytics | V1 is forward-looking planning only |
| ERP integration (live) | V1 ingests from static files (CSV/JSON); live connectors are V2 |
| Demand sensing / ML forecasts | Forecast is an input, not generated by the engine |
| S&OP process workflow | No approval workflows, no plan versioning beyond scenario deltas |
| Cost optimization across sourcing | Supplier selection in V1 is priority-rule-based, not cost-optimal |

---

## 5. Acceptance Criteria

These criteria must all pass for V1 to be declared proven. They are designed to be verifiable by a skeptical operator who has seen APS projects fail.

### AC-01 — Shortage Detection Accuracy

**Test:** Load the baseline dataset (100 SKUs, 5 locations, 300 POs, 500 orders, 1,300 forecast rows). Run full projection. Output all detected shortages.

**Pass condition:**
- Every shortage identified by the engine corresponds to a period where `demand > supply` in the raw data — verifiable by an analyst with a spreadsheet
- No false positives: no shortage reported where sufficient supply exists
- Shortage severity scores rank shortages in the same order a planner would rank them by manual review
- **Tolerance:** Zero false negatives on shortages affecting priority-1 customer orders; <2% false negative rate on forecast-only shortages

---

### AC-02 — Allocation Determinism

**Test:** Run the allocation engine on the same input dataset three times sequentially, then once after shuffling the order of demand records in memory.

**Pass condition:**
- All three sequential runs produce byte-identical `pegged_to` edge sets
- The shuffled-order run produces the same result as the sequential runs
- Log a hash of the pegged_to edge set for each run — all four hashes are identical

---

### AC-03 — Incremental Propagation Correctness

**Test:** Run full baseline projection. Then apply event `po_delay` (PO-991 moves +10 days). Run incremental propagation.

**Pass condition:**
- The set of `ProjectedInventory` nodes updated by incremental propagation equals the set that would have changed in a full re-projection
- No nodes outside the affected subgraph are re-computed (verify via calculation audit log)
- The final state after incremental propagation is byte-identical to a fresh full projection run on the modified data

---

### AC-04 — Incremental Propagation Speed

**Test:** Apply any of the 6 supported event types to the baseline graph. Measure wall-clock time from event receipt to stable state (no more dirty flags).

**Pass condition:** <5 seconds at proof scale (100 SKUs, 5 locations, full 13-week horizon) on commodity hardware (4-core / 8GB RAM)

**Secondary target:** <2 seconds for single-SKU events (the common case)

---

### AC-05 — Scenario Isolation

**Test:** Apply scenario S1 (`po_delay`). Verify that the baseline graph is unmodified.

**Pass condition:**
- After S1 computation: all nodes tagged `scenario_id = "baseline"` have identical values to their pre-scenario state
- Zero writes to baseline nodes during scenario processing (verified via write audit log)
- Running baseline projection after S1 produces identical results to pre-S1 baseline

---

### AC-06 — Scenario Diff Completeness

**Test:** Run all 6 scenarios (S0–S5). For each of S1–S5, produce a diff against S0.

**Pass condition:**
- Every shortage that exists in a scenario but not in baseline appears in that scenario's `shortage_diff`
- Every shortage that exists in baseline but not in scenario appears as resolved
- `severity_delta` is arithmetically correct (sum of individual shortage severity changes)
- S5 (`combined_stress`) diff reflects the combined impact of S1 and S2 — not just one of them

---

### AC-07 — Explainability Completeness

**Test:** Query the explanation for the top 10 shortages by severity in the baseline scenario.

**Pass condition for each explanation:**
- `causal_path` has ≥2 steps (not just "shortage exists")
- Each step references a real node in the graph (node_id is valid)
- Each step references a real edge type from the edge dictionary
- The `root_cause_node_id` is a supply or demand node, not a result node
- The `summary` field accurately describes the shortage in plain English
- The `detail` field contains enough information for a planner to act without querying any other system

---

### AC-08 — AI Agent Integration

**Test:** Run the reference AI agent demo (per Roadmap M7). The agent must complete the following sequence without human intervention:

1. Query `/issues` → receive shortages sorted by severity
2. Query `/explain?node_id=<top_shortage>` → receive structured explanation
3. Call `/simulate` with scenario `expedite_transfer` (S3) → receive diff
4. Compare diff to current baseline
5. Output a structured recommendation: `{action, expected_impact, confidence, explanation_ref}`

**Pass condition:**
- All 5 API calls succeed with HTTP 200
- Agent recommendation is logically consistent with the explanation received (verifiable by a planner reading both)
- Agent completes the full sequence in <30 seconds wall clock

---

### AC-09 — Data Load & Reproducibility

**Test:** Load the reference dataset from CSV/JSON files. Delete all derived nodes (ProjectedInventory, Shortage, pegged_to edges). Re-run full projection. Compare to original output.

**Pass condition:**
- Identical shortage set (same node_ids, same quantities, same severity scores)
- Identical pegged_to edge set
- This test must pass on a fresh database with zero prior state

---

### AC-10 — Operator Sanity Check (Manual Review)

**Test:** Present the baseline shortage report to an experienced supply chain planner (not the engine author) for 30 minutes.

**Pass condition:**
- Planner agrees that ≥90% of reported shortages "make sense" given the input data
- Planner can navigate from any shortage to its root cause using only the API (no internal DB access)
- Planner does not identify any shortage as "obviously wrong" upon investigation
- Planner is able to articulate what action they would take based on the engine's output

---

## Appendix A: Reference Dataset Specification

The V1 proof requires a synthetic but realistic reference dataset. Minimum spec:

### SKU Catalog (100 items)
- 40 finished goods (2 families × 20 SKUs)
- 30 purchased components (direct-buy, not BOM-derived)
- 30 raw materials (simplistic, single-level supply only)
- Safety stock policies assigned per family (not per SKU — tests policy `governed_by` fan-out)

### Demand Profile
- Seasonal variation: weeks 4–7 are +25% above average (tests surge handling)
- 3 priority tiers in customer orders: P1 (10%), P2 (40%), P3 (50%)
- 15% of SKUs have no confirmed orders — forecast-only (tests net forecast logic)

### Supply Profile
- Average lead time: 14 days (purchased), 7 days (transfer)
- 10% of POs have confirmed = false (unconfirmed date — tests uncertainty handling)
- PO-991 is specifically set as the "critical" PO used in scenario S1 (single point of supply for 5 P1-demand SKUs)
- DC-WEST has systematically lower on-hand levels than DC-EAST (tests asymmetric network)

### Known Baseline Shortages (seeded)
- At least 8 shortages must exist in the baseline plan before any scenario is applied
- At least 2 of these must impact P1 customer orders (critical shortages)
- At least 3 must be forecast-only (medium/low severity)
- This ensures AC-01 and AC-07 have meaningful test material

---

## Appendix B: API Endpoints Required for V1 Proof

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/graph/nodes` | GET | Query nodes by type, item, location, scenario |
| `/graph/edges` | GET | Query edges by type, from/to node |
| `/projection` | GET | Get projected inventory for (item, location, horizon, scenario) |
| `/issues` | GET | Get shortages sorted by severity, filtered by scenario |
| `/explain` | GET | Get structured explanation for any result node |
| `/simulate` | POST | Apply a scenario override set, return diff vs baseline |
| `/events` | POST | Ingest a planning event (triggers incremental propagation) |
| `/scenarios` | GET | List all scenarios and their override counts |

These 8 endpoints are the minimum viable API surface for the AI agent demo and for acceptance testing.

---

## Appendix C: Implementation Sequencing Recommendation

Based on dependency analysis, the V1 milestones from the ROADMAP should be sequenced with the following internal ordering:

```
Week 1–2:   Reference dataset + SQL schema (nodes, edges, scenarios, explanations)
Week 3:     Temporal Bridge (FLAT disaggregation, coverage window)
Week 4–5:   Flow 1 — Inventory Projection (single item/location)
Week 5–6:   Flow 2 — Shortage Detection + severity scoring
Week 6–7:   Flow 3 — Allocation Engine (deterministic pegging)
Week 7–8:   Flow 4 — Incremental Propagation (event-triggered)
Week 8–9:   Flow 5 — Scenario Simulation + diff
Week 9–10:  Explanation generation (inline, ADR-004)
Week 10:    REST API (8 endpoints)
Week 11–12: AI Agent demo + AC testing
Week 12:    Operator sanity check (AC-10)
```

Critical path: Flows 1→2→3 must complete before Flows 4 and 5 can be verified against a meaningful baseline. Do not start scenario work until allocation is deterministic.

---

*This document is the domain architecture foundation for the V1 proof. Implementation decisions should be validated against the ADRs (ADR-001 through ADR-004) and this scope before any code is written.*
