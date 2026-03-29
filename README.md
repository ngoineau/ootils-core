# ootils-core

> **The first supply chain decision engine designed for the age of AI agents.**

[![Status](https://img.shields.io/badge/status-concept%20%2F%20white%20paper-orange)](https://github.com/ngoineau/ootils-core)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Contributions](https://img.shields.io/badge/contributions-welcome-brightgreen)](CONTRIBUTING.md)

---

## The Problem

Every Advanced Planning System (APS) on the market today — Kinaxis, SAP IBP, Blue Yonder, o9 — was designed for the same paradigm:

- **Humans as central operators** — a planner sitting in front of a screen
- **Screen as primary interface** — dashboards, worksheets, alerts for human consumption
- **Batch recalculation** — run a job, get a frozen snapshot, repeat
- **Global time buckets** — the whole system runs on the same calendar rhythm
- **KPI tables as output** — numbers without explanations
- **Black box decisions** — you see *what* broke, never *why*

This paradigm made sense in 1985. It made sense in 2005.

**It will not work in 2030.**

Supply chains are becoming too complex, too dynamic, and too interconnected to be managed through periodic batch cycles and human-readable dashboards.

The next generation of supply chain operations will be driven by AI agents that:
- Sense changes in real time
- Simulate thousands of scenarios in seconds
- Interrogate the plan causally ("why is there a shortage at DC-East on April 15th?")
- Act autonomously within defined guardrails
- Escalate to humans only when necessary

None of the current tools are architecturally ready for this.

**Ootils is.**

---

## What is Ootils

Ootils is an open source supply chain decision engine built API-first, with AI agents as first-class consumers.

It is **not** another APS.
It is **not** an AI layer on top of an existing planning tool.
It is **not** a dashboard or a UI product.

It is a **deterministic, explainable, graph-based planning engine** designed to be interrogated, simulated, and operated by both humans and AI agents.

```
Traditional APS:         Ootils:
Human → Screen           Agent / Human
   ↓                          ↓
 Batch                    Event / API call
   ↓                          ↓
KPI Table                 Causal Graph
   ↓                          ↓
"Shortage detected"       "Order B (150u) consumes
                           OnHand (20u). PO-991
                           delayed to Apr 18.
                           No substitute active.
                           Shortage: 130u."
```

---

## Five Core Pillars

### 1. Business Graph with Semantic Edges
The planning model is a directed graph where **nodes are typed business objects** (demand, supply, inventory, capacity, policy) and **edges carry business semantics** (consumes, replenishes, pegged_to, bounded_by, impacts...).

Not a table. Not a cube. A network of objects with causal relationships.

### 2. Object-Local Time (Elastic Time)
Time is a property of the object, not a global axis imposed on the system.

- A purchase order has an exact date
- A forecast has a monthly bucket
- A capacity bucket has a weekly rhythm
- A projected stock is computed daily

The engine reconciles these through a **Temporal Bridge** — without forcing everyone into the same calendar.

### 3. Deterministic Incremental Propagation
When something changes, the engine does not recompute everything.

It identifies the **impacted subgraph**, propagates in topological order, and stops when nothing changes downstream. The result is always deterministic and reproducible.

No stochastic magic. No "why did the plan change?" mystery.

### 4. Native Explainability (Root Cause Chain)
Every result — every shortage, every delay, every allocation decision — carries a **complete causal path**.

```
GET /explain?node=shortage-PUMP01-DC1-20260415

{
  "shortage_qty": 130,
  "root_cause": [
    { "type": "demand", "id": "CO-778", "qty": 150, "priority": 2 },
    { "type": "onhand_exhausted", "available": 20 },
    { "type": "supply_delayed", "id": "PO-991", "original": "Apr 10", "new": "Apr 18" },
    { "type": "no_substitute_active" }
  ],
  "summary": "Order CO-778 exceeds available stock. PO-991 delayed 8 days. No alternative supply."
}
```

### 5. Lightweight Scenarios
A scenario is a set of overrides on a baseline — not a physical copy of the entire dataset.

Change a PO date, a demand quantity, a priority rule → the engine recalculates only what changed → you see the delta immediately.

Branching costs near zero. Merging is explicit.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    CONSUMERS                            │
│   Human Planner    │    AI Agent    │    External API   │
└──────────────┬──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────┐
│                     API LAYER                           │
│   POST /events   GET /projection   GET /explain         │
│   POST /simulate GET /issues       GET /graph           │
└──────────────┬──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────┐
│                  PLANNING ENGINE                        │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Graph Model │  │  Propagation │  │ Explainability│  │
│  │  Nodes/Edges │→ │   Engine     │→ │    Engine     │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│           ↑                 ↑                           │
│  ┌──────────────┐  ┌──────────────┐                     │
│  │  Temporal    │  │  Scenario    │                     │
│  │  Bridge      │  │  Manager     │                     │
│  └──────────────┘  └──────────────┘                     │
└──────────────┬──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────┐
│                 DATA LAYER (SQL)                        │
│   nodes · edges · events · scenarios · explanations    │
└──────────────┬──────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────┐
│              OPERATIONAL DATA (ERP / WMS)               │
│   On Hand · POs · Orders · Forecasts · Capacity        │
└─────────────────────────────────────────────────────────┘
```

---

## Node Types (V1)

| Family | Nodes |
|--------|-------|
| Reference | Item, Location, Resource, Supplier, Policy |
| Demand | ForecastDemand, CustomerOrderDemand, DependentDemand, TransferDemand |
| Supply | OnHandSupply, PurchaseOrderSupply, WorkOrderSupply, TransferSupply, PlannedSupply |
| Constraints | CapacityBucket, MaterialConstraint |
| Results | ProjectedInventory, Shortage |

## Edge Types (V1)

`replenishes` · `consumes` · `depends_on` · `requires_component` · `produces` · `uses_capacity` · `bounded_by` · `governed_by` · `transfers_to` · `originates_from` · `pegged_to` · `substitutes_for` · `prioritized_over` · `impacts`

---

## Roadmap

### V1 — Core Engine (current focus)
- [ ] Node/edge graph model with typed semantics
- [ ] Elastic time model (Temporal Bridge)
- [ ] Deterministic incremental propagation
- [ ] Projected inventory calculation
- [ ] Shortage detection
- [ ] Root cause explanation engine
- [ ] REST API (events, projection, explain, simulate)
- [ ] Basic scenario (override + diff)

### V2 — Planning Intelligence
- [ ] Multi-echelon support
- [ ] Capacity constraints
- [ ] Substitution rules
- [ ] Fair-share allocation
- [ ] Full pegging
- [ ] Scenario versioning (Dolt integration pilot)

### V3 — AI-Native Operations
- [ ] Native AI agent SDK (Python + REST)
- [ ] Autonomous decision execution within guardrails
- [ ] LLM-readable explanation format
- [ ] Streaming events for agent subscriptions
- [ ] Multi-agent orchestration primitives

---

## Current Status

> ⚠️ **Concept Phase** — This repository currently contains the architecture specification, data model, and white papers. No executable code yet.

What exists today:
- ✅ Architecture documentation
- ✅ Node/edge dictionary (18 types + 14 types)
- ✅ Competitive analysis (13 players mapped)
- ✅ Academic bibliography (35+ references)
- ✅ Core algorithm specifications
- ⬜ First executable engine (in design)

**We are building in public from day 0.** The architecture decisions, the failures, and the trade-offs will all be documented here.

---

## Why Open Source

Supply chain planning has been held hostage by closed, expensive, inflexible systems for 40 years.

The infrastructure for AI-native supply chains should be a public good — not a $2M/year SaaS license.

Ootils core engine will remain free and open source. Commercial offerings (hosted, enterprise support, agent integrations) will fund the development.

---

## Community

- **Discussions:** [GitHub Discussions](https://github.com/ngoineau/ootils-core/discussions)
- **Architecture decisions:** See `/docs/adr/` (Architecture Decision Records)
- **White papers:** See `/docs/`

---

## Contributing

We are looking for people who have felt the pain firsthand.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to get involved.

---

## License

Apache 2.0 — see [LICENSE](LICENSE)

---

*Built by supply chain practitioners who are tired of explaining to their boards why a $2M system can't answer "why is there a shortage here?"*
