# Ootils Roadmap

> This is a living document. Priorities may shift based on community feedback and architectural discoveries.

---

## Current Status: Concept Phase

The architecture is documented. The first line of code hasn't been written yet.

We are building in public — architectural decisions are being made openly in GitHub Discussions before implementation begins.

---

## V1 — Core Engine (Target: 2026 Q3/Q4)

**Goal:** Prove the architectural thesis. A minimal but technically rigorous engine that demonstrates graph-based, incremental, explainable supply chain planning — consumable by an AI agent.

### Milestones

- [ ] **M1 — Data Model** (weeks 1–2)
  - SQL schema: nodes, edges, events, scenarios, explanations
  - Node/edge type registry
  - Basic ingestion from flat files (CSV/JSON)

- [ ] **M2 — Core Engine** (weeks 3–6)
  - Temporal Bridge (elastic time reconciliation)
  - Projected inventory calculation (single item/location)
  - Incremental propagation (dirty flag + subgraph expansion)
  - Allocation engine (priority-based, deterministic)

- [ ] **M3 — Explainability** (weeks 5–7)
  - Root cause chain generation (inline during calculation)
  - Structured explanation storage
  - Explanation API endpoint

- [ ] **M4 — Shortage Detection** (weeks 6–8)
  - Shortage node generation
  - Severity scoring (qty × days × $ impact)
  - Shortage → explanation linkage

- [ ] **M5 — Scenarios** (weeks 7–9)
  - Override mechanism (lightweight delta)
  - Simulation endpoint
  - Baseline vs scenario diff

- [ ] **M6 — API** (weeks 8–10)
  - REST API: /events, /projection, /issues, /explain, /simulate, /graph
  - OpenAPI spec
  - Basic auth

- [ ] **M7 — AI Agent Demo** (weeks 10–12)
  - Python agent that: queries issues → gets explanation → runs simulation → recommends action
  - Documented example in repo
  - This is the V1 proof

### V1 Out of Scope
- UI (any UI)
- Multi-echelon planning
- Capacity constraints
- Substitution rules
- Optimization (MILP/LP)
- Production-grade auth
- Cloud hosting

---

## V2 — Planning Intelligence (2027)

- Multi-echelon support (BOM explosion, multi-level netting)
- Capacity constraints (CapacityBucket integration)
- Substitution rules
- Fair-share allocation
- Full pegging (complete supply→demand linkage)
- Scenario versioning (Dolt integration pilot)
- Performance optimization (graph indexing at scale)
- WebSocket/SSE streaming for real-time agent subscriptions

---

## V3 — AI-Native Operations (2028+)

- Native AI agent SDK (Python + REST)
- Autonomous decision execution with guardrails
- LLM-readable explanation format (natural language wrapper on structured explanations)
- Multi-agent orchestration primitives
- Self-healing plan suggestions
- Continuous learning from planner overrides

---

## How to Influence the Roadmap

Open a GitHub Discussion tagged `Ideas` or `Architecture` and make your case.

Decisions are made based on:
1. Architectural soundness
2. Community consensus
3. Founder's 20+ years of operational experience

Not based on: feature requests without justification, vendor pressure, or hype cycles.

---

*Last updated: 2026-03-29*
