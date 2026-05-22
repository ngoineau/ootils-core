# Ootils Roadmap

> This is a living document. Priorities may shift based on community feedback and architectural discoveries.

---

## Current Status: V1 Alpha — Hardening

All seven V1 milestones (M1–M7) are implemented and shipping in the runtime. The repository contains a working `ootils-core` service: 32 SQL migrations, ~50 `/v1/*` REST endpoints, an LLM agent tool surface, and live Phase 1 demo endpoints. The current focus is hardening (security headers, observability, scalability against the breaking points in `docs/SCALABILITY.md`) and the items tracked in [REVIEW-2026-05](docs/REVIEW-2026-05.md).

We are still building in public — architectural decisions go through ADRs (see [`docs/INDEX.md`](docs/INDEX.md) for the ADR map).

---

## V1 — Core Engine (Shipping)

**Goal:** Prove the architectural thesis. A minimal but technically rigorous engine that demonstrates graph-based, incremental, explainable supply chain planning — consumable by an AI agent.

### Milestones

- [x] **M1 — Data Model**
  - SQL schema: nodes, edges, events, scenarios, explanations (32 migrations)
  - Node/edge type registry (`docs/node-dictionary.md`, `docs/edge-dictionary.md`)
  - Ingestion from flat files (CSV/JSON) via `/v1/ingest/*`

- [x] **M2 — Core Engine**
  - Temporal Bridge (`engine/kernel/temporal/bridge.py`)
  - Projected inventory calculation (`engine/kernel/calc/projection.py`)
  - Incremental propagation, dirty-flag + subgraph expansion (`engine/orchestration/propagator.py`)
  - Allocation engine (`engine/kernel/allocation/engine.py`)

- [x] **M3 — Explainability**
  - Root cause chain generation (`engine/kernel/explanation/builder.py`)
  - Structured explanation storage (`explanations` and `causal_steps` tables)
  - Explanation API endpoint (`/v1/explain/*`)

- [x] **M4 — Shortage Detection**
  - Shortage node generation (`engine/kernel/shortage/detector.py`)
  - Severity scoring (qty × days × cost proxy)
  - Shortage → explanation linkage

- [x] **M5 — Scenarios**
  - Override mechanism via `scenario_overrides` table
  - Simulation endpoint (`/v1/simulate`)
  - Baseline vs scenario diff (`/v1/scenarios/diff`)

- [x] **M6 — API**
  - REST API: ~50 endpoints (`/v1/events`, `/v1/projection`, `/v1/issues`, `/v1/explain`, `/v1/simulate`, `/v1/graph`, `/v1/ingest`, …)
  - OpenAPI spec (`docs/openapi.json`)
  - Bearer-token auth with `hmac.compare_digest` (`api/auth.py`)

- [x] **M7 — AI Agent Demo**
  - Three agent tools: `get_active_issues`, `simulate_override`, `trigger_recalculation` (`tools/agent_tools.py`)
  - Phase 1 end-to-end demo (`demo/phase1.py`, `tests/integration/test_phase1_e2e.py`)

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

*Last updated: 2026-05-22 — V1 alpha hardening pass.*
