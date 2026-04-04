# Ootils — Strategic Vision & Open Questions

**Author:** Nicolas GOINEAU  
**Date:** 2026-04-04  
**Status:** Living document — decisions evolve as the project matures

---

## The Core Vision

A complete supply chain function, operated by specialized AI agents, for 20% of the cost of a traditional organization.

This is not a "better APS" thesis. It is a **operational transformation** thesis.

Today, an effective supply chain function is expensive — not primarily because of software licenses, but because of the humans compensating for the gaps in those systems. Planners triaging exceptions manually. Analysts cleaning data. Controllers validating parameters. Consultants producing reports. Coordinators bridging systems.

The vision: a fleet of specialized AI agents replaces or radically augments each of these roles, running on a common planning substrate (Ootils).

---

## The Agent Fleet (future)

Each agent is a specialized autonomous operator:

| Agent | Role |
|-------|------|
| **Data Quality Agent** | Detects and corrects data anomalies continuously |
| **Parameter Agent** | Statistically calibrates safety stocks, lead times, MOQs automatically |
| **Demand Planner Agent** | Manages forecast, detects weak signals, adjusts in real time |
| **QC Agent** | Monitors global system coherence end-to-end |
| **Reporting Agent** | Produces analyses on demand, not in batch |
| **Interface Agent** | Bridges systems (ERP, WMS, suppliers, customers) |

All agents run on Ootils as their planning substrate. Without the motor, the agents are unreliable — they hallucinate, lose context, cannot explain their decisions.

---

## The Sequencing

```
Phase 1 — The Engine (now)
  Ootils core: graph, propagation, elastic time, scenarios, explainability
  Proof-of-Architecture: 100 SKUs × 5 locations × 5-6 scenarios
  Goal: convince that the architecture works operationally

Phase 2 — First Business Agents (on the engine)
  Demand planner agent
  Data quality agent
  Parameter calibration agent
  Each agent uses Ootils as its substrate

Phase 3 — Complete Suite
  Full agent fleet + interfaces + reporting
  Complete supply chain ops at 20% of traditional cost
```

**The engine is the key.** It is the piece that cannot be bought and that 30 years of operational domain knowledge allows to design correctly from the first attempt. The agents come after.

---

## The Competitive Context

**What changed:** AI agents have eliminated the development resource barrier. A system that previously required a 3-5 year team effort can now be built by one founder with AI agents in weeks. This is true for Ootils — and equally true for any well-funded competitor.

**What remains as real moat:**

1. **30 years of domain knowledge** — knowing *what* to build, in what order, and why. A competitor can code fast but will make wrong architectural decisions because they don't truly understand operational supply chain. The ADRs produced on 2026-04-04 are evidence of this advantage.

2. **First paying reference** — once achieved, it cannot be taken away.

3. **Community** — if built correctly (see below).

---

## Open Question: Open Source vs. Proprietary

**Context:** The speed of AI-assisted development changes the strategic calculus. Open source traditionally compensates for development resource constraints. If that constraint no longer exists, the moat argument for open source weakens. But the competitive defense argument strengthens.

**The risk:** A competitor with $5M and AI agents can reproduce the engine in 3-6 months if they understand the correct architecture. The technical barrier has collapsed for everyone.

**Community as defense AND offense:**
- **Defensive:** An active community creates public visibility, technical credibility, and an ecosystem that slows competitors. Copying a product is easier than copying a community.
- **Offensive:** Agents built by the community extend the suite without requiring the founder to build everything.

**A possible structure (not decided):**

```
Open source                    Proprietary / commercial
──────────────────             ──────────────────────────
Ootils core engine             Advanced business agents
(motor, ADRs,                  (demand planner, param
interfaces)                    calibration, QC global...)

→ Technical credibility        ERP/WMS connectors
→ Builder community
→ Hard to fork without         Managed service
  domain knowledge             (SaaS, full operations)
```

Reference model: dbt, Airbyte, Grafana — open core, business in the cloud/service layer.

**⚠️ DECISION PENDING.** This question remains open until:
1. Proof-of-Architecture is complete
2. First commercial validation (paid POC)
3. Clearer view on competitor landscape at that time

---

## Commercial Direction (parallel to PoA)

**The message to the market (business layer):**
> "A complete supply chain function, operated by AI agents, for 20% of the cost of a traditional organization."

**The message to technical buyers:**
> "The planning substrate your AI agents can actually use — graph-native, event-sourced, fully explainable."

**First customer profile:**
- $200M–$2B manufacturer or distributor
- Already has a modern data stack
- Frustrated that their APS cannot interface with AI agents
- Has a technical champion + a business buyer
- Budget: $150K–$500K/year

**Priority:** Start commercial validation conversations in parallel with PoA. Do not wait for the engine to be complete before testing whether the narrative lands.

---

*This document captures strategic thinking as of 2026-04-04. Revisit quarterly or on major milestone completion.*
