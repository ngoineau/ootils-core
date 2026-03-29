# The Vision: AI-Native Supply Chain Operations

*A technical manifesto.*

---

## The Current Reality

A typical $1B+ manufacturing supply chain runs on:
- 20–40 supply chain planners
- 3–5 planning systems (APS, ERP, BI, Excel)
- Weekly S&OP cycles
- Monthly batch recalculations
- Constant firefighting between systems that don't talk to each other

The systems were designed to **help humans decide**. The humans sit in front of screens, interpret dashboards, run what-if scenarios manually, email each other Excel files, and meet in conference rooms to align on a plan that will be obsolete in 48 hours.

This is not a failure of the people. It is a failure of the architecture.

---

## What Changes With AI Agents

AI agents can now:
- Process structured data at machine speed
- Maintain complex state across thousands of variables
- Run simulations in parallel
- Make rule-based decisions within defined guardrails
- Escalate to humans when genuine judgment is required

The question is no longer *"can AI help plan a supply chain?"*

The question is: **"what does a planning engine need to look like for AI agents to operate it effectively?"**

The answer is not "add a chatbot to Kinaxis."

---

## The Architecture Gap

Current APS systems are architecturally incompatible with AI-native operations:

**Problem 1: Screen-centric design**
Everything is optimized for human readability — dashboards, alerts, worksheets. An AI agent doesn't need a screen. It needs a clean API that returns structured, queryable, explainable data.

**Problem 2: Batch processing**
Agents work in real-time. A system that recalculates once a night and serves a frozen snapshot is useless for continuous autonomous operations.

**Problem 3: Black box outputs**
An agent taking a decision needs to understand *why* the current state is what it is. "Shortage detected" is not enough. The agent needs the full causal chain to act correctly and explain its actions.

**Problem 4: Global time buckets**
Real supply chains have heterogeneous time horizons. A PO has a date. A forecast has a monthly bucket. A capacity limit has a weekly rhythm. An agent working across these objects needs the engine to reconcile them natively — not force everything into weekly buckets.

**Problem 5: No scenario primitives**
Running thousands of micro-simulations — "what if I expedite PO-991? what if I substitute component A with B? what if demand drops 20%?" — requires lightweight scenario branching. Current systems either don't support it or make it prohibitively expensive.

---

## The Ootils Thesis

A supply chain decision engine designed for AI-native operations must be:

**1. Graph-native**
The world is not a table. Supply chains are networks. Objects depend on other objects. Shortages propagate. Constraints ripple. The engine must model this as a graph — not flatten it into rows.

**2. API-first**
The primary consumer is a program, not a human. The API must be clean, consistent, and designed for programmatic interrogation. The UI is secondary.

**3. Deterministic**
AI agents are responsible for their actions. They need to trust that the same inputs always produce the same outputs. No stochastic variation in the core planning logic.

**4. Explainable by design**
Every result must carry its causal provenance. An agent querying "why is there a shortage?" must get a structured, traversable answer — not a summary KPI.

**5. Time-elastic**
Different objects live in different time dimensions. The engine must reconcile them without destroying the business semantics of each.

**6. Lightweight scenarios**
Simulating "what if?" must be cheap. An agent running 1000 scenarios per decision cycle cannot afford to copy a 10M-row dataset each time.

---

## The Future We're Building Toward

**2026:** Ootils V1 — core engine runs. AI agents can query, simulate, and receive explained results via API. Human planners still drive decisions, but the engine is AI-ready.

**2027–2028:** First autonomous decision loops. Agents handle routine replenishment, exception triage, and scenario ranking. Humans focus on strategic decisions, supplier negotiations, and edge cases.

**2029–2030:** AI-native supply chain operations become viable at scale. A $1B manufacturing supply chain runs with a 5–10 person team — not because the people are smarter, but because the engine is architecturally capable of supporting autonomous operations.

This is not science fiction. Every component exists today. What doesn't exist is the right engine to connect them.

---

## What We Are Not Building

**We are not building a UI product.**
Ootils is infrastructure. Interfaces will be built on top of it.

**We are not building an AI model.**
The planning logic is deterministic. AI is a consumer of the engine, not its replacement.

**We are not building "Kinaxis with AI."**
We are not adding features to the existing paradigm. We are replacing the paradigm.

**We are not building for a niche.**
Every company that manufactures, distributes, or moves goods has a supply chain. The addressable market is the entire industrial economy.

---

## An Invitation

If you have worked in supply chain planning and felt the frustration of systems that can't answer "why?" — you understand why this matters.

If you have built distributed systems, graph engines, or event-driven architectures — you understand how to build it.

If you are building AI agents that need to operate in the physical world — you understand what's missing.

We are looking for all three.

The architecture is being designed in public. The code will follow. The vision is clear.

**Join us.**

---

*Ootils is being built by practitioners who have managed multi-billion dollar supply chains and know exactly where the current tools fail. The project is open source because the infrastructure for AI-native supply chains should belong to everyone.*
