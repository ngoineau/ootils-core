# Contributing to Ootils

> This project now has real code, migrations, APIs, and tests.
> Contributions still shape the architecture, but they must match running repo reality, not an old white-paper snapshot.

---

## Who We're Looking For

### Supply Chain Practitioners
You've used SAP, Kinaxis, Blue Yonder, or built your own tools. You've hit the walls. You know what "the system can't explain why" feels like at 11pm before a board meeting.

Your job: challenge the business logic, the node model, the edge semantics. Tell us where our model breaks against reality.

### Graph & Engine Engineers
You understand DAGs, topological sort, incremental computation, event-driven systems. You've built things that propagate state efficiently.

Your job: challenge the propagation model. Find the edge cases. Propose better algorithms.

### Operations Research / Optimization
You know LP, MILP, constraint programming. You've modeled supply-demand problems formally.

Your job: validate that our deterministic core computation is sound. Also call out where determinism claims should be narrowed to exclude UUID generation, audit timestamps, or other non-computational metadata.

### AI / Agent Builders
You're building autonomous agents. You know what a well-designed API looks like from an agent's perspective. You've seen tools that are impossible to use programmatically.

Your job: define what "AI-native" really means for a planning engine. What does an agent need that humans don't?

---

## How to Contribute Right Now (No Code Required)

### 1. Challenge the Architecture
Read the [README](README.md) and the `/docs/` folder. Open a Discussion with:
- "This won't work because..." → we want this
- "You're missing..." → we want this
- "In my experience at [company], the real problem is..." → we *really* want this

### 2. Share Real War Stories
The business model, the node types, the edge semantics — they were designed from 20 years of real SC operations. But every supply chain is different.

Open a Discussion tagged `war-story`. Tell us about a real planning failure. How would Ootils have handled it? How should it?

### 3. Review Architecture Decision Records (ADRs)
Every significant architectural choice will be documented in `/docs/adr/`. Comment, challenge, propose alternatives.

### 4. Help Define V1 Test Cases
What are the 10 most important scenarios a supply chain planning engine must handle correctly? Open a Discussion tagged `test-case`.

---

## Contribution Principles

**No supply chain debt**
We are not recreating MRP with a better UI. Every design decision must be validated against the AI-native vision.

**Explicit over magic**
Every calculation must be traceable. If you can't explain why the engine produced a result, the design is wrong.

**API first, UI never (for now)**
We build the engine. The interface is someone else's problem for now. Do not propose UI features in V1.

**Determinism is non-negotiable**
The same inputs must always produce the same outputs. No randomness in the core engine.

**Fail loudly**
If the engine can't compute something, it should say so clearly — not silently produce a wrong answer.

---

## Code of Conduct

Be direct. Be technical. Be respectful.

We have zero tolerance for:
- Marketing speak in technical discussions
- Feature requests that contradict the AI-native vision
- "Why don't you just use [existing tool]?" without a genuine architectural argument

---

## Getting Started

1. Read [README.md](README.md) — all of it
2. Read [VISION.md](VISION.md)
3. Browse [GitHub Discussions](https://github.com/ngoineau/ootils-core/discussions)
4. Pick a thread that interests you and contribute

That's it. No CLA. No bureaucracy. Just good engineering discussions.

---

*The best contribution you can make right now is to tell us where we're wrong.*
