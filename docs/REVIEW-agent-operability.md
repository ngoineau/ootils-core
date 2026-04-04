# Ootils — AI Agent Operability Review

**Document type:** Architecture evaluation  
**Date:** 2026-04-04  
**Scope:** API design, explanation model, scenario system, temporal model, token efficiency, HITL, failure modes  
**Target demo:** M7 overnight replenishment cycle (100 SKUs × 5 locations × 5 scenarios)  
**Verdict:** Conditional Yes — architecture is genuinely agent-operable with 3 targeted fixes

---

## Executive Summary

Ootils is architecturally better-suited for LLM agent operation than any existing commercial APS. The graph-native model with first-class structured explanations addresses the single biggest failure point of agent-driven planning: the hallucination that occurs when an agent cannot tell *why* something is wrong and must guess.

The core triage→explain→simulate→recommend chain works. The explanation format is correct. The scenario model is well-designed.

Three issues need to be fixed before an LLM agent can reliably drive the M7 demo without human scaffolding at the API layer:

1. **Async compute ambiguity**: `POST /simulate` must clarify whether it returns synchronously or queues. An agent acting on stale/incomplete scenario data will produce wrong recommendations silently.
2. **No cross-location query**: Cross-location arbitrage (Scenario 5) requires the agent to make 5 serial API calls and synthesize them under its own logic. This is hallucination-prone and token-expensive. A dedicated endpoint is required.
3. **No structured escalation primitive**: The API has no contract for "I cannot resolve this — human required." The agent produces escalations only in its handoff brief, which is natural language. This is insufficient for any real overnight cycle where escalations must be machine-trackable.

None of these are architectural redesigns. They are additive changes that can be shipped without touching the core engine.

---

## 1. API Design for Agents

### What works

The API surface is well-designed for agent consumption:

- **Scenario header (`X-Scenario-ID`)**: Every read is scenario-scoped by default. An agent can operate in a sandbox scenario without polluting baseline. This is essential — most APS APIs have no equivalent.
- **Structured error responses**: Errors return typed JSON (`error`, `message`, `status`). An agent can branch on `"error": "node_not_found"` vs. `"error": "scenario_stale"` — not parse free-text exceptions.
- **`explanation_url` in issues**: The `/issues` response includes a direct link to the explanation. The agent doesn't need to construct the explain URL — it's handed to it. This is good API hygiene.
- **`POST /events` for mutations**: All state changes go through a typed event stream. The agent cannot accidentally corrupt the graph with a malformed PATCH — mutations are constrained to the event type vocabulary.

### Gaps and ambiguities

**Gap 1 — Async vs. sync for `/simulate`**

The API spec example shows `/simulate` returning a computed `delta` synchronously. The scenario lifecycle model (draft → computing → computed) implies async computation. These two models cannot both be true without explicit documentation.

If `/simulate` is async: the agent needs to poll `GET /scenarios/{id}` for `status == "computed"` before reading the delta. If the agent doesn't know this, it will read a `computing` state and reason from an empty or partial delta. This is a silent correctness failure — no error is thrown, but the recommendation is wrong.

**Required:** Explicit documentation of sync vs. async behavior. If async, add `GET /scenarios/{id}/status` and a clear `computed_at` timestamp on the response. If sync with timeout, document the timeout and the behavior on timeout.

**Gap 2 — No cross-location query**

The M7 demo requires cross-location arbitrage: find excess inventory at LOC-CHI to cover a shortage at LOC-DAL. The current API requires 5 separate `/projection` or `/issues` calls (one per location for the same SKU), then agent-side synthesis.

This is both token-expensive and semantically risky. The agent must correctly:
- Identify that LOC-CHI is LOW urgency (excess)
- Compute that the excess quantity exceeds what LOC-DAL needs
- Compare transfer cost to new PO cost
- Account for timing difference in delivery

All of this requires holding the results of 5 API calls in context simultaneously and performing multi-step arithmetic. An LLM in a long reasoning chain will drift on arithmetic.

**Required:** `GET /inventory/cross-location?item_id=EMEC-008&as_of=today` → returns inventory state for all locations for this SKU in a single response. The agent gets the full picture in one call.

**Gap 3 — No supplier status endpoint**

The M7 demo (Scenario 2) requires detecting that `SUP-ARW` has gone inactive and tracing the cascade. There is no `GET /suppliers` or `GET /suppliers/{id}` endpoint in the API spec.

The agent currently has to discover the supplier disruption by observing that `recommend_order` returns an error for certain SKUs. This is a backward-inferential approach — the agent detects an effect and infers a cause — which is exactly the kind of reasoning that produces hallucinated root causes.

**Required:** `GET /suppliers` with `active: bool` flag. `GET /issues?type=supplier_disruption` or a typed event in the issues feed. The agent should be able to proactively query "which suppliers are active?" before diagnosing.

**Gap 4 — No pagination on `/issues`**

With 500 states generating 85+ recommendations, `/issues` will return large payloads. No pagination is specified. An agent making a single call gets everything at once — which may exceed practical token limits and forces the agent to handle a large unsorted array.

**Required:** `?limit=N&offset=M` pagination, or a `?top_n=20` convenience parameter for the common triage pattern (agent wants to focus on the top 20 most severe issues first).

**Gap 5 — No escalation endpoint**

The handoff brief identifies 2 items requiring human decision (ACTI-017, ACTI-023 — no active supplier). But the API has no `POST /escalations` endpoint. The escalation lives only in the agent's markdown output.

For a real overnight cycle, escalations must be:
- Machine-trackable (was this escalation acknowledged before morning?)
- Typed (no_supplier vs. budget_conflict vs. stockout_unavoidable)
- Linked to the original shortage node

This is not a cosmetic issue. If the overnight agent finishes at 02:00 and the planner doesn't read the handoff brief, the escalation is lost. A structured escalation record tied to the planning event log prevents this.

**Required:** `POST /escalations` with `{shortage_node_id, type, reason, created_by_agent, requires_response_by}`.

---

## 2. Explanation Quality

### Assessment: Good — with one structural weakness

The CausalStep chain (ADR-004) is the right design. Every fact in a causal step references a real node and a real edge type. An agent reading the structured explanation can produce correct natural-language output without hallucinating — because every sentence it generates can be anchored to a specific `fact` string.

The three detail levels (`summary` / `detail` / `structured`) are correctly designed for their audiences. Agents consume `structured`. The existence of the pre-built `detail` string means an agent can quote it verbatim in a handoff brief without needing to generate supply chain prose from scratch.

### What prevents hallucination

- `node_id` references in each step are graph IDs, not fuzzy names. An agent cannot confuse "PO-991" with "PO-919" if it's quoting `node_id: "po-PO991"`.
- `edge_type` vocabulary is closed (8 types). An agent cannot invent relationships.
- `root_cause_node_id` is explicitly set. The agent doesn't need to infer the root cause — it's handed to it.

### The structural weakness: PolicyCheck nodes with `node_id: null`

In the causal path example, step 4 is:
```json
{
  "node_id": null,
  "node_type": "PolicyCheck",
  "fact": "No active substitution rule for PUMP-01 at DC-ATL"
}
```

A `null` node_id breaks the agent's ability to verify this fact independently. The agent cannot call `GET /explain?node_id=null` to drill deeper. It cannot check whether the policy has since been updated. It must trust the `fact` string — which is engine-generated prose, not a structural fact.

This is the highest hallucination risk in the explanation model. The agent may quote "no substitution rule active" in a recommendation, but if the policy was updated after the explanation was generated, the agent's recommendation is now wrong. The agent has no way to detect this.

**Fix:** PolicyCheck steps must reference a real Policy node ID, even if the policy result is "null/not found." The node dictionary includes a `Policy` type — use it. A `PolicyCheck` causal step should reference the Policy node it evaluated (or the absence of one should reference a `NullPolicy` sentinel node with a stable ID like `policy-null-PUMP01-DCATL`).

### Root cause archetypes: sufficient for M7

The five archetypes (`supply_delay`, `supply_gap`, `demand_spike`, `allocation_conflict`, `capacity_bound`) cover the M7 demo scenarios. An agent can correctly classify and act on all five. No archetype is ambiguous enough to cause a wrong branch in the agent's decision tree.

One gap for post-M7: `allocation_conflict` is underspecified. An agent reading this archetype cannot determine whether the conflict is between two customers (priority dispute) or between two locations (network allocation). This distinction matters for the recommended action. Flag for V2.

---

## 3. Scenario Operations

### Assessment: Well-designed — one lifecycle ambiguity

The delta overlay model is correct for agent operation. The agent does not need to understand the full planning graph to run a simulation — it specifies which node to change and which field, and gets back a structured diff. This is the right level of abstraction.

**Minimal operation set for M7:**
1. `POST /simulate` — create scenario with overrides
2. `GET /scenarios/{id}/compare` — get diff vs baseline (if async)
3. `POST /scenarios/{id}/approve` — promote to baseline

The M7 demo only needs these three plus the read endpoints. This is achievable.

### The `stale` state is a correctness trap

The scenario lifecycle includes a `stale` state (scenario results exist but baseline has changed). An agent reading a stale scenario's comparison will receive results computed against an old baseline. This is precisely the silent correctness failure that produces wrong recommendations.

The API spec does not currently show `status` in the scenario comparison response. It must.

**Required:** Every `GET /scenarios/{id}/compare` response must include:
```json
{
  "scenario_id": "...",
  "status": "computed",  // or "stale"
  "baseline_snapshot_id": "calc-run-uuid",
  "current_baseline_calc_run_id": "calc-run-uuid",  // if different → stale
  "stale": false
}
```

If `stale: true`, the agent must reject the comparison and trigger recomputation before reasoning from it. Without this signal, the agent cannot distinguish a fresh comparison from a 3-day-old one.

### Branch depth limit (max 2) is correct for agents

Deeper branching would require the agent to track "this scenario is a variant of a variant of baseline" — a multi-hop reference resolution problem. Depth-2 is the right cap. The agent always knows: "my scenario is against baseline or against one named parent." This is unambiguous.

---

## 4. Temporal Model from an Agent Perspective

### Assessment: Correct design — high confusion risk at zone boundaries

The elastic time model (daily 0-90d / weekly 90-180d / monthly 180d+) is operationally correct. The `point_in_bucket` contribution rule eliminates proration complexity. The agent doesn't need to understand how the buckets are constructed — it just reads projected quantities at whatever grain it queries.

### Confusion risk 1: Zone boundary arithmetic

An agent querying `from=2026-05-01&to=2026-08-01` will receive a mix of daily, weekly, and monthly buckets in the response. If the response doesn't make the grain of each bucket explicit, the agent will perform incorrect arithmetic.

Example failure: An agent adds up "daily shortages" across a date range that includes weekly buckets. If each bucket is labeled `date` without an explicit `grain` field, the agent treats a weekly bucket as a daily snapshot and computes wrong totals.

**Required:** Every bucket in a projection response must carry an explicit `grain` field (`"day"`, `"week"`, `"month"`) and `span_start` / `span_end` date pair. The `date` field alone is insufficient.

### Confusion risk 2: The `has_sub_shortage` unresolved issue

ADR-002d explicitly acknowledges an open issue: a shortage invisible at weekly grain (the week is net-positive) but real within the week is not detected by the engine. The `has_sub_shortage` flag is mentioned in the architecture but not present in the API spec.

This is an agent-operability showstopper for any shortage in the 90-180 day weekly zone. The agent will report "no shortage" for a week that contains a 3-day gap. In a high-velocity supply chain (ACTI or SPEC families with long lead times), the weekly zone is exactly where critical shortages lurk.

**Required before production use:** The `has_sub_shortage` flag must appear on weekly and monthly PI nodes in the API response. When `has_sub_shortage: true`, the agent must be instructed to either drill down with a daily-grain query for that specific period, or flag the item for human review. The current architecture acknowledges this gap but leaves it unresolved.

### Confusion risk 3: Agent asking "what is the grain?"

An agent that doesn't know the current date-to-horizon mapping cannot correctly interpret query results. If the agent's system prompt doesn't include "as of today = 2026-04-04, daily zone covers up to 2026-07-03," the agent cannot correctly reason about which buckets correspond to which planning precision.

**Required:** The `as_of` field in API responses must also include `daily_zone_end`, `weekly_zone_end` so the agent always knows which zone its results fall into without needing to compute it.

---

## 5. Token Efficiency

### Estimate for M7 overnight cycle (500 states, 85 recommendations)

| Phase | API Calls | Est. Tokens per Call | Total |
|-------|-----------|---------------------|-------|
| Triage: `GET /issues` (with pagination, 3 pages) | 3 | ~5,000 | ~15,000 |
| Explain: top 27 critical/high items | 27 | ~1,500 | ~40,500 |
| Simulate: 5 scenario runs | 5 | ~2,000 | ~10,000 |
| Compare: 5 scenario diffs | 5 | ~1,500 | ~7,500 |
| Agent reasoning traces (system + chain of thought) | — | ~20,000 | ~20,000 |
| **Total** | **40** | — | **~93,000** |

At 128K context, this leaves ~35K tokens of headroom. It is workable but not comfortable. One bloated `graph_fragment` in an explain response could push the agent over limit.

### What will blow up the context

**`graph_fragment` in `/explain`**: The spec shows that `/explain` returns a `graph_fragment` with full node and edge data. At depth 3 with fan-out, a graph fragment can easily be 3,000-5,000 tokens. For 27 critical items, this alone is 80K-135K tokens — the entire context window.

**Fix:** `graph_fragment` must be opt-in, not default. The agent should be able to call `/explain?node_id=X&include_graph=false` to suppress it. The `structured` detail level is sufficient for agent reasoning. The graph fragment is for UI rendering, not agent consumption.

**Verbose `demand_detail` in projection responses**: The projection spec shows `demand_detail` arrays inside each bucket. For an item with 20 customer orders in a period, this adds significant token overhead. Make `demand_detail` opt-in as well.

### Token-efficient agent patterns

For the M7 demo, the agent should follow this pattern:
1. `GET /issues?severity=critical&horizon_days=30&limit=20` — start narrow, not broad
2. `GET /explain?node_id=X&detail_level=structured&include_graph=false` — suppress graph fragment
3. Only call `/projection` for items where the explanation is insufficient to understand the timeline
4. Batch scenario compares — don't interleave reasoning after each compare; collect all diffs then reason once

---

## 6. Multi-Step Reasoning (M7 Chain)

### Chain: Triage → Explain → Simulate → Recommend → Escalate

The API is designed to support this chain. Each step produces output that is the natural input to the next step. This is the strongest design aspect of the architecture.

Specific chain for M7:
1. `GET /issues` → get `node_id` list sorted by severity ✅
2. `GET /explain?node_id=X` → get `causal_path` with PO node IDs ✅
3. `POST /simulate` with `{override: po_node_id, field: due_date, value: earlier}` ✅
4. `GET /scenarios/{id}/compare` → get shortage delta ✅
5. Agent ranks simulations by `resolved_shortages` count and cost delta ✅ (but agent-side arithmetic)

### Missing intermediate endpoint: `/recommend`

The M7 demo requires "given these shortages, what orders should I place?" The architecture currently requires the agent to:
1. Call `recommend_order` (library interface) or construct it from first principles
2. Apply budget constraint (agent-side greedy algorithm)
3. Rank by urgency (agent-side sort)

For the Python library demo (M7 as designed), this works. For a REST API-based agent, there is no `/recommend` endpoint. The agent must derive order recommendations from explanation data alone — which is doable but fragile. The agent could hallucinate order quantities not grounded in EOQ math.

**Recommendation for post-M7:** Add `GET /recommend?item_id=X&location_id=Y&scenario_id=baseline` → returns `{recommended_qty, recommended_supplier, rationale}`. This grounds the agent's order recommendations in engine-computed EOQ/safety stock math, not in the agent's own arithmetic.

### Loop risk: simulate → compare → simulate cycle

The agent can get stuck in a loop if:
1. Simulation A partially resolves a shortage
2. Agent sees remaining shortage and creates Simulation B to address it
3. Simulation B creates a new shortage (e.g., overloads another PO)
4. Agent sees new shortage and creates Simulation C...

No loop detection is specified in the API. The agent needs a loop budget (max 3 simulation attempts per shortage) enforced at the agent system-prompt level, not at the API level. Add this to the agent prompt design.

---

## 7. Human-in-the-Loop

### Where the agent MUST escalate

Three categories require mandatory human escalation, based on the M7 scenarios:

**Category 1 — No active supplier (hard blocker)**  
`recommend_order` returns `status: "error", reason: "no_active_supplier"`. The agent cannot fix this. It must escalate with the SKU ID, the disrupted supplier, and the days until stockout.

**Category 2 — Unavoidable stockout (SPEC-004 pattern)**  
Lead time > days_of_supply, and no emergency source exists. The agent correctly identifies this from the explanation. But the escalation must include: expected stockout date, affected customer orders, and estimated impact. This is structured data, not prose.

**Category 3 — Budget insufficient for all critical items**  
If CRITICAL items alone exceed the budget, the agent cannot make the trade-off. It must escalate with the funded list and the unfunded critical items, sorted by days-to-stockout.

### Current API support for escalation: insufficient

The API has no escalation endpoint. The only surfacing mechanism is the handoff brief (markdown). This is appropriate for M7 (demo context), but is not production-ready.

The escalation contract needs to be first-class:
```json
POST /escalations
{
  "type": "no_active_supplier",          // typed
  "shortage_node_id": "shortage-...",
  "impacted_skus": ["ACTI-017"],
  "urgency": "critical",
  "days_to_stockout": 2.7,
  "blocking_reason": "SUP-ARW on credit hold — no fallback",
  "created_by": "agent-overnight-2026-04-04",
  "requires_response_by": "2026-04-04T08:00:00Z"
}
```

Without this, overnight escalations are only in a markdown file that a planner may or may not open before stockout occurs.

---

## 8. Failure Modes Specific to LLM Agents

### Hallucination risks

**Risk H1 — PolicyCheck with null node_id (HIGH)**  
As noted in Section 2: the agent cannot verify a null-node policy fact. It will quote the policy state in its recommendation. If the policy changed after the explanation was generated, the recommendation is wrong. The agent has no way to detect this. This is a pre-production showstopper.

**Risk H2 — Agent arithmetic on budget allocation (MEDIUM)**  
The M7 budget allocation requires the agent to sum `order_quantity × unit_cost` across 43 orders and stay within $75,000. LLMs are unreliable at multi-step arithmetic. An off-by-$800 error in budget calculation means either orders are missed (under-budget) or the plan exceeds the cap. Without a `POST /plan/validate` endpoint that returns `total_cost_usd` from engine-computed data, the agent's arithmetic is unverifiable.

**Risk H3 — Confusing scenario results with baseline (LOW, but costly)**  
If the agent sets `X-Scenario-ID: sim-expedite-po991` in its headers and forgets to reset it, all subsequent reads are in the scenario context, not baseline. The agent will report scenario results as if they were the current plan. The API spec says the header is optional and defaults to baseline — but LLMs sometimes propagate context incorrectly across a multi-call chain.

**Mitigation:** The agent should explicitly include `X-Scenario-ID: baseline` on all triage and explain calls, and only use scenario headers on simulate/compare calls. Add this to the agent system prompt.

### Overconfidence risks

**Risk O1 — Transfer recommendation without freight cost**  
The cross-location arbitrage (Scenario 5) requires comparing transfer cost to PO cost. The API does not currently expose freight cost as a structured field on transfer recommendations. The agent will estimate it or use a fixed assumption from the system prompt. If actual freight differs, the recommendation is wrong — but the agent will state it with high confidence because the rest of the math was grounded in API data.

**Risk O2 — Sole-source long-lead items**  
For SPEC family items (SPEC-004, lead time 42 days), the agent correctly identifies the unavoidable stockout. But if a new emergency broker has been added to the supplier catalog since the last triage run, the agent won't know — it will recommend "no emergency source" based on stale data. The `as_of` timestamp on issues responses is important here; the agent must include it in the handoff brief so the planner can assess data freshness.

### Underspecification risks

**Risk U1 — Severity scoring formula not exposed**  
The API returns `severity: "high"` but not the numeric score. The agent cannot compare two HIGH items to determine which is more critical. It must fall back to heuristics (shortage_qty, days_to_breach). Exposing `severity_score: 0.87` (normalized) in the issues response would allow the agent to rank deterministically.

**Risk U2 — Allocation priority not visible**  
The `prioritized_over` edge exists in the graph model, but priority tiers for customer orders are not surfaced in the `/issues` or `/explain` responses. The agent cannot tell if CO-778 is a P1 or P3 customer order from the API response. If it can't, it cannot correctly prioritize which shortages to address first.

**Required:** Add `customer_priority: 1` (integer, P1/P2/P3) to the `impacted_orders` array in the issues response.

### Loop risk

**Risk L1 — Stale scenario triggering re-simulation**  
If a scenario is `stale`, the agent may detect the staleness, trigger a recompute, wait for `computing → computed`, then detect it's stale again (because baseline changed during the compute). The agent loops between "stale → compute → stale" indefinitely.

**Mitigation:** The API must enforce that once a `POST /scenarios/{id}/recompute` is called, the scenario's `baseline_snapshot_id` is locked to the current baseline for the duration of that compute. After compute, the scenario is `computed` relative to that locked baseline, even if baseline has subsequently changed. Mark it `stale` only on the *next* baseline change, not during the compute window.

---

## 9. Comparison with Kinaxis and SAP AI Integration

### How Kinaxis does it (RapidResponse + Maestro/AI)

Kinaxis has an "AI advisor" layer (Project Maestro) that produces natural-language summaries of risk conditions. Structurally:
- The AI reads from pre-computed KPI cubes — not from a traversable graph
- Explanations are LLM-generated from KPI deltas — not from causal chains
- Scenarios are browser-driven — no agent-callable scenario API
- No machine-readable causal path — the AI infers causality from statistical correlation

The fundamental limitation: Kinaxis AI can describe *what* changed, not *why* it changed. An agent built on Kinaxis cannot reason from first principles about whether a simulation will resolve a shortage — it can only pattern-match to historical similar events.

### How SAP does it (IBP + Joule/CoPilot)

SAP IBP + Joule:
- Joule reads from IBP's hana-backed flat tables
- Answers natural language questions via RAG over documentation and plan data
- Cannot execute scenarios — it can describe them
- No scenario diff API that returns structured deltas

SAP's agent integration is essentially a read-only Q&A layer on top of a planning tool that still requires human scenario management.

### What Ootils does differently

| Dimension | Kinaxis | SAP IBP | Ootils |
|-----------|---------|---------|--------|
| Causal explanations | LLM-inferred | None | First-class, engine-generated |
| Agent-callable scenarios | No | No | Yes, via POST /simulate |
| Structured shortage diff | No | No | Yes, delta overlay model |
| Graph traversal for causality | No | No | Yes, typed nodes + edges |
| Escalation contract | Manual | Manual | Missing — add it |
| Agent-readable root cause | Inferred | None | 5-archetype classification |

Ootils is the only architecture in this comparison that makes agent operation a first-class design constraint rather than a bolt-on feature. The explanation model alone is a qualitative leap over both incumbents.

The risk Ootils faces: it's designing for agents before the agent patterns are fully understood. The gaps identified in this review (cross-location query, escalation endpoint, graph fragment size) are the kind of gaps that only appear when you actually run an agent against the API. They're fixable — but they need to be fixed from running actual agents, not from theoretical review.

---

## 10. Verdict

### Is this architecture genuinely agent-operable for the M7 demo?

**Conditional Yes.**

For the M7 demo as scoped (Python library interface, not REST API), the architecture is ready to validate the core thesis. The agent chain works end-to-end. The explanation model is sound. The scenario system is correctly designed. An agent running against the Python library can produce a coherent, defensible overnight brief.

For a REST API-based agent in a production-adjacent context, three changes are required before reliability can be claimed.

---

### The 3 Most Important Changes

#### Change 1 — Clarify and de-risk async scenario compute

**Problem:** The agent cannot distinguish a synchronous simulation result from a partial/stale `computing` state. Acting on stale data produces wrong recommendations with no error signal.

**Fix:**
- `POST /simulate` must explicitly declare: synchronous (returns result in body) or asynchronous (returns `{scenario_id, status: "computing", poll_url: "/scenarios/{id}/status"}`).
- Add `GET /scenarios/{id}/status` → `{status, computed_at, baseline_snapshot_id}`.
- Add `stale: bool` to every comparison response.
- Agent must not read or reason from a scenario in `computing` or `stale` state.

**Effort:** 1-2 days. No engine changes required.

#### Change 2 — Add cross-location inventory query

**Problem:** Cross-location arbitrage (a core use case in M7 Scenario 5) requires 5 serial API calls + agent-side synthesis. This is fragile, token-expensive, and arithmetic-risky.

**Fix:**
```
GET /inventory/portfolio?item_id=EMEC-008&scenario_id=baseline
```
Returns all location states for this SKU in a single response:
```json
{
  "item_id": "EMEC-008",
  "locations": [
    {"location_id": "LOC-CHI", "days_of_supply": 48, "excess_qty": 220, "urgency": "low"},
    {"location_id": "LOC-DAL", "days_of_supply": 2.1, "urgency": "critical"}
  ],
  "transfer_eligible": [{"from": "LOC-CHI", "to": "LOC-DAL", "max_qty": 220}]
}
```

The agent gets the arbitrage signal in one call. The `transfer_eligible` field can be computed by the engine (it knows location-to-location transfer rules and lead times).

**Effort:** 3-5 days. Requires engine-level cross-location awareness (likely already exists given the graph model).

#### Change 3 — Structured escalation endpoint

**Problem:** The overnight agent identifies escalations in prose only. There is no machine-trackable record that a given shortage requires human decision before a specified deadline. Escalations can be lost.

**Fix:**
```
POST /escalations
{
  "type": "no_active_supplier" | "unavoidable_stockout" | "budget_exceeded",
  "shortage_node_id": "...",
  "impacted_skus": [...],
  "created_by": "agent-run-id",
  "requires_response_by": "2026-04-04T08:00:00Z",
  "blocking_reason": "..."
}

GET /escalations?status=open&created_after=2026-04-04T00:00:00Z
```

The planner's morning dashboard shows open escalations as first-class items, not as items buried in a markdown file. If no response is recorded by `requires_response_by`, an alert fires.

**Effort:** 2-3 days. Purely additive — no engine changes. Similar to an issues tracker, not a planning system.

---

## Agent-Operability Showstoppers (Pre-Production)

These must be resolved before any agent is allowed to run against a live baseline:

| # | Showstopper | Severity | Status |
|---|-------------|----------|--------|
| S1 | PolicyCheck causal steps with `node_id: null` — unverifiable facts | HIGH | Not resolved |
| S2 | Intra-bucket shortage blind spot in weekly zone (ADR-002d open issue) | HIGH | Explicitly deferred — needs date |
| S3 | No `stale` indicator in scenario comparison responses | HIGH | Not implemented |
| S4 | `graph_fragment` not opt-in — token budget destruction at scale | MEDIUM | Not specified |
| S5 | `severity_score` not numeric — agent cannot rank within severity tier | MEDIUM | Not implemented |

---

## Summary Scorecard

| Dimension | Score | Notes |
|-----------|-------|-------|
| API design for agents | 7/10 | Solid foundation; 5 gaps identified |
| Explanation quality | 8/10 | Correct design; PolicyCheck null node is the weak point |
| Scenario operations | 8/10 | Well-designed; async ambiguity is the risk |
| Temporal model | 7/10 | Correct; zone boundary confusion + intra-bucket blind spot |
| Token efficiency | 6/10 | Workable at M7 scale; graph_fragment is a time bomb |
| Multi-step reasoning | 8/10 | Chain works; recommend endpoint missing |
| Human-in-the-loop | 5/10 | Escalation is prose-only; needs machine-trackable primitive |
| Failure mode resilience | 6/10 | Good structural grounding; arithmetic and loop risks remain |
| Competitive position | 9/10 | Genuinely ahead of Kinaxis and SAP on agent-native design |
| **Overall** | **7.1/10** | **Conditional Yes for M7; 3 changes needed for production** |

---

*Review authored by: Architecture AI Review | Date: 2026-04-04 | Status: For team review*  
*Next action: Nico to arbitrate on Change 1 (sync vs async) and Change 3 (escalation endpoint priority vs. M7 timeline)*
