# Autonomous Supply Chain Operations

> Status: strategic target
> Date: 2026-05-25
> Scope: product, business model, agent operating model, proof criteria

This document turns the Ootils thesis into a concrete product direction:
operate a large supply chain with a small human team by running a fleet of
specialized agents on top of a deterministic, fast, explainable planning
engine.

The target is not "a chatbot for planners". The target is a new operating
model where agents continuously monitor, diagnose, simulate, rank, and draft
actions, while humans supervise exceptions and irreversible decisions.

## 1. Core Business Thesis

Traditional planning organizations spend most of their time on work that is
not true judgment:

- monitoring dashboards and exception lists;
- reconciling ERP, APS, WMS, Excel, and BI outputs;
- identifying the root cause of shortages;
- preparing what-if scenarios;
- recalculating the impact of parameter or supply changes;
- writing reports for S&OP, supply review, customer service, and purchasing;
- chasing bad master data and stale transactional data.

Ootils should absorb that work.

The business promise:

> One planner supervises 10x more scope because Ootils agents absorb
> monitoring, diagnosis, simulation, and action preparation.

The long-term economic target:

> Run a large operational supply chain with 70-90% less manual planning
> effort, while improving response time and auditability.

This should not be sold as "fire 90% of planners" in early conversations.
The safer message is:

> Convert planners into supply-chain mission controllers.

## 2. Why Ootils Can Do This

Agents alone are not enough. A generic LLM agent connected to ERP data will
scale confusion. The missing layer is a deterministic supply-chain compute
substrate with these properties:

- graph-native dependency model;
- event-driven propagation;
- cheap scenario branching;
- deterministic shortage and projection logic;
- causal explanations;
- direct API/gRPC access;
- low-latency read and simulation paths;
- auditable event and calc-run history.

The Rust in-memory engine is the business unlock. It changes Ootils from a
planning API into an operating substrate. Agents can run many small
simulations because scenario and propagation cost no longer force a human
batch process.

## 3. Target Operating Model

The target operating loop is continuous:

1. Observe: ingest ERP/WMS/MES/customer/supplier changes, update graph state,
   and stream deltas to agents.
2. Detect: identify shortages, excess, capacity risks, stale data, incoherent
   parameters, supplier slippage, and demand anomalies.
3. Diagnose: build causal paths, identify likely root causes, and quantify
   severity, affected customers, revenue, margin, and service risk.
4. Simulate: fork scenarios, test corrective actions, and compare baseline vs
   candidate plans.
5. Recommend: rank actions by business objective and draft recommendations
   with evidence and side effects.
6. Govern: auto-apply only low-risk actions inside policy; require human
   approval for material, supplier-facing, customer-facing, financial, or
   irreversible actions.
7. Learn: measure accepted/rejected recommendations and tune policies,
   thresholds, and agent routing without making the deterministic engine
   stochastic.

## 4. Agent Fleet

The first product should not launch with one generalist agent. It should launch
with a small fleet of narrow agents, each owning a business surface.

### 4.1 Watcher Agents

Read-heavy agents that continuously monitor the state:

- Shortage Watcher: top shortages, new shortages, resolved shortages.
- Service Risk Watcher: customer orders at risk, promised-date violations.
- Supply Watcher: late POs, weak suppliers, missing confirmations.
- Inventory Watcher: excess, obsolete, negative stock, stranded stock.
- Capacity Watcher: overloaded resources, RCCP/CRP bottlenecks.
- Import Watcher: batch freshness, failed rows, missing files, late feeds,
  duplicate loads, source-system silence.
- Data Quality Watcher: missing master data, bad lead times, invalid calendars.
- Parameter Watcher: safety stock, MOQ, lot sizing, lead-time drift.

These agents should be event-driven. They should not poll the full graph every
few seconds unless a benchmark proves this is cheap enough. The engine should
emit deltas, and agents should maintain scoped working sets.

### 4.2 Scenario Agents

Agents that generate and test corrective actions:

- Expedite Agent: tests earlier PO dates or alternate suppliers.
- Reallocation Agent: tests moving stock between locations.
- Substitution Agent: tests alternate components or BOM variants.
- Capacity Shift Agent: tests overtime, alternate work centers, sequencing.
- Lot Size Agent: tests MOQ/lot sizing adjustments.
- Demand Shaping Agent: tests allocation or promise-date alternatives.

Scenario agents must always write their work into explicit scenarios. They
must never mutate baseline directly.

### 4.3 Governance Agents

Agents that decide whether a recommendation is safe to present or apply:

- Policy Agent: checks whether a proposed action is allowed.
- Finance Agent: estimates working-capital and margin impact.
- Customer Agent: ranks customer/service impact.
- Supplier Agent: checks supplier feasibility and commercial risk.
- Audit Agent: verifies traceability, data freshness, and explanation quality.

These agents are the difference between "AI suggestions" and an operational
control system.

### 4.4 Orchestrator Agent

The orchestrator is not the smartest agent. It is the traffic controller.

Responsibilities:

- assign issues to specialized agents;
- deduplicate overlapping investigations;
- stop runaway scenario loops;
- enforce budgets per agent and per business cycle;
- consolidate recommendations;
- escalate only the decisions that matter.

The orchestrator should optimize for planner attention, not for number of
agent actions.

## 5. Decision Ladder

Every action must live on a risk ladder.

| Level | Class | Examples | V1 policy |
|---|---|---|---|
| L0 | Read only | Query shortages, explain shortage, inspect projection | Fully autonomous |
| L1 | Draft recommendation | Propose expedite, transfer, parameter review | Fully autonomous, stays DRAFT |
| L2 | Internal low-risk action | Create scenario, refresh analysis, update draft | Auto-allowed if policy passes |
| L3 | Planning state mutation | Merge scenario, change parameter, approve export | Human approval |
| L4 | External execution | Push PO to ERP, send customer promise, alter schedule | Human approval only |

## 6. Product Wedge

The first sellable wedge should be:

> Autonomous shortage control tower with scenario-backed recommendations.

Why this wedge:

- shortage pain is obvious to business buyers;
- impact is measurable;
- Ootils explainability matters immediately;
- scenario speed is visible in demos;
- recommendations can stay human-approved, reducing adoption risk.

First workflow:

1. Ingest a realistic supply-chain dataset.
2. Agents continuously identify top service risks.
3. For each risk, agents explain root cause.
4. Scenario agents test three to five corrective actions.
5. Orchestrator ranks actions by service impact, cost, and feasibility.
6. Human approves or rejects recommendations.
7. Ootils records outcome and rationale.

## 7. Import and Data Quality Monitoring

Import and DQ monitoring are not support features. They are control-plane
features. If agents operate on stale or corrupted data, the system only makes
bad decisions faster.

### 7.1 Import Monitoring

The Import Watcher owns the health of inbound data flows.

It monitors:

- expected files or API batches by source system and entity;
- source freshness versus SLA;
- batch success/failure state;
- rejected row count and rejection reasons;
- duplicate idempotency keys or duplicate business records;
- row-count drift versus historical baseline;
- schema drift in inbound payloads;
- partial loads and missing entity dependencies;
- processing latency from received to approved;
- downstream propagation triggered by the import.

Example alerts:

- "SAP purchase orders feed is 42 minutes late."
- "WMS on-hand load succeeded but row count is 37% below normal."
- "Supplier-item import failed on 128 rows because preferred_supplier is missing."
- "Forecast import was accepted but did not trigger propagation."

The Import Watcher should create operational incidents, not supply-chain
recommendations. Its output is about data-flow reliability.

### 7.2 Data Quality Monitoring

The Data Quality Watcher owns semantic trust in the graph.

It monitors:

- missing master data;
- impossible or suspicious lead times;
- negative or zero quantities where not allowed;
- invalid calendars or closed-day supply dates;
- missing preferred suppliers;
- unsafe MOQ / lot-sizing parameters;
- stale transactional data;
- inconsistent units of measure;
- BOM cycles or inactive components;
- demand/supply outliers;
- DQ issues whose impact score crosses an action threshold.

DQ monitoring must be linked to business impact. A missing supplier for an
inactive SKU is noise. A missing supplier for a top shortage root cause is a
planning blocker.

### 7.3 Coordination With Scenario Agents

Scenario agents must check import and DQ status before making recommendations.

Hard rules:

- no recommendation if the relevant source feed is stale beyond SLA;
- no recommendation if the root-cause path contains unresolved critical DQ;
- scenario result must carry a data-confidence label;
- low-confidence recommendations stay in `DRAFT_NEEDS_DATA_REVIEW`;
- planner approval screen must show import freshness and DQ blockers.

This keeps the agent fleet from turning bad inputs into confident outputs.

### 7.4 Metrics

Minimum import metrics:

- import batches by source/entity/status;
- latest successful import timestamp per source/entity;
- rows received, accepted, rejected;
- rejection rate by reason;
- processing latency p50/p95;
- idempotent replays and conflicts;
- propagation triggered/not triggered.

Minimum DQ metrics:

- open issues by severity and domain;
- new/resolved issues per day;
- impact-weighted DQ score;
- critical DQ issues on shortage causal paths;
- average age of unresolved critical DQ;
- recommendation blocks caused by DQ.

## 8. What 50 Agents Actually Means

Fifty agents should not mean fifty independent LLM loops scanning the entire
supply chain. That would be expensive, noisy, and hard to govern.

Fifty agents should mean:

- 10-15 persistent watcher agents by domain and segment;
- 10-20 scenario workers spawned on demand;
- 5-10 governance/audit agents;
- 1-3 orchestrators;
- temporary worker agents for bursts.

The engine should decide what changed. Agents should decide what to do about
it.

## 9. Architecture Requirements

### 8.1 Engine

- Rust in-memory baseline graph.
- Scenario fork/read/propagate/merge exposed as first-class RPCs.
- Per-scenario propagation with isolation.
- Fast read paths that bypass Postgres write-behind lag.
- QueryShortages and GetNode for scenario and baseline.
- StreamChanges for agents and UI.
- WAL-backed durability for baseline mutations.
- Clear semantics for ephemeral scenarios.

### 8.2 Agent Runtime

- Agent registry: name, role, scopes, budgets, owner.
- Job queue: issue investigation, scenario test, recommendation review.
- Work ledger: every agent action logged with input, output, tool calls,
  scenario_id, calc_run_id, and policy result.
- Budget controls: max scenarios, max tokens, max wall-clock time, max write
  actions per cycle.
- Kill switches: global stop, per-agent pause, per-scope revoke.

### 8.3 API / Tooling

- Curated MCP/tool surface, not raw OpenAPI exposure.
- Read tools are side-effect-free.
- Write tools create explicit artifacts: scenario, recommendation, policy
  check, approval request.
- Idempotency on every write.
- Per-agent auth scopes.
- Correlation IDs across API, engine, scenarios, recommendations, and audit.

### 8.4 Governance

- Recommendation state machine.
- Approval workflow.
- Audit trail.
- Policy engine.
- Import health model with source/entity freshness SLAs.
- DQ issue model with business impact and causal-path linkage.
- Explainability required for all recommendations.
- Business KPI attribution after action.

## 10. Proof Package Before Claiming 90%

Before claiming large planner-effort reduction, create a reproducible proof
package. It should be synthetic if client data is not legally available.

### 9.1 Scale Proof

Dataset:

- 25K SKU minimum.
- Multi-site network.
- Realistic transactional nodes: on-hand, POs, WOs, transfers, forecasts,
  customer orders.
- Capacity/resources if the target customer cares about production.
- Multiple active scenarios.
- Realistic import batches with late, missing, duplicate, rejected, and stale
  feeds.
- DQ defects injected into master and transactional data.

Metrics:

- graph nodes and edges;
- PI nodes;
- memory RSS;
- boot time;
- p50/p95/p99 for GetNode, QueryShortages, ForkScenario, Propagate, Merge;
- WAL growth;
- write-behind lag;
- recovery time after kill.

### 9.2 Agent Proof

Simulate 50 agents:

- 15 watchers;
- 20 scenario workers;
- 10 governance agents;
- 3 orchestrators;
- 2 reporting/audit agents.

Workload:

- sustained monitoring;
- event bursts;
- 200 concurrent user/API sessions if that is the commercial claim;
- bounded number of scenarios per issue;
- random supplier/date/demand shocks.

Success criteria:

- no scenario bleed;
- no baseline corruption;
- no unbounded queue or WAL growth;
- p95 within agreed SLO;
- recommendations trace to explanations;
- recommendations are blocked when import freshness or DQ confidence fails;
- humans see fewer, higher-quality decisions.

### 9.3 Business Proof

Measure before/after on a reference process:

- number of exceptions humans review;
- time to root cause;
- time to recommendation;
- planner touches per issue;
- accepted recommendation rate;
- false positive rate;
- share of recommendations blocked by stale imports or critical DQ;
- service risk reduced;
- inventory/cost impact;
- action audit completeness.

The planner-reduction claim is credible only after business proof, not just
engine throughput proof.

## 11. Commercial Positioning

Primary message:

> Ootils turns supply-chain planning from manual exception handling into
> autonomous, scenario-backed operations.

Technical buyer message:

> A deterministic graph engine that lets agents query, simulate, explain, and
> govern supply-chain decisions in real time.

Business buyer message:

> Fewer planners trapped in firefighting; more decisions handled continuously,
> with humans focused on judgment and approvals.

Avoid early messaging:

- "Replace your planners."
- "Fully autonomous supply chain."
- "ERP writeback without human approval."

Use instead:

- "10x planner leverage."
- "Autonomous exception management."
- "Human-approved recommendations."
- "Scenario-backed decisions."

## 12. Delivery Roadmap

### Phase A - Engine Proof

- Finish per-scenario propagation.
- Expose scenario read/propagate/merge through the Rust service.
- Validate 25K SKU benchmark.
- Validate 50-agent synthetic load.
- Lock down WAL/write-behind/recovery proof.

### Phase B - Agent Tool Surface

- MCP/tool server with curated tools.
- Per-agent auth and scopes.
- Idempotency and action ledger.
- Recommendation table and state machine.
- StreamChanges or equivalent delta feed.
- Import health API and import freshness SLAs.
- DQ confidence API tied to causal paths and recommendations.

### Phase C - First Agent Fleet

- Shortage Watcher.
- Import Watcher.
- Data Quality Watcher.
- Expedite Scenario Agent.
- Reallocation Scenario Agent.
- Policy/Audit Agent.
- Orchestrator Agent.

### Phase D - Human Control Room

- Recommendation inbox.
- Scenario comparison view.
- Approval/rejection workflow.
- Audit reports.
- KPI impact dashboard.

### Phase E - Controlled Execution

- Export approved recommendations.
- ERP connector for approved actions only.
- Role/scoped approvals.
- Post-action outcome tracking.

## 13. Non-Negotiables

- Deterministic engine. LLMs do not own core calculations.
- No direct ERP mutation without explicit approval in early versions.
- Every recommendation must cite evidence.
- Every recommendation must show import freshness and DQ confidence.
- Every scenario must be isolated.
- Every agent write must be auditable.
- Agents must have budgets and kill switches.
- The product must optimize planner attention, not maximize automation for
  its own sake.

## 14. Strategic Conclusion

The biggest opportunity is not to build a better planning screen. It is to
build the substrate for autonomous supply-chain operations.

If Ootils proves that 50 agents can continuously monitor, simulate, and rank
actions on a 25K SKU supply chain with strong audit and governance, then the
business outcome is plausible: a large planning function run by a much smaller
team.

The near-term objective is therefore clear:

> Build the proof that agentic planning is operationally safe, economically
> useful, and technically faster than human-led planning cycles.
