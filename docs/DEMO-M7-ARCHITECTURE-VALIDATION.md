# M7 Architecture-Validation Demo Design
## Ootils Core — AI Agent Proof Demo

> **Milestone:** M7 — AI Agent Demo (V1 proof)
> **Goal:** Demonstrate that the engine is architecturally capable of supporting autonomous agent operations at realistic scale. Not a tutorial. Not a toy. A proof.

---

## 1. The Demo Story

### Setting

**MidCo Electronics Distribution** is a $180M/year B2B distributor of industrial electronics components — passive components, microcontrollers, connectors, power management ICs, and specialty custom parts. They have five stocking locations across North America and manage ~100 active SKUs across a 3-tier supplier network.

It's 23:00. The daily replenishment window opens. A human planner would normally spend 3 hours reviewing dashboards, cross-referencing POs, and drafting a replenishment proposal for morning approval. Instead, an AI agent runs the overnight cycle.

### The Overnight Cycle (What the Agent Actually Does)

The agent starts with no knowledge of what needs attention. It receives a snapshot of 100 SKUs × 5 locations — 500 inventory states — and a list of active suppliers. It must:

1. **Triage the full portfolio** — scan all 500 states, classify urgency, surface what needs action
2. **Investigate the critical items** — for each CRITICAL/HIGH item, retrieve the full rationale chain and understand _why_
3. **Detect the disruption** — one supplier was flagged inactive earlier in the day (the agent doesn't know which one); it must discover the impact
4. **Run counterfactuals** — for the two most impacted SKUs, simulate alternative supplier selections and compare outcomes
5. **Build the PO proposal** — given a $75k weekly purchasing budget, rank and select the orders to place, explain trade-offs, and flag what gets deferred and why
6. **Produce the handoff brief** — a structured, human-readable summary that a planner can approve in 10 minutes rather than 3 hours

The demo passes when a non-technical observer watches the agent work and says: _"That's what a good planner would do."_

---

## 2. Dataset Shape

### 2.1 Locations (5)

| ID | Name | Role | Demand Profile |
|----|------|------|----------------|
| `LOC-CHI` | Chicago DC | Primary distribution center | High volume, stable demand |
| `LOC-ATL` | Atlanta Hub | Regional fulfillment, Southeast | Moderate volume, seasonal peaks |
| `LOC-DAL` | Dallas Hub | Regional fulfillment, Southwest | Moderate volume, energy-sector spikes |
| `LOC-SEA` | Seattle Hub | Regional fulfillment, Pacific NW | Lower volume, tech-sector clients |
| `LOC-TOR` | Toronto DC | Canada cross-border | Lower volume, regulatory lead time premium |

### 2.2 SKU Catalog (100 SKUs)

Five product families, each with distinct demand and supply characteristics:

#### Family A — Passive Components (30 SKUs: `PASS-001` to `PASS-030`)
- Unit costs: $0.05–$2.50
- Daily demand: 50–500 units/day per location
- Demand CoV: 0.15–0.35 (stable but not trivial)
- Lead time: 7–14 days
- Ordering cost: $25–$50
- Service level: 0.95
- Suppliers: 2–3 per SKU (commodity market)
- Representative SKUs: ceramic capacitors, film resistors, ferrite beads

#### Family B — Active Components (25 SKUs: `ACTI-001` to `ACTI-025`)
- Unit costs: $1.50–$45.00
- Daily demand: 5–80 units/day per location
- Demand CoV: 0.25–0.50 (higher variability, project-driven)
- Lead time: 14–28 days
- Ordering cost: $75–$150
- Service level: 0.97
- Suppliers: 1–2 per SKU (concentrated supply)
- Representative SKUs: STM32 MCUs, op-amps, gate drivers, power MOSFETs

#### Family C — Electromechanical (20 SKUs: `EMEC-001` to `EMEC-020`)
- Unit costs: $0.50–$8.00
- Daily demand: 10–120 units/day per location
- Demand CoV: 0.20–0.40
- Lead time: 10–21 days
- Ordering cost: $40–$80
- Service level: 0.95
- Suppliers: 2 per SKU
- Representative SKUs: JST connectors, DIN rail terminals, tactile switches

#### Family D — Power Components (15 SKUs: `POWR-001` to `POWR-015`)
- Unit costs: $3.00–$25.00
- Daily demand: 8–60 units/day per location
- Demand CoV: 0.20–0.35
- Lead time: 14–21 days
- Ordering cost: $60–$120
- Service level: 0.97
- Suppliers: 2 per SKU
- Representative SKUs: buck converters, electrolytic capacitors (high-temp), power inductors

#### Family E — Specialty / Long-Lead (10 SKUs: `SPEC-001` to `SPEC-010`)
- Unit costs: $15.00–$180.00
- Daily demand: 1–15 units/day per location
- Demand CoV: 0.40–0.70 (high variability, custom/project orders)
- Lead time: 28–56 days
- Ordering cost: $150–$300
- Service level: 0.99
- Suppliers: 1 per SKU (sole-source or near sole-source)
- Representative SKUs: custom ASICs, RF modules, radiation-tolerant components

### 2.3 Supplier Pool (15 Suppliers)

| ID | Name | Specialty | Lead Time | Reliability | Price Multiplier |
|----|------|-----------|-----------|-------------|-----------------|
| `SUP-AVN` | Avnet Direct | Broad catalog | 7–14 days | 0.96 | 1.00 |
| `SUP-DKY` | DigiKey Fulfillment | Broad catalog | 3–7 days | 0.99 | 1.08 |
| `SUP-MOU` | Mouser Electronics | Broad catalog | 3–7 days | 0.98 | 1.07 |
| `SUP-ARW` | Arrow Electronics | Active ICs | 10–21 days | 0.94 | 0.97 |
| `SUP-TTI` | TTI Inc | Passive/EM | 7–14 days | 0.97 | 0.98 |
| `SUP-WRD` | Würth Direct | Passives only | 10–14 days | 0.95 | 0.93 |
| `SUP-VEN` | VendorX (contract) | Power only | 14–21 days | 0.88 | 0.85 |
| `SUP-FTG` | Future Electronics | Active ICs | 14–28 days | 0.92 | 0.96 |
| `SUP-PCC` | Chip1Stop | Specialty ICs | 21–35 days | 0.85 | 1.05 |
| `SUP-ALP` | AlphaElec | SPEC family | 28–42 days | 0.90 | 1.00 |
| `SUP-BTA` | BetaSource | SPEC family | 35–56 days | 0.82 | 0.94 |
| `SUP-LCL-CHI` | Local Broker CHI | Emergency | 2–3 days | 0.91 | 1.35 |
| `SUP-LCL-ATL` | Local Broker ATL | Emergency | 2–3 days | 0.89 | 1.38 |
| `SUP-LCL-DAL` | Local Broker DAL | Emergency | 2–3 days | 0.90 | 1.37 |
| `SUP-LCL-SEA` | Local Broker SEA | Emergency | 1–2 days | 0.93 | 1.42 |

**Critical design detail:** SKUs in Family B and E have only 1–2 eligible suppliers by product family. When a primary supplier is flagged inactive, the fallback selection changes urgency classifications for a non-trivial number of SKUs.

### 2.4 Inventory State Generation Rules

States are generated procedurally to ensure each scenario has the right distribution of urgencies:

**Baseline distribution across 500 states:**
- ~5% CRITICAL (≤25 states): current_stock ≈ 0–0.5 × safety_stock
- ~12% HIGH (≤60 states): current_stock ≈ 0.5–1.0 × safety_stock
- ~25% MEDIUM (~125 states): current_stock ≈ ROP to 1.2 × ROP
- ~58% LOW (~290 states): current_stock > 1.2 × ROP

**Scenario-specific mutations** (applied on top of baseline, documented per scenario in Section 3).

---

## 3. Scenario Set

### Scenario 0 — Baseline: Overnight Cycle (Control)

**Purpose:** Establishes the "normal" baseline. Agent runs the full portfolio, makes routine recommendations.

**State:** All suppliers active. Demand at nominal values. ~5% critical, ~12% high.

**What the agent does:**
1. Calls `evaluate_portfolio` across all 500 states
2. Returns sorted list of ~85 recommendations (those at or below ROP)
3. Segments by urgency: surfaces CRITICAL items first
4. For each CRITICAL item: calls `assess_risk` + `recommend_order` with full supplier list
5. Builds preliminary PO list with quantities and supplier assignments

**Key output:** A PO list with ~40–50 line items totaling ~$65–80k in order value.

**Architecture point validated:** `evaluate_portfolio` handles 500 states correctly, sorting is deterministic, rationale strings are populated and parseable.

---

### Scenario 1 — Demand Surge: Seattle Tech Spike

**Purpose:** A major Seattle-area customer pulled in a project order. Demand at `LOC-SEA` doubles for Family B (Active ICs) for the next 3 weeks.

**State mutation:** For all ACTI-* SKUs at LOC-SEA: `daily_demand` × 2.0, `current_stock` unchanged.

**Expected behavior:**
- 8–12 SKUs at LOC-SEA flip from LOW/MEDIUM to HIGH/CRITICAL
- EOQ increases (higher annual demand → larger optimal batch)
- ROP increases (higher daily demand × same lead time)
- Agent must identify that the Seattle surge is driving a cluster of issues (not random)

**What the agent does:**
1. Receives mutated states for LOC-SEA ACTI-* SKUs
2. Runs `assess_risk` on each — urgency reclassification is visible
3. Runs `recommend_order` — notices EOQ has jumped vs. baseline; calls `calculate_eoq` to surface the cost math
4. Flags that 3 suppliers for Active ICs have max_order_quantity constraints that cap the single-order fill; recommends split orders across two suppliers

**Architecture point validated:** EOQ formula responds correctly to demand changes; supplier constraints (min/max_order_quantity) are applied and visible in rationale.

---

### Scenario 2 — Supplier Disruption: Primary IC Supplier Goes Inactive

**Purpose:** `SUP-ARW` (Arrow Electronics) is flagged inactive due to credit hold. It's the primary or only supplier for 9 ACTI-* and 3 POWR-* SKUs.

**State mutation:** `SUP-ARW.active = False`. No stock changes.

**Expected behavior:**
- 9 Active IC SKUs lose their cheapest/fastest primary supplier
- Fallback selections are more expensive (e.g., DigiKey at 1.08× or Future at longer lead time)
- Longer lead times increase safety stock requirements → some SKUs that were MEDIUM become HIGH
- Sole-source `ACTI-017` and `ACTI-023` have NO active supplier → engine raises ValueError → agent must handle gracefully

**What the agent does:**
1. Runs portfolio scan with `SUP-ARW` inactive
2. Discovers 2 SKUs where `recommend_order` returns `{"status": "error", "error": "...no active supplier..."}` — logs them as BLOCKER items requiring human escalation
3. For the remaining impacted SKUs: runs `rank_suppliers` to show the delta between old best supplier and new best supplier
4. Computes dollar impact: sum of (new_price - old_price) × order_quantity across affected SKUs = ~$3,200 incremental cost this cycle
5. Produces a "supplier disruption brief": which SKUs are affected, which escalate to CRITICAL due to longer fallback lead times, estimated cost impact

**Architecture point validated:** Supplier inactivity correctly propagates through selection. `rank_suppliers` exposes the scoring math. Error handling for no-supplier condition is tested at scale.

---

### Scenario 3 — Cascading Shortage: Sole-Source Long Lead Time

**Purpose:** `SPEC-004` (a custom RF module, $95/unit, 42-day lead time, sole-source `SUP-ALP`) has zero open orders and current stock at 8 units. Daily demand is 3 units. No one noticed because it was never flagged before.

**State:** `current_stock = 8`, `open_order_quantity = 0`, `daily_demand = 3.0`, `demand_std_daily = 1.8`, `lead_time_days = 42`, `service_level = 0.99`.

**Expected computation:**
- Days of supply: 8 / 3 = 2.67 days → CRITICAL
- Safety stock: z(0.99)=2.326 × √(42×1.8² + 3²×4²) ≈ 2.326 × √(136.1 + 144) ≈ 2.326 × 16.7 ≈ 38.9 units
- ROP: 3×42 + 38.9 = 164.9 units
- EOQ: √(2 × 3×365 × 250 / (95×0.25)) ≈ √(547,500 / 23.75) ≈ 152 units
- Agent recommends ordering 152 units from SUP-ALP; expected arrival in 42 days; stockout in 2.67 days → gap of ~39 days with zero stock

**What the agent does:**
1. Identifies SPEC-004 as CRITICAL in portfolio scan
2. Calls `assess_risk` → days_of_supply = 2.67, lead_time_coverage_adequate = False
3. Calls `recommend_order` → CRITICAL, order 152 units from SUP-ALP
4. Flags the 39-day gap to the human planner: "Stock-out is unavoidable within 3 days. The order will arrive in 42 days. Recommend contacting account managers for affected customers immediately."
5. Checks local brokers — `SUP-LCL-*` don't carry SPEC family → no emergency source
6. Checks if any sibling location has excess SPEC-004 → LOC-TOR has 22 units (LOW urgency) → recommends emergency lateral transfer of 8 units + place PO for 152

**Architecture point validated:** The engine correctly classifies a situation where even placing an order cannot prevent a stockout. The agent's job is to surface this, not pretend it can fix it. Explainability is the value here.

---

### Scenario 4 — Budget Allocation: $75k Weekly PO Cap

**Purpose:** Finance has set a $75k weekly purchasing budget. The baseline scenario generates ~$68k in recommended orders, but Scenario 1 (demand surge) adds ~$28k more. Total demand is ~$96k. The agent must prioritize.

**State:** Overlay of Scenario 0 + Scenario 1 + Scenario 3 (the hard cases). Total unconstrained PO value: ~$96k.

**What the agent does:**
1. Runs full portfolio scan, collects all recommendations with order values (order_quantity × effective_unit_cost)
2. Builds a prioritized list using: urgency rank (CRITICAL first) then dollar risk (= unit_cost × daily_demand × max(0, lead_time_days - days_of_supply))
3. Greedy allocation: fills CRITICAL items first (they must be funded regardless), then HIGH, then MEDIUM until budget exhausted
4. At ~$75k, remaining budget: ~$0. Items deferred: 12 MEDIUM items totaling ~$21k
5. Produces two outputs:
   - **Approved PO list**: items, quantities, suppliers, total $74,850
   - **Deferred list**: items ranked by next-breach date (days until they cross into HIGH urgency)
6. Flags one MEDIUM item (`POWR-007` at LOC-ATL) that will breach into HIGH within 4 days → recommends moving it into this cycle's budget even if it means deferring two LOW items

**Architecture point validated:** The agent uses the engine outputs (not hardcoded heuristics) to build a budget-constrained plan. The deferred list with "days to next escalation" is derived from the rationale metadata.

---

### Scenario 5 — Cross-Location Arbitrage: Overstock vs. Stockout

**Purpose:** `EMEC-008` (a connector, $3.20/unit) has a stockout risk at `LOC-DAL` (CRITICAL, 2.1 days of supply) but `LOC-CHI` holds 4,200 units (LOW, 48 days of supply vs. ROP of 22 days). Transfer + freight cost = $0.45/unit. A new PO from the primary supplier would cost $3.20 × EOQ at LOC-DAL and arrive in 10 days.

**Expected math:**
- LOC-DAL needs ~180 units to cover lead time and safety stock
- Transfer from LOC-CHI: 180 units × ($3.20 + $0.45) = $657 and arrives in 2 days
- New PO: 180 units × $3.20 = $576 and arrives in 10 days (too late for CRITICAL)
- Delta: $81 more for transfer, but 8 days earlier → avoids stockout

**What the agent does:**
1. Identifies LOC-DAL EMEC-008 as CRITICAL
2. Attempts standard `recommend_order` → recommends new PO, but notes lead_time_coverage_adequate = False
3. Checks sibling location states for same SKU → LOC-CHI has excess (days_of_supply >> ROP)
4. Computes transfer economics: compares (transfer_cost × qty) vs. (new_PO_cost × qty) AND timing
5. Recommendation: "Initiate emergency transfer of 180 units from LOC-CHI (estimated $657, arrives in 2 days). Also place standard EOQ replenishment order for LOC-CHI ($3.20 × 280 units = $896) to rebuild its buffer, supplier: TTI, ETA 12 days."
6. Net result: resolves CRITICAL at LOC-DAL, maintains safety stock at LOC-CHI, total cost $1,553 vs. $1,152 for a delayed PO — agent explains the $401 premium is the cost of availability insurance

**Architecture point validated:** The engine handles cross-location logic when the agent has access to all location states simultaneously. The `no_action` signal from LOC-CHI (stock adequate) contrasted with the CRITICAL signal at LOC-DAL is the trigger the agent uses to initiate the transfer logic.

---

## 4. Agent Tasks and Expected Outputs

### 4.1 Task Structure

The agent is implemented as a Python script using `SupplyChainTools` directly (M7 uses the library interface; no REST API required). It operates as a deterministic loop with structured JSON outputs at each step.

```
agent_run/
├── 00_portfolio_scan.json       # Full 500-state scan results
├── 01_critical_items.json       # CRITICAL/HIGH items with full rationale
├── 02_supplier_disruption.json  # Disruption analysis (Scenario 2)
├── 03_shortage_cascade.json     # SPEC-004 analysis (Scenario 3)
├── 04_budget_allocation.json    # PO proposal under $75k cap (Scenario 4)
├── 05_arbitrage_analysis.json   # LOC-CHI → LOC-DAL transfer (Scenario 5)
└── 06_handoff_brief.md          # Human-readable summary
```

### 4.2 Detailed Agent Task Specifications

#### Task 1: Portfolio Triage

**Tool calls:** `evaluate_portfolio` (via direct engine) + `assess_risk` per CRITICAL item

**Input:** 500 `InventoryState` objects + 15 `Supplier` objects

**Output:**
```json
{
  "scan_timestamp": "2026-04-01T23:00:00Z",
  "total_states": 500,
  "recommendations_generated": 87,
  "by_urgency": {
    "critical": 8,
    "high": 19,
    "medium": 60,
    "low": 0
  },
  "top_critical": [
    {
      "sku": "SPEC-004",
      "location": "LOC-CHI",
      "days_of_supply": 2.67,
      "recommended_order": 152,
      "supplier": "SUP-ALP",
      "urgency": "critical",
      "rationale": "..."
    }
  ],
  "portfolio_order_value_usd": 67842.50
}
```

**Acceptance threshold:** All 500 states evaluated; urgency distribution within ±10% of expected; total order value within ±15% of expected range.

#### Task 2: Supplier Disruption Analysis

**Tool calls:** `rank_suppliers` (before/after for each impacted SKU), `recommend_order` with SUP-ARW inactive

**Output:**
```json
{
  "disrupted_supplier": "SUP-ARW",
  "impacted_skus": 12,
  "blockers": [
    {"sku": "ACTI-017", "reason": "no_active_supplier", "escalate": true},
    {"sku": "ACTI-023", "reason": "no_active_supplier", "escalate": true}
  ],
  "urgency_escalations": [
    {"sku": "ACTI-009", "from": "medium", "to": "high", "reason": "fallback_lead_time_delta_days": 14}
  ],
  "incremental_cost_usd": 3187.40,
  "recommended_actions": [...]
}
```

#### Task 3: Budget-Constrained PO Proposal

**Tool calls:** Multiple `recommend_order` calls, then agent-side ranking and allocation

**Output:**
```json
{
  "budget_usd": 75000,
  "approved_orders": {
    "count": 43,
    "total_usd": 74850.20,
    "by_urgency": {"critical": 8, "high": 19, "medium": 16}
  },
  "deferred_orders": {
    "count": 44,
    "total_usd": 21203.80,
    "soonest_escalation": {"sku": "POWR-007", "location": "LOC-ATL", "days_to_high": 4}
  }
}
```

#### Task 4: Handoff Brief (Human-Readable)

The agent produces a markdown brief structured as:

```markdown
## Overnight Replenishment Brief — 2026-04-02

### 🔴 Immediate Escalations (Human Decision Required)
1. **SPEC-004 / LOC-CHI**: Unavoidable stockout in 2.7 days. 42-day lead time.
   PO placed for 152 units ($14,440). Notify account managers for...
2. **ACTI-017**: No active supplier (SUP-ARW on hold). Sole-source.
   Action needed: Credit resolution or spot market approval.

### 🟠 Orders Placed (Budget: $74,850 / $75,000)
- 43 purchase orders across 8 suppliers
- Largest: ACTI-005 / LOC-DAL — 320 units from SUP-DKY ($12,480)
- All CRITICAL and HIGH items funded.

### 🟡 Deferred (Budget Exhausted)
- 44 MEDIUM items deferred. Next breach: POWR-007/ATL in 4 days.
- Recommend expedited approval of $4,200 for POWR-007 before next cycle.

### ↔️ Lateral Transfer Recommendation
- EMEC-008: Transfer 180 units CHI → DAL ($657). Avoids stockout at DAL.
  Standard replenishment PO for CHI placed ($896).

### Summary
- Scenarios processed: 5 locations × 100 SKUs = 500 states
- Engine runtime: [X]ms
- Recommendations actioned: 43 orders + 1 transfer
- Human decisions required: 2 (escalations)
```

---

## 5. What Makes This Genuinely Convincing (Not Toy Theater)

### 5.1 The Decisions Are Emergent, Not Scripted

The demo script does not hardcode which SKUs are CRITICAL. It generates the inventory states from the dataset definition and runs the engine. Every urgency classification, every EOQ, every supplier selection is computed. If you change one parameter in the dataset generator, the outputs change. A skeptic can perturb the data and watch the agent respond differently.

### 5.2 The Scale Is Real

500 states is not an academic toy. It's a small-to-midsize real distributor. The runtime should complete the full portfolio scan in under 500ms (pure Python, no I/O). This proves the engine can serve a real planning cycle in time.

### 5.3 The Failure Cases Are Honest

Scenario 3 shows an unavoidable stockout. The agent does not pretend it can fix it. It surfaces the problem, quantifies the gap, explains why, and escalates. This is the difference between a demo that hides hard cases and an architecture that's genuinely ready for production.

### 5.4 The Supplier Disruption Changes Real Outcomes

When `SUP-ARW` goes inactive, the outcomes change in a way that's mathematically traceable. The agent calls `rank_suppliers` before and after and shows the scoring delta. The urgency escalations from longer fallback lead times are not hardcoded — they fall out of the safety stock formula because longer lead times → higher ROP → existing stock may now be below ROP. This chain of causality is the architecture proof.

### 5.5 The Rationale Is Traversable

Every `recommend_order` call returns a `rationale` string and a `metadata` dict. The agent uses those fields — not assumptions — to build the handoff brief. The brief quotes specific numbers from the rationale. If you cross-check the brief against the engine outputs, every number matches. No summarization hallucination possible — the agent only writes what the engine returned.

### 5.6 The Budget Constraint Forces Real Trade-offs

The allocation algorithm in Task 3 is not trivial. The agent must sort by (urgency, dollar risk), fill greedily, flag the near-breach deferred item, and explain the POWR-007 exception. This is a real planning heuristic encoded in agent logic, not a demo hack.

### 5.7 Cross-Location Arbitrage Shows Multi-State Awareness

Scenario 5 only works if the agent queries all five location states for the same SKU simultaneously. This demonstrates the portfolio interface as the right abstraction: a single agent call across all locations finds the arbitrage opportunity that a single-location tool would miss.

---

## 6. Acceptance Criteria

The M7 demo passes when **all** of the following are true:

### Functional Correctness

| # | Criterion | How to Verify |
|---|-----------|---------------|
| F1 | `evaluate_portfolio` returns sorted results for 500 states with no missing items | Assert `len(recommendations) == expected_count`; assert sort order |
| F2 | Urgency distribution matches expected ranges (±10%) | Assert counts per urgency bucket |
| F3 | SPEC-004 is classified CRITICAL with `days_of_supply < 3` | Assert specific value from engine output |
| F4 | Safety stock for SPEC-004 matches the formula to within ±0.5 units | Assert `abs(ss_computed - ss_expected) < 0.5` |
| F5 | Deactivating `SUP-ARW` causes at least 2 SKUs to have no active supplier (error state) | Assert `status == "error"` for ACTI-017, ACTI-023 |
| F6 | At least 3 SKUs escalate urgency when `SUP-ARW` is deactivated due to longer fallback lead times | Assert urgency delta for specific SKU list |
| F7 | Budget allocation stays within $75k cap | Assert `sum(order_values) <= 75000` |
| F8 | Deferred list correctly identifies POWR-007 as next breach | Assert `soonest_escalation.sku == "POWR-007"` |
| F9 | Cross-location arbitrage is triggered for EMEC-008 when LOC-CHI is LOW and LOC-DAL is CRITICAL | Assert transfer recommendation exists in agent output |
| F10 | All tool calls return `{"status": "ok"}` or `{"status": "no_action"}` (no unexpected errors) | Assert no `"error"` status in expected-passing calls |

### Agent Behavior

| # | Criterion | How to Verify |
|---|-----------|---------------|
| A1 | Agent produces a structured handoff brief with all required sections | Assert markdown sections present and non-empty |
| A2 | Every number in the handoff brief is traceable to an engine output | Manual spot-check: 5 random numbers from brief cross-referenced to JSON outputs |
| A3 | Agent escalates the 2 no-supplier blockers explicitly with `escalate: true` | Assert in `02_supplier_disruption.json` |
| A4 | Agent does not make up supplier names, order quantities, or cost figures | Deterministic test: same inputs → same outputs across 3 runs |

### Scale & Performance

| # | Criterion | How to Verify |
|---|-----------|---------------|
| P1 | Full portfolio scan (500 states) completes in < 500ms | `time.perf_counter()` wrap around `evaluate_portfolio` |
| P2 | All 6 scenario outputs generated in < 5 seconds total wall time | End-to-end timing in CI |
| P3 | Memory footprint of the demo run is < 50MB | `tracemalloc` snapshot at peak |

### Data Integrity

| # | Criterion | How to Verify |
|---|-----------|---------------|
| D1 | All 100 SKUs are represented in at least one recommendation or `no_action` response | Assert SKU coverage |
| D2 | All 5 locations produce at least one CRITICAL or HIGH item in Scenario 1 | Assert per-location urgency distribution |
| D3 | Dataset generator is seeded and deterministic | Same seed → same outputs |

### Demo Quality (Human Review)

| # | Criterion | Reviewer |
|---|-----------|---------|
| Q1 | A domain expert (supply chain) finds the scenario parameters realistic | Nico review |
| Q2 | A non-technical observer understands the handoff brief without explanation | Cold reading test |
| Q3 | The scenario story hangs together as a single coherent night (not 5 disconnected tests) | Narrative review |

---

## 7. Implementation Notes

### Dataset Generator

Implement as `demo/generate_dataset.py`:
- Accepts `--seed INT` for reproducibility
- Outputs `demo/data/inventory_states.json` and `demo/data/suppliers.json`
- Scenario mutations are applied in `demo/scenarios.py` as pure functions on the baseline dataset

### Agent Runner

Implement as `demo/agent_run.py`:
- Uses `SupplyChainTools` directly (no LLM required for M7 validation)
- Simulates what an LLM agent would do: sequential tool calls with explicit reasoning logged
- All tool calls and responses are written to `demo/agent_run/` as JSON files
- Final brief is written to `demo/agent_run/06_handoff_brief.md`

### Optional LLM Wrapper (Bonus, Not Required for M7)

If time permits, `demo/llm_agent.py` wraps the same tool calls with an actual LLM (OpenAI or Anthropic) to demonstrate the native function-calling integration. The LLM's reasoning traces are logged alongside the tool outputs. This is additive — the deterministic agent must pass all acceptance criteria first.

---

## 8. What This Demo Is NOT

- It is **not** a UI demo. No dashboards, no graphs. Raw JSON + markdown brief.
- It is **not** a trained model. Every decision is deterministic and explainable.
- It is **not** a multi-echelon planner. No BOM explosion, no capacity constraints. That's V2.
- It is **not** a toy with only 5 pre-chosen SKUs. It runs 500 states and finds the interesting ones itself.

The point is architectural: **an AI agent can run a real overnight replenishment cycle using only the primitives in `ootils-core` — and produce output that is more actionable, faster, and more explainable than a human planner starting from a dashboard.**

---

*Document owner: [architecture team] | Last updated: 2026-04-03 | Status: DRAFT — for review*
