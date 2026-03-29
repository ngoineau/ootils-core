# ADR-004: Native Explainability — Root Cause Chain

**Status:** Accepted  
**Date:** 2026-03-29  
**Author:** Nicolas GOINEAU

---

## Context

### The black box problem

Every APS system produces results. None of them explain why.

A typical interaction with a modern APS:
```
System: "Shortage detected — PUMP-01, DC-ATL, April 15, 130 units"
Planner: "Why?"
System: [no answer]
Planner: [spends 2 hours in Excel reconstructing the causal chain]
```

This is not an edge case. Supply chain planners spend an estimated **60–70% of their time diagnosing** — figuring out why the plan is broken — before they can make any decisions.

### Why explainability is architecturally hard

Explainability cannot be bolted on after the fact. It requires:

1. **Structured causal relationships** — the engine must know that shortage X was caused by demand Y consuming supply Z, not just that the net balance is negative
2. **Persistent causal provenance** — every calculation must preserve a record of what inputs produced what outputs
3. **Traversable explanations** — the causal chain must be navigable, not just a log message
4. **Machine-readable format** — for AI agents to reason about explanations, not just humans to read them

This is why explainability is a first-class architectural concern, not a feature.

---

## Decision

**Every result node in Ootils carries a structured, traversable causal explanation. Explainability is generated as part of the calculation, not as post-processing.**

### Explanation model

```json
{
  "explanation_id": "uuid",
  "calc_run_id": "uuid",
  "target_node_id": "shortage-PUMP01-DC-ATL-20260415",
  "target_type": "Shortage",
  "root_cause_node_id": "po-PO991",
  "causal_path": [
    {
      "step": 1,
      "node_id": "co-CO778",
      "node_type": "CustomerOrderDemand",
      "edge_type": "consumes",
      "fact": "Order CO-778 requires 150 units due April 8, priority 2"
    },
    {
      "step": 2,
      "node_id": "onhand-PUMP01-DCATL",
      "node_type": "OnHandSupply",
      "edge_type": "consumes",
      "fact": "OnHand available: 20 units — exhausted by Order CO-778"
    },
    {
      "step": 3,
      "node_id": "po-PO991",
      "node_type": "PurchaseOrderSupply",
      "edge_type": "depends_on",
      "fact": "PO-991 delayed from April 10 to April 18 — 8-day gap"
    },
    {
      "step": 4,
      "node_id": null,
      "node_type": "PolicyCheck",
      "edge_type": "governed_by",
      "fact": "No active substitution rule for PUMP-01 at DC-ATL"
    }
  ],
  "summary": "Order CO-778 (150u) exhausts available stock (20u). PO-991 delayed 8 days. No substitute active.",
  "detail": "CustomerOrder CO-778 due April 8 requires 150 units of PUMP-01 at DC-ATL. Only 20 units are on hand. The next supply (PO-991, 200 units) was originally due April 10 but was moved to April 18. No substitution rule is active for this item/location. Resulting shortage: 130 units from April 8 to April 18.",
  "created_at": "2026-03-29T14:23:11Z"
}
```

### API endpoint

```
GET /explain?node_id=shortage-PUMP01-DC-ATL-20260415

Response:
{
  "explanation": { ... },  // structured as above
  "graph_fragment": {      // traversable subgraph for UI rendering
    "nodes": [...],
    "edges": [...]
  }
}
```

### Generation during calculation

Explanations are generated **inline** during the calculation pass, not as post-processing:

```python
def compute_shortage(item, location, date, demand, supply):
    shortage_qty = demand.qty - supply.available_qty
    
    if shortage_qty > 0:
        # Build explanation inline
        path = []
        path.append(CausalStep(demand, "consumes", f"Demand {demand.id} requires {demand.qty}u"))
        path.append(CausalStep(supply, "depends_on", f"Supply {supply.id} provides {supply.available_qty}u"))
        
        if supply.is_delayed:
            path.append(CausalStep(supply, "delayed", 
                f"{supply.id} delayed from {supply.original_date} to {supply.current_date}"))
        
        if not has_substitute(item, location):
            path.append(CausalStep(None, "governed_by", "No active substitution rule"))
        
        return Shortage(
            qty=shortage_qty,
            explanation=Explanation(path=path)
        )
```

### Explanation levels

The API returns explanations at three levels of detail:

| Level | Audience | Content |
|-------|----------|---------|
| `summary` | Human (quick scan) | 1-line plain English |
| `detail` | Human (investigation) | Full prose explanation |
| `structured` | AI agent / UI | Traversable JSON causal path |

AI agents consume `structured`. The cockpit UI renders `structured` as a visual graph. Humans read `summary` and `detail`.

---

## Consequences

**Positive:**
- Planners can understand any result in seconds, not hours
- AI agents can reason about *why* a state exists before taking action
- Audit trail for every planning decision — reproducible, timestamped
- Reduces diagnostic work by estimated 60-70% for human planners
- Enables agent guardrails: "before taking action X, verify the explanation justifies it"

**Negative / Trade-offs:**
- Storage overhead: explanation records add ~30-50% to result storage
- Calculation overhead: building the causal path adds processing time per result
- Explanation quality depends on edge semantic richness — incomplete edge typing produces weak explanations

**Mitigations:**
- Explanation storage is append-only (immutable audit log) — can be archived after TTL
- Causal path building is O(depth of causal chain) — bounded and fast in practice
- Edge dictionary (see edge-dictionary.md) is designed to maximize explanation richness

---

## What explainability enables (beyond planning)

1. **Agent guardrails** — an autonomous agent can verify that its intended action is justified by the explanation before executing
2. **Human-AI collaboration** — agent proposes action + explanation, human approves or overrides
3. **Audit compliance** — every allocation decision is traceable to inputs, rules, and priorities
4. **Model validation** — wrong explanations immediately reveal engine bugs
5. **Training data** — explained decisions are high-quality training data for future AI models

---

## Alternatives Considered

### Option A: Log-based explainability (rejected)
Store computation logs and reconstruct explanations from them. Expensive to query, not structured for programmatic consumption.

### Option B: Post-hoc explanation (rejected)
Run the engine, then run a separate "why?" query. Fails because the causal state may have changed between the calculation and the query.

### Option C: LLM-generated explanations (rejected for core engine)
Use an LLM to generate natural language explanations from results. Non-deterministic, expensive per query, and breaks the trust guarantee. LLM can be used as a *presentation layer* on top of structured explanations in V3.

---

## References
- [Enabling explainable AI in supply chain decision support — Olan et al. (2025)](BIBLIOGRAPHY.md)
- [XAI review neurosymbolic SC — Kosasih et al. (2024)](BIBLIOGRAPHY.md)
- [Explainable AI for Supply Chain Planning Optimization — Verheul et al., TU/e](BIBLIOGRAPHY.md)
