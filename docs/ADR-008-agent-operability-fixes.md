# ADR-008: Agent Operability Fixes

**Status:** Accepted  
**Date:** 2026-04-04  
**Author:** Architecture Review + Nicolas GOINEAU  
**Context:** Three agent operability issues identified by independent review (REVIEW-agent-operability.md). All resolved before M7 demo.

---

## A1 — Async Compute Ambiguity on /simulate

### Problem
`POST /simulate` returns immediately while computation is async. An agent acting on a `computing` or `stale` scenario produces wrong recommendations with no error signal.

### Resolution

**Three combined elements:**

**1. Explicit status on every scenario response**

```json
{
  "scenario_id": "uuid",
  "status": "computing",
  "result_valid": false,
  "estimated_completion_ms": 1200,
  "message": "Computation in progress. Poll GET /scenarios/{id}/status before reading results."
}
```

**2. Dedicated polling endpoint**

```
GET /scenarios/{scenario_id}/status
→ { status, result_valid, progress_pct, estimated_completion_ms }
```

**3. Guard on result-reading endpoints**

All endpoints returning computed results (`/compare`, `/projection`, `/issues`) check status before responding:

```python
def require_computed(scenario_id: UUID, db: Session):
    scenario = db.query(Scenario).get(scenario_id)
    if scenario.status in ('computing', 'draft'):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "scenario_not_ready",
                "status": scenario.status,
                "poll": f"/scenarios/{scenario_id}/status"
            }
        )
    if scenario.status == 'stale':
        # Return results with explicit warning — not a 409
        return {"warning": "stale_results", "stale_since": scenario.stale_since, ...}
```

**Recommended agent pattern (documented in API spec):**

```python
resp = POST /simulate { "delta": {...} }
scenario_id = resp["scenario_id"]

while True:
    status = GET /scenarios/{scenario_id}/status
    if status["result_valid"]: break
    sleep(status["estimated_completion_ms"] / 1000)

comparison = GET /scenarios/{scenario_id}/compare
```

---

## A2 — Missing Cross-Location Portfolio Query

### Problem
Detecting inter-location arbitrage opportunities (item critical at Dallas, excess at Chicago) requires 5 serial API calls + agent-side arithmetic synthesis. Token-expensive and hallucination-prone.

### Resolution

**New endpoint: `GET /inventory/portfolio`**

```
GET /inventory/portfolio?item_id={id}&scenario_id={id}&grain=week
```

Returns all location states for one item in a single call:

```json
{
  "item_id": "EMEC-008",
  "as_of": "2026-04-04",
  "scenario_id": "baseline",
  "locations": [
    {
      "location_id": "LOC-CHI",
      "status": "healthy",
      "days_of_supply": 48.2,
      "closing_stock": 4200,
      "has_shortage": false,
      "has_sub_shortage": false,
      "transferable_qty": 2800
    },
    {
      "location_id": "LOC-DAL",
      "status": "critical",
      "days_of_supply": 2.1,
      "closing_stock": 42,
      "has_shortage": true,
      "shortage_qty": 180,
      "has_sub_shortage": false
    }
  ],
  "arbitrage_opportunities": [
    {
      "from_location": "LOC-CHI",
      "to_location": "LOC-DAL",
      "max_transferable_qty": 2800,
      "resolves_shortage": true
    }
  ]
}
```

**Key design decisions:**

- `transferable_qty` = `closing_stock - safety_stock_qty` — what a location can give without putting itself at risk. Computed by the engine, not by the agent.
- `arbitrage_opportunities` is pre-computed by the engine. The agent evaluates or rejects; it does not do the arithmetic.
- One API call replaces 5 serial calls + agent-side synthesis.

---

## A3 — Missing Structured Escalation Primitive

### Problem
When an agent identifies a situation requiring human decision (no active supplier, unavoidable stockout, budget exhausted), it currently expresses this in markdown prose in the handoff brief. Escalations can be lost, missed, or lack the structure for a planner to act quickly.

### Resolution

**New endpoint: `POST /escalations` with typed records**

```
POST /escalations
```

```json
{
  "escalation_type": "no_active_supplier",
  "severity": "blocker",
  "item_id": "ACTI-017",
  "location_id": "LOC-CHI",
  "scenario_id": "baseline",
  "shortage_node_id": "uuid",
  "requires_response_by": "2026-04-05T08:00:00Z",
  "context": {
    "last_active_supplier": "SUP-ARW",
    "inactivation_reason": "credit_hold",
    "days_of_supply_remaining": 2.7,
    "alternatives_evaluated": 0
  },
  "agent_run_ref": "uuid",
  "suggested_actions": [
    "Resolve credit hold with SUP-ARW",
    "Approve spot market purchase at 1.35x standard cost",
    "Notify affected customers immediately"
  ]
}
```

**Escalation types:**

```python
class EscalationType(str, Enum):
    NO_ACTIVE_SUPPLIER   = "no_active_supplier"
    UNAVOIDABLE_STOCKOUT = "unavoidable_stockout"
    BUDGET_EXCEEDED      = "budget_exceeded"
    CYCLE_DETECTED       = "cycle_detected"
    DATA_QUALITY         = "data_quality"
    AGENT_UNCERTAINTY    = "agent_uncertainty"
```

**Lifecycle:**

```
open → acknowledged → resolved | dismissed
```

**Supporting endpoints:**

```
GET   /escalations?status=open&severity=blocker
GET   /escalations/{id}
PATCH /escalations/{id} { status, assignee }
POST  /escalations/{id}/resolve { resolution, action_taken }
```

**What this gives the agent:**
- Structured, typed escalation record — not prose markdown
- Timestamped and traceable in the events table
- Linked to the shortage node or scenario (graph traceability)
- `requires_response_by` deadline visible in the planner dashboard
- Full audit trail: who resolved, how, when

---

## Summary

| Fix | Endpoint | Key mechanism |
|-----|----------|---------------|
| A1 — Async ambiguity | `GET /scenarios/{id}/status` + 409 guard on result endpoints | `result_valid` flag + polling pattern |
| A2 — Cross-location query | `GET /inventory/portfolio?item_id=X` | Pre-computed `arbitrage_opportunities` |
| A3 — Escalation primitive | `POST /escalations` | Typed escalation records with lifecycle |
