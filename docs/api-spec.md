# Ootils API Specification — V1

> API-first is a core principle of Ootils. This document defines the V1 REST API contract.
> The API is designed for both human-facing tools and AI agent consumption.

---

## Design Principles

1. **Structured responses** — every response is machine-parseable JSON
2. **Explanations included** — results carry causal explanations by default
3. **Stateless queries** — GET requests never modify state
4. **Event-driven mutations** — state changes happen through POST /events only
5. **Scenario-scoped** — every request operates within a scenario context (default: `baseline`)

---

## Base URL

```
https://api.ootils.io/v1       # hosted
http://localhost:8000/v1       # self-hosted
```

---

## Authentication

```
Authorization: Bearer <token>
X-Scenario-ID: baseline        # optional, defaults to "baseline"
```

---

## Endpoints

---

### POST /events
Submit a supply chain event that triggers recalculation.

**Request:**
```json
{
  "event_type": "supply_date_changed",
  "trigger_node_id": "po-PO991",
  "scenario_id": "baseline",
  "payload": {
    "field": "due_date",
    "old_value": "2026-04-10",
    "new_value": "2026-04-18"
  },
  "source": "erp-sync"
}
```

**Event types:**
- `demand_qty_changed`
- `demand_date_changed`
- `supply_qty_changed`
- `supply_date_changed`
- `onhand_changed`
- `capacity_changed`
- `constraint_changed`
- `policy_changed`
- `structure_changed`
- `scenario_override_applied`

**Response:**
```json
{
  "event_id": "evt-uuid",
  "status": "queued",
  "affected_nodes_estimate": 12,
  "calc_run_id": "run-uuid"
}
```

---

### GET /projection
Get projected inventory for an item/location over a time horizon.

**Request:**
```
GET /projection?item_id=PUMP-01&location_id=DC-ATL&from=2026-04-01&to=2026-04-30&grain=day
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `item_id` | ✅ | Item identifier |
| `location_id` | ✅ | Location identifier |
| `from` | ✅ | Start date (ISO 8601) |
| `to` | ✅ | End date (ISO 8601) |
| `grain` | ❌ | `day` (default) / `week` / `month` |
| `scenario_id` | ❌ | Defaults to `baseline` |

**Response:**
```json
{
  "item_id": "PUMP-01",
  "location_id": "DC-ATL",
  "scenario_id": "baseline",
  "grain": "day",
  "projection": [
    {
      "date": "2026-04-01",
      "projected_qty": 100,
      "available_to_promise": 20,
      "incoming_supply": 0,
      "outgoing_demand": 0
    },
    {
      "date": "2026-04-05",
      "projected_qty": 20,
      "available_to_promise": 0,
      "incoming_supply": 0,
      "outgoing_demand": 80,
      "demand_detail": [
        { "node_id": "co-CO778", "qty": 80, "priority": 1 }
      ]
    },
    {
      "date": "2026-04-08",
      "projected_qty": -130,
      "available_to_promise": -130,
      "incoming_supply": 0,
      "outgoing_demand": 150,
      "shortage": {
        "qty": 130,
        "node_id": "shortage-PUMP01-DCATL-20260408"
      }
    }
  ]
}
```

---

### GET /issues
Get all active shortages and planning issues, optionally filtered.

**Request:**
```
GET /issues?location_id=DC-ATL&severity=high&horizon_days=30
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `item_id` | ❌ | Filter by item |
| `location_id` | ❌ | Filter by location |
| `severity` | ❌ | `high` / `medium` / `low` |
| `horizon_days` | ❌ | Look ahead N days (default: 60) |
| `scenario_id` | ❌ | Defaults to `baseline` |

**Response:**
```json
{
  "issues": [
    {
      "node_id": "shortage-PUMP01-DCATL-20260408",
      "type": "Shortage",
      "item_id": "PUMP-01",
      "location_id": "DC-ATL",
      "date": "2026-04-08",
      "shortage_qty": 130,
      "severity": "high",
      "impacted_orders": ["co-CO778"],
      "summary": "PO-991 delayed 8 days. Order CO-778 at risk.",
      "explanation_url": "/v1/explain?node_id=shortage-PUMP01-DCATL-20260408"
    }
  ],
  "total": 1,
  "as_of": "2026-03-29T20:00:00Z"
}
```

---

### GET /explain
Get the full root cause explanation for any node.

**Request:**
```
GET /explain?node_id=shortage-PUMP01-DCATL-20260408&detail_level=structured
```

**Parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `node_id` | ✅ | Target node to explain |
| `detail_level` | ❌ | `summary` / `detail` / `structured` (default) |

**Response:**
```json
{
  "node_id": "shortage-PUMP01-DCATL-20260408",
  "summary": "Order CO-778 (150u) exhausts stock (20u). PO-991 delayed 8 days. No substitute.",
  "detail": "CustomerOrder CO-778 due April 8 requires 150 units...",
  "causal_path": [
    {
      "step": 1,
      "node_id": "co-CO778",
      "node_type": "CustomerOrderDemand",
      "edge_type": "consumes",
      "fact": "Order CO-778 requires 150u due April 8 (priority 2)"
    },
    {
      "step": 2,
      "node_id": "onhand-PUMP01-DCATL",
      "node_type": "OnHandSupply",
      "edge_type": "consumes",
      "fact": "OnHand: 20u — exhausted"
    },
    {
      "step": 3,
      "node_id": "po-PO991",
      "node_type": "PurchaseOrderSupply",
      "edge_type": "depends_on",
      "fact": "PO-991 delayed Apr 10 → Apr 18 (8-day gap)"
    },
    {
      "step": 4,
      "node_id": null,
      "node_type": "PolicyCheck",
      "edge_type": "governed_by",
      "fact": "No substitution rule active for PUMP-01 @ DC-ATL"
    }
  ],
  "graph_fragment": {
    "nodes": [...],
    "edges": [...]
  }
}
```

---

### POST /simulate
Apply overrides to a scenario and return the computed delta vs baseline.

**Request:**
```json
{
  "scenario_id": "sim-expedite-po991",
  "base_scenario_id": "baseline",
  "overrides": [
    {
      "node_id": "po-PO991",
      "field": "due_date",
      "value": "2026-04-10"
    }
  ]
}
```

**Response:**
```json
{
  "scenario_id": "sim-expedite-po991",
  "delta": {
    "resolved_shortages": [
      {
        "node_id": "shortage-PUMP01-DCATL-20260408",
        "before": { "qty": 130, "date": "2026-04-08" },
        "after": null,
        "resolution": "shortage eliminated — PO arrives before demand date"
      }
    ],
    "new_shortages": [],
    "inventory_delta": [
      {
        "item_id": "PUMP-01",
        "location_id": "DC-ATL",
        "date": "2026-04-10",
        "before": -130,
        "after": 70
      }
    ]
  },
  "computed_at": "2026-03-29T20:15:00Z"
}
```

---

### GET /graph
Get the planning graph for an item/location — traversable by agents.

**Request:**
```
GET /graph?item_id=PUMP-01&location_id=DC-ATL&depth=3&from=2026-04-01&to=2026-04-30
```

**Response:**
```json
{
  "nodes": [
    { "id": "co-CO778", "type": "CustomerOrderDemand", "qty": 150, "date": "2026-04-08" },
    { "id": "onhand-PUMP01-DCATL", "type": "OnHandSupply", "qty": 20 },
    { "id": "po-PO991", "type": "PurchaseOrderSupply", "qty": 200, "date": "2026-04-18" }
  ],
  "edges": [
    { "from": "co-CO778", "to": "onhand-PUMP01-DCATL", "type": "consumes", "qty": 20 },
    { "from": "po-PO991", "to": "proj-PUMP01-DCATL", "type": "replenishes", "qty": 200 }
  ]
}
```

---

## AI Agent Usage Example

```python
import requests

BASE = "http://localhost:8000/v1"
HEADERS = {"Authorization": "Bearer <token>"}

# 1. Get active issues
issues = requests.get(f"{BASE}/issues?severity=high&horizon_days=14", headers=HEADERS).json()

for issue in issues["issues"]:
    # 2. Get full explanation
    explanation = requests.get(f"{BASE}/explain?node_id={issue['node_id']}", headers=HEADERS).json()
    
    # 3. Agent reasons about the causal path
    causal_path = explanation["causal_path"]
    
    # 4. Simulate a fix
    if any(step["node_type"] == "PurchaseOrderSupply" for step in causal_path):
        po_node = next(s for s in causal_path if s["node_type"] == "PurchaseOrderSupply")
        
        sim = requests.post(f"{BASE}/simulate", headers=HEADERS, json={
            "scenario_id": f"auto-expedite-{po_node['node_id']}",
            "base_scenario_id": "baseline",
            "overrides": [{"node_id": po_node["node_id"], "field": "due_date", "value": "2026-04-10"}]
        }).json()
        
        if sim["delta"]["resolved_shortages"]:
            print(f"Agent recommends: expedite {po_node['node_id']} → resolves {issue['node_id']}")
            # Agent escalates to human with explanation + recommendation
```

---

## Error Responses

```json
{
  "error": "node_not_found",
  "message": "Node 'shortage-UNKNOWN' does not exist in scenario 'baseline'",
  "status": 404
}
```

Standard HTTP status codes. Errors always return structured JSON.

---

*This specification is V1 — subject to change as the engine is implemented. Breaking changes will be versioned.*
