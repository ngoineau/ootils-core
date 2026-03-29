# Node Dictionary — V1

> Every object in the Ootils planning graph is a typed node. This document defines the 18 node types for V1.

---

## Structure of a Node

```json
{
  "node_id": "uuid",
  "node_type": "CustomerOrderDemand",
  "business_key": "CO-778-LINE-3",
  "scenario_id": "baseline",
  "item_id": "PUMP-01",
  "location_id": "DC-ATL",
  "qty": 150,
  "qty_uom": "EA",
  "time_grain": "exact_date",
  "time_ref": "2026-04-08",
  "time_span_start": "2026-04-08",
  "time_span_end": "2026-04-08",
  "status": "open",
  "attributes": {},
  "version_no": 1,
  "active": true
}
```

---

## A. Reference Nodes

### Item
The product, component, raw material, or semi-finished good being planned.

| Field | Description |
|-------|-------------|
| item_id | Unique identifier |
| item_type | `finished_good` / `component` / `raw_material` / `semi_finished` |
| uom | Unit of measure |
| status | `active` / `obsolete` / `phase_out` |
| attributes | Lead time, MOQ, batch size, safety stock policy ref, etc. |

---

### Location
A physical or logical node in the supply chain network.

| Field | Description |
|-------|-------------|
| location_id | Unique identifier |
| location_type | `plant` / `dc` / `warehouse` / `supplier_virtual` / `customer_virtual` |
| country | ISO country code |
| timezone | For date-sensitive operations |

---

### Resource
A capacity-bearing entity: machine, production line, labor pool.

| Field | Description |
|-------|-------------|
| resource_id | Unique identifier |
| resource_type | `machine` / `line` / `labor` / `storage` |
| location_id | Where the resource lives |
| calendar_ref | Working calendar reference |

---

### Supplier
A logical supplier entity (not a location).

| Field | Description |
|-------|-------------|
| supplier_id | Unique identifier |
| lead_time_policy | Default lead time rules |
| reliability_score | 0–1 (used in scenario modeling) |

---

### Policy
A versionable business rule that governs planning behavior.

| Policy Type | Examples |
|-------------|---------|
| Safety stock | Min stock = 2 weeks of demand |
| Allocation priority | Customer class A > B > C |
| Frozen zone | No planned changes within 2 weeks |
| MOQ / rounding | Minimum order = 100 units, round to 50 |
| Sourcing priority | Supplier A first, B as backup |

---

## B. Demand Nodes

### ForecastDemand
Statistical or consensus demand forecast, typically at monthly/weekly grain.

| Field | Description |
|-------|-------------|
| item_id | What item |
| location_id | Where |
| customer_segment | Optional segmentation |
| qty | Forecasted quantity |
| time_grain | `month` / `week` |
| time_ref | Period anchor (e.g., "2026-04") |

**Key rule:** ForecastDemand is consumed by CustomerOrderDemand in the same period. Net forecast = max(0, forecast - confirmed orders).

---

### CustomerOrderDemand
A confirmed customer order line.

| Field | Description |
|-------|-------------|
| order_id | Source order reference |
| line_id | Order line |
| item_id | What item |
| location_id | Ship-from location |
| qty | Ordered quantity |
| due_date | Requested ship/delivery date |
| priority | Numeric priority (lower = higher priority) |
| status | `open` / `partial` / `closed` |

---

### DependentDemand
A demand derived from another node (typically a work order requirement).

| Field | Description |
|-------|-------------|
| source_node_id | The work order or planned supply that creates this need |
| item_id | Required component |
| location_id | Required at location |
| qty | Required quantity |
| needed_date | When it's needed |

---

### TransferDemand
An inter-site replenishment need.

| Field | Description |
|-------|-------------|
| source_location_id | Requesting location |
| destination_location_id | Receiving location |
| item_id | What item |
| qty | Transfer quantity |
| need_date | When needed at destination |

---

## C. Supply Nodes

### OnHandSupply
Current physical inventory.

| Field | Description |
|-------|-------------|
| item_id | What item |
| location_id | Where |
| qty_available | Available quantity |
| lot | Optional lot/batch reference |
| quality_status | `available` / `hold` / `quarantine` |
| as_of_date | Snapshot date |

---

### PurchaseOrderSupply
An open purchase order from a supplier.

| Field | Description |
|-------|-------------|
| po_id | PO reference |
| line_id | PO line |
| supplier_id | Source supplier |
| item_id | What item |
| location_id | Delivery location |
| qty_open | Remaining open quantity |
| due_date | Confirmed delivery date |
| confirmed | Boolean — is this date firm? |

---

### WorkOrderSupply
An open manufacturing or conversion order.

| Field | Description |
|-------|-------------|
| wo_id | Work order reference |
| item_id | Item being produced |
| location_id | Production location |
| qty_open | Remaining quantity to produce |
| start_date | Scheduled start |
| end_date | Scheduled completion |
| status | `released` / `in_progress` / `planned` |

---

### TransferSupply
An in-transit or planned inter-site transfer.

| Field | Description |
|-------|-------------|
| transfer_id | Transfer reference |
| from_location_id | Origin |
| to_location_id | Destination |
| item_id | What item |
| qty | Transfer quantity |
| ship_date | Departure date |
| receipt_date | Expected arrival date |

---

### PlannedSupply
A supply proposal generated by the planning engine (not yet a real order).

| Field | Description |
|-------|-------------|
| proposal_id | Engine-generated ID |
| supply_type | `purchase` / `manufacture` / `transfer` |
| item_id | What item |
| location_id | Delivery location |
| qty | Proposed quantity |
| due_date | Needed by date |
| scenario_id | Which scenario generated this |
| status | `proposed` / `firmed` / `released` |

---

## D. Constraint Nodes

### CapacityBucket
Available capacity on a resource for a given time period.

| Field | Description |
|-------|-------------|
| resource_id | Which resource |
| capacity_qty | Available hours / units |
| time_grain | `week` / `day` |
| time_ref | Period reference |
| status | `available` / `constrained` / `blocked` |

---

### MaterialConstraint
An external constraint on material availability (supplier quota, allocation limit).

| Field | Description |
|-------|-------------|
| item_id | Constrained item |
| supplier_id | Optional — specific to a supplier |
| max_qty | Maximum available quantity |
| time_ref | Constraint period |
| rule_ref | Source rule or contract reference |

---

## E. Result Nodes (Engine-Generated)

### ProjectedInventory
The engine's calculated inventory position at a given point in time.

| Field | Description |
|-------|-------------|
| item_id | What item |
| location_id | Where |
| projected_qty | Total projected stock |
| available_to_promise | Uncommitted available qty |
| time_ref | Date or period |
| time_grain | `day` / `week` |
| explanation_ref | Link to causal explanation |

**Key principle:** ProjectedInventory is never stored as a static table. It is computed on demand by the engine and cached with an invalidation strategy.

---

### Shortage
A detected supply-demand imbalance that cannot be covered by available supply.

| Field | Description |
|-------|-------------|
| item_id | Affected item |
| location_id | Affected location |
| shortage_qty | Uncovered quantity |
| need_date | When the shortage occurs |
| impacted_demand_ref | Which demand(s) are affected |
| root_cause_ref | Link to explanation node |
| severity | Financial or service impact score |

---

## Summary Table

| Family | Node Type | Count |
|--------|-----------|-------|
| Reference | Item, Location, Resource, Supplier, Policy | 5 |
| Demand | ForecastDemand, CustomerOrderDemand, DependentDemand, TransferDemand | 4 |
| Supply | OnHandSupply, PurchaseOrderSupply, WorkOrderSupply, TransferSupply, PlannedSupply | 5 |
| Constraints | CapacityBucket, MaterialConstraint | 2 |
| Results | ProjectedInventory, Shortage | 2 |
| **Total** | | **18** |
