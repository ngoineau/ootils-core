# Edge Dictionary — V1

> Edges in Ootils are not just links — they carry business semantics. The type of an edge determines how the engine propagates changes and how explanations are constructed.

---

## Structure of an Edge

```json
{
  "edge_id": "uuid",
  "edge_type": "consumes",
  "from_node_id": "uuid",
  "to_node_id": "uuid",
  "scenario_id": "baseline",
  "priority": 1,
  "effective_start": "2026-01-01",
  "effective_end": null,
  "weight_ratio": 1.0,
  "attributes": {},
  "active": true
}
```

---

## Edge Types

### 1. `replenishes`
**A supply node increases projected inventory.**

- From: `OnHandSupply`, `PurchaseOrderSupply`, `WorkOrderSupply`, `TransferSupply`, `PlannedSupply`
- To: `ProjectedInventory`
- Propagation: when supply qty or date changes → ProjectedInventory is dirty

```
PO-991 (200 units, Apr 10) --replenishes--> ProjectedInventory(PUMP-01, DC-ATL)
```

---

### 2. `consumes`
**A demand node draws from supply or projected inventory.**

- From: `CustomerOrderDemand`, `ForecastDemand`, `TransferDemand`
- To: `OnHandSupply`, `ProjectedInventory`
- weight_ratio: 1.0 by default (can be fractional for partial allocations)
- Propagation: when demand qty/date changes → downstream supply nodes are dirty

```
CustomerOrder CO-778 (150 units) --consumes--> OnHandSupply(20) + ProjectedInventory
```

---

### 3. `depends_on`
**Logical dependency between two nodes — the upstream node must be resolved before the downstream node can be computed.**

- From: `ProjectedInventory`, `Shortage`, `PlannedSupply`
- To: Any node it depends on for calculation
- Propagation: change in dependency → dependant node is dirty

---

### 4. `requires_component`
**A work order requires a component, generating a dependent demand.**

- From: `WorkOrderSupply`
- To: `DependentDemand`
- weight_ratio: BOM ratio (e.g., 2.5 units of component per unit of parent)
- Propagation: WO qty change → DependentDemand qty changes proportionally

```
WO-456 (100 units of ASSEMBLY) --requires_component (ratio: 2.5)--> DependentDemand(COMPONENT-A, 250 units)
```

---

### 5. `produces`
**A work order or conversion produces a finished or semi-finished good.**

- From: `WorkOrderSupply`
- To: `ProjectedInventory` (of the produced item)
- Propagation: WO date/qty change → finished good ProjectedInventory is dirty

---

### 6. `uses_capacity`
**A supply order consumes capacity on a resource.**

- From: `WorkOrderSupply`, `PlannedSupply` (type=manufacture)
- To: `CapacityBucket`
- weight_ratio: Hours or capacity units consumed per unit produced
- Propagation: capacity constraint change → work order may be bounded

---

### 7. `bounded_by`
**A node's quantity or date is limited by a constraint.**

- From: `PurchaseOrderSupply`, `PlannedSupply`
- To: `MaterialConstraint`, `CapacityBucket`
- Propagation: constraint change → bounded supply is dirty

```
PlannedSupply(item=PUMP-01, qty=500) --bounded_by--> MaterialConstraint(max_qty=300)
→ effective qty = min(500, 300) = 300
```

---

### 8. `governed_by`
**A node's behavior is controlled by a policy.**

- From: Any node subject to a rule
- To: `Policy`
- Examples: safety stock policy governs minimum ProjectedInventory; allocation policy governs priority ordering

---

### 9. `transfers_to`
**An inter-site supply feeds inventory at the destination.**

- From: `TransferSupply`
- To: `ProjectedInventory` (at destination location)
- Propagation: transfer date/qty change → destination inventory is dirty

---

### 10. `originates_from`
**Traces a derived node back to its source — for auditability.**

- From: `DependentDemand`, `PlannedSupply`
- To: The source node that triggered creation
- Used for: pegging tree traversal, explanation construction

```
DependentDemand(COMPONENT-A) --originates_from--> WorkOrderSupply(ASSEMBLY-WO-456)
```

---

### 11. `pegged_to`
**Explicitly links a supply allocation to a specific demand — the core of order-level pegging.**

- From: Supply nodes (`OnHandSupply`, `PurchaseOrderSupply`, etc.)
- To: Demand nodes (`CustomerOrderDemand`, etc.)
- weight_ratio: Allocated quantity
- Created by: Allocation engine during netting
- Used for: "Which PO covers which customer order?"

```
PO-991 (200 units) --pegged_to (qty=150)--> CustomerOrder CO-778
                   --pegged_to (qty=50)---> CustomerOrder CO-812
```

---

### 12. `substitutes_for`
**Item B can substitute for item A under defined conditions.**

- From: `Item` (substitute)
- To: `Item` (primary)
- attributes: Substitution ratio, conversion factor, priority
- Used by: Allocation engine when primary item is short

*Note: Substitution logic is V2. This edge type is defined in V1 for schema completeness.*

---

### 13. `prioritized_over`
**Demand A has priority over Demand B for the same supply.**

- From: `CustomerOrderDemand` (higher priority)
- To: `CustomerOrderDemand` (lower priority)
- Defined by: Allocation policy (Policy node)
- Used by: Allocation engine to determine consumption order

---

### 14. `impacts`
**A result node (shortage, delay) impacts another business object — for cascading consequence tracking.**

- From: `Shortage`
- To: `CustomerOrderDemand` (service impact), `ProjectedInventory` (downstream)
- Used for: Service level calculation, escalation triggers, agent notifications

```
Shortage(PUMP-01, DC-ATL, Apr 8) --impacts--> CustomerOrder CO-778 (OTIF risk)
```

---

## Propagation Rules Summary

| Edge Type | When upstream changes | Downstream becomes |
|-----------|----------------------|-------------------|
| `replenishes` | Supply qty/date changes | ProjectedInventory → dirty |
| `consumes` | Demand qty/date changes | Supply/inventory → dirty |
| `depends_on` | Dependency resolves | Dependent node → dirty |
| `requires_component` | WO qty changes | DependentDemand qty → recalculated |
| `produces` | WO date/qty changes | Finished good inventory → dirty |
| `uses_capacity` | Capacity changes | Bounded supply → dirty |
| `bounded_by` | Constraint changes | Bounded node → dirty |
| `governed_by` | Policy changes | All governed nodes → dirty |
| `transfers_to` | Transfer changes | Destination inventory → dirty |
| `pegged_to` | Allocation changes | Pegged demand → recalculated |
| `impacts` | Shortage changes | Impacted orders → notified |

---

## Summary Table

| # | Edge Type | Direction | Business Meaning |
|---|-----------|-----------|-----------------|
| 1 | `replenishes` | Supply → Inventory | Adds to stock |
| 2 | `consumes` | Demand → Supply/Inventory | Draws from stock |
| 3 | `depends_on` | Node → Node | Calculation dependency |
| 4 | `requires_component` | WO → DependentDemand | BOM explosion |
| 5 | `produces` | WO → Inventory | Manufacturing output |
| 6 | `uses_capacity` | Supply → Capacity | Capacity consumption |
| 7 | `bounded_by` | Supply → Constraint | Hard limit |
| 8 | `governed_by` | Node → Policy | Rule application |
| 9 | `transfers_to` | Transfer → Inventory | Inter-site flow |
| 10 | `originates_from` | Derived → Source | Audit trail |
| 11 | `pegged_to` | Supply → Demand | Allocation link |
| 12 | `substitutes_for` | Item → Item | Substitution rule |
| 13 | `prioritized_over` | Demand → Demand | Allocation priority |
| 14 | `impacts` | Result → Business Object | Consequence chain |
