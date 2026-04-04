# ADR-002d: Elastic Time — Final Architecture Decision

**Status:** Accepted  
**Date:** 2026-04-04  
**Author:** Nicolas GOINEAU (decision) + Architecture Review  
**Supersedes:** ADR-002b (rejected — global zone model), ADR-002c (rejected — data-driven grain model)  
**Extends:** ADR-002 (origin principles retained)

---

## Context and Evolution

Three attempts to design the elastic time model:

- **ADR-002** — established the principle: time is a property of the object, not a global axis. Correct principle, left implementation open.
- **ADR-002b** — global ON/OFF zone model with calendar-triggered structural mutations. Rejected: imposed a single granularity policy on all node types; daily roll-forward was fragile and operationally meaningless.
- **ADR-002c** — data-driven effective grain model. Rejected: structurally unstable (grain restructuring triggered by data events created thrash); high node count on far-horizon data.

**The core insight that resolves all three attempts (from Nico, 2026-04-04):**

> "For each node type, there is a temporal management that is a parameter of that type. It is via this logic that we get significant computation gains. The more we approach the execution date, the more precise we must be. Managing precise dates beyond 90/120 days is operational nonsense. A customer order for 150 days out keeps its exact date, but its contribution to forecast consumption stays at monthly grain — no proration."

---

## Decision

### Core Principle: Node-Type Temporal Policy

**The time granularity of computed buckets is a parameter of the node type, not of the data source and not of a global calendar policy.**

Each node type declares its own `temporal_policy` — a configuration object that defines:
- Which granularity zones apply
- At what horizon distances each zone activates
- How source data at different native grains contributes to this node type's buckets

Source nodes always retain their native grain. A `CustomerOrderDemand` at day 150 retains its exact date. A `ForecastDemand` is always monthly. What changes is how each source node *contributes to the computation* of the target node type's buckets — and that is governed by the target type's temporal policy.

---

## The Three-Layer Stack (retained from ADR-002c, grain logic revised)

```
┌─────────────────────────────────────────────────────────────┐
│  PRESENTATION LAYER                                         │
│  TemporalBridge: query-time aggregation / disaggregation.   │
│  Read-only. Never writes to the graph.                      │
├─────────────────────────────────────────────────────────────┤
│  COMPUTATION LAYER                                          │
│  Computes PI buckets at the grain defined by the node       │
│  type's temporal policy. Shortage detection at bucket grain.│
│  Dirty-flag propagation. No presentation awareness.         │
├─────────────────────────────────────────────────────────────┤
│  STORAGE LAYER                                              │
│  Source nodes at native grain (exact_date, week, month).    │
│  PI nodes at policy-defined grain. No forced conversion     │
│  at ingestion. No global zone assignment.                   │
└─────────────────────────────────────────────────────────────┘
```

---

## Node-Type Temporal Policy

### Definition

```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class GranularityZone:
    grain:          str   # 'day' | 'week' | 'month' | 'quarter'
    horizon_from:   int   # days from today (inclusive)
    horizon_to:     int   # days from today (exclusive); -1 = unbounded

@dataclass
class NodeTypeTemporalPolicy:
    node_type:         str
    zones:             list[GranularityZone]
    native_grain:      Optional[str] = None  # if set, node always stores at this grain regardless of zones
    contribution_rule: str = 'point_in_bucket'  # how source nodes contribute to this type's buckets
    # 'point_in_bucket': source date maps to the bucket that covers it, no proration
    # 'split_flat': source quantity split across all covered buckets proportionally
    # 'split_profile': split using a historical demand profile
```

### Reference Temporal Policies

```python
TEMPORAL_POLICIES = {

    # --- Source nodes: native grain, no zone logic ---

    "ForecastDemand": NodeTypeTemporalPolicy(
        node_type    = "ForecastDemand",
        native_grain = "month",    # always monthly — this is the nature of forecast data
        zones        = [],
    ),

    "CustomerOrderDemand": NodeTypeTemporalPolicy(
        node_type    = "CustomerOrderDemand",
        native_grain = "exact_date",  # always exact date — preserved forever
        zones        = [],
    ),

    "PurchaseOrderSupply": NodeTypeTemporalPolicy(
        node_type    = "PurchaseOrderSupply",
        native_grain = "exact_date",
        zones        = [],
    ),

    "CapacityBucket": NodeTypeTemporalPolicy(
        node_type    = "CapacityBucket",
        native_grain = "week",
        zones        = [],
    ),

    # --- Computed nodes: zone-based grain ---

    "ProjectedInventory": NodeTypeTemporalPolicy(
        node_type    = "ProjectedInventory",
        zones = [
            GranularityZone(grain='day',   horizon_from=0,   horizon_to=90),
            GranularityZone(grain='week',  horizon_from=90,  horizon_to=180),
            GranularityZone(grain='month', horizon_from=180, horizon_to=-1),   # unbounded
        ],
        contribution_rule = 'point_in_bucket',  # a source node contributes to the PI bucket that covers it
    ),

    "Shortage": NodeTypeTemporalPolicy(
        node_type    = "Shortage",
        zones = [
            GranularityZone(grain='day',   horizon_from=0,   horizon_to=90),
            GranularityZone(grain='week',  horizon_from=90,  horizon_to=180),
            GranularityZone(grain='month', horizon_from=180, horizon_to=-1),
        ],
        contribution_rule = 'point_in_bucket',
    ),
}
```

### Resolving grain for a target node at a given horizon

```python
def resolve_grain(node_type: str, horizon_days: int, policies: dict) -> str:
    """
    Returns the grain at which a node of this type is computed,
    given its distance from today in days.
    """
    policy = policies[node_type]
    if policy.native_grain:
        return policy.native_grain  # source nodes: always their native grain
    for zone in policy.zones:
        if zone.horizon_from <= horizon_days < zone.horizon_to or \
           (zone.horizon_to == -1 and horizon_days >= zone.horizon_from):
            return zone.grain
    raise ValueError(f"No zone found for {node_type} at horizon {horizon_days}d")
```

---

## Contribution Rule: How Source Nodes Contribute to PI Buckets

### The key business rule (Nico, 2026-04-04)

> "A customer order for 150 days out keeps its exact date, but its contribution to forecast consumption stays at monthly grain — no proration."

**`point_in_bucket` rule (default for PI):**

A source node with `time_span_start = date_D` contributes its quantity to the PI bucket whose span covers `date_D`. No quantity is prorated across multiple buckets.

```
CustomerOrderDemand: qty=150, due_date=2026-10-15 (horizon: ~190 days → monthly PI zone)
  → Contributes 150 units to PI bucket [2026-10-01, 2026-10-31]
  → No proration across October days
  → The date 2026-10-15 is preserved on the CustomerOrderDemand node itself
```

```
ForecastDemand: qty=1200, period=2026-10 (monthly)
  → Contributes 1200 units to PI bucket [2026-10-01, 2026-10-31]
  → Net demand = max(0, 1200 - 150) = 1050 units (forecast consumption rule)
```

```
PurchaseOrderSupply: qty=300, due_date=2026-05-12 (horizon: ~38 days → daily PI zone)
  → Contributes 300 units to PI bucket [2026-05-12, 2026-05-12] (daily)
```

**`split_flat` rule (opt-in per edge, for specific use cases):**

Used when a coarse-grain supply must be spread across fine-grain PI buckets.
Example: a WorkOrder covering a full week contributes proportionally to each daily PI bucket within the week.
This is edge-declared (`split_mode = 'FLAT'` on the `replenishes` edge).

---

## PI Node Structure

### Storage: one node per bucket, grain from temporal policy

```python
@dataclass
class ProjectedInventoryNode:
    id:                   UUID
    scenario_id:          UUID
    item_id:              UUID
    location_id:          UUID
    projection_series_id: UUID   # groups all PI nodes for one (item, location, scenario)
    bucket_sequence:      int    # ordinal position — enables efficient successor queries

    # Bucket definition (grain from temporal policy, not from data)
    time_grain:      str    # 'day' | 'week' | 'month'
    time_ref:        date   # canonical reference date (day start, week start Monday, month first)
    time_span_start: date   # inclusive
    time_span_end:   date   # exclusive

    # Computed values
    opening_stock:   Decimal
    inflows:         Decimal
    outflows:        Decimal
    closing_stock:   Decimal  # = opening + inflows - outflows

    # Shortage flag (set by computation kernel at bucket grain)
    has_shortage:    bool
    shortage_qty:    Decimal

    # Propagation state
    is_dirty:        bool
    computed_at:     Optional[datetime]
```

### Node count per (item, location) pair — 18-month horizon

```
Daily zone  (0–90 days):   90 nodes
Weekly zone (90–180 days):  13 nodes  (90 days / 7 ≈ 13 weeks)
Monthly zone (180–540 days): 12 nodes (12 months)
─────────────────────────────────────
Total: ~115 nodes per (item, location) pair
```

At 10,000 (item, location) pairs: ~1.15M PI nodes. Well within Postgres range with proper indexing on `(projection_series_id, bucket_sequence)`.

---

## Zone Transition Mechanics

### Transitions happen at natural calendar boundaries — not rolling day counts

**Critical design decision (2026-04-04):** Zone transitions do not occur at a rolling "day N from today" threshold. They occur at **natural calendar boundaries**:

- **weekly → daily transition:** fires on **Monday morning** when the approaching week enters the daily zone
- **monthly → weekly transition:** fires on the **1st of the month** when the approaching month enters the weekly zone

This means the zone boundary for PI is not "today + 90 days" but rather "the Monday of the week that is N weeks out" and "the 1st of the month that is M months out." The exact cutoff is the nearest natural calendar boundary at or beyond the configured threshold.

```python
def next_weekly_boundary(today: date, daily_horizon_weeks: int) -> date:
    """
    Returns the Monday of the week that will enter the daily zone next.
    daily_horizon_weeks: number of full weeks in the daily zone (e.g., 13 for ~90 days).
    """
    cutoff = today + timedelta(weeks=daily_horizon_weeks)
    # Snap to Monday of that week
    return cutoff - timedelta(days=cutoff.weekday())

def next_monthly_boundary(today: date, weekly_horizon_months: int) -> date:
    """
    Returns the 1st of the month that will enter the weekly zone next.
    weekly_horizon_months: number of months in the weekly zone (e.g., 3 for ~90 days).
    """
    target_month = today.month + weekly_horizon_months
    target_year = today.year + (target_month - 1) // 12
    target_month = ((target_month - 1) % 12) + 1
    return date(target_year, target_month, 1)
```

### The PAST principle

**The engine never plans the past.** The planning horizon always starts at `max(today, as_of_date)`. All PI buckets have `time_span_start >= planning_start_date`. Past supply and demand events are historical — they inform `OnHandSupply` (the opening balance) but are never re-planned.

```python
PLANNING_START = max(date.today(), as_of_date)
# No PI node exists with time_span_start < PLANNING_START
# Past POs, past orders: contribute to on-hand snapshot only
```

### Zone transition jobs

Two scheduled jobs, not one daily job:

```python
def weekly_to_daily_transition(planning_date: date, db: Session):
    """
    Runs every Monday at 02:00 UTC.
    The week that is now entering the daily zone (was weekly last week)
    is split into 7 daily PI buckets per (item, location) pair.
    Old weekly bucket is archived. New daily buckets are marked dirty.
    Cost: 1 weekly bucket × N (item, location) pairs → N×7 new nodes.
    """
    ...

def monthly_to_weekly_transition(planning_date: date, db: Session):
    """
    Runs on the 1st of each month at 02:00 UTC.
    The month entering the weekly zone is split into ~4-5 weekly PI buckets.
    Old monthly bucket is archived. New weekly buckets are marked dirty.
    Cost: 1 monthly bucket × N (item, location) pairs → N×4-5 new nodes.
    """
    ...
```

**These are the only remaining structural mutations.** Both are bounded (one bucket per (item, location) per transition), calendar-triggered (not data-triggered), and idempotent (safe to re-run if the job was missed).

---

## Shortage Detection

### At bucket grain — no sub-bucket detection within the computation layer

The computation kernel flags shortages at the grain of the PI bucket. At 95 days (weekly zone), a 3-day gap within a week that is net-positive at weekly grain is **not a shortage at that horizon**. This is the correct operational behavior — weekly precision at 95 days is sufficient.

### ⚠️ OPEN ISSUE — Intra-bucket shortages (unresolved)

A shortage that is invisible at weekly grain (week is net-positive) but real at daily grain is a known blind spot of this model. This is an accepted trade-off for far-horizon planning, but it becomes operationally dangerous as buckets approach the zone transition boundary.

**No decision made on this point.** It is parked for a dedicated discussion. The issue is acknowledged and must be addressed before V1 goes into production use.

```python
@dataclass
class ShortageNode:
    id:              UUID
    scenario_id:     UUID
    item_id:         UUID
    location_id:     UUID

    # Absolute date span (grain-independent, survives zone transitions)
    time_span_start: date
    time_span_end:   date
    time_grain:      str     # grain at which the shortage was detected

    shortage_qty:    Decimal
    severity:        Decimal  # qty × margin × days_at_risk × priority_weight
    root_cause_class: str    # supply_delay | supply_gap | demand_spike | allocation_conflict | capacity_bound
    explanation_id:  Optional[UUID]
```

---

## TemporalBridge: Presentation Layer Only

The TemporalBridge is read-only and presentation-only. It does not participate in propagation.

**What it does:**
- Aggregates fine-grain PI nodes into coarser views on query (daily → weekly, weekly → monthly)
- Disaggregates coarse-grain PI nodes for fine-grain queries (marked `approximated: true`)
- Surfaces shortage summary across grain boundaries

**What it does NOT do:**
- Write to any node or edge
- Participate in dirty-flag propagation
- Apply contribution rules (that is the computation kernel's job)

---

## Configuration: Policy Parameters (Nico's calls)

The zone breakpoints are configuration, not constants. Stored in a `node_type_policies` table:

```sql
CREATE TABLE node_type_policies (
    node_type            TEXT PRIMARY KEY,
    daily_horizon_days   INTEGER,   -- horizon_to for the daily zone
    weekly_horizon_days  INTEGER,   -- horizon_to for the weekly zone
    -- beyond weekly_horizon_days → month
    contribution_rule    TEXT NOT NULL DEFAULT 'point_in_bucket',
    native_grain         TEXT,      -- if set, node always at this grain
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Default values (Nico's call on exact numbers):
INSERT INTO node_type_policies VALUES
  ('ProjectedInventory', 90, 180, 'point_in_bucket', NULL, now()),
  ('Shortage',           90, 180, 'point_in_bucket', NULL, now()),
  ('ForecastDemand',     NULL, NULL, 'point_in_bucket', 'month', now()),
  ('CustomerOrderDemand', NULL, NULL, 'point_in_bucket', 'exact_date', now()),
  ('PurchaseOrderSupply', NULL, NULL, 'point_in_bucket', 'exact_date', now()),
  ('CapacityBucket',     NULL, NULL, 'split_flat', 'week', now());
```

### ⚠️ ARBITRATION — Exact zone breakpoint values

**Proposed:** daily_horizon_days=90, weekly_horizon_days=180 for ProjectedInventory.  
**Alternative:** 60/120, 30/90, or item-category-specific overrides.  
These values cannot be changed without a zone transition migration. Nico's call.

---

## What This Model Eliminates

| Problem | Status |
|---------|--------|
| Global ON/OFF zone gates (ADR-002b) | Eliminated — zones are per node type |
| Data-triggered grain restructuring (ADR-002c) | Eliminated — PI grain follows policy, not data |
| Proration of far-horizon exact-date orders | Eliminated — contribution rule is point_in_bucket |
| Unbounded node count at fine grain for far horizons | Eliminated — zone policy caps precision |
| TemporalBridge in the propagation path | Eliminated — Bridge is presentation-only |

## What This Model Retains (Necessary)

| Mechanism | Why Retained |
|-----------|--------------|
| Zone transition roll-forward (weekly→daily) | Time passes — this is unavoidable. Now bounded, scheduled, policy-driven. |
| Edge-based temporal rules | Still needed for contribution rule declaration (point_in_bucket vs split_flat) |
| Native grain on source nodes | Core principle — source data is never forced into a different grain |
| Horizon Extension (append-only) | Far-horizon monthly PI nodes added as planning horizon grows |

---

## Summary

| Topic | Decision |
|-------|----------|
| Grain assignment for PI | Node-type temporal policy (configurable per type) |
| Source node grain | Always native — never converted at ingestion |
| Contribution rule | `point_in_bucket` by default — no proration for far-horizon exact-date sources |
| Structural mutations | Only zone transitions (weekly→daily at 90d boundary), policy-driven, daily scheduled job |
| TemporalBridge | Presentation layer only — never in propagation |
| Shortage detection | At PI bucket grain — no sub-bucket detection in engine |
| Roll-forward | Retained as zone-transition roll only — bounded and predictable |
| Horizon Extension | Append-only maintenance job — no restructuring |
| Configuration | `node_type_policies` table — zone breakpoints are config, not constants |
