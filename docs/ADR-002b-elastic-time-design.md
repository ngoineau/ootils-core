# ADR-002b: Elastic Time Architecture — Complete Design

**Status:** Proposed  
**Author:** Architecture Review (Expert Analysis)  
**Supersedes:** ADR-002 (partial — extends, does not replace)  
**Date:** 2026-04-04  

---

## Context

ADR-002 established that time is a property of the object, not a global axis. Each node carries `time_grain`, `time_ref`, `time_span_start`, `time_span_end`. The TemporalBridge was named but left as a sketch. The edge-based temporal rule idea was discussed but not designed.

This ADR closes six open questions:
1. Edge-based temporal rule data model (exact schema)
2. Elastic granularity breakpoints: global vs. per-object
3. ProjectedInventory storage model across granularity zones
4. Shortage node representation spanning multiple zones
5. TemporalBridge architecture: centralized vs. distributed
6. Horizon roll-forward mechanics

**System constraints that shape every decision:**
- PostgreSQL persistence, no graph DB
- Dirty-flag propagation (topological, incremental)
- 15–18 month horizon; elastic granularity (daily/weekly/monthly)
- AI agents must query the graph cleanly — no opaque blobs
- Future Rust kernel replacement of the low-level computation layer

---

## Q1 — Edge-Based Temporal Rules: Exact Schema

### Decision

Edges carry temporal conversion metadata. The edge is the contract between two nodes of potentially different granularities. The TemporalBridge reads this contract to execute the conversion at propagation time.

**An edge does NOT execute the conversion itself.** It declares the rule; the Bridge executes it. This keeps computation centralized and testable while keeping the rule co-located with the relationship.

### Edge SQL Schema

```sql
CREATE TABLE edges (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id         UUID NOT NULL REFERENCES scenarios(id),
    edge_type           TEXT NOT NULL,          -- consumes | replenishes | pegged_to | depends_on | impacts | governed_by
    source_node_id      UUID NOT NULL REFERENCES nodes(id),
    target_node_id      UUID NOT NULL REFERENCES nodes(id),

    -- Temporal conversion rule (NULL = same grain, no conversion needed)
    temporal_rule       TEXT,                   -- NULL | SPLIT | AGGREGATE | ALIGN | PASSTHROUGH
    split_mode          TEXT,                   -- FLAT | FRONT_LOAD | HISTORICAL_PROFILE (only when temporal_rule = SPLIT)
    profile_id          UUID,                   -- FK to temporal_profiles table (only when split_mode = HISTORICAL_PROFILE)
    alignment_mode      TEXT,                   -- OVERLAP | PROPORTIONAL | SNAP_START (only when temporal_rule = ALIGN)
    
    -- Consumption / coverage semantics
    consumption_offset_days  INTEGER DEFAULT 0, -- shift applied after conversion (e.g., lead time offset)
    coverage_mode       TEXT DEFAULT 'FULL',    -- FULL | PARTIAL_FRONT | PARTIAL_BACK

    -- Audit
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata            JSONB DEFAULT '{}'::jsonb,

    CONSTRAINT valid_temporal_rule CHECK (
        temporal_rule IS NULL OR temporal_rule IN ('SPLIT', 'AGGREGATE', 'ALIGN', 'PASSTHROUGH')
    ),
    CONSTRAINT valid_split_mode CHECK (
        split_mode IS NULL OR split_mode IN ('FLAT', 'FRONT_LOAD', 'HISTORICAL_PROFILE')
    ),
    CONSTRAINT valid_alignment_mode CHECK (
        alignment_mode IS NULL OR alignment_mode IN ('OVERLAP', 'PROPORTIONAL', 'SNAP_START')
    ),
    CONSTRAINT split_requires_mode CHECK (
        temporal_rule != 'SPLIT' OR split_mode IS NOT NULL
    )
);

CREATE INDEX idx_edges_source ON edges(source_node_id);
CREATE INDEX idx_edges_target ON edges(target_node_id);
CREATE INDEX idx_edges_scenario ON edges(scenario_id);
CREATE INDEX idx_edges_type ON edges(edge_type);
```

### Python Dataclass

```python
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from uuid import UUID
from datetime import datetime

class TemporalRule(str, Enum):
    SPLIT       = "SPLIT"        # coarse source → fine target (e.g., monthly demand → daily PI)
    AGGREGATE   = "AGGREGATE"    # fine source → coarse target (e.g., daily actuals → weekly view)
    ALIGN       = "ALIGN"        # same grain, different anchor (e.g., Mon-week vs. Sun-week)
    PASSTHROUGH = "PASSTHROUGH"  # same grain, no conversion — explicit no-op

class SplitMode(str, Enum):
    FLAT                = "FLAT"
    FRONT_LOAD          = "FRONT_LOAD"
    HISTORICAL_PROFILE  = "HISTORICAL_PROFILE"

class AlignmentMode(str, Enum):
    OVERLAP        = "OVERLAP"        # use overlapping days, proportional weight
    PROPORTIONAL   = "PROPORTIONAL"   # split by calendar-day ratio
    SNAP_START     = "SNAP_START"     # align to target bucket start

class CoverageMode(str, Enum):
    FULL           = "FULL"
    PARTIAL_FRONT  = "PARTIAL_FRONT"
    PARTIAL_BACK   = "PARTIAL_BACK"

@dataclass
class Edge:
    id:                      UUID
    scenario_id:             UUID
    edge_type:               str
    source_node_id:          UUID
    target_node_id:          UUID
    temporal_rule:           Optional[TemporalRule]  = None
    split_mode:              Optional[SplitMode]     = None
    profile_id:              Optional[UUID]          = None
    alignment_mode:          Optional[AlignmentMode] = None
    consumption_offset_days: int                     = 0
    coverage_mode:           CoverageMode            = CoverageMode.FULL
    created_at:              datetime                = field(default_factory=datetime.utcnow)
    updated_at:              datetime                = field(default_factory=datetime.utcnow)
    metadata:                dict                    = field(default_factory=dict)
```

### Concrete Example

Edge: `ForecastDemand (monthly)` → `replenishes` → `ProjectedInventory (daily)`

```python
Edge(
    edge_type       = "replenishes",
    source_node_id  = forecast_node.id,   # time_grain = "month"
    target_node_id  = pi_node.id,         # time_grain = "day"
    temporal_rule   = TemporalRule.SPLIT,
    split_mode      = SplitMode.FLAT,     # or HISTORICAL_PROFILE if profile exists
    coverage_mode   = CoverageMode.FULL,
    consumption_offset_days = 0,
)
```

When the propagation engine traverses this edge, it calls:
```python
bridge.split(source_value, source_node.time_span_start, source_node.time_span_end,
             target_grain="day", mode=edge.split_mode, profile_id=edge.profile_id)
```

**What edges do NOT carry:** the actual quantity, the result of the conversion, or any mutable planning state. Edges are structural + rule declarations only.

---

## Q2 — Elastic Granularity Breakpoints: Global or Per-Object?

### Decision: Option C — Hybrid (global defaults + per-item policy override)

### Rationale

**Option A (pure global) fails** for the following reason: a sole-source item with a 42-day lead time that has been manually expedited needs daily visibility at day 30. A bulk commodity with 5-day lead time does not. A single global cutoff of "daily within 30 days" will simultaneously over-compute for most items and under-compute for the critical ones. At scale, this is both wasteful and dangerous.

**Option B (pure per-object) fails** for a different reason: it makes the planning horizon a per-node property that propagates inconsistently through the graph. If item A is daily to day 45 and item B (which A depends on) is daily to day 30, the propagation window for A is incoherent at days 31–45. You need a common reference to resolve inter-item temporal alignment.

**Option C (hybrid) is correct.** The global policy sets the floor. Per-item Policy nodes override upward (finer resolution further out). The override is expressed through the `governed_by` edge to a `GranularityPolicy` node.

### Schema

```sql
CREATE TABLE granularity_policy (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    is_global       BOOLEAN NOT NULL DEFAULT FALSE,

    -- Breakpoints: how many calendar days from today
    daily_horizon_days   INTEGER NOT NULL DEFAULT 30,   -- [0, daily_horizon_days) → day
    weekly_horizon_days  INTEGER NOT NULL DEFAULT 90,   -- [daily_horizon_days, weekly_horizon_days) → week
    -- beyond weekly_horizon_days → month

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata        JSONB DEFAULT '{}'::jsonb
);

-- Exactly one row where is_global = TRUE. Enforced by partial unique index.
CREATE UNIQUE INDEX idx_granularity_policy_global ON granularity_policy(is_global) WHERE is_global = TRUE;
```

```python
@dataclass
class GranularityPolicy:
    id:                   UUID
    name:                 str
    is_global:            bool
    daily_horizon_days:   int  = 30   # [today, today+N) → day grain
    weekly_horizon_days:  int  = 90   # [today+N, today+M) → week grain
                                       # beyond today+M → month grain

    def grain_for_date(self, target_date: date, reference_date: date) -> str:
        delta = (target_date - reference_date).days
        if delta < self.daily_horizon_days:
            return "day"
        elif delta < self.weekly_horizon_days:
            return "week"
        else:
            return "month"
```

### Resolution at Runtime

```python
def resolve_policy(item_id: UUID, db: Session) -> GranularityPolicy:
    # Look for a per-item override via governed_by edge
    override = db.query(GranularityPolicy)\
        .join(Edge, Edge.target_node_id == GranularityPolicy.id)\
        .filter(Edge.source_node_id == item_id, Edge.edge_type == "governed_by")\
        .first()
    if override:
        return override
    return db.query(GranularityPolicy).filter_by(is_global=True).one()
```

### Recommended Default Breakpoints (Nico's Call — see §Decisions Requiring Human Arbitration)

Proposed starting values: `daily_horizon_days=28`, `weekly_horizon_days=91` (4 weeks / 13 weeks). These align with natural calendar months and quarters, which matters for forecast consumption reconciliation. The actual values are a business decision.

### Failure Modes

| Mode | Failure Mode |
|------|-------------|
| Global only | Critical long-lead items under-resolved; commodity items over-computed |
| Per-object only | Inter-item temporal inconsistency; propagation window resolution breaks |
| Hybrid (chosen) | Override proliferation if not governed; need monitoring on policy assignment coverage |

---

## Q3 — ProjectedInventory Storage Model

### Decision: Option A — One Node Per Time Bucket, with Segment Grouping

### Rationale

**Option B (JSONB timeline on one node) is disqualified** by the dirty-flag system. If the PI node for item X / location Y is a single node with 547 time buckets in a JSONB array, any bucket becoming dirty marks the entire node dirty, triggering re-computation of all 547 buckets. This is the antithesis of incremental computation. Additionally, AI agents cannot peg a `Shortage` node to a specific time bucket within a JSONB blob — they need a real node with a real ID.

**Option A (one node per bucket) is correct** — with a crucial optimization: buckets are grouped by a `projection_series_id` to allow bulk queries without full graph traversal.

### Node Schema Extensions

```sql
-- Add to nodes table:
ALTER TABLE nodes ADD COLUMN projection_series_id UUID;  -- groups all PI buckets for one (item, location)
ALTER TABLE nodes ADD COLUMN bucket_sequence      INTEGER; -- ordinal within series (for ordering without date parsing)

CREATE INDEX idx_nodes_series ON nodes(projection_series_id) WHERE projection_series_id IS NOT NULL;
CREATE INDEX idx_nodes_series_seq ON nodes(projection_series_id, bucket_sequence) WHERE projection_series_id IS NOT NULL;
```

### Node Count at Scale

For a 15-month horizon:
- 28 days daily = 28 nodes
- 63 days weekly (9 weeks) = 9 nodes  
- ~365 days monthly (12 months, minus the above) ≈ 10 nodes

**~47 PI nodes per (item, location) pair.** For 10,000 (item, location) pairs = 470,000 PI nodes. Entirely manageable in PostgreSQL with proper indexing.

### Dirty-Flag Interaction

```python
# When propagation modifies bucket at date T:
dirty_node_id = lookup_pi_node(item_id, location_id, date_T)
dirty_nodes_set.add(dirty_node_id)

# Downstream propagation only re-computes from the specific dirty bucket forward
# (topological sort guarantees this — each bucket's successor is the next bucket in series)
```

The `bucket_sequence` column enables efficient "compute all successors of dirty bucket N" queries:

```sql
SELECT id FROM nodes
WHERE projection_series_id = $1
  AND bucket_sequence >= $2
ORDER BY bucket_sequence;
```

### PI Node Creation

PI nodes are created (or destroyed) by the roll-forward process (see Q6). They are never created on the fly during propagation — projection_series membership is stable within a planning cycle.

---

## Q4 — Shortage Representation Spanning Multiple Granularity Zones

### Decision: One Shortage Node per Contiguous Shortage Event, with explicit zone anchoring

### Rationale

A shortage is a planning event, not a time bucket. It has a start, an end, a severity, and a resolution. Splitting it into two nodes (one weekly, one daily) because it crosses a zone boundary creates a coordination problem: both nodes must be kept consistent, queries for "all active shortages" must union across representations, and AI agents can't reason about a shortage as a single object.

**One Shortage node per shortage event.** The node carries the full span (`time_span_start`, `time_span_end`) in absolute dates, regardless of which granularity zone those dates fall in. The Shortage node's `time_grain` is the **finest grain that touches it** — if the shortage ends in the daily zone, the node is `time_grain = "day"`.

### Shortage Node Schema

```python
@dataclass
class ShortageNode:
    id:                UUID
    scenario_id:       UUID
    node_type:         str = "Shortage"
    item_id:           UUID
    location_id:       UUID
    
    # Temporal span (absolute dates, grain-independent)
    time_span_start:   date    # first day of shortage
    time_span_end:     date    # last day of shortage (inclusive)
    time_grain:        str     # finest grain in the span ("day" if any daily bucket affected)
    
    # Shortage severity
    shortage_qty:      Decimal # peak shortage quantity
    cumulative_qty:    Decimal # sum across all affected buckets
    
    # Resolution
    resolved:          bool    = False
    resolved_at:       Optional[date] = None
    resolution_type:   Optional[str] = None  # SUPPLY_ADDED | DEMAND_REDUCED | HORIZON_PASSED
    
    # Explanation
    root_cause_node_id: Optional[UUID] = None  # node that triggered this shortage
    explanation:       Optional[str]  = None
```

### Agent Query Interface

```python
# Query: all active shortages for an item, ordered by start date
def get_active_shortages(item_id: UUID, as_of: date) -> list[ShortageNode]:
    return db.query(ShortageNode)\
        .filter(
            ShortageNode.item_id == item_id,
            ShortageNode.resolved == False,
            ShortageNode.time_span_end >= as_of
        )\
        .order_by(ShortageNode.time_span_start)\
        .all()

# Query: does this shortage span a zone boundary?
def spans_zone_boundary(shortage: ShortageNode, policy: GranularityPolicy, today: date) -> bool:
    start_grain = policy.grain_for_date(shortage.time_span_start, today)
    end_grain   = policy.grain_for_date(shortage.time_span_end, today)
    return start_grain != end_grain
```

### Cross-Zone Shortage Buckets (for detailed breakdown)

For AI agents that need bucket-by-bucket detail within a shortage span, the Shortage node links to specific `ProjectedInventory` nodes via `impacts` edges. The PI nodes carry the per-bucket detail; the Shortage node is the aggregate planning object.

```
ShortageNode ──impacts──► PI_node(week_8)
ShortageNode ──impacts──► PI_node(week_9)
ShortageNode ──impacts──► PI_node(day_45)
ShortageNode ──impacts──► PI_node(day_46)
```

This gives agents a clean entry point (one Shortage node) and full traceability to the underlying buckets.

---

## Q5 — TemporalBridge: Centralized Component or Edge-Invoked Functions?

### Decision: Centralized TemporalBridge, edge-declared rules, propagation engine as the caller

### Rationale

If temporal conversion logic is distributed into edge methods, you get:
- Untestable logic scattered across 10,000+ edge instances
- Impossible to audit "what disaggregation assumptions are in this plan?"
- No single place to swap the HISTORICAL_PROFILE implementation
- Cross-edge consistency is unenforceable (two edges can disagree on how to split the same monthly bucket)

The TemporalBridge must be a centralized, stateless service. Edges carry **what rule to apply**. The Bridge knows **how to apply it**. The propagation engine (ADR-003) is the orchestrator that reads the edge rule and calls the Bridge.

### TemporalBridge Python Protocol

```python
from typing import Protocol, Sequence
from decimal import Decimal
from datetime import date
from dataclasses import dataclass

@dataclass
class TimeBucket:
    grain:      str    # "day" | "week" | "month" | "quarter"
    span_start: date
    span_end:   date   # inclusive
    value:      Decimal

@dataclass
class SplitRequest:
    source_value:   Decimal
    source_start:   date
    source_end:     date
    target_grain:   str
    mode:           SplitMode
    profile_id:     Optional[UUID] = None  # for HISTORICAL_PROFILE

@dataclass
class AggregateRequest:
    source_buckets: Sequence[TimeBucket]
    target_grain:   str
    target_start:   date
    target_end:     date

@dataclass
class AlignRequest:
    source_bucket:  TimeBucket
    target_start:   date
    target_end:     date
    mode:           AlignmentMode

class TemporalBridgeProtocol(Protocol):
    """
    Stateless temporal conversion service.
    All methods are pure functions of their inputs (+ optional profile data).
    Profile data is loaded by the Bridge from the DB at construction time (cached).
    """

    def split(self, req: SplitRequest) -> list[TimeBucket]:
        """
        Disaggregate a coarse-grain value into fine-grain buckets.
        FLAT: equal distribution by calendar days.
        FRONT_LOAD: geometric decay, heavier near start.
        HISTORICAL_PROFILE: weighted by historical demand pattern from profile_id.
        
        Contract: sum(result[i].value) == req.source_value (mass conservation).
        """
        ...

    def aggregate(self, req: AggregateRequest) -> TimeBucket:
        """
        Aggregate fine-grain buckets into a coarse-grain bucket.
        Partial overlap: proportional inclusion.
        
        Contract: result.value == sum of proportionally included source values.
        """
        ...

    def align(self, req: AlignRequest) -> Decimal:
        """
        Extract the value attributable to [target_start, target_end] 
        from a source bucket that partially overlaps.
        Used when two nodes share the same grain but different anchors
        (e.g., Mon-week vs. Sun-week).
        """
        ...

    def coverage_window(self, demand_node: 'Node', policy: GranularityPolicy) -> tuple[date, date]:
        """
        Determine the consumption window for a demand node.
        Returns (window_start, window_end) in absolute dates.
        Used by propagation engine to scope PI node loading.
        """
        ...

    def net_demand(self, forecast_daily: Decimal, confirmed_orders_daily: Decimal) -> Decimal:
        """
        ADR-002 forecast consumption rule: max(0, forecast - confirmed_orders).
        Centralized here to ensure consistent application.
        """
        return max(Decimal(0), forecast_daily - confirmed_orders_daily)
```

### Integration with ADR-003 Propagation Engine

```python
class PropagationEngine:
    def __init__(self, bridge: TemporalBridgeProtocol, ...):
        self.bridge = bridge

    def traverse_edge(self, edge: Edge, source_value: Decimal, source_node: Node, target_node: Node) -> Decimal | list[TimeBucket]:
        """Called during topological propagation for each edge."""
        if edge.temporal_rule is None or edge.temporal_rule == TemporalRule.PASSTHROUGH:
            return source_value
        
        if edge.temporal_rule == TemporalRule.SPLIT:
            return self.bridge.split(SplitRequest(
                source_value = source_value,
                source_start = source_node.time_span_start,
                source_end   = source_node.time_span_end,
                target_grain = target_node.time_grain,
                mode         = edge.split_mode,
                profile_id   = edge.profile_id,
            ))
        
        if edge.temporal_rule == TemporalRule.AGGREGATE:
            # source_value here is actually a list of fine-grain buckets
            return self.bridge.aggregate(AggregateRequest(...))
        
        if edge.temporal_rule == TemporalRule.ALIGN:
            return self.bridge.align(AlignRequest(...))
```

**Consequence:** The propagation engine owns the edge traversal logic. The Bridge owns the math. Edges own the rule declaration. Clean separation, each layer independently testable, Rust kernel replacement can reimplement the Bridge protocol without touching edge storage or propagation orchestration.

---

## Q6 — Horizon Roll-Forward

### Decision: Event-triggered scheduled job, with grain-change as a structural mutation producing an event

### Roll-Forward Mechanics

Roll-forward is **not** a background process that silently mutates the graph. It is an explicit planning operation that:
1. Emits a `HORIZON_ROLL_FORWARD` event into the events table
2. Executes structural mutations to PI nodes
3. Marks affected nodes dirty
4. Triggers incremental re-propagation for the changed region

### Trigger

Roll-forward runs **once per day**, triggered by a scheduled job (cron or Celery beat), not by real-time clock drift. It executes at a configurable time (e.g., 02:00 UTC) after the previous day's transactions are closed.

```python
@dataclass
class HorizonRollForwardEvent:
    event_type:     str = "HORIZON_ROLL_FORWARD"
    rolled_date:    date           # the date that "fell off" the daily horizon
    new_daily_date: date           # the date newly entering the daily zone
    # (which was previously in weekly zone)
    policy_id:      UUID           # which GranularityPolicy governs this roll
    affected_series: list[UUID]   # projection_series_ids that were mutated
```

### Structural Mutation: Weekly → Daily (Grain Refinement)

When day D enters the daily zone, the weekly bucket that contained D must be split.

```python
def refine_weekly_to_daily(series_id: UUID, week_bucket: Node, policy: GranularityPolicy, db: Session, dirty_set: set) -> list[Node]:
    """
    Replace one weekly PI node with 7 daily PI nodes.
    Old node is soft-deleted (or archived, not physically deleted — event sourcing).
    New nodes are inserted with appropriate bucket_sequence.
    All new nodes are marked dirty.
    """
    # 1. Archive the weekly node
    week_bucket.archived = True
    week_bucket.archived_reason = "ROLLED_TO_DAILY"
    db.add(week_bucket)
    
    # 2. Create 7 daily nodes
    daily_nodes = []
    for i in range(7):
        day = week_bucket.time_span_start + timedelta(days=i)
        node = ProjectedInventoryNode(
            projection_series_id = series_id,
            time_grain           = "day",
            time_span_start      = day,
            time_span_end        = day,
            bucket_sequence      = compute_sequence(series_id, day, db),
            # Initial value: split weekly value FLAT (will be recomputed via dirty propagation)
            projected_qty        = week_bucket.projected_qty / 7,
        )
        daily_nodes.append(node)
        dirty_set.add(node.id)
    
    db.bulk_save_objects(daily_nodes)
    
    # 3. Rewire edges
    # Any edge pointing to week_bucket must now point to the appropriate daily node
    rewire_edges(week_bucket.id, daily_nodes, db)
    
    # 4. Any Shortage nodes linked to week_bucket are re-evaluated via dirty propagation
    # (they will be re-detected or resolved during the next propagation cycle)
    
    return daily_nodes
```

### What Happens to Dirty Flags on Grain Change

**The old node is archived, not updated.** New nodes are created dirty. This is critical: the dirty-flag system operates on node IDs. If you mutate an existing node from weekly to daily, you'd need to update all in-memory references. Instead:

- Archived node is removed from `dirty_nodes` table (it will never be re-computed)
- New daily nodes are inserted into `dirty_nodes` immediately upon creation
- In-memory `dirty_nodes_set` is updated before propagation continues

```python
# In roll-forward transaction:
dirty_nodes_set.discard(week_bucket.id)
db.execute("DELETE FROM dirty_nodes WHERE node_id = :id", {"id": week_bucket.id})

for node in daily_nodes:
    dirty_nodes_set.add(node.id)
    db.execute("INSERT INTO dirty_nodes (node_id, dirtied_at) VALUES (:id, now())", {"id": node.id})
```

### What Happens to Shortage Nodes on Grain Refinement

Shortage nodes are **not automatically updated** during roll-forward. They are marked dirty and re-evaluated during the next propagation cycle.

Specific behavior:
1. A `Shortage` node with `time_span_start` in a now-daily zone retains its span dates (absolute dates are grain-independent — see Q4 design).
2. The `time_grain` of the Shortage node is updated from `"week"` to `"day"` if the finest affected bucket is now daily.
3. The `impacts` edges from the Shortage node are rewired from the archived weekly PI node to the new daily PI nodes.
4. The Shortage node is marked dirty, triggering re-detection. The re-detection may:
   - Confirm the shortage (now with daily precision)
   - Resolve it (daily visibility reveals supply that was invisible at weekly grain)
   - Split it into multiple shorter shortages

```python
def handle_shortage_on_roll(shortage: ShortageNode, old_pi: Node, new_daily_pis: list[Node], db: Session, dirty_set: set):
    # Update grain to finest
    if any(n.time_grain == "day" for n in new_daily_pis):
        shortage.time_grain = "day"
    
    # Rewire impacts edges
    for edge in db.query(Edge).filter_by(source_node_id=shortage.id, edge_type="impacts", target_node_id=old_pi.id):
        affected_days = [n for n in new_daily_pis 
                         if n.time_span_start >= shortage.time_span_start 
                         and n.time_span_end <= shortage.time_span_end]
        for daily_pi in affected_days:
            db.add(Edge(source_node_id=shortage.id, target_node_id=daily_pi.id, edge_type="impacts"))
        db.delete(edge)
    
    dirty_set.add(shortage.id)
```

### Far-End Bucket Creation (Monthly → Weekly)

As the horizon extends by one day (the "far end" rolls forward), a new monthly bucket may need to be created at month_start+15_months. This is simpler:
- A new PI node is inserted at the far end of each active projection_series
- It is marked dirty
- Propagation fills it forward from the last computed bucket

### Roll-Forward Transaction Guarantee

The entire roll-forward for a given `rolled_date` must be **atomic** (single Postgres transaction). If it fails mid-way, the next run detects the partially-rolled state via the events table and replays from the checkpoint.

```python
def roll_forward(rolled_date: date, db: Session):
    with db.begin():
        # 1. Emit event (if not already emitted for this date)
        if event_exists("HORIZON_ROLL_FORWARD", rolled_date, db):
            return  # idempotent
        
        policy = get_global_policy(db)
        cutoff_daily = date.today() + timedelta(days=policy.daily_horizon_days)
        
        # 2. Find all weekly PI nodes that now fall within daily zone
        weekly_to_refine = get_weekly_nodes_entering_daily_zone(cutoff_daily, db)
        
        # 3. Refine each
        affected_series = []
        for node in weekly_to_refine:
            refine_weekly_to_daily(node.projection_series_id, node, policy, db, dirty_nodes_set)
            affected_series.append(node.projection_series_id)
        
        # 4. Extend far end (if month boundary crossed)
        extend_far_end_if_needed(db, dirty_nodes_set)
        
        # 5. Emit event
        emit_event(HorizonRollForwardEvent(
            rolled_date      = rolled_date,
            new_daily_date   = cutoff_daily,
            policy_id        = policy.id,
            affected_series  = affected_series,
        ), db)
```

---

## Consolidated Schema Addenda

### temporal_profiles table (for HISTORICAL_PROFILE split mode)

```sql
CREATE TABLE temporal_profiles (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    grain       TEXT NOT NULL,          -- "day" | "week" — what grain the weights are expressed in
    weights     JSONB NOT NULL,         -- {"1": 0.12, "2": 0.15, ...} (day-of-month or day-of-week → weight)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### nodes table additions (to existing schema)

```sql
ALTER TABLE nodes ADD COLUMN projection_series_id  UUID;
ALTER TABLE nodes ADD COLUMN bucket_sequence        INTEGER;
ALTER TABLE nodes ADD COLUMN archived               BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE nodes ADD COLUMN archived_reason        TEXT;
```

---

## Decisions Requiring Human Arbitration (Nico's Call)

### ⚠️ ARBITRATION-1: Granularity Breakpoint Values

**Question:** What are the actual values for `daily_horizon_days` and `weekly_horizon_days`?

**Proposed:** 28 days daily, 91 days weekly (aligns with 4-week month and 13-week quarter)

**Alternative:** 30/90 (clean round numbers, misalign with ISO weeks), or 14/60 (higher precision near-term, typical for MTO environments)

**Impact:** Determines node count, storage sizing, and AI agent precision in the critical 30–90 day window. Cannot be changed without a roll-forward migration.

---

### ⚠️ ARBITRATION-2: Roll-Forward Time of Day

**Question:** What time (UTC) does the daily roll-forward job execute?

**Impact:** This determines when the daily zone shifts. If your largest customers send orders before 06:00 UTC and the roll happens at 02:00, you may process their orders in a stale daily bucket. Coordinate with transaction close time.

---

### ⚠️ ARBITRATION-3: Default Split Mode for Forecast Disaggregation

**Question:** Should the global default for monthly forecast → daily be `FLAT` or `HISTORICAL_PROFILE`?

**Proposed:** `FLAT` as default, `HISTORICAL_PROFILE` as opt-in per item category.

**Impact:** `FLAT` underestimates end-of-month demand surge for many consumer goods. `HISTORICAL_PROFILE` requires data infrastructure (profile loading, maintenance). This is a data readiness question as much as an architectural one.

---

### ⚠️ ARBITRATION-4: Shortage Re-Detection Scope on Roll-Forward

**Question:** After roll-forward grain refinement, should shortage re-detection be scoped to only the newly-daily buckets, or should the full affected (item, location) pair be re-propagated from the beginning of the daily zone?

**Proposed:** Full re-propagation from zone start. Partial re-detection risks leaving stale shortage boundaries.

**Impact:** Determines propagation cost of the daily roll-forward job. At 10,000 (item, location) pairs, full zone re-propagation may take minutes. If acceptable, simpler. If not, partial re-detection requires additional dirty-flag scoping logic.

---

## Consequences

### Positive

- Edge schema is fully self-describing: an agent can inspect any edge and know exactly what temporal conversion is applied, without querying the Bridge
- Shortage nodes are grain-agnostic by design: absolute date spans survive roll-forward without structural mutation
- PI node-per-bucket enables sub-node dirty marking, preserving ADR-003 incremental guarantees
- TemporalBridge as a centralized protocol enables Rust replacement of the kernel without changing edge storage or propagation orchestration
- Roll-forward is atomic, idempotent, and event-logged: full auditability

### Negative / Accepted Trade-offs

- Node count is higher than Option B (JSONB). 470K PI nodes for 10K (item, location) pairs is manageable but requires disciplined index maintenance
- Roll-forward is a structural mutation (node creation/archival), not a simple update. Requires careful transaction management
- Per-item GranularityPolicy overrides require governance: without monitoring, policies proliferate and the system becomes inconsistent at the horizon boundary between items

### Risks

- **Profile data quality:** HISTORICAL_PROFILE split mode is only as good as the historical data. Stale or sparse profiles silently produce wrong disaggregation. Mitigate: profile staleness check at Bridge initialization, fallback to FLAT if profile age > threshold.
- **Roll-forward failure:** If the scheduled job misses a day (infra issue), two days of weekly→daily refinement must run together. The idempotency check on the events table handles this, but double refinement at once may spike propagation load. Mitigate: rate-limit refinement to one day at a time, loop if catching up.
- **Zone boundary pegging:** A `pegged_to` edge from a daily demand node to a weekly supply node at the zone boundary will require an ALIGN or AGGREGATE conversion. If the edge was created before the zone shifted, its `temporal_rule` may be stale (was PASSTHROUGH when both were weekly, now needs ALIGN). Mitigate: roll-forward job includes an edge audit pass that updates temporal_rule on edges touching refined nodes.

---

## Summary of Decisions

| Question | Decision |
|----------|----------|
| Q1 — Edge temporal rules | Edges carry rule declaration (temporal_rule, split_mode, alignment_mode). Bridge executes. |
| Q2 — Breakpoints | Hybrid: global GranularityPolicy + per-item override via governed_by edge |
| Q3 — PI storage | One node per bucket, grouped by projection_series_id, bucket_sequence for ordering |
| Q4 — Shortage spanning zones | One Shortage node per event, absolute dates, finest grain wins, impacts edges to PI nodes |
| Q5 — TemporalBridge | Centralized Protocol/ABC; propagation engine calls Bridge using edge-declared rules |
| Q6 — Roll-forward | Daily scheduled job, atomic transaction, node archival + creation, dirty-flag handoff |
