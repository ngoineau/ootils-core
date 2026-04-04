# ADR-002c: Elastic Time Architecture — Layered Grain Model

**Status:** Proposed  
**Author:** Architecture Review (Expert Analysis)  
**Supersedes:** ADR-002b (rejected — zone-based ON/OFF model)  
**Date:** 2026-04-04  

---

## Context and Rejection Rationale

ADR-002b proposed an elastic time model with hard breakpoints (daily/weekly/monthly zones) and daily roll-forward structural mutations. The lead architect (Nico) rejected this on the following grounds:

> "I'm not convinced — we're on an ON/OFF logic that doesn't work for me. Think in layers: you can have day, month, or week at the same time. The blocking point is the raw data. After that, it's the presentation that changes."

The core objection: zone-based grain assignment treats granularity as a global, time-distance-dependent property applied to nodes. Nico's model treats granularity as an intrinsic property of the raw data source, and views as a separate concern applied at query time.

**What was wrong with ADR-002b:**

1. `ProjectedInventory` nodes were assigned a grain based on how far they were from today — an artificial coupling between calendar proximity and storage resolution.
2. Roll-forward mutated the graph structure daily, converting weekly PI nodes to daily ones as they crossed the 28-day threshold. This is a structural mutation driven by calendar arithmetic, not by data events.
3. `GranularityPolicy` with breakpoints acted as zone gates: once a node was in the "weekly zone," it could not coexist with daily data for the same (item, location) pair at the same time.
4. The TemporalBridge was positioned as a propagation-time service that converted grains during node computation, conflating the engine's job with the presentation layer's job.

This ADR replaces the zone model entirely.

---

## Design Principle: Three Layers, Three Responsibilities

```
┌─────────────────────────────────────────────────────────────┐
│  PRESENTATION LAYER                                         │
│  What agents and planners see. Grain is a query parameter.  │
│  TemporalBridge lives here. Aggregation/disaggregation      │
│  happens here. No mutable state.                            │
├─────────────────────────────────────────────────────────────┤
│  COMPUTATION LAYER                                          │
│  ProjectedInventory computed at finest resolvable grain     │
│  for each (item, location) pair. Dirty-flag propagation.    │
│  Shortage detection. No awareness of presentation needs.    │
├─────────────────────────────────────────────────────────────┤
│  STORAGE LAYER                                              │
│  Nodes stored at native grain. A PO is exact_date.          │
│  A forecast is month. A capacity constraint is week.        │
│  No forced conversion at ingestion. No zone assignment.     │
└─────────────────────────────────────────────────────────────┘
```

**What each layer is NOT responsible for:**

- Storage layer: NOT responsible for knowing what grain the planner needs.
- Computation layer: NOT responsible for presentation. It computes at the finest grain its inputs allow and stops there.
- Presentation layer: NOT responsible for planning logic. It is a read-only query service. It never writes to the graph.

---

## Evaluation of Interpretations

### Interpretation A — Native grain storage, view-layer aggregation

**Description:** Every node stored at native grain. ProjectedInventory computed and stored at the finest grain of its inputs. Query layer aggregates on the fly. No structural mutations.

**Assessment:**
- ✅ Raw data at native grain — satisfied.
- ✅ No zone transitions — satisfied.
- ✅ All grains can coexist — satisfied (a monthly forecast and daily PO both exist, both feed PI at daily grain).
- ✅ Agents query at any grain — satisfied via aggregation.
- ⚠️ Shortage detection: a daily shortage is visible because PI is daily. Aggregation at monthly grain may mask it — this must be handled explicitly in the presentation layer.
- ⚠️ Dirty-flag granularity: if PI is stored daily for a 15-month horizon (~450+ daily nodes per (item, location) pair), a change to a monthly forecast propagates dirty flags to every daily PI node within that month. That is up to 31 dirty flags per update — acceptable. A change to a single PO date dirtifies only the PI nodes from that date forward. Efficient.
- ⚠️ Disaggregation of monthly forecast into daily PI: requires an allocation rule. "How many units does this monthly forecast contribute to day 15?" — this requires a SplitMode (flat, front-loaded, profile-based). This rule must live somewhere (edge metadata is fine, inherited from ADR-002b).

**Verdict: VIABLE — this is the recommended base model.** Key condition: the disaggregation rule for coarse→fine mapping must be explicit and testable.

---

### Interpretation B — Multi-grain simultaneous representation

**Description:** For each (item, location) pair, maintain three parallel ProjectedInventory sets (daily, weekly, monthly), all computed and kept in sync. Engine writes to all three on propagation.

**Assessment:**
- ✅ All grains pre-computed — fast query.
- ✅ No zone transitions.
- ❌ Propagation cost: a single PO change triggers dirty-flag propagation for three parallel PI chains. At 15–18 month horizon, that's ~450 daily + ~65 weekly + ~18 monthly nodes per (item, location) pair — all dirtied and recomputed. Cascading three-way sync is expensive and adds failure surface.
- ❌ Consistency risk: if the weekly and daily chains disagree (due to rounding, split mode differences, or a recomputation ordering bug), the system holds contradictory truths simultaneously. This is a data integrity problem with no clean resolution path.
- ❌ Complexity cost: three times the storage, three times the propagation, three times the test surface.
- ❌ Disaggregation from monthly forecast to daily PI and to weekly PI simultaneously means two split computations per update — and they must sum identically or agents will see incoherence across grains.

**Verdict: NOT VIABLE.** The consistency problem alone disqualifies it. Pre-computing all three grains sounds clean but creates a distributed state consistency problem in a single-node planning engine. The presentation layer can aggregate from daily; pre-computed coarser grains add no correctness, only risk.

---

### Interpretation C — Native grain storage + lazy view materialization

**Description:** Engine operates at native grain. Presentation layer uses Postgres materialized views or cached projections refreshed on each propagation run.

**Assessment:**
- ✅ Clean separation of engine grain from view grain.
- ✅ Native grain storage — satisfied.
- ✅ No zone transitions.
- ⚠️ Materialized view refresh on every propagation run: if propagation runs frequently (event-triggered, per ADR-003), full materialized view refresh is expensive and negates the benefit of incremental propagation.
- ⚠️ Postgres `REFRESH MATERIALIZED VIEW` is non-incremental unless using `CONCURRENTLY`, which still requires a full re-scan. For 15-month horizons across many items, this does not compose well with the dirty-flag model.
- ✅ However: the lazy view concept is correct for the presentation layer. The question is implementation — Postgres materialized views are too blunt. A targeted, dirty-aware projection cache (refreshed only for dirty (item, location) pairs) is the right form.

**Verdict: VIABLE WITH CONDITIONS.** The concept is sound; the implementation must not be naive Postgres `REFRESH MATERIALIZED VIEW`. Instead, the presentation layer maintains a grain-aggregated read cache that is invalidated per (item, location) pair when that pair's PI nodes are recomputed. This becomes the recommended TemporalBridge implementation (see §TemporalBridge Redefinition).

---

### Interpretation D — Proposed: Native grain storage + finest-resolvable PI + dirty-aware projection cache

This is the recommended model. It synthesizes A and C, avoids B, and adds a key mechanism: the **effective grain** concept.

**Core idea:** ProjectedInventory is computed at the *finest grain resolvable from the available inputs* for each (item, location, time_span) combination. The grain is not assigned by zone; it is derived from the inputs.

- If the only input covering a future period is a monthly forecast → PI bucket for that period is monthly.
- If a daily PO is added to that same period → the engine creates daily PI buckets for that period (splitting the monthly forecast using the declared split mode) and recomputes.
- The grain of PI is dynamic in the sense that it follows the data, not the calendar.

This directly implements Nico's statement: "The blocking point is the raw data."

---

## Storage Layer: Mixed-Grain Node Model

### Node Table (unchanged from ADR-001/002b)

```sql
CREATE TABLE nodes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id     UUID NOT NULL REFERENCES scenarios(id),
    node_type       TEXT NOT NULL,
    item_id         UUID,
    location_id     UUID,

    -- Native grain (set at ingestion, never mutated by the engine)
    time_grain      TEXT NOT NULL,    -- 'exact_date' | 'week' | 'month' | 'quarter' | 'year'
    time_ref        DATE NOT NULL,    -- canonical reference date for the bucket
    time_span_start DATE NOT NULL,    -- inclusive start of the time bucket
    time_span_end   DATE NOT NULL,    -- exclusive end of the time bucket

    quantity        NUMERIC,
    unit            TEXT,
    is_dirty        BOOLEAN NOT NULL DEFAULT FALSE,
    computed_at     TIMESTAMPTZ,
    metadata        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Concrete coexistence example: same (item, location) pair, same calendar week

Three nodes can and do coexist for item X / location Y, all with overlapping time spans:

```
Node 1: ForecastDemand
  node_type     = 'ForecastDemand'
  time_grain    = 'month'
  time_ref      = '2026-05-01'
  time_span_start = '2026-05-01'
  time_span_end   = '2026-06-01'
  quantity      = 1200      -- monthly bucket from demand planning system

Node 2: PurchaseOrder
  node_type     = 'PurchaseOrder'
  time_grain    = 'exact_date'
  time_ref      = '2026-05-12'
  time_span_start = '2026-05-12'
  time_span_end   = '2026-05-13'
  quantity      = 300       -- specific receipt

Node 3: CapacityConstraint
  node_type     = 'CapacityConstraint'
  time_grain    = 'week'
  time_ref      = '2026-05-11'        -- week start (Mon)
  time_span_start = '2026-05-11'
  time_span_end   = '2026-05-18'
  quantity      = 800       -- weekly throughput cap
```

These three nodes are valid simultaneously. None is converted to another grain at ingestion. No zone logic assigns them to a zone. The engine reads all three and must reconcile them when computing ProjectedInventory.

### How they contribute to ProjectedInventory computation

The computation kernel determines the **effective grain** for the (item, location) pair's PI for May 2026 by taking the finest grain of all inputs with non-zero overlap for that period:

```python
def effective_grain_for_period(
    inputs: list[Node],
    period_start: date,
    period_end: date
) -> str:
    """
    Returns the finest grain among all input nodes overlapping the period.
    Grain ordering: exact_date < week < month < quarter < year
    """
    GRAIN_ORDER = {'exact_date': 0, 'week': 1, 'month': 2, 'quarter': 3, 'year': 4}
    
    overlapping = [
        n for n in inputs
        if n.time_span_start < period_end and n.time_span_end > period_start
    ]
    if not overlapping:
        return 'month'  # default coarsest grain when no inputs
    
    return min(overlapping, key=lambda n: GRAIN_ORDER[n.time_grain]).time_grain
```

For May 2026 in our example: finest grain is `exact_date` (the PO). Therefore, the engine computes daily PI buckets for May 2026. The monthly forecast is SPLIT (flat or profile-based) across the 31 days. The weekly capacity constraint is SPLIT across 7 days. The PO contributes to its specific day.

For a far-horizon month (e.g., October 2027) where the only input is a monthly forecast: effective grain is `month`. One PI node covers the whole month.

**This is the key mechanism.** The grain of ProjectedInventory is data-driven, not calendar-driven.

---

## ProjectedInventory Redefined

### Storage model: one node per time bucket, grain follows effective grain of inputs

ProjectedInventory (PI) nodes remain one-per-bucket (per ADR-002b Q3 rationale — JSONB blobs break dirty-flag isolation). What changes is how the grain of each bucket is determined.

```sql
-- Additional columns on nodes table for PI nodes specifically
-- (stored in metadata JSONB or as typed columns, TBD — ⚠️ HUMAN ARBITRATION NEEDED)

-- For ProjectedInventory nodes:
--   effective_grain  TEXT: the grain at which this PI node was computed
--   source_grain_mix JSONB: {'exact_date': 2, 'week': 1, 'month': 1} — how many inputs of each grain contributed
--   projection_series_id UUID: groups all PI nodes for the same (item, location, scenario) together
```

```sql
-- Projection series: groups PI nodes for bulk queries
CREATE TABLE projection_series (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id     UUID NOT NULL REFERENCES scenarios(id),
    item_id         UUID NOT NULL,
    location_id     UUID NOT NULL,
    horizon_start   DATE NOT NULL,
    horizon_end     DATE NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_computed_at TIMESTAMPTZ,
    UNIQUE(scenario_id, item_id, location_id)
);
```

```python
@dataclass
class ProjectedInventoryNode:
    id:                   UUID
    scenario_id:          UUID
    item_id:              UUID
    location_id:          UUID
    projection_series_id: UUID
    
    # Bucket definition
    time_grain:      str      # effective grain: 'exact_date' | 'week' | 'month'
    time_ref:        date     # canonical reference date
    time_span_start: date     # inclusive
    time_span_end:   date     # exclusive
    
    # Computed values
    opening_stock:   Decimal
    inflows:         Decimal  # POs, production orders, etc. arriving in this bucket
    outflows:        Decimal  # demand, consumption
    closing_stock:   Decimal  # = opening_stock + inflows - outflows
    
    # Shortage flag (set by computation kernel, not presentation layer)
    has_shortage:    bool
    shortage_qty:    Decimal
    
    # Propagation state
    is_dirty:        bool
    computed_at:     Optional[datetime]
    
    # Diagnostic
    source_grain_mix: dict    # {'exact_date': 2, 'week': 1, 'month': 0}
```

### Dirty-flag interaction

When a PO date changes for item X / location Y:

1. The PO node is marked dirty.
2. Propagation engine traverses edges from PO → PI nodes (via `replenishes` edges).
3. All PI nodes in the projection series for (item X, location Y) from the PO's date forward are marked dirty (cascade via `closes_on`/`feeds_forward` edges between sequential PI nodes).
4. The computation kernel recomputes dirty PI nodes in topological order.
5. If the change in PO date causes the effective grain for a period to change (e.g., the only daily-grain input for May was this PO, and it moved to June), the engine must restructure the PI buckets for May:
   - Delete existing daily PI nodes for May that no longer have daily-grain inputs.
   - Create a single monthly PI node for May (or weekly, depending on remaining inputs).
   - This is a structural mutation — but it is data-driven, not calendar-driven.

**Critical distinction from ADR-002b:** structural mutations happen when the *data* changes in a way that changes the effective grain, not when *time passes* and a node crosses an artificial horizon boundary.

### ⚠️ HUMAN ARBITRATION REQUIRED — PI restructuring frequency

**Question:** Effective grain changes when data changes. A new PO added to a month converts that month's PI from monthly to daily. This is a structural mutation with real cost (delete N nodes, create M nodes, re-propagate). At high data change frequency (many PO updates per day), this could thrash. 

**Options:**
1. Accept the cost — structural mutations only happen on data changes, which are bounded.
2. Add a minimum granularity floor per horizon segment: "within 60 days, PI is always daily regardless of inputs." This re-introduces a soft zone concept but without the calendar-cutoff ON/OFF logic — it's a floor, not a zone gate.
3. Deferred restructuring: batch structural mutations to off-peak windows, serve stale structure with dirty flags in the interim.

**Nico must decide.** Option 2 is operationally pragmatic but moves slightly back toward ADR-002b. Option 1 is architecturally pure but may need performance guardrails.

---

## TemporalBridge Redefined

### What the Bridge is NOT (in this model)

The Bridge is **not** involved in propagation-time computation. It does not split, aggregate, or align during the dirty-flag propagation pass. That work is done by the computation kernel when it resolves mixed-grain inputs into a PI bucket.

### What the Bridge IS

The Bridge is the **presentation-layer query service**. It has two responsibilities:

**1. Grain aggregation for queries**

When an agent asks: "what is projected inventory for item X at location Y for week 12 of 2026?" — the Bridge:
- Fetches all PI nodes for that (item, location) pair whose `time_span` overlaps week 12.
- If those PI nodes are daily-grain: aggregates `closing_stock` as the minimum within the week (for shortage surface) and the end-of-week closing stock for the point-in-time view, plus sum of inflows/outflows.
- Returns a unified `ProjectionView` object at the requested grain.

**2. Disaggregation for queries that request finer grain than stored**

If an agent asks for a daily breakdown of a period where PI is stored at monthly grain (far horizon, monthly-only inputs):
- The Bridge applies the same split logic as the computation kernel (flat by default, profile-based if available).
- Marks the response as `granularity_approximated: true` — the agent knows this is an estimate, not a computed daily value.
- This is a read-only, stateless operation. The Bridge never writes to the graph.

```python
class TemporalBridge:
    """
    Presentation-layer query service. Read-only. No propagation involvement.
    """
    
    def query_projection(
        self,
        item_id: UUID,
        location_id: UUID,
        query_start: date,
        query_end: date,
        requested_grain: str,   # 'day' | 'week' | 'month'
        scenario_id: UUID,
        db: Session
    ) -> ProjectionView:
        """
        Returns a ProjectionView at the requested grain.
        If PI is finer than requested: aggregates.
        If PI is coarser than requested: disaggregates with approximation flag.
        """
        pi_nodes = self._fetch_pi_nodes(item_id, location_id, query_start, query_end, scenario_id, db)
        buckets = self._resolve_to_requested_grain(pi_nodes, requested_grain, query_start, query_end)
        return ProjectionView(
            item_id=item_id,
            location_id=location_id,
            requested_grain=requested_grain,
            buckets=buckets,
            has_approximated_buckets=any(b.is_approximated for b in buckets),
        )
    
    def _resolve_to_requested_grain(
        self,
        pi_nodes: list[ProjectedInventoryNode],
        requested_grain: str,
        query_start: date,
        query_end: date,
    ) -> list[ProjectionBucket]:
        GRAIN_ORDER = {'day': 0, 'week': 1, 'month': 2}
        result = []
        for period_start, period_end in self._enumerate_periods(requested_grain, query_start, query_end):
            overlapping = [n for n in pi_nodes if n.time_span_start < period_end and n.time_span_end > period_start]
            if not overlapping:
                continue
            if GRAIN_ORDER[self._coarsest_grain(overlapping)] <= GRAIN_ORDER[requested_grain]:
                # PI is finer or same: aggregate
                bucket = self._aggregate(overlapping, period_start, period_end, requested_grain)
            else:
                # PI is coarser: disaggregate with approximation flag
                bucket = self._disaggregate(overlapping[0], period_start, period_end, mark_approximated=True)
            result.append(bucket)
        return result
```

### Bridge and the projection cache

For performance, the Bridge maintains a per-(item, location, grain) projection cache in Redis or Postgres (⚠️ HUMAN ARBITRATION NEEDED — see below). When PI nodes for an (item, location) pair are recomputed, the computation kernel invalidates the relevant cache entries. The Bridge then lazy-refreshes on next query.

This implements the "lazy view materialization" pattern from Interpretation C, but targeted (per pair, per grain) rather than full Postgres materialized views.

### ⚠️ HUMAN ARBITRATION REQUIRED — Bridge cache backend

**Options:**
1. Redis: fast, volatile. Lose cache on restart, warm-up cost at startup.
2. Postgres table `projection_cache (item_id, location_id, scenario_id, grain, period_start, period_end, data JSONB, computed_at)`: durable, slower writes, simpler ops.
3. No cache: recompute at query time from PI nodes every time. Correct, potentially slow for large catalogs.

Recommendation: start with option 3 (no cache), add option 2 (Postgres cache) when query latency becomes a bottleneck. Avoid Redis unless there is a clear operational reason to manage a second stateful service.

---

## Shortage Detection

### At what grain does the engine detect shortages?

**At the grain of the PI nodes.** The computation kernel sets `has_shortage = True` and `shortage_qty` on each PI node during propagation, not the presentation layer.

This means:

- A daily PI node with `closing_stock < 0` → shortage flagged at daily grain.
- A monthly PI node with `closing_stock < 0` → shortage flagged at monthly grain.
- A daily shortage masked at monthly grain: the daily PI node carries the shortage flag; the monthly aggregate may show positive stock (inflows in the second half of the month offset the shortage in the first half). This is correct and expected.

### How agents surface hidden shortages

The presentation layer (TemporalBridge) always includes shortage metadata in the `ProjectionView`:

```python
@dataclass
class ProjectionBucket:
    period_start:        date
    period_end:          date
    grain:               str
    closing_stock:       Decimal
    is_approximated:     bool
    
    # Shortage surface
    bucket_has_shortage: bool          # shortage at THIS bucket's grain
    sub_bucket_shortage: bool          # shortage at finer grain WITHIN this bucket (if PI is daily, hidden at weekly view)
    sub_bucket_shortage_detail: list   # list of (date, shortage_qty) for sub-bucket shortages
    shortage_qty:        Decimal       # qty short at this grain (0 if sub-bucket only)
```

When an agent queries a weekly view but there is a daily shortage within the week:
- `bucket_has_shortage = False` (week is net positive)
- `sub_bucket_shortage = True` (a daily node within the week has a shortage)
- `sub_bucket_shortage_detail = [{'date': '2026-05-14', 'shortage_qty': 45}]`

**Agents must check both flags.** The system never silently masks shortages in aggregated views.

This answers the core requirement: a shortage invisible at monthly grain IS visible at daily grain. The system surfaces both, always.

---

## Roll-Forward: Eliminated

### Decision: roll-forward as defined in ADR-002b is eliminated.

Roll-forward in ADR-002b did two things:

1. **Archived weekly PI nodes and replaced them with 7 daily nodes** when they crossed the daily-zone threshold. This is calendar-driven structural mutation. Eliminated.

2. **Rolled the projection horizon forward by one day** each day, adding new far-horizon PI buckets. This is actually a legitimate need.

### What replaces it: Horizon Extension

The planning engine runs a periodic **Horizon Extension** job (not daily mutation, but a scheduled maintenance operation):

```python
def extend_horizon(scenario_id: UUID, target_horizon_end: date, db: Session):
    """
    For each (item, location) pair, ensure PI nodes exist through target_horizon_end.
    Does NOT restructure existing nodes.
    Only creates nodes for periods not yet covered.
    Uses coarsest available grain for new far-horizon periods (typically 'month').
    """
    for series in db.query(ProjectionSeries).filter_by(scenario_id=scenario_id):
        if series.horizon_end < target_horizon_end:
            # Create new PI nodes for [series.horizon_end, target_horizon_end)
            # at 'month' grain (no daily-grain inputs exist for far future)
            _create_far_horizon_nodes(series, series.horizon_end, target_horizon_end, db)
            series.horizon_end = target_horizon_end
```

This is not a daily structural mutation. It is a maintenance job that only appends new nodes at the far end when the horizon needs to grow. No existing nodes are restructured.

**Triggered by:**
- System startup (ensure horizon is at least 18 months from today).
- When the engine date advances past a configurable threshold (e.g., horizon_end < today + 15 months).
- On demand via API.

**What it does NOT do:**
- Does not restructure existing nodes from weekly→daily grain.
- Does not archive nodes.
- Does not change existing PI node grain based on calendar proximity.

---

## Edge Schema: Retained with Clarification

Edge-based temporal rules from ADR-002b are retained. The `temporal_rule`, `split_mode`, and `alignment_mode` fields on edges remain valid and are now more important — they are the contract that tells the computation kernel how to handle grain mismatches when computing PI at the effective grain.

One addition: edges must now carry a `disaggregation_default` flag for the Bridge to use when it needs to disaggregate far-horizon PI for fine-grain agent queries:

```sql
ALTER TABLE edges ADD COLUMN disaggregation_default TEXT DEFAULT 'FLAT'
    CHECK (disaggregation_default IN ('FLAT', 'FRONT_LOAD', 'PROFILE'));
```

This is the default split mode the Bridge applies when disaggregating coarse PI nodes at query time (marked as approximated).

---

## Incremental Propagation: Unchanged

The dirty-flag system (ADR-003) is unchanged. The delta from this ADR:

1. **Dirty propagation now includes effective-grain recalculation.** When a PI node is recomputed, the kernel checks whether the effective grain of the period has changed. If it has, the kernel triggers a **grain restructuring** before propagation continues.

2. **Grain restructuring is a transactional operation:** delete old PI nodes for the affected period, create new ones at the new grain, update `dirty_nodes` accordingly — all within one DB transaction.

3. **Cascade on grain change:** when a period's PI is restructured from monthly to daily (because a new exact-date PO was added), the PI nodes for subsequent periods (which carry forward the closing stock) are also dirtied — standard propagation cascade applies.

```python
def recompute_pi_node(node: ProjectedInventoryNode, db: Session, dirty_set: set):
    """
    Recomputes a single PI node. Called by propagation engine in topological order.
    """
    inputs = _fetch_input_nodes(node, db)
    new_effective_grain = effective_grain_for_period(inputs, node.time_span_start, node.time_span_end)
    
    if new_effective_grain != node.time_grain:
        # Grain has changed — restructure this period
        new_nodes = _restructure_period(node, new_effective_grain, db)
        # Add new nodes to dirty set for this propagation pass
        dirty_set.update(n.id for n in new_nodes)
        return  # original node is gone, new nodes will be picked up in the same pass
    
    # Normal recomputation
    opening = _get_opening_stock(node, db)
    inflows = _compute_inflows(inputs, node.time_span_start, node.time_span_end, node.time_grain)
    outflows = _compute_outflows(inputs, node.time_span_start, node.time_span_end, node.time_grain)
    
    node.opening_stock = opening
    node.inflows = inflows
    node.outflows = outflows
    node.closing_stock = opening + inflows - outflows
    node.has_shortage = node.closing_stock < 0
    node.shortage_qty = abs(min(node.closing_stock, Decimal(0)))
    node.is_dirty = False
    node.computed_at = datetime.utcnow()
    db.add(node)
```

---

## ⚠️ Decisions Requiring Human Arbitration (Summary)

| # | Decision | Options | Recommendation |
|---|----------|---------|----------------|
| 1 | **PI restructuring cost policy** | (a) pure data-driven, (b) minimum grain floor within N days, (c) deferred batch | Start with (a), add (b) only if thrash is observed |
| 2 | **Bridge projection cache backend** | Redis, Postgres, none | Start with none; add Postgres cache at scale |
| 3 | **Horizon extension schedule** | Daily cron, on-demand, startup | Startup + on-demand API; cron only if needed |
| 4 | **Disaggregation default** | FLAT, FRONT_LOAD, PROFILE | FLAT for now; revisit when demand pattern data is available |
| 5 | **Source grain mix tracking** | Typed columns on nodes vs. JSONB metadata | JSONB for flexibility; typed columns if queried frequently |
| 6 | **Sub-bucket shortage detail depth** | Always include, optional via query param, never include | Optional via query param `include_sub_shortage_detail=true` |

---

## Architecture Decisions Summary

| Topic | ADR-002b (rejected) | ADR-002c (this) |
|-------|---------------------|-----------------|
| PI grain assignment | Zone-based (calendar proximity) | Data-driven (finest input grain) |
| Roll-forward | Daily structural mutation | Horizon Extension (append-only, maintenance job) |
| GranularityPolicy | Zone gates with breakpoints | Eliminated as a gate; retained only as a default for far-horizon init grain |
| TemporalBridge | Propagation-time conversion | Presentation-layer query service only |
| Shortage detection | At zone grain | At native PI grain; presentation layer surfaces sub-grain shortages |
| Structural mutations | Calendar-triggered | Data-triggered only |
| Mixed-grain coexistence | Not possible in same zone | Native — three grains in the same (item, location, period) tuple |

---

## References

- ADR-001: Graph model — node/edge type system
- ADR-002: Elastic time — origin principles (time as object property)
- ADR-002b: Rejected — zone-based model
- ADR-003: Incremental propagation — dirty-flag system
- ADR-005: Storage layer — PostgreSQL persistence decisions
