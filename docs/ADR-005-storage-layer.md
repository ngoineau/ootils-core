# ADR-005: Storage Layer and Data Model

**Status:** Superseded for runtime persistence, retained as historical design context  
**Date:** 2026-04-03  
**Author:** Architecture Review (Claw / subagent)  
**Milestone:** M1 — Data Model

---

## Context

The graph domain model is settled (ADR-001). The object-local time model is settled (ADR-002). The incremental propagation algorithm is settled (ADR-003). The explainability model is settled (ADR-004).

What is not settled: **how does everything actually get stored, indexed, and queried?**

This ADR answers the original proof-stage question set:
1. Which persistence technology to use at proof stage
2. The complete SQL schema (nodes, edges, events, scenarios, explanations)
3. How scenario isolation works without full data copies
4. The indexing strategy for the access patterns the engine actually uses
5. The upgrade path when the proof grows up

**Proof-stage sizing:**
- ~100 SKUs (Items)
- 5 Locations
- 5–6 Scenarios (1 baseline + 4–5 variants)
- Planning horizon: 52 weeks (daily grain = 365 rows per item/location)
- Estimated live nodes: ~60,000–100,000
- Estimated live edges: ~200,000–350,000
- Estimated events (rolling 90 days): ~50,000
- Estimated explanations: ~10,000–30,000
- Total database footprint: **< 100 MB**

---

> Status note, the live runtime is PostgreSQL via psycopg3. The SQLite-first sections
> below describe an earlier proof-stage design and should not be read as the current
> production or demo runtime architecture.
>
> JSONB note: the live runtime still avoids JSONB for core planning structures. Limited
> exceptions are allowed for diagnostic or staging payloads whose shape is intentionally
> dynamic, such as `dq_agent_runs.summary` and staging-layer raw import payloads.

## Decision 1: Persistence Technology

**Historical proof-stage choice: use SQLite for the proof stage.**

The reasoning is in section 5 of this ADR. The key points:

- The data volume is trivially small for SQLite (well under its practical ceiling of ~1 GB for write-heavy, ~35 GB for read-heavy)
- All graph traversal happens at the application layer (ADR-001 — confirmed)
- SQLite requires zero infrastructure: no server, no connection string in config, no container to run, portable single file
- The schema design here ports to Postgres without modification (SQLite's TYPE dialect is a near-strict subset)
- If the proof stage succeeds and V2 needs Postgres, migration is a `pg_dump`-equivalent away

---

## Decision 2: Schema Design Philosophy

**Single nodes table, single edges table. Type-specific fields live in a JSON `attributes` column.**

Why not 18 separate node-type tables?

1. At proof scale, 18 tables add zero performance benefit and massive join complexity
2. The engine treats nodes generically (typed but polymorphic) — a single table mirrors that
3. The 5–6 fields shared across all node types (`item_id`, `location_id`, `qty`, `time_ref`, `status`) cover 80% of all queries; type-specific fields rarely need to be queried at the SQL layer
4. Schema migration when adding node types is a no-op (add a new `node_type` value + update `attributes` contract)

SQLite supports JSON extraction (`json_extract(attributes, '$.field')`) with full index support since 3.38 — good enough for proof stage.

---

## Decision 3: Scenario Isolation Strategy

**Scenarios use a delta/override model, not full data copies.**

- The `baseline` scenario owns all reference nodes and all initial supply/demand nodes
- Variant scenarios store only the `scenario_overrides` deltas (field-level overrides on baseline nodes) and any net-new nodes that don't exist in baseline
- The engine resolves the effective state of any node as: `scenario_override(node) COALESCE baseline(node)`
- This means 5 scenario variants share ~95% of their data, not 5× the storage

---

## Schema

### 0. Reference tables (scenario-independent)

```sql
CREATE TABLE items (
    item_id      TEXT PRIMARY KEY,              -- e.g. "SKU-001"
    name         TEXT NOT NULL,
    item_type    TEXT NOT NULL                  -- finished_good | component | raw_material | semi_finished
                 CHECK (item_type IN ('finished_good','component','raw_material','semi_finished')),
    uom          TEXT NOT NULL DEFAULT 'EA',
    status       TEXT NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active','obsolete','phase_out')),
    attributes   TEXT NOT NULL DEFAULT '{}',    -- JSON: lead_time, moq, batch_size, safety_stock_policy_ref
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE locations (
    location_id   TEXT PRIMARY KEY,             -- e.g. "DC-ATL"
    name          TEXT NOT NULL,
    location_type TEXT NOT NULL
                  CHECK (location_type IN ('plant','dc','warehouse','supplier_virtual','customer_virtual')),
    country       TEXT,                         -- ISO-2
    timezone      TEXT,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE suppliers (
    supplier_id       TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    reliability_score REAL NOT NULL DEFAULT 1.0
                      CHECK (reliability_score BETWEEN 0.0 AND 1.0),
    attributes        TEXT NOT NULL DEFAULT '{}',  -- JSON: lead_time_policy, payment_terms, etc.
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE policies (
    policy_id      TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    policy_type    TEXT NOT NULL               -- safety_stock | allocation_priority | frozen_zone | moq | sourcing
                   CHECK (policy_type IN ('safety_stock','allocation_priority','frozen_zone','moq','sourcing','custom')),
    scope_item_id     TEXT REFERENCES items(item_id),      -- NULL = applies globally
    scope_location_id TEXT REFERENCES locations(location_id),
    effective_start   TEXT,                    -- ISO date; NULL = always effective
    effective_end     TEXT,
    parameters        TEXT NOT NULL DEFAULT '{}',  -- JSON: type-specific rule parameters
    version_no        INTEGER NOT NULL DEFAULT 1,
    active            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
```

---

### 1. Scenarios

```sql
CREATE TABLE scenarios (
    scenario_id        TEXT PRIMARY KEY,        -- e.g. "baseline", "scenario-expedite-po991"
    name               TEXT NOT NULL,
    description        TEXT,
    parent_scenario_id TEXT REFERENCES scenarios(scenario_id),  -- NULL for baseline
    is_baseline        BOOLEAN NOT NULL DEFAULT FALSE,
    status             TEXT NOT NULL DEFAULT 'active'
                       CHECK (status IN ('active','running','archived','failed')),
    created_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- Guaranteed baseline record; application enforces exactly one is_baseline = TRUE
INSERT INTO scenarios (scenario_id, name, is_baseline)
VALUES ('baseline', 'Baseline', TRUE);
```

---

### 2. Nodes

```sql
CREATE TABLE nodes (
    -- Identity
    node_id      TEXT PRIMARY KEY,              -- UUID v4
    node_type    TEXT NOT NULL,                 -- Item | Location | ForecastDemand | CustomerOrderDemand |
                                                -- DependentDemand | TransferDemand | OnHandSupply |
                                                -- PurchaseOrderSupply | WorkOrderSupply | TransferSupply |
                                                -- PlannedSupply | CapacityBucket | MaterialConstraint |
                                                -- ProjectedInventory | Shortage | Resource | Supplier | Policy
    business_key TEXT NOT NULL,                 -- Human-readable source reference; e.g. "CO-778-LINE-3"
    scenario_id  TEXT NOT NULL REFERENCES scenarios(scenario_id),

    -- Common planning dimensions (NULL where not applicable to node_type)
    item_id      TEXT REFERENCES items(item_id),
    location_id  TEXT REFERENCES locations(location_id),
    supplier_id  TEXT REFERENCES suppliers(supplier_id),

    -- Quantity
    qty          REAL,
    qty_uom      TEXT,

    -- Time (object-local, per ADR-002)
    time_grain       TEXT CHECK (time_grain IN ('exact_datetime','day','week','month','quarter','timeless')),
    time_ref         TEXT,                      -- ISO date or period anchor; e.g. "2026-04-15" or "2026-04"
    time_span_start  TEXT,                      -- ISO date (inclusive)
    time_span_end    TEXT,                      -- ISO date (inclusive)

    -- State
    status       TEXT NOT NULL DEFAULT 'active',
    is_dirty     BOOLEAN NOT NULL DEFAULT FALSE,-- Dirty flag for incremental propagation (ADR-003)
    active       BOOLEAN NOT NULL DEFAULT TRUE,

    -- Type-specific fields
    attributes   TEXT NOT NULL DEFAULT '{}',   -- JSON blob; see node-dictionary.md for per-type contracts

    -- Audit
    version_no   INTEGER NOT NULL DEFAULT 1,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
```

---

### 3. Edges

```sql
CREATE TABLE edges (
    edge_id      TEXT PRIMARY KEY,              -- UUID v4
    edge_type    TEXT NOT NULL,                 -- replenishes | consumes | depends_on | requires_component |
                                                -- produces | uses_capacity | bounded_by | governed_by |
                                                -- transfers_to | originates_from | pegged_to |
                                                -- substitutes_for | prioritized_over | impacts
    from_node_id TEXT NOT NULL REFERENCES nodes(node_id),
    to_node_id   TEXT NOT NULL REFERENCES nodes(node_id),
    scenario_id  TEXT NOT NULL REFERENCES scenarios(scenario_id),

    -- Edge properties
    priority     INTEGER NOT NULL DEFAULT 0,    -- Lower = higher priority (for ordered traversal)
    weight_ratio REAL    NOT NULL DEFAULT 1.0,  -- Allocation ratio, BOM ratio, etc.
    effective_start TEXT,                       -- ISO date; NULL = always effective
    effective_end   TEXT,

    -- Type-specific properties
    attributes   TEXT NOT NULL DEFAULT '{}',   -- JSON blob

    -- State
    active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
```

---

### 4. Events (immutable log)

```sql
CREATE TABLE events (
    event_id        TEXT PRIMARY KEY,           -- UUID v4
    event_type      TEXT NOT NULL,              -- supply_date_changed | demand_qty_changed |
                                                -- onhand_updated | policy_changed |
                                                -- structure_changed | scenario_created | calc_triggered
    scenario_id     TEXT NOT NULL REFERENCES scenarios(scenario_id),
    trigger_node_id TEXT REFERENCES nodes(node_id), -- The node that changed (NULL for structural events)

    -- Delta payload
    payload         TEXT NOT NULL DEFAULT '{}', -- JSON: {"before": {...}, "after": {...}}

    -- Provenance
    source          TEXT NOT NULL DEFAULT 'api'
                    CHECK (source IN ('api','ingestion','engine','user','test')),
    user_ref        TEXT,                       -- Optional: who triggered it
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- Events are insert-only. No UPDATE or DELETE. This is the audit log.
```

---

### 5. Calculation runs

```sql
CREATE TABLE calc_runs (
    calc_run_id          TEXT PRIMARY KEY,       -- UUID v4
    scenario_id          TEXT NOT NULL REFERENCES scenarios(scenario_id),
    trigger_event_id     TEXT REFERENCES events(event_id),  -- NULL for full recompute
    is_full_recompute    BOOLEAN NOT NULL DEFAULT FALSE,

    -- Scope of this run
    dirty_node_count     INTEGER,               -- Nodes marked dirty before run
    nodes_recalculated   INTEGER DEFAULT 0,
    nodes_unchanged      INTEGER DEFAULT 0,     -- Propagation stopped early (delta = 0)

    -- Timing
    status               TEXT NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending','running','complete','failed')),
    started_at           TEXT,
    completed_at         TEXT,
    error_message        TEXT,                  -- NULL unless status = failed

    created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
```

---

### 6. Explanations

```sql
-- One explanation per (target_node, calc_run) pair
CREATE TABLE explanations (
    explanation_id      TEXT PRIMARY KEY,        -- UUID v4
    calc_run_id         TEXT NOT NULL REFERENCES calc_runs(calc_run_id),
    target_node_id      TEXT NOT NULL REFERENCES nodes(node_id),
    target_type         TEXT NOT NULL,           -- Shortage | ProjectedInventory
    root_cause_node_id  TEXT REFERENCES nodes(node_id),  -- Terminal node in the causal chain

    -- Human-readable outputs
    summary             TEXT,                    -- 1-line plain English
    detail              TEXT,                    -- Full prose narrative

    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- Normalized causal steps (the structured path AI agents traverse)
CREATE TABLE explanation_steps (
    step_id          TEXT PRIMARY KEY,
    explanation_id   TEXT NOT NULL REFERENCES explanations(explanation_id),
    step_order       INTEGER NOT NULL,           -- 1-indexed, ascending = cause → effect
    node_id          TEXT REFERENCES nodes(node_id),   -- NULL for policy/rule steps
    node_type        TEXT,
    edge_type        TEXT,                       -- Edge type connecting this step to the next
    fact             TEXT NOT NULL,              -- Plain-English statement of this step's contribution
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (explanation_id, step_order)
);
```

---

### 7. Scenario delta / override

```sql
-- Field-level overrides for scenario variants
-- Resolving effective node state: scenario_override COALESCE baseline node
CREATE TABLE scenario_overrides (
    override_id    TEXT PRIMARY KEY,             -- UUID v4
    scenario_id    TEXT NOT NULL REFERENCES scenarios(scenario_id),
    node_id        TEXT NOT NULL REFERENCES nodes(node_id),  -- baseline node being overridden
    override_type  TEXT NOT NULL,               -- qty | date | status | attribute | full_replace
    override_value TEXT NOT NULL,               -- JSON: {"field": "due_date", "value": "2026-04-18"}
    rationale      TEXT,                        -- Why this override exists (agent-provided or user note)
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (scenario_id, node_id, override_type)
);
```

---

## Indexing Strategy

### Access patterns the engine actually uses

| Pattern | Table | Frequency |
|---------|-------|-----------|
| Get all dirty nodes for a scenario | nodes | Every propagation cycle |
| Get all nodes for (item, location, scenario) | nodes | Core engine loop |
| Get all nodes of type X in a scenario | nodes | Engine + API queries |
| Get all nodes in a time window | nodes | Temporal bridge |
| Outbound graph traversal (from_node_id) | edges | Dirty flag propagation |
| Inbound graph traversal (to_node_id) | edges | Explanation construction |
| Get edges of type X from node Y | edges | Engine: pegged_to, consumes, replenishes |
| Get recent events for a scenario | events | Event processor |
| Get all events for a trigger node | events | Audit / agent queries |
| Get explanation for a result node | explanations | API: /explain |
| Get all steps for an explanation | explanation_steps | API: /explain |
| Get active overrides for a scenario | scenario_overrides | Engine: scenario resolution |

### Index definitions

```sql
-- nodes: the hot table
CREATE INDEX idx_nodes_scenario_type
    ON nodes (scenario_id, node_type) WHERE active = TRUE;

CREATE INDEX idx_nodes_item_location_scenario
    ON nodes (item_id, location_id, scenario_id) WHERE active = TRUE;

-- Time-bounded scans: the Temporal Bridge's primary access path
CREATE INDEX idx_nodes_time_window
    ON nodes (scenario_id, item_id, location_id, time_span_start, time_span_end)
    WHERE active = TRUE;

CREATE INDEX idx_nodes_dirty
    ON nodes (scenario_id, is_dirty) WHERE is_dirty = TRUE;

CREATE INDEX idx_nodes_business_key
    ON nodes (business_key);

-- edges: traversal in both directions
CREATE INDEX idx_edges_from
    ON edges (from_node_id, edge_type) WHERE active = TRUE;

CREATE INDEX idx_edges_to
    ON edges (to_node_id, edge_type) WHERE active = TRUE;

CREATE INDEX idx_edges_scenario
    ON edges (scenario_id, edge_type) WHERE active = TRUE;

-- events: chronological + by node
CREATE INDEX idx_events_scenario_type
    ON events (scenario_id, event_type, created_at DESC);

CREATE INDEX idx_events_trigger_node
    ON events (trigger_node_id, created_at DESC);

-- explanations
CREATE INDEX idx_explanations_target
    ON explanations (target_node_id, calc_run_id);

CREATE INDEX idx_explanation_steps_explanation
    ON explanation_steps (explanation_id, step_order);

-- scenario_overrides
CREATE INDEX idx_overrides_scenario_node
    ON scenario_overrides (scenario_id, node_id) WHERE active = TRUE;
```

---

## Scenario Resolution Query Pattern

The engine resolves the effective state of a node for a scenario variant using this pattern:

```sql
-- Get effective node state for a scenario (with fallback to baseline)
-- Used by engine for every node read during propagation

WITH baseline_node AS (
    SELECT * FROM nodes
    WHERE node_id = :node_id AND scenario_id = 'baseline'
),
override AS (
    SELECT override_value FROM scenario_overrides
    WHERE node_id = :node_id
      AND scenario_id = :scenario_id
      AND override_type = 'full_replace'
      AND active = TRUE
    LIMIT 1
)
SELECT
    CASE WHEN override.override_value IS NOT NULL
         THEN json_patch(baseline_node.attributes, override.override_value)
         ELSE baseline_node.attributes
    END AS effective_attributes,
    baseline_node.*
FROM baseline_node
LEFT JOIN override ON TRUE;
```

For simple field-level overrides (qty, date), the engine applies them post-fetch in Python — no need to push complex JSON merge into SQL at this stage.

---

## Dirty Flag Propagation Query

The incremental propagation algorithm (ADR-003) uses two queries in a loop:

```sql
-- 1. Fetch all dirty nodes for a scenario (topological sort happens in Python)
SELECT node_id, node_type, item_id, location_id, time_span_start, time_span_end
FROM nodes
WHERE scenario_id = :scenario_id
  AND is_dirty = TRUE
  AND active = TRUE
ORDER BY time_span_start;  -- Rough chronological order; engine does topological sort in-memory

-- 2. Mark downstream neighbors dirty after a node recomputes
UPDATE nodes
SET is_dirty = TRUE, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE node_id IN (
    SELECT to_node_id FROM edges
    WHERE from_node_id = :recomputed_node_id
      AND active = TRUE
      AND edge_type IN ('replenishes','consumes','depends_on','produces','transfers_to','impacts')
);
```

---

## Data Volume Estimates at Proof Scale

| Table | Est. rows | Est. size |
|-------|-----------|-----------|
| items | 100 | < 50 KB |
| locations | 5 | < 5 KB |
| suppliers | 20–50 | < 25 KB |
| policies | 50–200 | < 100 KB |
| scenarios | 6 | < 5 KB |
| nodes | 80,000 | ~40 MB |
| edges | 250,000 | ~50 MB |
| events (90-day rolling) | 50,000 | ~10 MB |
| calc_runs | 5,000 | < 5 MB |
| explanations | 20,000 | ~5 MB |
| explanation_steps | 80,000 | ~10 MB |
| scenario_overrides | 500 | < 1 MB |
| **Total** | | **< 125 MB** |

This is well within SQLite's practical performance envelope. A full-table scan of `nodes` at 80K rows takes < 5 ms. Indexed queries are sub-millisecond.

---

## Technology Tradeoff: SQLite vs Postgres vs Graph DB

### At proof stage (100 SKUs, 5 locations, 6 scenarios)

| Criterion | SQLite | Postgres | Neo4j/Neptune |
|-----------|--------|----------|---------------|
| Data volume | ✅ Ideal | ✅ Fine | ✅ Fine |
| Operational complexity | ✅ Zero | ❌ Server required | ❌❌ Server + license |
| Graph traversal performance | ✅ Sufficient (app-layer) | ✅ Sufficient | ✅ Native (overkill) |
| JSON column support | ✅ json_extract() | ✅ JSONB (better) | n/a |
| Write concurrency | ⚠️ Single writer | ✅ MVCC | ✅ |
| Portability | ✅ Single file | ❌ Connection config | ❌ |
| Schema migration | ✅ Easy | ✅ Easy | ❌ Schema-less risk |
| Read-only analytics | ✅ Excellent | ✅ Excellent | ⚠️ Query language cliff |
| Migration to Postgres later | ✅ Trivial | — | ❌ Rewrite |
| Ecosystem / Python drivers | ✅ stdlib | ✅ psycopg3 | ⚠️ py2neo |

**Verdict:**
- **SQLite: use it.** For a proof that runs on a laptop, is developed by a small team, and fits in 125 MB, SQLite is not a compromise — it is the right tool. Its single-writer limitation is irrelevant; the engine is single-threaded by design (ADR-003: deterministic ordered propagation).
- **Postgres: the upgrade target.** When V2 adds multi-user API access, concurrent writes from an agent fleet, or data volume crosses 1 GB, migrate. The schema is identical; add `JSONB` for `attributes` columns, add `RETURNING`, done.
- **Graph DB (Neo4j/Neptune): definitively deferred.** ADR-001 already justified SQL+app-layer over native graph. At proof scale (100 SKUs), native graph traversal gains are in microseconds. The operational overhead and ecosystem lock-in are not worth it. Revisit at V3 if traversal depth exceeds 10 hops routinely and latency becomes the bottleneck.

---

## File Layout

```
ootils-core/
└── src/
    └── ootils_core/
        ├── db/
        │   ├── __init__.py
        │   ├── connection.py       # SQLite connection management + WAL mode config
        │   ├── migrations/
        │   │   ├── 001_initial_schema.sql   # Tables above, exact copy
        │   │   └── 002_seed_baseline.sql    # INSERT baseline scenario + reference test data
        │   └── schema.py           # Python dataclass representations of each table row
        ├── models/                 # Existing — domain objects (unchanged)
        ├── engine/                 # Existing — add db-aware variants of policies.py
        └── tools/                  # Existing
```

The `connection.py` module must enable WAL mode on every connection:
```python
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA foreign_keys=ON")
conn.execute("PRAGMA synchronous=NORMAL")   # Safe with WAL; faster than FULL
```

---

## Concrete Recommendations

1. **Ship `001_initial_schema.sql` as the M1 deliverable.** It is the canonical source of truth for the data model. Every other component (engine, API, tests) reads from this file.

2. **Use UUIDs for all primary keys** (`uuid.uuid4()` in Python, stored as TEXT). Never use auto-increment integers — they create coupling between scenarios and make delta/override reasoning harder.

3. **All timestamps in ISO 8601 UTC** (`2026-04-03T14:23:11.000Z`). Stored as TEXT in SQLite. No datetime type ambiguity.

4. **Make `events` insert-only from day one.** Enforce this in code (no UPDATE/DELETE in the events module). This is your audit trail and your agent's ground truth for "what changed and when."

5. **`ProjectedInventory` and `Shortage` nodes are engine-owned.** The ingest layer (CSV/JSON import) never writes to these node types. They are created and updated exclusively by `calc_runs`. This separation is critical for incremental propagation correctness.

6. **Do not cache ProjectedInventory in a separate table.** Store it in `nodes` like everything else, with `calc_run_id` tracked in `attributes`. The `is_dirty` flag is the cache invalidation mechanism.

7. **The scenario_overrides table is the simulation primitive.** When an AI agent asks "what if PO-991 moves from April 10 to April 18?", it writes one row to `scenario_overrides`, creates a `calc_run`, and reads back the resulting `Shortage` nodes. This is the entire M5 simulation loop.

8. **Start with a single DB file: `ootils.db`.** No sharding, no partitioning, no read replicas. Add WAL mode (above). That's all the performance work this proof needs.

9. **Write a `db_health_check()` function** that verifies: (a) all foreign keys resolve, (b) no orphaned edges (from/to nodes that are inactive), (c) no `calc_runs` stuck in `running` status older than 60 seconds. Run it in tests. It will catch schema drift early.

10. **Pin the Postgres upgrade trigger:** if any of these thresholds are hit, migrate.
    - Database file > 500 MB
    - `calc_run` duration > 30 seconds for a 100-SKU run
    - More than 2 concurrent API writers needed
    - Deployment moves from laptop to cloud VPS with multiple consumers

---

## What This Enables for M2–M7

| Milestone | Schema dependency |
|-----------|------------------|
| M2 — Core Engine | `nodes(is_dirty)`, `edges(edge_type)`, `calc_runs` |
| M3 — Explainability | `explanations`, `explanation_steps` |
| M4 — Shortage Detection | `nodes(node_type=Shortage)`, `explanations` linkage |
| M5 — Scenarios | `scenarios`, `scenario_overrides` |
| M6 — API | All tables via read endpoints; events via write endpoint |
| M7 — AI Agent Demo | `/explain` = `explanations + explanation_steps`; `/simulate` = `scenario_overrides + calc_runs` |

The schema is complete. Nothing in M2–M7 requires a structural change to these tables.

---

## Alternatives Considered

### Event sourcing only (rejected, per ADR-001)
Pure event log with projections. Elegant but: (a) "what is the current state of node X?" becomes an expensive log replay, (b) the dirty-flag propagation model requires a mutable state layer.

### Separate tables per node type (rejected for proof stage)
18 tables. Correct for type safety, wrong for development velocity. Revisit in V2 if type-specific constraints become important enough to enforce at DB layer.

### DuckDB instead of SQLite (considered, not adopted)
DuckDB is excellent for analytics but is column-oriented. The engine's access pattern is row-oriented (fetch one node, update its dirty flag, write one explanation). SQLite is the right fit. DuckDB could be a read-only analytics layer in V2.

### Redis for dirty flags (considered, not adopted)
Using Redis as a fast dirty-flag store, SQLite for persistent data. Adds an operational dependency with no benefit at proof scale. SQLite dirty-flag reads with the partial index (`WHERE is_dirty = TRUE`) are sub-millisecond.

---

## References

- [ADR-001 — Graph-Based Domain Model](ADR-001-graph-model.md)
- [ADR-002 — Object-Local Time](ADR-002-elastic-time.md)
- [ADR-003 — Deterministic Incremental Propagation](ADR-003-incremental-propagation.md)
- [ADR-004 — Native Explainability](ADR-004-explainability.md)
- [Node Dictionary — V1](node-dictionary.md)
- [Edge Dictionary — V1](edge-dictionary.md)
- [SQLite Limits and Performance](https://www.sqlite.org/limits.html)
- [SQLite WAL Mode](https://www.sqlite.org/wal.html)
