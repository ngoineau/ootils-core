# Scalability Analysis — Ootils Core

> **Status:** Current system is validated for demo and pilot scale. SMB (500 items) requires Tier 1 fixes. Mid-market (5K+ items) requires architectural investment.

---

## Volume projections

| Size | Items | Locations | PI Nodes | Edges | Status |
|------|-------|-----------|----------|-------|--------|
| Demo | 2 | 2 | ~400 | ~800 | ✅ OK |
| Pilot (50 items) | 50 | 5 | ~30K | ~75K | ✅ OK with latency |
| SMB (500 items) | 500 | 10 | ~600K | ~1.5M | ⚠️ **CRITICAL — Tier 1 required** |
| Mid-market (5K items) | 5,000 | 50 | ~50M | ~150M | 🚫 **IMPOSSIBLE — Tier 2 required** |
| Enterprise (50K items) | 50,000 | 200 | ~3B | ~10B | 🚫 Not feasible without architectural overhaul |

---

## Breaking points

### Breaking point #1: Propagation — O(N × 10–20 queries per PI node)

`propagator.py _recompute_pi_node` executes 10–20 SQL queries per PI node:

- 1× `get_node` (load PI node)
- 1× `get_edges_to` (feeds_forward for predecessor)
- N× `get_node` (load predecessor/supply/demand nodes)
- 1× `get_edges_to` (replenishes)
- 1× `get_edges_to` (consumes)
- 1× `update_pi_result`
- 2× `get_node` (fresh reload for explanation + shortage)

Additionally, `topological_sort` does **1 query per node** (`get_edges_to` in loop) before computation starts.

| Scale | Dirty PI nodes | Queries/node | Total queries | Est. time @ 1ms/query |
|-------|---------------|-------------|---------------|-----------------------|
| Demo | 90 | 15 | 1,350 | 1.3s |
| Pilot | 500 | 15 | 7,500 | 7.5s |
| SMB | 5,000 | 15 | 75,000 | **75s** |
| Mid-market | 50,000 | 15 | 750,000 | **12+ min** |

**Fix (Tier 1):** Batch-load all edges and nodes for the dirty subgraph in 2 queries before propagation begins, then process in-memory.

---

### Breaking point #2: `expand_dirty_subgraph` — O(n²) list operations

```python
queue: list[UUID] = [trigger_node_id]
while queue:
    current_id = queue.pop(0)  # O(n) shift on every call
```

`list.pop(0)` shifts the entire list. At 50K nodes: ~1.25 billion memory shift operations. Additionally, each iteration runs `get_node` + `get_edges_from`.

**Fix (Tier 1):** Replace `list.pop(0)` with `collections.deque.popleft()` — O(1).

---

### Breaking point #3: Advisory lock — single-threaded per scenario

`pg_advisory_lock` per scenario serialises all calc runs. Since baseline is the main scenario, all propagation is effectively single-threaded. No horizontal parallelism is possible with the current design.

**Fix (Tier 2):** Partition by scenario_id to enable isolated parallel runs.

---

### Breaking point #4: `nodes` table — unpartitioned god table

At 50M rows:
- Each `UPDATE` writes a full new row version (~25 columns, ~500 bytes per row)
- Partial indexes on `active`/`is_dirty` prevent HOT updates
- Default autovacuum (20% scale factor) triggers only after 10M dead tuples
- Estimated bloat after 1 week of heavy use: 30–50%

| Scale | Rows | Table size | Index size | WAL/day |
|-------|------|-----------|-----------|---------|
| SMB | 600K | ~300 MB | ~500 MB | ~2 GB |
| Mid-market | 50M | ~25 GB | ~40 GB | ~150 GB |

**Fix (Tier 2):** Partition `nodes` and `edges` by `scenario_id`. Drop the partition to clean up a scenario — faster than DELETE, no vacuum needed.

---

### Breaking point #5: Row-by-row inserts — no batching

**Ingest:** Each PO/forecast triggers 2 queries (SELECT + INSERT/UPDATE). 5K POs = 10K queries. Should use `INSERT … ON CONFLICT … DO UPDATE` with `executemany`.

**Allocation:** Per demand node: `get_node` + `update_pi_result` + `upsert_edge` = 3 queries. No composite index on the 4-column edge lookup (now fixed by migration 014).

**DQ pipeline:** `ingest_rows` inserted one row at a time.

**Fix (Tier 1):** Use `executemany` / `COPY` for batch ingest. Composite edge index added in migration 014.

---

### Breaking point #6: Synchronous DB calls in async FastAPI handlers

All FastAPI handlers are `async def` but use synchronous `psycopg.Connection`. Every `db.execute()` blocks the event loop. With 10 concurrent API requests, the server is effectively single-threaded.

**Fix (Tier 1):** Switch handlers to `def` (non-async), letting FastAPI dispatch them to a thread pool — unblocks the event loop immediately with minimal code change. Tier 2 alternative: migrate to `psycopg3 async` connection pool.

---

## What migration 014 fixes

Migration `014_missing_indexes.sql` addresses Breaking point #5 (allocation upsert performance) and adds indexes for the post-propagation hot paths:

| Index | Fixes |
|-------|-------|
| `idx_edges_composite_lookup` | Allocation upsert per demand node (was full seq scan on 4-column predicate) |
| `idx_nodes_item_scenario_type_timeref` (partial) | Ghost engine day-by-day supply load |
| `idx_shortages_scenario_active` (partial) | Post-propagation shortage retrieval by scenario |
| `idx_shortages_item_active` (partial) | Impact scoring loops |
| `idx_calc_runs_scenario_completed` (partial) | Latest completed run lookup per scenario |

These indexes reduce query time on hot paths by 10–100× at pilot scale. They do **not** address the O(N) query count in the propagation loop — that requires Tier 1 batching.

---

## Recommended fixes by tier

### Tier 1 — Support 500 items (SMB)

| Fix | Effort | Impact |
|-----|--------|--------|
| Batch propagation queries (load subgraph in 2 queries) | Medium | 🔥 Eliminates O(N) query loop |
| `deque.popleft()` in `expand_dirty_subgraph` | Trivial | Fixes O(n²) queue |
| `executemany` / `COPY` for ingest | Small | 10× ingest throughput |
| Switch handlers to `def` (sync in thread pool) | Small | Unblocks async event loop |
| ✅ Done: 5 critical missing indexes (migration 014) | Done | 10–100× hot path queries |

### Tier 2 — Support 5,000 items (mid-market)

| Fix | Effort | Impact |
|-----|--------|--------|
| In-memory propagation (load subgraph → compute → batch persist) | Large | Enables 50K+ PI nodes |
| Partition `nodes`/`edges` by `scenario_id` | Large | Isolated vacuum, fast cleanup |
| Aggressive autovacuum on nodes/edges (`scale_factor = 0.05`) | Trivial | Prevents table bloat |
| Connection pooling (PgBouncer or asyncpg pool) | Medium | Eliminates connection overhead |
| Async DB layer (psycopg3 async) | Medium | True async I/O concurrency |

---

## Decision gates

Use these milestones to decide when to invest in each tier:

| Gate | Trigger | Action |
|------|---------|--------|
| Tier 1 start | Propagation >10s at pilot scale OR SMB customer signed | Implement batch queries + deque fix |
| Tier 2 start | Consistent >60s propagation at SMB OR mid-market prospect | Architecture spike: in-memory propagation + partitioning PoC |
| Partitioning PoC | nodes table >100M rows OR scenario cleanup taking >60s | Validate partition strategy on production clone |
| Async I/O | API p99 >500ms under 10 concurrent users | Profile event loop blocking; migrate to async psycopg3 |

---

## References

- Issue #134: Scalability analysis — system breaks at 500+ items
- Issue #130: Missing indexes (addressed by migration 014)
- `docs/ADR-003-incremental-propagation.md`: Propagation design decisions
- `src/ootils_core/engine/propagator.py`: Current propagation implementation
