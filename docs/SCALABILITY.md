# Scalability Analysis — Ootils Core

> **Status:** Demo and pilot scale are validated interactively. **Batch** planning is measured as viable up to the mid-market lower edge (SMB annual ≈ 182 K nodes in ~38 s, **post-tune**; 2 000 items × 365 d ≈ 730 K nodes in ~3.8 min, **post-Tier-2 but pre-tune** — a post-tune run would be faster) after the Tier 1 + Tier 2 fixes below — all on a **2-core VM** (`scripts/bench_propagation.py`, 2026-05-23). Two caveats before reading the table: those numbers are **batch** wall-clock, not proof of **interactive** latency; and the Tier 3 SQL-window spike that would push further is a prototype, **not merged** (see "What the spike does NOT cover"). Enterprise (5K+ items) still requires architectural investment.

---

## Volume projections

The **PI Nodes** column is the full-network projection (all buckets × locations).
The **measured** propagation figures below (§ "Measured perf landscape") run on a
narrower annual slice — e.g. SMB is benched at 500 items × 365 d ≈ 182 K nodes,
not the ~600 K full-network figure — so read the two columns as different axes.
All measurements are on a **2-core VM**; nothing here is proven at interactive
latency, and the Tier 3 SQL-window path is **not merged**.

| Size | Items | Locations | PI Nodes | Edges | Status |
|------|-------|-----------|----------|-------|--------|
| Demo | 2 | 2 | ~400 | ~800 | ✅ OK (interactive) |
| Pilot (50 items) | 50 | 5 | ~30K | ~75K | ✅ OK (interactive, with latency) |
| SMB (500 items) | 500 | 10 | ~600K | ~1.5M | ✅ **Viable in batch** (measured: 182 K nodes / ~38 s, post-Tier-2 + post-tune) · ⚠️ interactive unproven |
| Mid-market (5K items) | 5,000 | 50 | ~50M | ~150M | ⚠️ **Batch-viable only up to ~2 000 items** (measured: 730 K nodes / ~3.8 min, post-Tier-2 pre-tune — not yet re-benched post-tune); 5 K items **over a 2-year horizon** ≈ 3.6 M nodes / ~19 min is **extrapolated**, needs Tier 3 (unmerged) |
| Enterprise (50K items) | 50,000 | 200 | ~3B | ~10B | 🚫 Not feasible without architectural overhaul |

---

## Breaking points

### Breaking point #1: Propagation — O(N × 10–20 queries per PI node) — **FIXED**

**Status:** Resolved 2026-05-23 (REVIEW-2026-05 R2). See PR for the refactor.

Before the fix, `propagator.py _recompute_pi_node` executed 10–20 SQL queries per PI node:

- 1× `get_node` (load PI node)
- 1× `get_edges_to` (feeds_forward for predecessor)
- N× `get_node` (load predecessor/supply/demand nodes)
- 1× `get_edges_to` (replenishes)
- 1× `get_edges_to` (consumes)
- 1× `update_pi_result`
- 2× `get_node` (fresh reload for explanation + shortage)
- 1× safety-stock query

The fix pre-loads everything `_propagate` needs in **4 batch queries** before the per-node loop:

1. `get_nodes_by_ids(dirty_list)` — all dirty nodes
2. `get_edges_to_nodes(dirty_list, edge_types=[feeds_forward, replenishes, consumes])` — all incoming edges
3. `get_nodes_by_ids(source_ids)` — all edge sources (skips ids already in the cache)
4. `SELECT … FROM item_planning_params` — all safety stocks for the touched (item, location) pairs

Per-node work then reads from the in-memory caches and shrinks to **2 queries/node** (`clear_dirty` + `update_pi_result`).

#### Measured impact (`scripts/bench_propagation.py`)

| Scale | Dirty PI nodes | Before — queries / wall | After — queries / wall | Speedup |
|-------|---------------|--------------------------|------------------------|---------|
| Demo (10 items × 14 buckets) | 140 | 1,421 / 5.8s | 285 / 1.2s | **4.8×** |
| Pilot (50 items × 30 buckets) | 1,500 | 15,101 / 61.8s | 3,005 / 12.0s | **5.1×** |

Throughput climbs from ~24 nodes/sec to ~125 nodes/sec, with `queries/node` dropping from 10.07 to 2.0. Scaling stays linear in nodes (no quadratic blow-up).

#### Re-bench projection (post-fix)

| Scale | Dirty PI nodes | Queries/node | Total queries | Est. time @ 1ms/query |
|-------|---------------|-------------|---------------|-----------------------|
| Demo | 90 | 2.0 | 184 | <1s |
| Pilot | 500 | 2.0 | 1,004 | ~1s |
| SMB | 5,000 | 2.0 | 10,004 | **~40s** (was 75s+) |
| Mid-market | 50,000 | 2.0 | 100,004 | **~100s** (was 12+ min) |

The SMB tier no longer breaks; the mid-market tier becomes viable for batch runs even before further investment.

#### Tier 2 — also shipped (2026-05-23)

After Tier 1, the hot path was bound by per-node round-trips (2 queries × ~4ms tunnel latency = ~8ms/node → ~125 nps ceiling). Tier 2 collapses those into a single round-trip per propagation run:

- `update_pi_result` calls are accumulated and flushed via one
  `UPDATE … FROM UNNEST(%s::uuid[], %s::numeric[], …)` at the end of
  `_propagate`.
- `clear_dirty` calls are batched into one
  `DELETE FROM dirty_nodes WHERE node_id = ANY(%s)`.

Result: **7 queries total per propagation regardless of dirty count**
(4 pre-load + 1 safety-stock + 1 batched UPDATE + 1 batched DELETE).

| Scale | Dirty PI | Tier 1 wall | Tier 2 wall | Tier 2 speedup vs Tier 1 |
|-------|---------|-------------|-------------|--------------------------|
| 140 nodes | 140 | 1.17 s | **0.12 s** | ~10× |
| 1.5 K nodes | 1,500 | 11.5 s | **0.39 s** | ~30× |
| 6 K nodes | 6,000 | 48.7 s | **1.40 s** | ~35× |
| 15 K nodes | 15,000 | extrap. ≥120 s | **3.66 s** | ≥30× |

Throughput climbs from ~125 nps (Tier 1) to **~4 000 nps** (Tier 2),
the rate at which PostgreSQL can ingest a single bulk-update statement.
Cumulative wall-time improvement vs the original pre-R2 implementation:
**~50× at small scale, ≥150× at mid scale**.

#### Tier 3 spike — profile first, then SQL window functions (2026-05-23)

Profiling `_propagate` on a 100 × 365 (36.5 K nodes) bench revealed the
"compute-bound" assumption was wrong. Self-time breakdown:

| Hotspot | Self time | % of wall |
|---|---|---|
| `select.select` (libpq waiting on Postgres) | 2.94 s | 28 % |
| `graphlib.TopologicalSorter` driven by UUID hash/eq | 1.96 s | 19 % |
| psycopg `array.dump_list` (UNNEST serialization) | 1.3 s | 13 % |
| UUID `__init__` / `__str__` / `__hash__` / `__eq__` | 1.8 s | 17 % |
| `_row_to_node` + `_row_to_edge` deserialization | 0.88 s | 8 % |
| **`_recompute_pi_node` (the "compute")** | **0.78 s** | **7.5 %** |

The kernel compute is **not** the bottleneck. A `UUID → str` PoC eliminated
the hash/eq cost but only bought 5 % wall-time because `UUID.__init__` in
`_row_to_*` and `__str__` for SQL output stayed put. NumPy vectorisation /
Rust kernel target the wrong 7.5 %.

The lever that *did* move the needle: **rewrite the projection chain as a
single window-function UPDATE**. The inventory recurrence
`opening[N] = OH + Σ_{k<N}(inflows[k] − outflows[k])` is a running sum,
naturally expressed as:

```sql
SUM(inflows - outflows) OVER (
    PARTITION BY projection_series_id
    ORDER BY bucket_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
)
```

`WITH RECURSIVE` was tried first and ran ~5× *slower* than Python — recursion
forces sequential bucket processing in Postgres. Window functions scan once
and parallelise across series.

| Scale | Python wall (Tier 2) | SQL window wall | Speedup | Parity |
|---|---|---|---|---|
| 100 × 365 (36.5 K) | 10.0 s | **4.0 s** | **2.5×** | ✅ |
| 500 × 365 (182.5 K — SMB annual) | 53 s | **13.5 s** | **3.9×** | ✅ |

Throughput climbs from 3 500 → 13 500 nps as scale grows (Postgres amortises
plan/parse cost over the batch — opposite of the Python loop). See
`scripts/spike_sql_propagate.py` for the prototype.

**What the spike does NOT cover** (must land before merging):
- demand events (`consumes` edges) with multi-day prorating
- shortage detection / safety-stock conditional logic
- explanation tracking (causal chains currently built in Python)
- dirty subgraph propagation (the spike recomputes the whole series)
- non-baseline scenarios / advisory-lock integration
- bit-exact parity tests against the Python kernel across diverse inputs

Estimated effort to production: **1–2 weeks of focused work**, gated by a
parity test harness comparing both engines on the same fixtures.

#### Other Tier 3 candidates (deferred — likely unnecessary)

- **NumPy vectorisation / Rust kernel (issue #197)** — would save ≤ 7.5 %
  even with perfect implementation. Not worth pursuing unless the SQL path
  hits an unforeseen wall.
- Batch shortage-detector writes (currently 1 INSERT per shortage —
  acceptable while shortages stay rare).
- Streaming / pipelined propagation for very large dirty sets (> 1 M).

#### Measured perf landscape (2026-05-23, post-Tier-2)

| Horizon × Items | PI nodes | Propagation wall | Throughput |
|---|---|---|---|
| 90 d × 50 | 1.5 K | 0.39 s | 3 800 nps |
| 90 d × 500 | 15 K | 3.7 s | 4 090 nps |
| 365 d × 100 | 36.5 K | 8.9 s | 4 094 nps |
| 365 d × 500 (SMB) | 182.5 K | 53 s | 3 420 nps |
| 365 d × 2 000 (mid-market) | 730 K | 3.8 min | 3 187 nps |

Extrapolated 2-year × 5 K items (~3.6 M nodes) ≈ 19 min. The Tier 3 levers
target this range — anything beyond ~1 M PI nodes is where Python compute
becomes the dominant cost.

#### Measured — FIRST pilot-scale run over the **API path** (2026-07-07, #414)

The figures above are **engine-direct on synthetic seeds**. The first real
measurement of the full HTTP path on the pilot base (`ootils_pilote_test`,
36 635 items, 211 K raw nodes), following `docs/RUNBOOK-pilot-propagation.md` —
fork-first, baseline untouched:

| Step | Measured |
|---|---|
| Fork of baseline via `POST /v1/simulate` (deep-copy, 211 K nodes) | **23.8 s** |
| `bootstrap_pi` — 300 finished items → 856 after BOM closure → 1 837 pairs × 120 d | **220 440 PI nodes in 66.4 s** |
| Full recompute via `POST /v1/calc/run` (220 440 PI) | **464.4 s → 475 nps** |
| Active shortages detected on the fork (item-day grain) | 174 769 |
| Fork-on-fork what-if (431 K-node scenario), honest delta | PASS |

Two structural lessons, both now measured rather than estimated:

- **The API path runs ~8× slower than engine-direct synthetic benches**
  (475 nps vs ~4 000 nps) — HTTP stack + remote-LAN DB round-trips dominate.
  The 7.7-minute synchronous HTTP request is issue #193 (async calc workers)
  demonstrating itself.
  **⚠ Correction (2026-07-11, #455):** this "~8× is all HTTP/LAN" reading was
  partly wrong. The 2026-07 VM re-bench uncovered a latent O(N²) plan
  regression in the SQL engine — stale `dirty_nodes` stats made the planner
  pick a per-row nested loop (43 nps vs the expected ~8 400). The pilot's
  464 s run went through that pathology, so an unknown (likely large) share
  of it was the stats bug, **not** HTTP/LAN. Fixed by `ANALYZE dirty_nodes`
  in `flush_to_postgres` (#455). The SCALE-1 API-path re-profiling must
  re-decompose the 464 s **after** this fix ships to the pilot before
  attributing the remainder to the transport layer.
- **The deep-copy fork is O(N) in STORAGE as well as time**: each pilot fork
  duplicates the full node/edge set (211 K → the fork carried 431 K rows after
  bootstrap). Five concurrent forks of a bootstrapped pilot base would add
  ~2 M rows. The lazy-CoW / Rust-ArcSwap arbitration is roadmap SCALE-2.

Side observation from the same run: 174 K fork-scoped `shortages` rows slowed
the **baseline** outcome evaluator (~35 min vs ~3 min) — scenario-scoped index
coverage on `shortages` is a quick-fix candidate.

**Quick-fix applied (PERF-1 PR-A, migration
`075_shortages_outcome_index.sql`):** `idx_shortages_scenario_item_loc_active`
— `(scenario_id, item_id, location_id, severity_score DESC, shortage_date)
WHERE status = 'active'` — targets `_load_observed_shortages`'s uncovered
`DISTINCT ON (item_id, location_id) ... ORDER BY item_id, location_id,
severity_score DESC, shortage_date` (`engine/outcome/evaluator.py:574-586`),
which previously forced a Seq Scan + Sort over the whole active-shortage set
regardless of which scenario was being evaluated. Expected: <1 min on the
same dataset (order of magnitude — verify with `EXPLAIN ANALYZE` against the
pilot base). Kept alongside, not merged into, `idx_shortages_scenario_active`
(migration 014) — that index stays the right shape for its own hot path
(severity-ordered active shortages with no per-item/location grouping).

#### Postgres tuning gain — applied 2026-05-23

The dev VM's Postgres ran the `postgres:16-alpine` defaults (`shared_buffers
= 128 MB`, `work_mem = 4 MB`, `jit = on`), which is unusable for window
functions and bulk UNNEST. After applying the tuning baked into
`docker-compose.yml` (shared_buffers 1 GB, work_mem 32 MB, jit off, parallel
workers capped to the 2 physical cores), re-bench at SMB annual:

| Workload | Pre-tune | Post-tune | Gain |
|---|---|---|---|
| Python propagator (Tier 2) | 53 s / 3 420 nps | **38.2 s / 4 773 nps** | **+39 %** |
| SQL window spike (Tier 3) | 13.5 s / 13 488 nps | **11.1 s / 16 423 nps** | **+22 %** |

The Python path gained more because each of its 7 round-trips reads a
warmer cache and skips disk spills (`work_mem` was 8× too small for the
internal sorts). The SQL path was already doing one big query — tuning helps
but the gain is smaller because there was less waste to recover.

**Hardware ceiling**: 2-core VM is the real limit. Bumping to 4 cores would
let `max_parallel_workers_per_gather` go to 2 and parallelise the window
function across series partitions — another ~30-50 % on the SQL path,
basically nothing on the Python path. **Do not pay for more RAM at current
data sizes** — the 251 MB bench DB fits entirely in OS page cache.

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

### Breaking point #6: Synchronous DB calls in async FastAPI handlers — **FIXED**

**Status:** Resolved 2026-05-31. All 62 route handlers across the 19 routers
were converted from `async def` to `def`; FastAPI now dispatches them to its
anyio worker thread pool, so a blocking `psycopg` call no longer freezes the
event loop. Only `require_auth` (a trivial-CPU dependency, no I/O) stays async.

**Thread-pool sizing (important):** the worker pool is now bounded to the DB
connection pool size via `OOTILS_THREADPOOL_SIZE` (defaults to
`OOTILS_DB_POOL_MAX_SIZE`, i.e. 10). This is deliberate: anyio's default of 40
worker threads would have 40 handlers fighting over ≤10 connections, moving the
bottleneck without resolving it. Raise **both** together to scale concurrency
(threads must not exceed available connections, or threads block on
`pool.connection()`). Set in the `app.py` lifespan startup.

Tier 2 alternative (deferred): migrate to a `psycopg3 async` connection pool for
true async I/O instead of thread-pool offload.

---

### Breaking point #7: `inventory_snapshots` — daily append, unpartitioned

**Status:** Not a V1 blocker. The proof machine (ADR-030, #393 A3) writes one
`inventory_snapshots` row per `(item, location)` per capture day. At demo scale
this is negligible (a handful of rows/day). At pilot scale it grows linearly:

| Scale | Rows/day | Rows/year (365 daily captures) |
|-------|----------|--------------------------------|
| Demo (2 items × 2 loc) | ~4 | ~1.5K |
| Pilot (50 items × 5 loc) | ~250 | ~90K |
| Pilot upper edge (36K item·location coords) | ~36K | ~13M |

At the pilot upper edge (36K item·location coordinates → 36K rows/day) a year of
daily captures is ~13M rows — the point where retention and/or `RANGE`
partitioning by `as_of_date` (drop old partitions instead of DELETE) must be
addressed. This is **vague C1 work, not a V1 blocker**: the table is additive,
indexed by `(scenario_id, as_of_date)` and `(item_id, location_id, as_of_date DESC)`
(migration 067), and V1 captures baseline only. `recommendation_outcomes`
(migration 069) grows far slower (one verdict per reco per evaluated day, and
recos are bounded by shortages), so it is not a near-term concern.

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
