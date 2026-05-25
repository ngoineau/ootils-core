# ADR-017 — Architecture B : Rust in-memory engine service

**Status** : Draft — 2026-05-24
**Owner** : ngoineau
**Extends** : [ADR-015 Rust readiness](ADR-015-rust-readiness.md), [ADR-016 Rust engine foundation (Architecture A)](ADR-016-rust-engine-foundation.md)
**Stakes** : Strategic. This is the core competitive moat decision — perf vs Kinaxis.

---

## TL;DR

Build a **dedicated Rust service** that maintains the propagation graph + active scenarios entirely **in RAM**, with Postgres relegated to a **durable journal + source of truth**. The hot path (single events, full propagation, scenario forks) becomes 10-30× faster than the current Rust-on-Postgres engine because we remove DB roundtrips from the critical path.

This is **the** technical investment that lets us claim "objectively faster than Kinaxis at every measurable scale" — and back it with reproducible benches.

---

## 1. Why Architecture B (and not A++)

### 1.1 The wall we hit with Architecture A

Measured on profile L (227K PI nodes) post-chantier-A:

| Operation | Time | Where the time goes |
|---|---|---|
| Single event incremental | 121 ms | 95% Postgres roundtrips (load + UPDATE + SHORTAGES) |
| Full propagation | 15.3 s | 99% Postgres I/O (UPDATE 227K rows, SHORTAGES JOIN, WAL) |
| Scenario fork | 5–15 s | 100% Postgres (insert 227K snapshot rows) |
| Rust compute itself | **<150 ms** | the kernel is essentially free |

The bottleneck is **not** compute. It's the Postgres I/O on the hot path. No amount of Rust optimization in the current architecture can recover this — we're bound by the database.

The POC kernel showed **32× Python on pure compute**. Architecture A delivers **2.4× SQL** in production because 99% of the time is spent outside the kernel.

### 1.2 The fundamental shift

Architecture B moves the **source of truth for the hot path** from Postgres to a Rust process's RAM:

- **Read** : zero Postgres roundtrips. State is in RAM, accessed by pointer.
- **Compute** : pure CPU cache arithmetic. Sub-microsecond per bucket.
- **Write** : in-RAM mutation. Postgres gets the update via async write-behind, never blocks the request.
- **Durability** : a local WAL on the Rust process guarantees no-data-loss on crash (replay log + Postgres checkpoint).

This is the architecture of **Redis, EventStoreDB, Materialize, Aerospike** — proven in extreme-perf domains.

### 1.3 Why now

Three signals aligned:

1. **Positioning** : "Kinaxis-killer in perf" requires sub-50ms p95 incremental at any scale. Architecture A cannot deliver that.
2. **Architecture A as foundation** : the kernel (`rust/ootils_kernel`) is already Rust, parity-validated, byte-identical. ~50% of Architecture B's compute layer is already written and tested.
3. **2026 Rust ecosystem maturity** : tokio 1.40+, axum 0.8, mimalloc, io_uring (compio 0.13+), portable SIMD stable — the stack is production-ready in ways it wasn't in 2024.

---

## 2. Technology choices — 2026 state of the art

### 2.1 Runtime & async

| Concern | Choice | Why |
|---|---|---|
| Async runtime | `tokio 1.40+` multi-threaded | Industry standard, mature, work-stealing scheduler |
| HTTP/RPC | `tonic 0.12+` (gRPC over HTTP/2) | Bi-directional streams (for SSE-equivalent), typed wire format |
| Local IPC | Unix Domain Sockets | Lower latency than TCP loopback (~0.5ms saved per RPC) |
| Tokio runtime sizing | `worker_threads = num_cpus` | Saturate the box on full propagation |

**Rejected alternatives**:
- `actix-web` : excellent but tokio-based axum/tonic are now the de-facto standard
- Custom binary protocol over UDS : tonic gives us reflection + typed clients for ~free; the latency overhead (~0.5ms) is below noise

### 2.2 In-memory state

| Concern | Choice | Why |
|---|---|---|
| Allocator | `mimalloc 0.1+` as global | 5-15% faster than system on multi-threaded loads, low fragmentation, mature |
| Node storage | `Vec<Node>` arena, indexed by `u32` | Cache-friendly, no Box overhead, fits in L2/L3 efficiently |
| Concurrent hash | `dashmap 6+` for scenario index | Lock-free reads, sharded writes |
| COW snapshots | `arc-swap 1.7+` for atomic pointer swap | RCU-style: writers swap an `Arc<Graph>`, readers hold their own `Arc` for the duration |
| Edges | CSR (Compressed Sparse Row) format | 5× less memory than `Vec<Edge>`, better cache locality for traversal |
| Decimal | `rust_decimal 1.36+` | Parity-validated with Python Decimal (chantier A) |

**Rejected alternatives**:
- `parking_lot::Mutex` on the whole graph : works but kills concurrent reads
- `im` (persistent data structures) : great semantics but 3-5× slower than COW pointers for our access pattern
- `f64` instead of Decimal : loses precision parity → would break parity tests, non-starter

### 2.3 Persistence layer

| Concern | Choice | Why |
|---|---|---|
| Postgres client (async) | `tokio-postgres 0.7+` | Native async, supports binary COPY |
| Bootstrap (sync) | `postgres 0.19+` | Simpler for startup-time graph load |
| Local WAL | Custom append-only file + `bincode` records, `fsync()` per event | Battle-tested pattern (Redis AOF, Kafka log) |
| Async fsync | `compio 0.13+` (io_uring on Linux) | True async fsync, 3-5× lower latency than blocking I/O at high throughput |
| Write-behind batching | Tokio task + `tokio::time::interval(100ms)` | Bulk COPY batches every 100ms or every 10K ops, whichever comes first |

**Rejected alternatives**:
- `sled` or embedded KV : adds complexity, we already have Postgres as durable backend
- Synchronous fsync per event : 5-10ms latency per event = unacceptable. The WAL is async, durability guarantee is "100ms RPO".
- No WAL, only Postgres : crash window of 100ms write-behind = unacceptable for client data.

### 2.4 Compute primitives

| Concern | Choice | Why |
|---|---|---|
| Parallel iteration (full prop) | `rayon 1.10+` | Work-stealing, drop-in replacement for Iterator |
| SIMD (per-bucket arithmetic) | `std::simd` (stable since 1.79) | Portable, no external deps |
| Hashing | `ahash 0.8+` | 2-3× faster than std `SipHash`, sufficient quality for our use |
| Per-request arena | `bumpalo 3.16+` | Avoid heap allocations in hot path, drop arena = drop all per-request data |

### 2.5 Observability

| Concern | Choice | Why |
|---|---|---|
| Structured logging | `tracing 0.1+ ` | Spans, fields, structured events |
| Metrics | `prometheus-client 0.22+` | Cardinality-controlled, standard format |
| Distributed tracing | `opentelemetry 0.27+` + OTLP exporter | Propagation across Python ↔ Rust boundary |
| Profiling | `pprof-rs 0.13+` for flame graphs on demand | Sub-second snapshot via signal |

### 2.6 Build & deployment

| Concern | Choice | Why |
|---|---|---|
| Cargo workspace | Multi-crate (`ootils_kernel`, `ootils_engine`, `ootils_proto`) | Reuse kernel from Architecture A as-is |
| Docker base | `rust:1.85-slim` builder + `gcr.io/distroless/cc-debian12` runner | Smallest, most secure, no shell |
| Cross-platform | `cross 0.2.5+` for Linux ARM64 + AMD64 builds | Multi-arch deployment ready |
| Cargo profiles | `[profile.release] lto = "fat", codegen-units = 1, opt-level = 3, panic = "abort"` | Max optimization |
| Process supervision | Systemd unit file + `Restart=on-failure` | Battle-tested on Linux |

---

## 3. Component architecture

### 3.1 The graph (in-RAM model)

```rust
/// The complete graph state. Owned by Arc<Graph> for atomic snapshot swaps.
struct Graph {
    /// All nodes in one contiguous Vec — cache-friendly, indexable by u32.
    /// 130 bytes packed per Node × 230K nodes ≈ 30 MB on profile L.
    nodes: Vec<Node>,

    /// All edges in CSR format. Compact + sequential access.
    /// (from_offset[u32], to_indices[u32], edge_types[u8])
    /// 460K edges × ~16 bytes ≈ 7 MB on profile L.
    edges: EdgesCsr,

    /// Indexes — every lookup needed by the propagator:
    by_node_id: HashMap<Uuid, NodeIndex, ahash::RandomState>,
    by_item_location: HashMap<(Uuid, Uuid), Vec<NodeIndex>, ahash::RandomState>,
    by_projection_series: HashMap<Uuid, Vec<NodeIndex>, ahash::RandomState>,
    by_calc_run_dirty: dashmap::DashMap<Uuid, Vec<NodeIndex>>, // concurrent

    /// Generation counter — bumped on every mutation, used for snapshot ordering.
    generation: AtomicU64,
}

#[repr(C, packed)]
struct Node {
    node_id: Uuid,           // 16
    item_id: Uuid,           // 16
    location_id: Uuid,       // 16
    series_id: Uuid,         // 16
    opening_stock: Decimal,  // 16
    inflows: Decimal,        // 16
    outflows: Decimal,       // 16
    closing_stock: Decimal,  // 16
    time_span_start: i32,    // 4 (days since epoch — saves 4 bytes vs NaiveDate)
    time_span_end: i32,      // 4
    bucket_sequence: i32,    // 4
    flags: NodeFlags,        // 1 (is_dirty | has_shortage | active | reserved)
}  // 141 bytes packed, 144 with alignment
```

**Estimated memory** (profile L):
- 230K nodes × 144 bytes = **33 MB**
- 460K edges (CSR) = **7 MB**
- Indexes (hashmap overhead) = **~50 MB**
- Bookkeeping (calc_runs, scenarios, dirty sets) = **~10 MB**
- **Total : ~100 MB per tenant baseline**

Per active scenario fork: +5-20 MB (overlay only contains modified nodes).

### 3.2 Scenario manager (COW)

```rust
struct Scenario {
    id: Uuid,
    baseline: Arc<Graph>,        // shared, immutable reference
    overlay: DashMap<NodeIndex, Node>, // only nodes modified in THIS scenario
    parent: Option<Uuid>,        // for nested scenarios
    created_at: Instant,
}

struct ScenarioManager {
    active: DashMap<Uuid, Arc<RwLock<Scenario>>>,
    baseline: ArcSwap<Graph>,    // atomic pointer to current baseline
}
```

**Operations** :
- `fork(scenario_id) → new_scenario_id` : `Arc::clone(&baseline)` + empty overlay. **20–50 ms** (allocation only).
- `read(scenario, node_idx)` : check overlay, fallback to baseline. **~10 ns**.
- `write(scenario, node_idx, new)` : insert into overlay. **~50 ns**.
- `merge(scenario → baseline)` : atomic `ArcSwap::store(new_graph)`. Triggers async Postgres flush.

### 3.3 Propagator (in-RAM)

The hot loop, post-Architecture-B:

```rust
fn propagate_events(scenario: &Scenario, events: &[Event]) -> PropagationResult {
    // 1. Mark dirty (in-RAM bitmap, O(events))
    let dirty = scenario.expand_dirty_set(events);

    // 2. Topological sort (in-RAM, no DB)
    let ordered = scenario.topological_sort(&dirty);

    // 3. Per-node compute — port from rust/ootils_kernel/src/kernel.rs
    //    Optionally parallelized via rayon for full prop
    let results: Vec<PiResult> = ordered.par_iter()
        .map(|idx| compute_pi_bucket(scenario.get_node(*idx), ...))
        .collect();

    // 4. Apply results — atomic per-scenario, no Postgres
    scenario.apply_results(&results);

    // 5. Detect shortages — pure in-RAM scan
    let shortages = scenario.detect_shortages(&results);

    // 6. Enqueue write-behind to Postgres
    WRITE_BEHIND_QUEUE.push(results, shortages);

    PropagationResult { /* ... */ }
}
```

**Estimated perf** (profile L full prop):
- Mark dirty + topo sort : ~10 ms
- Compute 227K PIs (rayon, 8 threads) : ~50 ms (vs 15s in Architecture A)
- Apply + shortage detect : ~20 ms
- Total **synchronous** : ~80 ms
- Write-behind to Postgres (async, doesn't block) : ~3-5s, invisible

**Estimated perf** (incremental, 91 PIs):
- Total synchronous : **2-5 ms**

### 3.4 Write-behind queue + WAL

```rust
struct WriteBehindQueue {
    pending: Mutex<VecDeque<NodeDelta>>,
    wal_writer: WalWriter,      // append-only local file
    pg_client: Arc<PgClient>,
    last_flush: AtomicI64,
}

// Per propagation:
async fn record(&self, deltas: Vec<NodeDelta>) {
    // 1. Append to WAL synchronously (durability barrier)
    self.wal_writer.append_and_fsync(&deltas).await?;
    // (compio io_uring : ~200 µs on NVMe)

    // 2. Push to pending queue (non-blocking)
    self.pending.lock().extend(deltas);
}

// Background tokio task:
async fn flush_loop(&self) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        interval.tick().await;
        let batch = self.pending.lock().drain(..).collect();
        if !batch.is_empty() {
            self.pg_client.copy_in_bulk(batch).await?;
            self.wal_writer.checkpoint().await?; // truncate WAL
        }
    }
}
```

**Durability contract** :
- Event acked to user → WAL fsynced → no data loss possible (matches PostgreSQL's `synchronous_commit = on`).
- Postgres state lags RAM by max 100ms in steady state.
- Crash recovery : replay WAL from last checkpoint → state reconciled.

### 3.5 gRPC interface

```protobuf
syntax = "proto3";
package ootils.engine.v1;

service Engine {
  // Synchronous propagation (waits for compute, not for Postgres flush)
  rpc Propagate(PropagateRequest) returns (PropagateResponse);

  // Scenario lifecycle
  rpc ForkScenario(ForkRequest) returns (ScenarioInfo);
  rpc MergeScenario(MergeRequest) returns (MergeResult);
  rpc ListScenarios(google.protobuf.Empty) returns (ScenarioList);

  // Reads (bypass Postgres lag)
  rpc GetNode(NodeQuery) returns (NodeState);
  rpc QueryShortages(ShortagesQuery) returns (stream Shortage);

  // Real-time stream (replaces SSE in front-end)
  rpc StreamChanges(StreamRequest) returns (stream ChangeEvent);

  // Ops
  rpc Health(google.protobuf.Empty) returns (HealthStatus);
  rpc Metrics(google.protobuf.Empty) returns (EngineMetrics);
}
```

Python FastAPI proxies its existing routes to this gRPC service via `tonic-python` (or `grpc-python`). Migration is gradual : route by route, the Python implementation can stay as a fallback.

---

## 4. Performance budgets (target)

| Operation | Architecture A (today) | Architecture B (target) | Gain |
|---|---|---|---|
| Single event incremental (91 PIs) | 121 ms | **3-8 ms** | **15-40×** |
| Burst 100 events sustained | 12 s | **~1 s** | **12×** |
| Full prop L (227K PIs) | 15.3 s | **80-300 ms** | **50-200×** |
| Scenario fork | 5-15 s | **20-50 ms** | **100-300×** |
| Dashboard read (10K shortages) | 100 ms | **2-10 ms** | **10-50×** |
| Multi-scenario parallel (5 active) | locked sequentially | **truly parallel** | ∞ |
| **p95 incremental any scale** | 660 ms (outliers) | **< 50 ms** garanti | — |

These targets are based on the POC measurements + standard expectations for in-RAM systems. They will be re-evaluated after Phase 3 (first end-to-end working version).

---

## 5. Phased delivery (8-9 weeks)

| Phase | Weeks | Deliverable | Go/no-go gate |
|---|---|---|---|
| 1. Service skeleton | 1 | Standalone Rust binary, axum/tonic stub, Postgres connection pool, OTLP/Prometheus exposed | `health` endpoint returns 200, `cargo bench` passes |
| 2. In-RAM graph + bootstrap | 1 | Load profile L graph from Postgres at startup, indexes built, snapshot via arc-swap working | Boot < 5s for L, memory < 200MB, parity 1-shot read |
| 3. Propagator native | 1.5 | Port kernel + traversal from `ootils_kernel`. No Postgres in propagate path. | Parity 3-way on S, M, L. Compute < 100ms on L. |
| 4. Scenario manager | 1 | COW fork, merge, list. 10+ scenarios concurrent. | Fork < 50ms, parallel scenarios isolated, zero deadlocks under load. |
| 5. WAL + write-behind | 1.5 | Local WAL with compio io_uring on Linux, async Postgres flush, crash recovery. | `kill -9` recovery clean 10× in a row, no data loss measurable. |
| 6. gRPC interface | 1 | Tonic server + Python tonic client wrapped behind FastAPI routes. | End-to-end roundtrip < 10ms, parity preserved across the boundary. |
| 7. Stress + observability | 1 | 100 events/sec sustained, OTLP tracing across Python↔Rust, memory profiling, flame graphs. | p95 < 50ms under load, no memory leaks at 1h soak. |
| 8. Production rollout | 0.5-1 | Feature flag `OOTILS_ENGINE=rust-svc`, hybrid with engine SQL for safety. | Opt-in flag works, gradual ramp 1% → 10% → 100% traffic. |

**Total : 8-9 weeks of focused work.**

---

## 6. Risks & mitigations

| Risk | Impact | Probability | Mitigation |
|---|---|---|---|
| Memory bloat at >100K SKU | High (OOM kills) | Medium | Tenant-per-process model, hard memory limit, OOM observability |
| WAL corruption / partial fsync | High (data loss) | Low | Checksums per record, CRC32 on append, fsync barrier per event |
| Crash during compute → inconsistent state | High | Low-medium | Atomic apply via arc-swap : either old or new state, never partial |
| Postgres staleness for cross-process reads | Medium (stale dashboards) | High | Route reads via service when freshness matters; explicit lag SLA (100ms) |
| Python ↔ Rust IPC latency | Medium (limits sub-10ms ceiling) | High | UDS not TCP; bench shows ~0.5-2ms overhead, acceptable |
| Scope creep on the service | High (8 weeks → 16) | Medium | Strict ADR scope, phase gates as hard checkpoints |
| Rust ecosystem upgrade churn (tokio, etc) | Low | Low | Pin versions, monthly bump cadence post-launch |
| Operational complexity (2 processes, monitoring) | Medium | High | Comprehensive `tracing` + OTLP from day 1, runbooks per failure mode |

---

## 7. Success criteria

The project is **a success** if **all** of the following are true at the end of Phase 8:

1. **Perf** :
   - p95 incremental < 50ms on profile L under sustained 50 events/sec
   - Full prop L < 500ms wall-clock
   - Scenario fork < 100ms
   - p99 latency under burst < 200ms (no autovacuum-style outliers)

2. **Correctness** :
   - Parity 3-way (Python, SQL, Rust-svc) — 0 mismatch on S, M, L, XL profiles
   - Crash recovery verified by `kill -9` × 100 with no data loss

3. **Operational** :
   - OTLP traces flow from Python through Rust to Postgres
   - Memory stable over 24h soak test (no leaks)
   - Restart < 10s on profile L
   - Single binary < 30 MB, container image < 100 MB

4. **Business** :
   - Reproducible bench script in `scripts/bench_kinaxis_comparison.py` that produces the marketing-ready numbers
   - Sales demo : full prop on 10K SKU finishes before the demo room blinks

If **any** of these fail, we go back to Architecture A in production and re-plan.

---

## 8. Decision: anti-patterns we will NOT do

- ❌ **Rewrite FastAPI in Rust** : Python stays for routes, validation, agents, ingest. The Rust service has a narrow, typed API.
- ❌ **Custom RDBMS in Rust** : Postgres is the durable source of truth. We don't replace it, we cache it.
- ❌ **Distributed multi-node** : single-process per tenant is enough for years. Distributed comes if we need 1M+ SKU per tenant.
- ❌ **WASM plugins for user rules** : tempting but ten times the complexity. v2 if ever.
- ❌ **Async-everywhere** : the kernel stays synchronous; only IO is async. Saner debugging, predictable perf.
- ❌ **GC'd language for any component** : the entire Rust service is GC-free. No JVM/Go pauses interfering with sub-50ms SLA.

---

## 9. Open questions to resolve in Phase 1

1. **Tenant isolation model** : one process per tenant, or shared process with namespacing? (Lean toward per-process for v1.)
2. **gRPC vs Cap'n Proto for IPC** : tonic is the safe choice; Cap'n Proto would save 1-2ms but adds toolchain complexity. (Lean toward tonic.)
3. **WAL file format** : custom binary, bincode, or rkyv? (Lean toward bincode — fastest mature option.)
4. **Postgres write-behind contention model** : single bulk-copy worker or sharded by tenant? (Lean toward single worker per tenant, isolation by process.)
5. **Multi-arch build** : do we ship ARM64 from day 1 or just AMD64? (Lean toward AMD64 only for v1, ARM64 added in Phase 8 if there's signal.)

These will be answered with explicit ADRs (`ADR-018+`) as Phase 1-2 reveal constraints.

---

## 10. References

- [Redis AOF persistence model](https://redis.io/docs/management/persistence/)
- [Materialize architecture](https://materialize.com/docs/overview/architecture/)
- [PostgreSQL WAL design](https://www.postgresql.org/docs/current/wal-intro.html)
- [Tokio Postgres async client](https://docs.rs/tokio-postgres/)
- [arc-swap RCU pattern](https://docs.rs/arc-swap/)
- [io_uring on Linux via compio](https://docs.rs/compio/)
- [ootils-core ADR-016 (Architecture A)](ADR-016-rust-engine-foundation.md)
- [ootils-core POC results (`poc/rust_kernel`)](../poc/rust_kernel/)
