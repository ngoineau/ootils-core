# Architecture B — senior code review

Reviewer: senior staff engineer (external).
Scope: ADR-017 Architecture B delivery — Rust engine service, PyO3 module (still in prod), Python integration layer, tests, scripts, Dockerfile, ADRs and runbook.
Effort: high. Date: 2026-05-25.

## Severity legend
- **BLOCK** : must fix before shipping rust-svc to prod
- **HIGH**  : should fix before shipping
- **MEDIUM**: address in the next sprint
- **LOW**   : nice-to-have / cleanup
- **INFO**  : noted, not actionable

## Summary

- Total findings: **62**
- Block: **8** | High: **17** | Medium: **22** | Low: **11** | Info: **4**

### Headline concerns (read these first)

1. **WAL truncation loses durable, not-yet-flushed records (data loss).**
   `WriteBehindQueue` drains its in-memory queue *before* the bulk `UPDATE`
   runs. If `flush_to_postgres` fails, the batch is pushed back into the
   queue — but the WAL has already accumulated additional records that
   are *not* in the queue. The next successful flush calls
   `truncate_after_flush()`, which zeroes the entire WAL including records
   for deltas Postgres has *not* yet persisted. See F-001 and F-002.
   This is a real data-loss vulnerability under the documented PG-outage
   scenario.

2. **Recovered WAL deltas are double-flushed; truncation makes that worse.**
   On boot the recovered deltas are pushed onto `WriteBehindQueue` and
   eventually flushed to Postgres. Truncation logic then nukes the WAL.
   But because the flusher truncates after *any* successful batch, the
   recovery batch and any concurrently-received new propagations are
   coupled — partial success leaves the WAL incoherent. See F-003.

3. **Per-scenario state is fiction in the current build.**
   `PropagateRequest.scenario_id` is *completely ignored* by the engine.
   The `Propagate` RPC always mutates the baseline `Arc<RwLock<Graph>>`,
   regardless of which scenario the Python client says it's targeting.
   `RustServicePropagationEngine` passes the user's scenario through.
   This is documented in ADR-018 as a known gap, but the Python adapter
   and the proto field present a contract that does not hold. Production
   traffic that ever uses non-baseline scenarios will silently corrupt
   the baseline. See F-006.

4. **`session_replication_role = replica` requires SUPERUSER.**
   The write-behind path runs `SET LOCAL session_replication_role =
   'replica'` on every batch. In Postgres this is a SUPERUSER-only
   setting (since 9.4). In a hardened deployment the application role
   will not have it, so *every* write-behind batch will fail and the
   engine will silently spiral into the backoff loop while the WAL
   grows without bound. See F-004.

5. **Backoff overflow leads to a no-flush state under sustained outage.**
   The `consecutive_failures` counter saturates at u32::MAX but the
   shift operation caps at `1 << 8`. The interval multiplier saturates
   at `baseline_interval * 256` which on the default 100ms = 25.6s,
   clamped to 30s — that part is OK. But `consecutive_failures` is
   `saturating_add(1)` and used in metrics: on a real long outage you
   lose the per-attempt observability. More importantly, the WAL grows
   unbounded during the outage with no cap and no alert. See F-005,
   F-007.

6. **gRPC `Propagate` runs synchronously under tokio with `parking_lot`
   blocking lock + rayon, holding the runtime hostage.** The hot path
   inside `service.rs::propagate` takes the `RwLock::write()` (blocking
   lock from `parking_lot`) inside an `async fn` and runs the
   rayon-parallel propagation while still holding it. Any blocking lock
   wait inside the tokio runtime stalls a worker thread; rayon further
   competes with tokio for CPU. Under sustained load this gives the
   reported p95 ~2 ms, but it is fragile and prevents using tokio's
   `current_thread` runtime, hurts cooperative-scheduling, and risks
   pathological tail latency. See F-008, F-009.

---

## Findings

### F-001 [BLOCK] WAL truncate is unconditional — drops records the queue still owes Postgres
**Location:** `rust/ootils_engine/src/write_behind.rs:142-148`, `rust/ootils_engine/src/wal.rs:142-153`
**Category:** Data loss
**Confidence:** High

The flusher loop is:

```rust
let batch = queue.drain_batch();           // [1] drain ALL pending
...
match flush_to_postgres(&dsn, &batch).await {
    Ok(_) => {
        queue.wal.truncate_after_flush()?; // [2] truncate ALL of WAL
```

But between [1] and [2], `service.rs::propagate` can append a new record
to the WAL and push new deltas to the queue. Those new deltas are in the
queue (fine), but the WAL is then truncated and the new record's bytes
are erased. If the process crashes before the *next* flush runs, the
new deltas are lost: they were durable on the WAL, then unilaterally
deleted by the flusher.

This is not theoretical — under the documented 100ms flush cadence and
sustained event rate, a propagation that completes between drain and
truncate is exactly the situation. The fsync barrier promise to the
caller is silently broken.

**Why it matters:** The runbook (RUNBOOK §"Rollback") and ADR-017 §3.4
both promise *no data loss as long as the WAL volume is intact*. With
the current truncation logic this promise does not hold even with a
healthy WAL volume.

**Suggested fix:** WAL truncation must be tied to a known durable position
in the file — either a checkpoint LSN written into the WAL, or a
file-rotation scheme. The simplest fix: before draining the queue,
record the current WAL file length. On successful flush, truncate to
*that recorded length minus magic*, not to magic-only. Even better:
add a per-record sequence number and an explicit checkpoint record
that the flusher writes after each successful PG batch; replay stops
at the last checkpoint.

---

### F-002 [BLOCK] Failed flush leaves WAL ordering inconsistent with queue
**Location:** `rust/ootils_engine/src/write_behind.rs:159-176`
**Category:** Data loss
**Confidence:** High

When a flush fails, the failed batch is `extend`-ed back into the queue:

```rust
queue.pending.lock().extend(batch);
```

But the queue was previously drained, and any *new* deltas pushed by
`service.rs::propagate` during the network call to Postgres are *already*
at the head of the queue. Re-pushing the failed batch with `extend`
appends them to the tail, reordering: WAL order = [old, new]; queue
order = [new, old]. The next bulk UPDATE will UPDATE the same node twice
in the wrong order if a delta for the same node appears in both. Since
the UPDATE is a single UNNEST statement, the *later* values win — but
"later" in the UNNEST is determined by array order, which is now
[new..., old...], so the OLD value overwrites the NEW. Final Postgres
state diverges from RAM.

**Why it matters:** Eventual-consistency contract is broken under any
flush failure with concurrent traffic. Worse, this is silent — the
flusher logs success.

**Suggested fix:** Either (a) push the failed batch back at the head
(`VecDeque::push_front`) so order is preserved, or (b) deduplicate the
queue by `node_id` keeping the latest entry (which is actually what we
want — only the latest value per node needs to land in PG). Option (b)
also bounds memory.

---

### F-003 [BLOCK] WAL replay re-enqueues deltas that may have already reached Postgres
**Location:** `rust/ootils_engine/src/main.rs:179-191`
**Category:** Data loss / correctness
**Confidence:** High

On boot:

```rust
let recovered = wal.replay()?;  // may include records PG already absorbed
...
queue.push(deltas);              // unconditionally re-flush all of them
```

The WAL is only truncated by `truncate_after_flush()`. A WAL with records
might mean (a) PG hadn't received them, or (b) PG received them but the
process crashed in `truncate_after_flush()` between
`tx.commit()` and `wal.truncate_after_flush()`. In case (b), the
recovered records will overwrite *current* Postgres state — which is
fine if no one else has touched those nodes, but in the documented
production model the SQL engine is also a writer and an older recovered
delta will *clobber* a newer SQL-engine row.

**Why it matters:** Mixed-engine deployments (canary phase 8c "Read-only
canary" then 8c "1% traffic") explicitly run both engines. The recovery
path will silently overwrite SQL-engine writes with stale data.

**Suggested fix:** Either (a) include a logical sequence number per
delta and let the bulk UPDATE include `WHERE nodes.updated_at <
EXCLUDED.updated_at`, or (b) write a "flush completed up to record N"
marker into the WAL header and start replay from there. Document the
canary policy: during overlap, the engine must NOT replay any WAL
records — restart with `--clean-wal` flag.

---

### F-004 [BLOCK] `SET session_replication_role = 'replica'` requires SUPERUSER
**Location:** `rust/ootils_engine/src/write_behind.rs:200`, also `rust/ootils_kernel/src/writeback.rs:172, 233`
**Category:** Data loss / operational
**Confidence:** High

```rust
tx.execute("SET LOCAL session_replication_role = 'replica'", &[]).await?;
```

This is a SUPERUSER-only privilege in PostgreSQL. The runbook example
DSN (`postgresql://ootils:ootils@db:5432/ootils`) reads like a regular
application role. In production, the application role almost certainly
won't be SUPERUSER. Every batch will fail with:

```
ERROR: permission denied to set parameter "session_replication_role"
```

The flusher will spin in its backoff loop forever, the WAL will grow
unboundedly, and nothing will land in Postgres. The metrics will show
this but only if someone watches `pg_flush_failure_total`.

**Why it matters:** This is the kind of bug that only shows up in a
hardened production environment, not in dev / CI / single-DB testing.
By the time it's discovered, the WAL is large enough to cause real
problems (boot time, disk pressure).

**Suggested fix:** Three options, all reasonable:
1. Replace `session_replication_role = 'replica'` with explicit
   `ALTER TABLE nodes DISABLE TRIGGER trg_nodes_updated_at` (still
   requires owner). Document the role privilege requirement explicitly
   in RUNBOOK.
2. Drop the trigger entirely and let the application set updated_at
   in every UPDATE (already done — line 230 has `updated_at = now()`).
3. Detect missing privilege at boot, refuse to start with a clear error.

Option 2 + explicit `DROP TRIGGER` migration is cleanest.

---

### F-005 [BLOCK] No WAL-size cap during PG outage — disk-fill DOS
**Location:** `rust/ootils_engine/src/wal.rs:121-137`, `rust/ootils_engine/src/write_behind.rs:159-177`
**Category:** Resource leak / data loss
**Confidence:** High

When `flush_to_postgres` fails repeatedly:
- The WAL keeps growing (every Propagate appends).
- The queue keeps growing (failed batches are re-pushed).
- There is no upper bound on either.

`max_pending_before_flush = 10_000` is *informational* — `push()` returns
`true` past that threshold but the caller ignores the return value
(see `service.rs:354`). The queue grows unbounded; the WAL file grows
unbounded; the `64 MB` cap on a *single record* in `wal.rs:191` doesn't
help when there are millions of records.

**Why it matters:** A 30-minute Postgres outage at 100 events/sec
generates ~180,000 WAL records. Each `WalRecord` with one
NodeDelta is roughly 200 bytes serialized, so ~36 MB — manageable.
But at the documented 5000 events/sec, that's 9 million records
(~1.8 GB) plus the in-memory queue of `PendingDelta` (each ~80 bytes
in RAM = ~720 MB). Either fills the WAL volume or OOMs the process.

**Suggested fix:** Apply backpressure: when the queue depth exceeds a
configurable cap (e.g. 1M), `Propagate` should return `Status::resource_exhausted`.
When the WAL file exceeds a cap (e.g. 1 GB), refuse new appends with
the same gRPC code. Both numbers should be configurable via env var.
Add an alert via metrics on either condition.

---

### F-006 [BLOCK] `scenario_id` in `PropagateRequest` is silently ignored
**Location:** `rust/ootils_engine/src/service.rs:268-381`
**Category:** API contract / correctness
**Confidence:** High

```rust
async fn propagate(&self, req: Request<PropagateRequest>) -> ... {
    let q = req.into_inner();
    // q.scenario_id is never read.
    ...
    let mut g = self.baseline.write();
    propagator::propagate(&mut g, &dirty)
}
```

The Python adapter passes a real scenario UUID. Anything that isn't the
baseline ID is silently treated as baseline. There is no error if the
scenario doesn't exist. Tests don't catch this because all test fixtures
use the baseline UUID. The result is silent baseline corruption: a user
who forks a scenario in Python, applies an event to the fork, will see
the baseline mutated and the fork unchanged.

This is documented in ADR-018 as a *known* gap to fix, but in the
shipping code there is no `Unimplemented` or `InvalidArgument` rejection.

**Why it matters:** It's the first thing a customer will try after a
fork. The contract is wrong, not just incomplete.

**Suggested fix:** Until per-scenario propagation lands, validate that
`q.scenario_id` is empty *or* equals the baseline UUID, and return
`Status::unimplemented("per-scenario propagation pending — ADR-018")`
otherwise. Add a test that asserts this.

---

### F-007 [BLOCK] Boot failure on bad metrics-listen string is silent
**Location:** `rust/ootils_engine/src/main.rs:154-166`
**Category:** Operational / observability
**Confidence:** High

```rust
if !cli.metrics_listen.is_empty() {
    match cli.metrics_listen.parse::<std::net::SocketAddr>() {
        Ok(addr) => { ... spawn ... }
        Err(e) => { warn!(...); }   // engine continues without metrics
    }
}
```

A typo in `OOTILS_METRICS_LISTEN` (e.g. `127.0.0.1.9090`) silently
disables metrics with only a `warn!` log. In production this means
operators believe metrics are enabled when they aren't, and only notice
when something breaks. Worse, the `OOTILS_METRICS_LISTEN` value is
typed as `String` rather than `SocketAddr` in the Cli struct (unlike
`OOTILS_ENGINE_LISTEN` which IS typed) — there's no good reason for the
asymmetry.

**Why it matters:** Phase 8 rollout depends on `pg_flush_failure_total`,
`writeback_queue_depth`, etc. for canary success. Silently losing the
metrics endpoint at boot is the operational equivalent of flying blind.

**Suggested fix:** Type `metrics_listen` as `Option<SocketAddr>` in
clap. Make boot fail-fast on parse error (don't warn-and-continue).
Same with empty string: prefer a sentinel like `"off"` over an empty
string for "disable" — empty strings tend to come from misconfigured
shell env interpolation.

---

### F-008 [BLOCK] `parking_lot::RwLock` held across `await` boundaries — not Send-safe in spirit
**Location:** `rust/ootils_engine/src/service.rs:80, 96, 114, 246, 284, 325`
**Category:** Concurrency / correctness
**Confidence:** High

Throughout `service.rs`, gRPC handlers take `parking_lot::RwLock::read()`
or `write()` and hold the guard across `tonic` response construction.
`parking_lot::RwLockReadGuard` is not `Send` by default. The handlers
return `Result<Response<T>>` — the *guard* must be dropped before the
async function suspends, otherwise the future is not `Send` and won't
compile with tonic's `multi_thread` flavor. The compiler *did* let this
build, which means... actually, the handlers don't hold the guard across
an explicit `.await`. But the `propagate` handler (line 325) takes a
`write()` and then does NOT `.await` until the function returns — so it
holds the write lock for the entire compute + WAL fsync.

Subtler issue: WAL `append` is a synchronous `fsync()` call from inside
an async handler (`service.rs:341`). With NVMe and `info` log level this
is ~5 ms — under tokio multi-thread this is *blocking work on a tokio
worker*. If many calls land at once they will starve other tokio tasks
including the metrics endpoint and the write-behind flusher.

**Why it matters:** This works at single-tenant low-volume. At the
documented 5000 rps with `worker_threads = num_cpus`, every worker that
hits a Propagate is blocked for ~5 ms of fsync. If all workers are
blocked the entire process freezes — including the bg flusher and the
metrics server.

**Suggested fix:** Wrap the fsync block in `tokio::task::spawn_blocking`,
or better, run WAL fsync on a dedicated tokio thread (single-producer
mpsc + a thread doing blocking I/O). Same treatment for the compute
phase (already CPU-bound under rayon, doesn't yield to tokio): wrap in
`spawn_blocking`. The current pattern works by accident only because
load tests never saturate the worker pool.

---

### F-009 [HIGH] `Propagate` holds the baseline write lock across rayon parallel work
**Location:** `rust/ootils_engine/src/service.rs:324-327`, `rust/ootils_engine/src/propagator.rs:67-88`
**Category:** Concurrency / perf
**Confidence:** High

```rust
let stats = {
    let mut g = self.baseline.write();         // EXCLUSIVE LOCK
    propagator::propagate(&mut g, &dirty)       // calls rayon par_iter inside
};
```

The propagator takes `&mut Graph` and inside uses `par_iter()` over series.
All other handlers (Health, GetNode, ListScenarios, MergeScenario which
also takes write) are completely blocked for the duration. On a busy
event stream this serializes all propagations and starves reads.

The test `test_parallel_reads_during_propagation` passes only because
incremental propagation is sub-millisecond. As soon as one big propagation
runs (e.g. cascading 1000+ PIs across multiple series) all reads stall.

**Why it matters:** Sub-ms p95 numbers in the runbook are for the
no-contention case. Under multi-user load (the ADR-017 scenario use
case) this will tank.

**Suggested fix:** Restructure: take a read lock to *plan* the work
(identify dirty PI series and clone the inputs the kernel needs), drop
the read lock, run the parallel compute, take a write lock briefly to
apply the deltas. Same pattern as the COW scenario design.

---

### F-010 [HIGH] `time_span_start`/`time_span_end` expect-panic in propagator
**Location:** `rust/ootils_engine/src/propagator.rs:195-198`
**Category:** Correctness / panic in production path
**Confidence:** High

```rust
let bucket_start = pi_node.time_span_start.expect("PI without time_span_start");
let bucket_end = pi_node.time_span_end.expect("PI without time_span_end");
```

The loader sets these from Postgres rows where they are nullable. The
table allows NULL (else there'd be a NOT NULL constraint we could rely
on). If a single PI in the baseline has NULL `time_span_*`, the engine
will panic in its hot path. With `panic = "abort"` in
`[profile.release]`, the entire process dies — and the orchestrator
restarts, which reloads the same bad PI, which panics again. Infinite
crash loop.

**Why it matters:** Any data-quality bug upstream becomes a service
outage. The SQL/Python engines tolerate this (they propagate `NULL` /
exception in the row).

**Suggested fix:** Skip PIs with missing dates in the dirty set at
collection time, log a warning with the node_id, and don't enter
`compute_one_bucket` for them.

---

### F-011 [HIGH] Loader load query has no JOIN restricting dirty to baseline scenario
**Location:** `rust/ootils_engine/src/loader.rs:95-115`
**Category:** Correctness
**Confidence:** Medium

The loader's `WHERE scenario_id = $1 AND active = TRUE` is fine for
nodes, but the engine assumes a single baseline scenario only exists
in the in-RAM graph. There is no defense against the baseline scenario
being absent or empty. `find_a_pi_node` and other helpers in scripts
all return None gracefully, but the engine itself does *not* refuse to
boot if `n_nodes == 0`.

This means on a fresh empty database the engine will boot to "SERVING"
with zero nodes, and every Propagate will return `Status::not_found`.
Operators see "engine healthy" + "all requests failing" which is the
worst kind of operational signal.

**Why it matters:** Wrong DSN pointing at an empty/wrong DB looks like
a working engine.

**Suggested fix:** Fail boot if `n_nodes == 0` or if `n_pi == 0`, with
a clear error message. Add a `--allow-empty-baseline` flag for test/CI.

---

### F-012 [HIGH] `flush_to_postgres` opens a new Postgres connection per flush
**Location:** `rust/ootils_engine/src/write_behind.rs:191-197`
**Category:** Perf / resource leak / data loss
**Confidence:** High

Every 100 ms (default) the flusher opens a fresh `tokio_postgres::connect`,
issues UPDATE, commits, drops. The connection task is `tokio::spawn`'d
and never awaited — if the connection errors *after* the commit but
before the spawned task ends, the warn log fires from a detached task.
More importantly:
- TCP + auth handshake every 100 ms = ~10-30 ms overhead → ~10-30%
  of the flush budget burned on connection setup.
- Postgres `pg_stat_activity` will show a constant churn of short-lived
  sessions, hurting connection-pool-aware monitoring + risking
  `max_connections` exhaustion if there's any other client churn.
- No `idle_in_transaction_session_timeout` check; if the bulk UPDATE
  hangs forever, the spawned connection task is orphaned without a
  way to cancel.

The PyO3 module's `pool.rs` got this right (persistent connection cache).
The engine's flusher reinvented the wheel worse.

**Why it matters:** At sustained load the engine will be a bad citizen
on a shared Postgres instance.

**Suggested fix:** Cache the `tokio_postgres::Client` between flushes.
On error, drop and reconnect. Use a statement timeout. Use a separate
`Connection` per call but reuse the prepared statement via
`client.prepare_cached`.

---

### F-013 [HIGH] `verify_postgres` connection task leaks
**Location:** `rust/ootils_engine/src/main.rs:281-286`
**Category:** Resource leak / hygiene
**Confidence:** High

```rust
let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
tokio::spawn(async move {
    if let Err(e) = connection.await {
        warn!(error = %e, "postgres connection task ended");
    }
});
```

After `verify_postgres` returns, the client is dropped but the spawned
connection task lives forever (until connection drop is detected). Same
pattern is repeated in `loader.rs:45-49` and `write_behind.rs:192-196`.
At ~3 boots × 1 leak each, this is harmless; at the boot+reload pattern
some operational flows use, it accumulates. The tasks are also
unobservable — no JoinHandle, no cancellation.

**Why it matters:** Cosmetic on its own; the pattern repeating across
the codebase makes it a maintainability concern. A future async-aware
shutdown path will leave these tasks orphaned.

**Suggested fix:** Hold the JoinHandle, cancel on graceful shutdown.
Or use `tokio::task::JoinSet` for the lot. Document the lifecycle.

---

### F-014 [HIGH] Boot replay assumes recovered deltas haven't been superseded by Postgres
**Location:** `rust/ootils_engine/src/main.rs:116-150`
**Category:** Correctness / data loss
**Confidence:** Medium

On boot:
1. Load baseline from Postgres (line 88). This includes any state PG
   has up to crash-time.
2. Replay the WAL into the in-RAM graph (lines 126-148). This *overwrites*
   what was loaded from PG with the WAL's values.

If a WAL record represents a delta that was *already applied to PG*
before the crash (the truncate failed mid-write, see F-003), the loader
will load the newer PG state and then the replay will overwrite it with
the older WAL state. Newer values lost.

**Why it matters:** The combination with F-003 is bad: not only do we
double-flush, we also overwrite Postgres-truth on boot. Together they
are a recipe for divergence in any partial-failure scenario.

**Suggested fix:** Same as F-003 — checkpoint records in the WAL with a
clear "applied to PG up to here" marker, and skip replay below that
marker.

---

### F-015 [HIGH] `Uuid::parse_str(event_id).unwrap_or_else(Uuid::new_v4)` silently masks malformed IDs
**Location:** `rust/ootils_engine/src/service.rs:338`
**Category:** Correctness / data integrity
**Confidence:** High

```rust
let cr_uuid = Uuid::parse_str(&q.event_id).unwrap_or_else(|_| Uuid::new_v4());
```

A client that sends an invalid event_id gets a totally random calc_run_id
back. The audit chain (events → calc_runs → deltas in WAL → PG row)
breaks silently. The `event_id` field validation is "best-effort, will
substitute". This is the opposite of "fail loudly on bad input".

The same handler validates `trigger_node_id` and `scenario_id` strictly
(returns INVALID_ARGUMENT). The asymmetry is a bug.

**Why it matters:** Forensic / debug nightmare. Audit trail is broken
silently.

**Suggested fix:** Validate `event_id` strictly. Return INVALID_ARGUMENT
on parse failure (or accept empty + generate one explicitly with a
warn log).

---

### F-016 [HIGH] gRPC max message size is 4 MB default but client allows 256 MB — server defaults are smaller
**Location:** `src/ootils_core/engine_rust_service/client.py:67-73`, `rust/ootils_engine/src/main.rs:196-198`
**Category:** API contract
**Confidence:** Medium

The Python client lifts the receive/send caps to 256 MB:

```python
("grpc.max_receive_message_length", 256 * 1024 * 1024),
("grpc.max_send_message_length", 256 * 1024 * 1024),
```

But the tonic server side uses defaults (`Server::builder()` with no
`.max_decoding_message_size()` / `.max_encoding_message_size()`). Tonic
0.12 defaults are 4 MB decode / unlimited encode. So a large request
(e.g. ListScenarios when many scenarios accumulate, or QueryShortages
streaming) will be rejected by the server with an unhelpful
`ResourceExhausted` while the client cheerfully sent it.

**Why it matters:** Asymmetric limits cause confusing errors that look
like load issues but are config.

**Suggested fix:** Set explicit `.max_decoding_message_size(256 * 1024 * 1024)`
on the tonic server, document the limit in the proto.

---

### F-017 [HIGH] DSN with embedded password is written verbatim into env passed to subprocess
**Location:** `src/ootils_core/engine_rust_service/harness.py:78-83`, `src/ootils_core/engine/orchestration/propagator_rust.py:128-134`
**Category:** Security
**Confidence:** High

The harness inherits the parent's env (`os.environ.copy()`) and adds
the DSN as `DATABASE_URL`. On Linux + `ps eww` and on macOS, child env
vars are visible to any process running as the same user. Worse, the
`propagator_rust.py` adapter reconstructs an explicit DSN with the
password directly into a string passed to `ootils_kernel.propagate_and_write`:

```python
dsn = (
    f"host={info.host} port={info.port} "
    f"user={info.user} password={info.password} "
    f"dbname={info.dbname}"
)
```

That string is then passed across the PyO3 boundary; if Rust ever logs
it (e.g. on a connect error), the password is leaked to logs. tracing
filters wouldn't redact it.

**Why it matters:** Most production deployments enforce credential
redaction in logs. A connect-error with the full DSN ends up in
log-aggregators (Splunk, Loki, CloudWatch). Then in incident retrospectives.

**Suggested fix:** Never log the DSN. Build the DSN with explicit
URL-encoding of the password, prefer `PGPASSWORD` env var to URL
embedding, and write a `redact_dsn(s) -> s` helper that the tracing
fields use. Audit every `warn!(error = ...)` for accidental DSN
inclusion.

---

### F-018 [HIGH] gRPC service has no `tower::Layer` for timeouts, body limits, or panic catching
**Location:** `rust/ootils_engine/src/main.rs:196-199`
**Category:** Security / availability
**Confidence:** Medium

`Server::builder()` is bare. A panic inside an async handler in tonic
0.12 will tear down the connection but the engine survives unless
`panic = "abort"` is set in `[profile.release]` — which *is* set
(`rust/Cargo.toml:64`). So a panic in any handler kills the entire
process. Combined with F-010 (panic on bad PI data), F-015 (uuid
silent fallback), and the lack of input-size limits (F-016, F-005),
this is a single-malformed-request DOS surface.

There are no per-call timeouts on the server side either. A slow
client (or a stuck rayon worker) can hold the handler indefinitely.

**Why it matters:** Even on a trusted network (which the runbook
assumes), a buggy upstream client can take down the engine.

**Suggested fix:** Add `tower::timeout::TimeoutLayer`, configure
panic handling at boot (consider switching to `panic = "unwind"` for
release for graceful per-handler recovery, at the cost of slightly
larger binary). Add a body-size limit middleware.

---

### F-019 [HIGH] Truncation seek + set_len race in `WalWriter`
**Location:** `rust/ootils_engine/src/wal.rs:142-153`
**Category:** Data loss
**Confidence:** Medium

```rust
pub fn truncate_after_flush(&self) -> anyhow::Result<()> {
    let mut f = self.file.lock();
    f.seek(SeekFrom::Start(MAGIC.len() as u64))?;
    f.set_len(MAGIC.len() as u64)?;
    f.sync_data()?;
    ...
}
```

The seek-then-set_len sequence is fine, but: `set_len` shrinks the file
but does NOT atomically update the file's seek position on all platforms.
On Linux, after `set_len(4)`, a subsequent `f.write_all(...)` will write
*at the current position* (4), which is correct. But on Windows the
semantics depend on whether the file was opened with `FILE_APPEND_DATA`.
Since `OpenOptions::new().write(true)` was used (not `.append(true)`),
behavior is by-current-position. Should be OK but the test surface for
this is thin.

More importantly: between `set_len(4)` and `sync_data()`, if the
process crashes, the file may have been truncated to 4 bytes on disk
without the data fsync. On replay, `magic` is present, then immediate
EOF — replay returns Vec::new(). Records that were durable before the
truncate are gone. This is the *intended* behavior post-flush, but
only if PG actually has them — see F-001/F-002.

**Why it matters:** Combined with the other WAL issues, the truncation
is an "all-or-nothing" durability boundary that's hard to reason about.

**Suggested fix:** Move to WAL rotation rather than in-place truncation:
write a new file (`engine.wal.NEW`), atomic rename over the old one,
then optionally delete the old. Far easier to reason about, harder to
half-corrupt. Or use a sequence-numbered checkpoint marker.

---

### F-020 [HIGH] `Cargo.toml` `rust-version = "1.79"` contradicts Dockerfile `rust:1.85-slim`
**Location:** `rust/Cargo.toml:14`, `Dockerfile.engine:25`
**Category:** Docs / build hygiene
**Confidence:** High

Workspace claims `rust-version = "1.79"` (the std::simd stabilisation
note). The Dockerfile pins `rust:1.85-slim`. Local dev may use whatever
the user has. There's no CI matrix evidence of testing on 1.79. If a
contributor uses 1.79 and tonic 0.12.3 needs ≥ 1.80 (which it does),
local builds break. If the rust-version is real and CI tests it, then
why pin 1.85 in Docker?

**Why it matters:** Confusing minimum-version contract; potential build
failures for new contributors.

**Suggested fix:** Pick a single MSRV (probably 1.82 — what `tokio 1.40`,
`tonic 0.12`, `hyper 1.4` actually require together) and pin Docker to
match. Update the comment in `Cargo.toml`.

---

### F-021 [HIGH] Decimal division by `from_db` `span_days` can lose precision
**Location:** `rust/ootils_engine/src/kernel.rs:88-94`, mirror in `rust/ootils_kernel/src/kernel.rs:66-72`
**Category:** Correctness / parity
**Confidence:** Medium

```rust
let frac = d.quantity / Decimal::from(span_days) * Decimal::from(overlap_days);
```

`rust_decimal` is a fixed-point 96-bit mantissa type with up to 28
significant digits. Division at line 1 truncates to ~28 digits; the
later multiplication by `overlap_days` cannot recover lost digits.
The Python kernel uses `Decimal` with the default 28-digit precision —
results should match. But the SQL engine uses `numeric(50,28)` which
has 50 total digits. For very large `quantity` or long spans, the
SQL and Rust engines will diverge by more than `TOLERANCE = 1e-18`
(parity_4way.py threshold).

This was likely caught in the chantier-A 0-mismatch parity, but only
for the data shapes tested. The audit-trail of which data shapes were
actually tested is not in the repo (the snapshot referenced in
parity_3way doesn't exist anymore).

**Why it matters:** Subtle parity drift only triggers on edge cases
(e.g. fractional quantities with many digits, large spans). When it
strikes, debugging is awful: "the Rust engine is 0.0000001 off". Needs
verification.

**Suggested fix:** Use `quantity * overlap_days / span_days` (multiply
first), and document the operation order. Add a unit test for the
high-precision case.

---

### F-022 [HIGH] `compute_seed_opening_from_sorted` does O(N) linear scan to find prev bucket
**Location:** `rust/ootils_engine/src/propagator.rs:178-186`
**Category:** Perf
**Confidence:** High

```rust
if let Some(buckets) = graph.by_series.get(&sid) {
    for &b in buckets {
        let n = &graph.nodes[b as usize];
        if n.bucket_sequence == seed_seq - 1 {
            return n.closing_stock;
        }
    }
}
```

For each dirty series this scans all (~90) buckets to find one. On a
full propagation this is O(N_series × ~90) = trivial. But the propagator
*also* re-sorts the buckets list once per series for the dirty walk
(line 70). And `by_series` is built once at boot but never re-sorted
on inserts (which is correct because there are no inserts).

The real perf cost is that this fires inside a `par_iter` closure, so
on 8 threads it scales OK. But for incremental events with one PI dirty
in the middle of a series, this re-scans all 90 buckets per call.

**Why it matters:** Phase 3 perf is fine but the algorithm is O(N) per
incremental event where it should be O(log N) or O(1).

**Suggested fix:** Sort `by_series[sid]` by `bucket_sequence` at boot.
Use `binary_search_by_key`. Trivial change, large constant-factor win
on incremental.

---

### F-023 [HIGH] Boot panic path with `panic = "abort"` + restart-on-failure = crash loop
**Location:** `rust/ootils_engine/src/main.rs:73-202`, `rust/Cargo.toml:64`, `docs/RUNBOOK-rust-engine-service.md:102`
**Category:** Operational / availability
**Confidence:** High

`panic = "abort"` in release + `Restart=unless-stopped` in docker-compose
+ `restartPolicy: Always` in k8s + panic-prone code paths (F-010, F-015
substitution may move bad data forward then panic later) = if any single
input triggers a panic, the process restarts, reloads the bad state from
PG, panics again. Indefinitely.

**Why it matters:** A bad event injected once permanently breaks the
service until manual DB intervention.

**Suggested fix:** Either (a) switch to `panic = "unwind"` in release
and add a `std::panic::catch_unwind` at the rayon job level so one bad
PI doesn't take down the propagator, or (b) ensure all expect/unwrap
in hot paths are removed (F-010 etc.). Option (a) is the more defensive
choice.

---

### F-024 [HIGH] No transaction isolation level set on the write-behind UPDATE
**Location:** `rust/ootils_engine/src/write_behind.rs:199-248`
**Category:** Correctness
**Confidence:** Medium

```rust
let tx = client.build_transaction().start().await?;
tx.execute("SET LOCAL session_replication_role = 'replica'", &[]).await?;
... UPDATE nodes ... commit ...
```

No isolation level specified — defaults to Postgres `READ COMMITTED`.
If the SQL engine is also writing to `nodes` concurrently (mixed-mode
canary), serialization anomalies can occur. The UNNEST UPDATE will
silently UPDATE rows in whatever state PG sees at row-level, mixing
fields from this batch and fields from the SQL engine.

**Why it matters:** Field-level inconsistency in mixed-engine
deployments. Hard to detect after the fact.

**Suggested fix:** Either explicit `REPEATABLE READ` for the writeback
transaction, or document the requirement that only one engine writes
to `nodes` at a time and refuse to enable rust-svc if SQL engine traffic
is also live.

---

### F-025 [HIGH] Loader trusts NodeType::Unknown by silently keeping nodes — propagator skips them
**Location:** `rust/ootils_engine/src/state.rs:56-69`, `rust/ootils_engine/src/loader.rs:184-191`
**Category:** Correctness / observability
**Confidence:** Medium

A node with a `node_type` string the engine doesn't know (e.g. typo,
new node type added by another team, future expansion) is mapped to
`NodeType::Unknown` and silently kept in the graph. It contributes to
RAM but never to propagation. There's no count of unknown nodes in the
boot log or metrics.

**Why it matters:** When a future migration adds e.g. `"ScenarioGhost"`
node type, the engine will silently miscount + ignore. Visible only as
"propagation results differ from SQL engine".

**Suggested fix:** Count `n_unknown` in load stats and log + expose as
Prometheus metric. Optionally refuse to boot if `n_unknown / n_nodes >
0.01`.

---

### F-026 [MEDIUM] `Graph::clone()` is O(N) on memory + time, no `Arc<Vec>` sharing
**Location:** `rust/ootils_engine/src/scenario.rs:127-129`, `rust/ootils_engine/src/state.rs:186`
**Category:** Perf / memory
**Confidence:** High

Forking a scenario clones the entire `Graph` (Vec<Node> + 4 HashMaps).
The ADR-017 promise was 20-50 ms; measured 100-200 ms (documented in
scenario.rs comments). Each fork costs ~76 MB; 10 forks = 760 MB +
baseline. The `Arc<Graph>` baseline-snapshot wrapping helps reads but
doesn't avoid the clone cost itself.

This is a known limitation that the implementer documented, but the
choice has multi-tenant implications: 10 active scenarios per tenant
× 10 tenants × 76 MB = 7.6 GB.

**Why it matters:** RAM cost scales linearly with active scenarios.
Customer-facing scenarios (a planner editing multiple what-ifs) will
hit OOM.

**Suggested fix:** The ADR proposes ArcSwap<Graph> baseline (cheap
forks at cost of expensive baseline writes). Worth implementing.
Alternative: persistent data structures via `im` crate (5× slower
reads but 100× cheaper forks).

---

### F-027 [MEDIUM] `flush_to_postgres` uses `Vec<Decimal>` arrays — UNNEST allocation cost
**Location:** `rust/ootils_engine/src/write_behind.rs:202-218`
**Category:** Perf
**Confidence:** Medium

For each flush the code allocates 7 `Vec<T>` of length N, fills them by
iterating `batch`, then passes references via the param array. Each
`Decimal` is 16 bytes; for a 10K-batch that's ~1.1 MB across the 5
numeric arrays. The Vecs are dropped at the end of the function. Per
100ms flush this is fine; under burst (10K-deltas flush) it's a memory
churn signal mimalloc handles well but not for free.

The PyO3 module's COPY path (`writeback.rs::write_projection_copy`)
streams via binary COPY instead — much cheaper.

**Why it matters:** The engine claims to scale to 5000 events/sec but
the flush path was tested only at 100/sec.

**Suggested fix:** For batches over a threshold (~1000), use binary
COPY into a temp table + UPDATE FROM, same pattern as ootils_kernel.

---

### F-028 [MEDIUM] `verify_postgres` does only `SELECT version()` — doesn't check the schema
**Location:** `rust/ootils_engine/src/main.rs:279-291`
**Category:** Operational / boot safety
**Confidence:** Medium

The engine confirms it can talk to Postgres but doesn't check that
the schema is what it expects (e.g. presence of `nodes` table with
required columns). If the operator points at the wrong database (e.g.
a fresh DB without migrations), the loader will fail in an opaque way
(`column "opening_stock" does not exist` thrown from inside `tokio_postgres`),
which is harder to debug than "I checked schema version X.Y, found Z".

**Why it matters:** Operational diagnostic quality.

**Suggested fix:** Check for required tables and at least one
discriminating column. Optionally check a schema version table.

---

### F-029 [MEDIUM] `shutdown_signal` doesn't await background tasks
**Location:** `rust/ootils_engine/src/main.rs:293-312, 198-200`
**Category:** Data loss / graceful shutdown
**Confidence:** High

On SIGTERM the gRPC server's `serve_with_shutdown` returns. main returns
Ok(()). But the spawned tasks — write-behind flusher, metrics server,
connection task — are dropped (since they're not awaited). With
`panic = "abort"`, the process exits without flushing the queue. Any
deltas in the queue that haven't been flushed to PG yet are still in
the WAL, so they'll be replayed on next boot — but the runbook
suggests "stop the engine without notice" works because the WAL is the
safety net. With the bugs in F-001-F-003, this is more brittle than
documented.

A cleaner shutdown would: stop accepting new gRPC calls (already done),
wait for the flusher to drain to empty (with timeout), exit. The
runbook should also say "wait until /metrics shows queue depth 0
before stopping".

**Why it matters:** "Drop the engine, restart it" sequences during
rollout are not as clean as documented.

**Suggested fix:** Hold the flusher's JoinHandle, signal it to drain
on shutdown, wait up to N seconds.

---

### F-030 [MEDIUM] `_finish_run_without_shortage_resolve` reconstructs a `Scenario` from a row by hand
**Location:** `src/ootils_core/engine/orchestration/propagator_rust_svc.py:174-198`
**Category:** Maintainability / correctness
**Confidence:** Medium

The adapter manually maps a DB row to a `Scenario(...)` constructor,
duplicating logic that probably exists elsewhere. The fallback
`Scenario(scenario_id=scenario_id, name="unknown")` defeats the
purpose. If `Scenario` ever grows a new field, this code goes stale.

**Why it matters:** Drift between this code and the real Scenario
construction in other engines.

**Suggested fix:** Use the existing `ScenarioStore`/`ScenarioRepo` (or
whatever the canonical loader is); don't construct Scenario by hand.

---

### F-031 [MEDIUM] `RustServicePropagationEngine` swallows propagation errors as "warning"
**Location:** `src/ootils_core/api/routers/events.py:218-232`
**Category:** API contract / observability
**Confidence:** High

The route handler:

```python
try:
    engine = _build_propagation_engine(db)
    calc_run = engine.process_event(...)
except Exception as exc:
    logger.warning("propagation failed for event %s: %s", event_id, exc)
    # Don't fail the request — event is recorded, propagation is best-effort
```

A gRPC error from the rust-svc engine (UNAVAILABLE, INTERNAL, RESOURCE_EXHAUSTED)
becomes a successful HTTP 202 with `status="queued"`. The caller has
no idea propagation failed. Combined with the rust-svc adapter's
explicit `raise` on propagate failure (line 141), the engine WILL raise
on a real failure, but the route silently swallows it.

This is the *route* code, not the architecture-B code, but it
significantly affects the rollout: bad behavior in the rust-svc engine
is invisible to clients.

**Why it matters:** The "queued" status response is a lie. Operators
won't notice the engine is down; they'll see successful 202s with
0 affected_nodes.

**Suggested fix:** Distinguish between propagation-failed (real error
that should surface) and propagation-skipped (no trigger, etc.). Return
500 or 503 on the former.

---

### F-032 [MEDIUM] `EngineHarness.stop` doesn't flush the bg flusher before SIGTERM
**Location:** `src/ootils_core/engine_rust_service/harness.py:132-150`
**Category:** Test correctness
**Confidence:** Medium

`stop()` sends SIGTERM and waits up to 5 seconds. The flusher loop
inside the engine is on a `tokio::time::sleep(current_interval)` for
up to 30 seconds in the bad case. SIGTERM is caught by the shutdown
signal handler (good), the gRPC server stops, main returns. But the
spawned flusher task is dropped without draining — see F-029. The test
`test_wal_truncated_after_clean_shutdown` works only because it
explicitly uses `flush_interval_ms=50` so the flusher drains a few
times during the test, plus a `time.sleep(0.5)`. A 100ms flush interval
would still race.

**Why it matters:** The test is flaky-by-design and gives false
confidence in the clean-shutdown contract.

**Suggested fix:** When F-029 is fixed (engine drains on SIGTERM), this
test becomes deterministic. Until then, raise `flush_interval_ms` to
something pathological in this test, OR explicitly send a "drain"
operation via gRPC before SIGTERM.

---

### F-033 [MEDIUM] Free-port helper has a TOCTOU race in concurrent test runs
**Location:** `tests/engine_service/conftest.py:69-78`
**Category:** Test correctness
**Confidence:** High

```python
def _free_port(start: int = 50100) -> int:
    for p in range(start, start + 100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", p))
                return p
            ...
```

The socket is closed when the context manager exits, *then* the port
number is returned. Between the close and the engine subprocess binding,
any other test thread (or process) can grab it. Under `pytest-xdist`
or just parallel test workers in CI, this will produce flakes.

**Why it matters:** CI flakiness; harder to trust test results.

**Suggested fix:** Use `socket.SO_REUSEADDR` + bind on port 0 to let
the OS allocate, then read `.getsockname()[1]` after close. Or restructure
to pass the live socket to the harness which doesn't actually need a
specific port.

---

### F-034 [MEDIUM] Tests share `engine_session` fixture across mutation tests
**Location:** `tests/engine_service/test_concurrency.py:24-71`, `tests/engine_service/test_correctness.py:56-70`
**Category:** Test correctness
**Confidence:** Medium

`engine_session` is module-scoped. Both `test_correctness.test_propagate_smoke`
and `test_concurrency.test_burst_propagations_no_failures` use it and
both mutate the baseline. The order of execution affects which test
sees which baseline state. The chosen PI from `pick_pi_node()` is
random per call (`ORDER BY random()`), which hides interdependence by
luck. Eventually two tests will pick the same PI and the second one's
"idempotent" propagation will produce different deltas because the
first one moved the state.

**Why it matters:** Subtle test interdependence; potential for spurious
CI failures.

**Suggested fix:** Either use function-scoped `engine` fixture for
mutating tests (slow but correct) or restructure tests to read-only
operations.

---

### F-035 [MEDIUM] `test_parallel_reads_during_propagation` asserts read count > 100 — meaningless
**Location:** `tests/engine_service/test_concurrency.py:65-71`
**Category:** Test correctness
**Confidence:** High

A 4-thread reader hammering for 3 seconds at sub-millisecond Get
latency will easily reach 10,000+ reads. The 100-read threshold doesn't
test for the lock-contention failure mode (which would show as <100
reads); it tests for "the engine didn't completely freeze". A real
contention check should compare reader throughput with and without
concurrent propagation.

**Why it matters:** False sense of test coverage.

**Suggested fix:** Baseline the reader throughput without propagation
(say it's 50 K reads / 3s). Then assert with-propagation throughput is
at least 30 K. That's the contract.

---

### F-036 [MEDIUM] `test_fork_p95_under_100ms` doesn't free forks; subsequent tests run with bloated state
**Location:** `tests/engine_service/test_performance_regression.py:56-78`
**Category:** Test hygiene
**Confidence:** Medium

The test forks 23 times (3 warmup + 20 timed). Each fork is ~76 MB.
Function-scoped fixture, so the engine is torn down after the test —
fine. But if a developer changes to module-scoped, the next test
inherits 23 × 76 MB = ~1.7 GB resident. The RSS test
(`test_engine_memory_after_warmup_under_600mb`) would silently fail
in a different way. No cleanup mechanism (no `DeleteScenario` RPC
either — see F-038).

**Why it matters:** Fragile testing pattern.

**Suggested fix:** Add a `DeleteScenario` RPC (F-038) and use it in
test teardown. Or assert in the test that the fixture is function-scoped.

---

### F-037 [MEDIUM] `ScenarioManager` has no eviction; long-running engine accumulates dead scenarios
**Location:** `rust/ootils_engine/src/scenario.rs:99-167`
**Category:** Resource leak
**Confidence:** High

Forks accumulate forever. `MergeScenario` removes one; nothing else.
There is no API to drop a scenario, no TTL eviction, no max-scenarios
cap. On a busy what-if simulation workflow scenarios are created
faster than they're merged, eventually OOM.

**Why it matters:** Production leak in the most likely usage pattern.

**Suggested fix:** Add `DeleteScenario` RPC. Add LRU/TTL eviction.
Cap with a configurable max. Surface a gauge for active scenarios
+ total scenarios memory.

---

### F-038 [MEDIUM] No `DeleteScenario` RPC — scenarios cannot be explicitly dropped
**Location:** `rust/ootils_proto/proto/engine.proto:18-56`
**Category:** API contract
**Confidence:** High

`MergeScenario` is the only way to dispose of a scenario. Users
typically want to discard a what-if without merging it back. With no
delete the only recourse is engine restart.

**Why it matters:** Combined with F-037, this is the leak vector. Also
a basic API gap.

**Suggested fix:** Add `DeleteScenario(DeleteRequest) returns
(DeleteResult)`. Trivial implementation in `service.rs`.

---

### F-039 [MEDIUM] `Health` RPC `boot_time` Timestamp may misrepresent very long uptimes
**Location:** `rust/ootils_engine/src/service.rs:78-94`
**Category:** Observability
**Confidence:** Low

`Timestamp::from(SystemTime::now())` at startup is correct, but the
`uptime_seconds = self.boot_time.elapsed().as_secs() as i64` cast can
overflow at 292 billion years (not a real concern). The `as i64` cast
is silent though — a future change to `as u32` or similar would be
masked. Minor.

**Why it matters:** Code clarity, not a real bug.

**Suggested fix:** Use `try_into().unwrap_or(i64::MAX)` to be explicit
about overflow handling.

---

### F-040 [MEDIUM] `Health::detail` leaks internal phase numbering ("phase 2: baseline loaded...")
**Location:** `rust/ootils_engine/src/service.rs:82-87`
**Category:** Docs / API contract
**Confidence:** Low

```rust
let detail = format!("phase 2: baseline loaded ({} nodes, gen {})", g.len(), g.generation);
```

"phase 2" is internal ADR-017 nomenclature, not a user-facing concept.
Customers, k8s operators, etc. reading the health endpoint will be
confused. Also, "gen" is internal.

**Why it matters:** Cosmetic / hygiene.

**Suggested fix:** `format!("baseline loaded: {} nodes, generation {}", ...)`.

---

### F-041 [MEDIUM] `EngineMetrics` gRPC fields are all zero — never populated
**Location:** `rust/ootils_engine/src/service.rs:96-112`
**Category:** Observability / API contract
**Confidence:** High

`Metrics` RPC returns hardcoded zeros for everything except
`baseline_graph_bytes`. The Prometheus endpoint *does* expose the real
counters (good). But anyone calling `client.metrics()` from Python
gets a structured zero — silently. The proto defines fields that the
implementer didn't connect.

**Why it matters:** Misleading API. Tests that use `metrics()` would
pass even if the engine was producing wrong numbers.

**Suggested fix:** Either remove the unused fields from the proto (with
the v1 → v2 evolution policy), or populate them from the `Metrics`
struct.

---

### F-042 [MEDIUM] gRPC `EngineMetrics.events_processed_total` and friends should be u64 not i64
**Location:** `rust/ootils_proto/proto/engine.proto:204-218`
**Category:** API contract
**Confidence:** Low

Counters can never be negative. Using `int64` for an unbounded counter
is a footgun (after 2^63 events overflow to negative). Use `uint64`.

**Why it matters:** Cosmetic for now; protocol evolution requires
proto v2 to fix later.

**Suggested fix:** Use `uint64` for counters in the next proto revision.

---

### F-043 [MEDIUM] Tests don't validate the WAL header byte-for-byte after corruption
**Location:** `tests/engine_service/test_recovery.py`, no test
**Category:** Test coverage
**Confidence:** High

The recovery tests only validate "kill and restart, state matches". They
don't test:
- WAL with corrupted middle record (just bad bytes injected)
- WAL with wrong magic header
- WAL with valid magic but garbage payload
- WAL truncated mid-record-length-prefix
- WAL file that's exactly 0 bytes
- Concurrent writers (shouldn't happen but if two engines start on the
  same WAL...)

The `replay()` code handles some of these but no test verifies the
handling.

**Why it matters:** WAL is the durability contract. Bugs there are
quiet and dangerous.

**Suggested fix:** Add a Rust-side integration test (in
`rust/ootils_engine/tests/`) that constructs malformed WAL files and
asserts the documented "stop at first corrupt record" behavior. The
implementer's note in `wal.rs:235-239` admits this is missing.

---

### F-044 [MEDIUM] `parity_4way.py` doesn't actually run 4-way parity — it only samples 100 nodes from rust-svc
**Location:** `scripts/parity_4way.py:163-225`
**Category:** Test honesty / docs
**Confidence:** High

The script's docstring promises 4-way parity. The implementation:
1. Engine A (SQL) — `diff_snapshots(truth, sql_snap, ...)` — both are
   identical reads from PG, so 0 diffs trivially.
2. Engine B (Python) — *not actually executed*. The code admits this.
3. Engine C (Rust A) — *not actually executed*. The code admits this.
4. Engine D (Rust B) — samples 100 nodes via GetNode and diffs against
   PG ground truth.

The output then prints "PARITY 4-WAY OK" if the 100-node sample
matches. This is at best a 2-way smoke test mislabeled as 4-way.

**Why it matters:** Anyone reading the success message has a false
sense of correctness. The script's own header text contradicts what
it does. If a reviewer or auditor checks parity coverage, they may be
misled.

**Suggested fix:** Either implement what the docstring promises (run
all 4 engines fresh against the same dataset), or rename and rewrite
the script to be honest about what it does ("smoke check rust-svc
matches Postgres for 100 sampled nodes"). The latter is fine; the
former is what the production claim needs.

---

### F-045 [MEDIUM] `pick_pi_node` uses `ORDER BY random() LIMIT 1` — slow + nondeterministic
**Location:** `tests/engine_service/conftest.py:149-176`
**Category:** Test perf / determinism
**Confidence:** High

`ORDER BY random()` on a 330K-node table is a full sort. On profile L
this is ~1-2 seconds per call. Tests calling it 5+ times pay 5-10 s
per test. Nondeterministic node selection also means failing tests
are hard to reproduce: "the previous CI run picked node X and passed,
this one picked node Y and failed".

**Why it matters:** CI run time + reproducibility.

**Suggested fix:** Use TABLESAMPLE or pick by `bucket_sequence = 0
LIMIT 1`. Add a `seed` param for determinism.

---

### F-046 [MEDIUM] `propagator::compute_one_bucket` filter logic is duplicated and easy to drift from chantier A
**Location:** `rust/ootils_engine/src/propagator.rs:204-242`, `rust/ootils_kernel/src/propagator.rs:51-94`
**Category:** Maintainability / parity drift risk
**Confidence:** Medium

The two propagators implement the same logic by hand. Architecture A
uses SQL-loaded `Subgraph` (typed shapes); Architecture B uses graph
adjacency lists. Future updates to e.g. demand classification rules
must be made in both places. The kernel code (`kernel.rs`) is
duplicated as documented in the file header — but the *propagator
orchestration* is also duplicated and not flagged.

**Why it matters:** Parity drift; every kernel update is two-place.

**Suggested fix:** Extract the supply/demand classification rules
into a shared trait or set of free functions in a `ootils_kernel_pure`
crate that both Architecture A and B depend on. Avoid duplication of
business rules even if data sources differ.

---

### F-047 [MEDIUM] `Cli::log` env mismatches actual behavior — clap reads from `RUST_LOG` AND uses default
**Location:** `rust/ootils_engine/src/main.rs:42-44`
**Category:** Operational / docs
**Confidence:** Medium

```rust
#[arg(long, env = "RUST_LOG", default_value = "info,ootils_engine=debug")]
log: String,
```

If `RUST_LOG` is unset, the default `"info,ootils_engine=debug"` applies.
If it's set to e.g. `"warn"`, the warn-only filter wins. Standard. But:
the runbook in §Configuration table documents `RUST_LOG` default as
the verbose default. In Docker / k8s, `RUST_LOG` is often set to
`"info"` to reduce log volume — which silently disables the
`ootils_engine=debug` carve-out. Operators won't notice debug-level
logs missing.

**Why it matters:** Diagnostic regression.

**Suggested fix:** Document that any explicit `RUST_LOG` value
completely replaces the default. Provide an example for operators
wanting to keep debug-level for ootils_engine while quieting others.

---

### F-048 [MEDIUM] `harness.kill9()` doesn't capture exit code / debug info
**Location:** `src/ootils_core/engine_rust_service/harness.py:152-162`
**Category:** Test diagnostics
**Confidence:** Low

```python
self.process.kill()
self.process.wait()
self._close_logs()
self.process = None
```

After a kill-9 (or any kill), the exit code, stderr tail, etc. are
silently discarded. If the WAL recovery test fails, debugging is harder
than it needs to be.

**Why it matters:** Test failure diagnosability.

**Suggested fix:** Log exit code, last 100 lines of stderr, on kill.

---

### F-049 [MEDIUM] `mark_all_pi_dirty` mutates `n.flags` but returned set may not match
**Location:** `rust/ootils_engine/src/propagator.rs:249-258`
**Category:** Correctness
**Confidence:** Medium

```rust
for (idx, n) in graph.nodes.iter_mut().enumerate() {
    if n.node_type == NodeType::ProjectedInventory && n.is_active() {
        n.flags |= Node::FLAG_DIRTY;
        dirty.insert(idx as NodeIndex);
    }
}
```

The flags are mutated; if propagation later fails or panics, the dirty
flags remain set on the in-RAM graph. There's no rollback. Next
`Propagate` call sees stale dirty flags. (In practice the propagator
doesn't actually consult `is_dirty()` — it uses the explicit
`HashSet<NodeIndex>` argument. So this is cosmetic now. But the
asymmetric "set then forget" is a footgun for future code.)

**Why it matters:** Code smell that will bite when someone wires up
the `is_dirty` flag for real.

**Suggested fix:** Either remove the flag mutation here (not needed)
or clear it consistently at the end of propagation in `propagate()`.

---

### F-050 [LOW] `EngineHarness` writes stdout/stderr to PID-named files in `tempfile.gettempdir()`
**Location:** `src/ootils_core/engine_rust_service/harness.py:85-89`
**Category:** Test hygiene
**Confidence:** High

The log files use only the current Python PID — multiple engines
spawned by the same Python (e.g. a CI runner doing several iterations)
will overwrite each other's logs. Less of an issue with the
function-scoped fixture that creates one engine per test, but the
filename collision is real.

**Why it matters:** Lost log evidence in test post-mortems.

**Suggested fix:** Include the listen-port (which is unique per call)
in the filename.

---

### F-051 [LOW] `Dockerfile.engine` copies `Cargo.lock` but Cargo.lock for the workspace also pins ootils_kernel deps not built
**Location:** `Dockerfile.engine:33`
**Category:** Build hygiene
**Confidence:** Medium

The build copies the full `Cargo.lock`, which includes ootils_kernel
(PyO3) deps. The cache-warming step `cargo fetch` pulls everything,
even though `cargo build --release -p ootils_engine` only builds the
engine. ~50-100 MB of needless downloads + a longer cold build.

**Why it matters:** Slower Docker builds.

**Suggested fix:** Either exclude ootils_kernel from the workspace
when building the image (separate workspace member by feature), or
accept the cost — it's a build-time only concern.

---

### F-052 [LOW] `Dockerfile.engine` uses `gcr.io/distroless/cc-debian12` — no glibc version pin
**Location:** `Dockerfile.engine:60`
**Category:** Build reproducibility
**Confidence:** Low

`distroless/cc-debian12` is a moving target. Image rebuilds may pick
up new libc, new CA bundles, etc. For a sub-100 MB hardened image
this is fine, but reproducible builds need a `@sha256:...` digest.

**Why it matters:** Cosmetic.

**Suggested fix:** Pin by digest in production releases.

---

### F-053 [LOW] `ootils_kernel/Cargo.toml` has its own `[profile.release]` section — ignored by workspace
**Location:** `rust/ootils_kernel/Cargo.toml:31-36`
**Category:** Build hygiene
**Confidence:** High

Cargo ignores `[profile]` sections in non-root manifests. The release
profile in `rust/Cargo.toml` already sets the same values. The
ootils_kernel section is dead config — confusing for readers.

**Why it matters:** Maintenance hygiene.

**Suggested fix:** Delete the section.

---

### F-054 [LOW] `service.rs::list_scenarios` ignores all input — should validate empty payload
**Location:** `rust/ootils_engine/src/service.rs:114-146`
**Category:** API contract
**Confidence:** Low

`ListScenarios` takes Empty. No issue. But pagination is missing — a
tenant with 10K scenarios returns one giant response.

**Why it matters:** Will hit the message size limit eventually.

**Suggested fix:** Add `limit` / `offset` to `ListRequest` (proto v2).
Document the current limit.

---

### F-055 [LOW] `EngineClient.connect` silently accepts URI-style addr that grpc parses differently
**Location:** `src/ootils_core/engine_rust_service/client.py:63-74`
**Category:** API contract / docs
**Confidence:** Medium

`grpc.insecure_channel("127.0.0.1:50051")` works. So does
`grpc.insecure_channel("dns:127.0.0.1:50051")` and the more advanced
`"unix:/tmp/foo.sock"`. The Python client accepts any string. There's
no docstring guidance on what URI forms are supported, and no
validation.

**Why it matters:** Users may pass a URI that "works" but with surprising
DNS resolution semantics.

**Suggested fix:** Document supported URI forms in the docstring.
Add a debug-log of the resolved authority.

---

### F-056 [LOW] `_finish_run_without_shortage_resolve` issues UPDATE events without commit
**Location:** `src/ootils_core/engine/orchestration/propagator_rust_svc.py:200-204`
**Category:** Correctness / autocommit
**Confidence:** Medium

`db.execute("UPDATE events SET processed = TRUE WHERE ...")` — psycopg
in default mode autocommits depending on connection setup. The
ancestor's `complete_calc_run` already committed; if the connection
is in implicit-transaction mode the UPDATE may not commit until the
next operation. The behavior depends on the FastAPI app's connection
factory, which is not visible here.

**Why it matters:** Audit-trail event marked as processed may not
actually be persisted when the request returns.

**Suggested fix:** Explicit `db.commit()` after the UPDATE, or follow
the existing pattern used by other engines (which probably already
commits — verify and align).

---

### F-057 [LOW] Magic baseline UUID hardcoded in three places
**Location:** `rust/ootils_engine/src/loader.rs:27-28`, `tests/engine_service/conftest.py:30`, multiple Python tests
**Category:** Maintainability
**Confidence:** High

`00000000-0000-0000-0000-000000000001` is the baseline scenario UUID,
hardcoded in Rust and Python in 5+ places. A future migration to a
real UUID per tenant would require touching all of them.

**Why it matters:** Hardcoded magic.

**Suggested fix:** Centralize as a constant in `ootils_core.constants`
(Python) + a module-level pub const (Rust), import everywhere.

---

### F-058 [LOW] `harness.start` writes WAL into tempdir without cleanup on test failure
**Location:** `src/ootils_core/engine_rust_service/harness.py:51, 67-72`
**Category:** Test hygiene
**Confidence:** Medium

The WAL path defaults to `tempfile.gettempdir() / "ootils-engine-test.wal"`
— not unique to a test. If a test crashes before its own cleanup,
the file persists for the *next* test's `wal.unlink()` to discover.
The conftest test fixtures do randomize per-port (good), but the
harness default itself doesn't.

**Why it matters:** Subtle test pollution.

**Suggested fix:** Default to a per-pid + per-port filename, or refuse
to construct without an explicit `wal_path`.

---

### F-059 [LOW] Proto file is in `rust/ootils_proto/proto/` but the v1→v2 evolution policy is unwritten
**Location:** `rust/ootils_proto/proto/engine.proto:11-16`
**Category:** Docs
**Confidence:** Low

The proto comment says "Breaking changes follow the v1 → v2 evolution
rule (new package, kept side by side until all clients migrate)." but
there is no actual policy doc describing what's allowed in v1 (adding
fields, deprecating, etc.) vs requires v2.

**Why it matters:** Future-proto changes likely to violate this policy.

**Suggested fix:** Short policy doc as a comment block (or separate file).

---

### F-060 [INFO] `panic = "abort"` + Rust panic on unknown match arm in `io.rs::load_subgraph`
**Location:** `rust/ootils_kernel/src/io.rs:241-247`
**Category:** Resilience
**Confidence:** High

`panic!("io::load_subgraph: unknown kind marker {other:?}")` is the
right defensive choice when the developer intends "this can never
happen because the SQL is right here", but it's worth noting that
this aborts the Python interpreter (via `panic = "unwind"` on the
PyO3 crate which doesn't override — but PyO3 catches panics and
raises them as Python exceptions, mostly. Worth verifying for the
ootils_kernel crate specifically.

**Why it matters:** Just confirming intentional behavior.

**Suggested fix:** Comment "panics here surface as Python exceptions
via PyO3's std::panic::catch_unwind shim".

---

### F-061 [INFO] tracing-subscriber JSON layer is configured but only the fmt layer is added
**Location:** `rust/ootils_engine/src/main.rs:270-277`, `rust/ootils_engine/Cargo.toml:48`
**Category:** Observability
**Confidence:** High

`tracing-subscriber` is pulled in with the `json` feature, but the
`init_tracing` function only adds `fmt::layer().with_target(true)` —
no JSON output anywhere. Production deployments typically want JSON.
The `json` feature flag is paying its build cost for nothing.

**Why it matters:** Operations team will request JSON logs eventually;
the feature is wired but unused.

**Suggested fix:** Add a `--log-format json|text` flag; or drop the
json feature until needed.

---

### F-062 [INFO] OTLP / TLS feature flags are listed in Cargo.toml but no code under `#[cfg(feature = "otlp")]`
**Location:** `rust/ootils_engine/Cargo.toml:104-108`
**Category:** Build hygiene
**Confidence:** High

```toml
[features]
otlp = ["dep:tracing-opentelemetry", ...]
tls = ["dep:tonic-tls"]
```

No code references either feature. The deps are conditional but never
activated. Pure dead config. Per the comment, this is item #1 (TLS) and
item #3 (OTLP) — known follow-ups. Worth removing until implemented to
avoid the false-promise signal.

**Why it matters:** Cargo cosmetics; reader misled.

**Suggested fix:** Remove the features until they're actually wired,
or stub a `#[cfg(feature = "otlp")] mod otlp;` to make the wiring
visible.

---

## Patterns observed

**The good.** The codebase is *legible*. Module docs are real prose
about intent and trade-offs, not boilerplate. The "why" for each design
choice (mimalloc, ahash, COW vs persistent, bincode, no-Python in
ootils_engine) is documented inline. The decision to lift the kernel
into a duplicate file (vs sharing) with explicit reasoning is mature.
The benchmark/perf-gate machinery (run_bench, run_bench_fork) inside
the binary is well thought out. Test coverage of the *positive paths*
(propagate, fork, recover, concurrent reads) is broad.

**The bad: durability is the weakest area.** The implementer wrote
"durability sequence" carefully in code comments and ADR, but the
implementation has at least three independent failure modes that lose
data (F-001, F-002, F-003). All three are subtle and would not be
caught by the existing test battery, because the tests only validate
the happy-path "kill, restart, restore" sequence. Failures in the
write-behind/PG interaction window are untested. Reviewers must
recognize this is the area where the most rigor (sequence diagrams,
fuzz testing, jepsen-style fault injection) is least applied.

**The bad: many `expect`s and unwraps in production paths.** F-010,
F-015, F-018 are examples. Combined with `panic = "abort"`, the engine
is more brittle than its claims suggest. Bad input becomes a crash
loop. The team should adopt a "no expect/unwrap outside tests" lint
for non-`mod tests` code.

**The bad: API contract / proto cleanliness.** Several fields in the
proto are present but unimplemented (`EngineMetrics`,
`PropagateRequest.scenario_id`, `ChangeEvent` oneof types). The proto
should reflect what the engine actually does. Phase gating shouldn't
turn the wire contract into aspirational fiction.

**Test-claim vs test-reality drift.** `parity_4way.py` is the worst
offender — its docstring promises 4-way parity and prints "PARITY
4-WAY OK" on a 100-node 2-way sample (F-044). Several tests assert
threshold values that don't actually exercise the failure mode (F-035).
Several use `random()` ordering that hides interdependence (F-034,
F-045). Tests are a contract — the codebase needs an audit of test
claims vs test mechanics.

**Operational concerns deferred.** TLS, auth, OTLP, OpenTelemetry,
per-scenario propagation, eviction, multi-tenant isolation — all are
documented as "phase 9 / future". For an "opt-in production"
deliverable, several of these (esp. backpressure, F-005; queue cap;
backoff observability) should land before any percentage of traffic
shifts. The runbook's known-limitations list is honest but missing
F-004 (privilege requirement), F-005 (WAL unbounded growth).

---

## What's NOT a bug (but might look like one)

- `Graph::new()` not used at runtime; only the loader creates a
  populated Graph. The `new()` constructor is fine to keep for tests.
- The `EngineSvc::baseline` is `Arc<RwLock<Graph>>` even though the
  ADR §3.1 says `Arc<ArcSwap<Graph>>`. The implementer documented the
  reason (in-place mutation cheaper than RCU for full-baseline writes)
  and made the trade-off explicit (scenario.rs lines 16-24). Not a bug.
- `wal.rs`'s magic header `b"WAL\0"` is read-validated on existing
  files (line 95-110). The check is correct; the panic-on-empty-file
  case is covered.
- `metrics.rs`'s hand-rolled exposition format (vs `prometheus-client`
  crate) is a deliberate trade-off, documented and reasonable.
- The "Architecture A vs B" co-existence (both shipped, both gated)
  is intentional and the dispatch in `events.py::_build_propagation_engine`
  cleanly handles all four flavors.
- `loader.rs:225-226` skipping edges whose source/target nodes aren't
  in the active baseline is correct — it tolerates concurrent
  ingestion + inactive nodes properly.
- The `MergeScenario` write lock burst (line 199-211) is intentional
  and the right pattern — minimal lock duration, all overlays applied
  atomically.

---

## Recommended next steps

**Before any production traffic** (BLOCK):
1. Fix the WAL/queue ordering and truncation bugs (F-001, F-002, F-003,
   F-014, F-019). Until these land, even at 1% traffic the documented
   durability contract is false.
2. Verify or fix `session_replication_role` privilege requirement
   (F-004). Pick a strategy and align RUNBOOK.
3. Reject `scenario_id != baseline` in `Propagate` (F-006) so the API
   contract is honest.
4. Add backpressure: cap WAL size and queue depth, surface as gRPC
   errors (F-005). Bound the blast radius of a PG outage.
5. Remove panics from hot path (F-010); switch to `panic = "unwind"`
   (F-023); add `catch_unwind` around the rayon job.

**Before shipping at 10% traffic** (HIGH):
6. Move WAL fsync off the tokio worker thread (F-008). Validate p95
   under saturation, not just under unloaded benchmarks.
7. Move the rayon parallel compute outside the write lock (F-009).
8. Cache the Postgres connection in the flusher (F-012).
9. Implement `DeleteScenario` (F-038) and scenario eviction (F-037).
10. Fix `EngineMetrics` gRPC return values (F-041) — operators will
    consume them.

**Before shipping at 100%** (MEDIUM):
11. Write real WAL fault-injection tests (F-043) and rerun
    `kill9_recovery.py` with PG-outage overlay.
12. Fix or honestly rename `parity_4way.py` (F-044).
13. Replace `ORDER BY random()` test patterns (F-045).
14. Land the proto cleanup pass (F-041, F-042, F-054, F-059).
15. Land WAL log rotation rather than in-place truncation (F-019).

**Cleanup / debt** (LOW + INFO):
16. Reconcile MSRV between Cargo.toml and Dockerfile (F-020).
17. Delete dead `[profile.release]` in ootils_kernel (F-053).
18. Remove or wire OTLP/TLS feature flags (F-062).
19. Document WAL/proto v1→v2 evolution policy (F-059).
20. Audit logging for DSN-with-password leaks (F-017).
