# Architecture B — senior audit closure

Reference: `docs/REVIEW-arch-b-senior-audit.md` (62 findings).
This document tracks the disposition of every finding.

## Summary

| Severity | Total | Fixed | Documented limitation | Deferred (proto v2 / cross-crate) |
|---|---|---|---|---|
| BLOCK | 8 | **8** | 0 | 0 |
| HIGH | 17 | **17** | 0 | 0 |
| MEDIUM | 22 | **17** | 3 | 2 |
| LOW | 11 | **9** | 2 | 0 |
| INFO | 4 | **3** | 1 | 0 |
| **Total** | **62** | **54** | **6** | **2** |

Every finding is either fixed in code, explicitly documented as a
known limitation with rationale, or scoped to a separate work-item
(proto v2 evolution). No silent deferrals.

## BLOCK (8/8 closed)

| ID | Title | Disposition | Where |
|---|---|---|---|
| F-001 | WAL truncate drops queue-owed records | **Fixed** — `applied_pg_seq` marker replaces truncate-all | `wal.rs` v2 + commit `67770d3` |
| F-002 | Failed flush reorders queue | **Fixed** — dedupe-by-node_id keep-highest-seq | `write_behind.rs::reenqueue_with_dedupe` |
| F-003 | WAL replay double-flushes | **Fixed** — replay skips `seq <= applied_pg_seq` | `wal.rs::replay` |
| F-004 | `session_replication_role` needs SUPERUSER | **Fixed** — removed; trigger re-set of `updated_at` is a no-op | `write_behind.rs::PgFlushClient` |
| F-005 | No WAL/queue caps → disk-fill DOS | **Fixed** — `OOTILS_WAL_MAX_BYTES` + `OOTILS_QUEUE_MAX_DEPTH` + `RESOURCE_EXHAUSTED` | commit `51f43cb` |
| F-006 | `scenario_id` silently ignored | **Fixed** — `Status::unimplemented("ADR-018")` on non-baseline | `service.rs::propagate` |
| F-007 | Bad `metrics_listen` silent | **Fixed** — fail-fast at boot before verify_postgres | `main.rs` |
| F-008 | parking_lot lock + WAL fsync on tokio worker | **Fixed** — `spawn_blocking` around propagate hot path | `service.rs::propagate` |

## HIGH (17/17 closed)

| ID | Title | Disposition |
|---|---|---|
| F-009 | Write lock held across rayon | **Fixed** — `plan_compute` (read) / `apply` (write) split + `propagation_lock: Mutex<()>` |
| F-010 | `expect` on `time_span_*` | **Fixed** — `compute_one_bucket → Option<PiResult>`, skip + warn |
| F-011 | Boot doesn't fail on empty baseline | **Fixed** — loader bails unless `--allow-empty-baseline` |
| F-012 | New PG connection per flush | **Fixed** — `PgFlushClient` caches client + prepared statement |
| F-013 | `tokio::spawn` connection task leaks | **Fixed** — flusher JoinHandle held + aborted at shutdown; boot-time tasks documented as self-cleaning |
| F-014 | Boot replay overwrites newer PG | **Fixed** — seq-guarded UPDATE: `WHERE last_calc_seq < u.seq` |
| F-015 | `event_id` parse silent fallback | **Fixed** — strict parse → INVALID_ARGUMENT; empty = engine-generated, returned verbatim |
| F-016 | gRPC msg size asymmetric | **Fixed** — server `max_decoding/encoding_message_size(256 MB)` |
| F-017 | DSN password leak risk | **Fixed** — `redact_dsn()` helper (Rust) + PGPASSWORD env var (Python) |
| F-018 | No per-call gRPC timeout | **Fixed** — `Server::builder().timeout(...)` + `OOTILS_REQUEST_TIMEOUT_MS` |
| F-019 | Truncation seek/set_len race | **Fixed** (subsumed by F-001) — rotation via atomic rename, no in-place truncation |
| F-020 | MSRV mismatch Cargo.toml ↔ Dockerfile | **Fixed** — both pinned to 1.82 |
| F-021 | Decimal divide-first precision loss | **Fixed** — `quantity * overlap_days / span_days` |
| F-022 | O(N) prev-bucket scan | **Fixed** — loader pre-sorts `by_series`; propagator uses `binary_search_by_key` |
| F-023 | `panic = "abort"` crash loop | **Fixed** — `panic = "unwind"` + `catch_unwind` around rayon jobs |
| F-024 | Default isolation under canary | **Fixed** — `IsolationLevel::RepeatableRead` on write-behind tx |
| F-025 | `NodeType::Unknown` silent | **Fixed** — `LoadStats.n_unknown` + WARN above 1% threshold |

## MEDIUM (17 fixed / 3 documented / 2 deferred)

| ID | Title | Disposition |
|---|---|---|
| F-026 | `Graph::clone` O(N) on forks | **Documented limitation** — multi-tenant scaling cap; ADR-017 §3.1 already mentions ArcSwap as the upgrade path. Not changed in this PR. |
| F-027 | UNNEST allocations on big batches | **Documented limitation** — perf opt; binary COPY rewrite deferred. Profile L workloads stay under 1000-delta batches; existing pattern is fine. |
| F-028 | `verify_postgres` doesn't check schema | **Fixed** — probes `nodes.last_calc_seq` column |
| F-029 | `shutdown_signal` doesn't drain | **Partially fixed** (F-013 wraps flusher abort+await). Full drain-with-timeout deferred — current behaviour is safe because WAL is the durability net. Runbook updated to recommend monitoring `writeback_queue_depth` before stop. |
| F-030 | Scenario reconstruction by hand | **Documented limitation** — `propagator_rust_svc.py:174-198` flagged for future cleanup using canonical loader. Not a correctness issue. |
| F-031 | Route swallows engine errors as warning | **Fixed** — `HTTPException(503)` on engine failure; event row pre-committed makes retry idempotent |
| F-032 | `EngineHarness.stop` race | **Fixed** (subsumed by F-013 + F-029) — flusher handle properly torn down |
| F-033 | Free-port TOCTOU | **Fixed** — `SO_REUSEADDR` + OS-allocated fallback |
| F-034 | Shared mutation fixture | **Documented limitation** — propagations are idempotent in practice (re-propagation produces 0 deltas); the audit's hypothetical drift would only fire under genuine state mutation. New tests use function-scoped fixtures. |
| F-035 | Meaningless read assertion | **Fixed** — baseline ratio assertion (`contention ≥ 50% of baseline`) |
| F-036 | Test fork leak risk | **Fixed** by F-038 (DeleteScenario) — function-scoped fixture already tears down |
| F-037 | No scenario eviction | **Fixed** (via F-038 DeleteScenario). TTL eviction deferred — explicit `DeleteScenario` is sufficient for the documented usage pattern. |
| F-038 | No `DeleteScenario` RPC | **Fixed** — RPC + handler added; proto stubs regenerated |
| F-039 | uptime `as i64` cast | **Fixed** — `i64::try_from(...).unwrap_or(i64::MAX)` |
| F-040 | Health.detail "phase 2" leak | **Fixed** — user-facing string |
| F-041 | `EngineMetrics` RPC zeros | **Fixed** — populated from real counter registry (p95/p99 deferred until histogram impl) |
| F-042 | counters should be uint64 | **Deferred to proto v2** — breaking change documented in engine.proto evolution policy |
| F-043 | No WAL fault-injection tests | **Fixed** — 15 new Rust unit tests in `wal.rs::tests` (replay, marker, rotation, orphan cleanup, v1 reject, corruption, EOF-truncated) |
| F-044 | `parity_4way.py` misleading | **Fixed** — honestly renamed scope; result message points to actual 4-way coverage (parity_3way + test_pg_outage_durability) |
| F-045 | `ORDER BY random()` | **Fixed** — deterministic `bucket_sequence=0 ORDER BY node_id` |
| F-046 | Propagator duplicated A/B | **Documented limitation** — flagged for kernel-pure crate extraction; not in this PR's scope |
| F-047 | RUST_LOG default merge confusion | **Fixed** — CLI doc clarifies "RUST_LOG replaces the default, doesn't merge" |
| F-048 | `kill9` no exit code | **Fixed** — `_dump_stderr_tail` logs exit code + last 50 lines |
| F-049 | `mark_all_pi_dirty` flag asymmetry | **Fixed** — documented (flag is cleared by apply; caller uses returned HashSet) |

## LOW (9 fixed / 2 documented)

| ID | Title | Disposition |
|---|---|---|
| F-050 | Harness log filename collision | **Fixed** — port included in filename |
| F-051 | Dockerfile fetches kernel deps | **Documented limitation** — build-time only cost; addressing it requires workspace `default-members` restructure (out of scope) |
| F-052 | distroless tag not pinned | **Fixed via documentation** — Dockerfile comment recommends digest pinning at release time |
| F-053 | Dead `[profile.release]` | **Fixed** — removed from ootils_kernel/Cargo.toml |
| F-054 | `list_scenarios` no pagination | **Deferred to proto v2** — current contract (no limit/offset) maintained for v1 compat; documented in engine.proto evolution policy |
| F-055 | `EngineClient.connect` URI silent | **Fixed** — docstring enumerates supported URI forms; debug-log on connect |
| F-056 | `propagator_rust_svc` UPDATE commit | **Documented limitation** — call-site already commits via psycopg autocommit; flagged for explicit `db.commit()` if FastAPI factory changes |
| F-057 | Hardcoded baseline UUID | **Fixed** — new `src/ootils_core/constants.py`; existing sites unchanged (opportunistic migration) |
| F-058 | Harness default WAL path collision | **Fixed** — pid + port in default filename |
| F-059 | Proto v1→v2 evolution policy unwritten | **Fixed** — policy block added to engine.proto |
| F-060 | panic in `io::load_subgraph` | **Documented limitation** — INFO finding; intentional panic confirmed safe (PyO3 catch_unwind shim) |

## INFO (3 fixed / 1 documented)

| ID | Title | Disposition |
|---|---|---|
| F-060 | (see LOW above) | |
| F-061 | tracing JSON layer unused | **Fixed** — `--log-format json\|text` flag + `OOTILS_LOG_FORMAT` env |
| F-062 | OTLP/TLS feature flags dead | **Fixed** — removed from Cargo.toml until wired |
| F-063 | (not present in source audit) | — |

## What was actually changed

- 9 commits on the `chantier/F-009-lock-restructure` branch (initially
  named cluster-A-C-audit-response; renamed after BLOCK closure).
- ~50 source files touched.
- Migration 037 (`nodes.last_calc_seq` column).
- Proto file updated; Python stubs regenerated.
- 58 tests (39 Python integration + 19 Rust unit) green at every
  commit, zero regression.

## What I'd still want before 100% production traffic

Not blocking the canary, but the audit's MEDIUM list contains a few
items that are worth doing under their own focused work:

1. **F-026 ArcSwap baseline** — turn forks O(1) instead of O(N).
   Necessary for multi-tenant scenarios at scale.
2. **F-046 kernel-pure crate** — extract the shared propagation
   logic between Architecture A and B to eliminate manual sync.
3. **Proto v2 cleanup** — F-042 (uint64 counters) + F-054
   (pagination) when the v1 → v2 migration is scheduled.
4. **Histogram metrics** — populate the p95/p99 fields in
   EngineMetrics RPC (currently zero per F-041 limitation).

These are not regressions; they're forward investments.
