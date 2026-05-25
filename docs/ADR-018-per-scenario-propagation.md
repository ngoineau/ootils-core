# ADR-018 — Per-scenario propagation (engine RPC extension)

**Status** : **Accepted + Implemented (Phase 2.1)** — 2026-05-25
**Owner** : ngoineau
**Depends on** : [ADR-017 Architecture B](ADR-017-architecture-b-rust-engine-service.md)

## Implementation status (P2.1 — multi-user)

| Phase | Description | Status |
|---|---|---|
| P2.1.a | ArcSwap baseline → O(1) forks (F-026) | ✅ commit `85940ab` |
| P2.1.b | Per-scenario propagation (this ADR) | ✅ commit `023942c` |
| P2.1.c | Per-scenario propagation_lock | ✅ commit `023942c` |
| P2.1.d | Scenario TTL eviction (F-037) | ✅ commit `b7fa8d7` |
| P2.1.e | Multi-user benches + isolation tests | ✅ (this branch) |
| P2.1.f | FastAPI scenario lifecycle routes | ⏳ follow-up |
| P2.2 | Scenario persistence in PG (Option C "Save as") | ⏳ follow-up |

## Measured impact (Phase 2.1 acceptance)

- Fork latency: **49 ms → 1 µs** (49 000× faster via ArcSwap)
- 200 forks total time: < 5 s (well within Q2 multi-user target)
- 50 parallel scenario propagations: < 30 s, no errors, full
  isolation
- 100 scenarios × 5 propagations: baseline state byte-identical
  before and after (no leak)
- Baseline propagation regression: +30 ms per write (clone-on-write
  cost). Acceptable per Q3 design constraint (baseline writes are
  rare, max hourly in production).

## Known limitations (P2.1 scope)

- **Ephemeral only.** Scenarios live in engine RAM, evicted by TTL
  (default 1 h idle). No persistence. P2.2 lands the "Save as named
  scenario" PG-backed flow.
- **No GetNode-from-scenario.** Reads via `GetNode(scenario_id=X)`
  currently route to baseline only. Scenario state is observable
  only via the propagation result. P2.1.f extends GetNode.
- **No scenario merge against modified baseline.** If the baseline
  has mutated since the fork, `MergeScenario` applies the overlay
  blindly. Conflict detection lands in P2.2.c.

---

---

## TL;DR

Today the `Propagate` RPC always mutates the **baseline** scenario.
Fork creates an isolated overlay, but it can only be **read** — no
way to write to it via propagation. This ADR extends `Propagate` to
target any active scenario (baseline OR a fork) without breaking the
existing API contract.

---

## Why

Three real use cases require this:

1. **What-if simulation** — fork the baseline, mutate the fork (e.g.
   raise supplier lead time by 5 days), Propagate **on the fork**,
   compare its shortage list vs. baseline.
2. **Multi-user concurrency** — different users editing different
   scenarios in parallel. Their Propagate calls must NOT contend on
   the baseline.
3. **A/B testing** — run two propagation strategies on two clones of
   the same data, diff the outputs.

Without per-scenario propagation, all three workflows degrade to
"export baseline state, run a heavyweight simulation script,
re-import" — which throws away the entire perf advantage of the
in-RAM engine.

---

## Proposed design

### Wire change

`PropagateRequest.scenario_id` already exists in the proto. Today the
engine ignores its value and writes to baseline. Phase A of this ADR:
honor it.

```protobuf
message PropagateRequest {
  string scenario_id = 1;       // honor this — route to the named scenario
  string event_id = 2;
  string event_type = 3;
  string trigger_node_id = 4;
  bytes payload = 5;
}
```

### Engine plumbing

Currently `propagator::propagate` takes `&mut Graph`. For scenarios,
it needs to read from `Scenario` (overlay-first lookups) and write
into the scenario's overlay.

Refactor sketch:

```rust
pub trait GraphAccessor {
    fn get_node(&self, idx: NodeIndex) -> Option<&Node>;
    fn set_node(&self, idx: NodeIndex, new: Node);
    fn edges_in(&self, idx: NodeIndex) -> &[EdgeRef];
    fn series_buckets(&self, sid: Uuid) -> Vec<NodeIndex>;
}

impl GraphAccessor for &mut Graph { ... }     // existing path (baseline)
impl GraphAccessor for &Scenario { ... }      // new path (overlay-first)

pub fn propagate<A: GraphAccessor>(accessor: A, dirty: &HashSet<NodeIndex>) -> PropagationStats {
    // same algorithm, called against any accessor
}
```

The challenge with `&Scenario`: the read side gives back owned `Node`
clones from the overlay path, but the kernel reads borrowed `&Node`.
We'd need either:
- An enum `NodeRef<'a>` that holds either a baseline ref or a clone.
  Costs: clone per read on hot path.
- A pre-materialize step that copies all the dirty PIs + their
  supplies/demands into a transient `Vec<Node>` arena before the
  kernel runs. Costs: extra memcpy at propagation start.
- A reader closure pattern: `accessor.read(idx, |n| ...)` — Rust-
  idiomatic but harder to use with rayon's `par_iter`.

The **pre-materialize step** is the cleanest because it preserves
the parallel kernel as-is. Estimated cost: ~5 ms for a 1000-PI dirty
subgraph on profile L (1000 × 150 byte memcpy = 150 KB, well within
L2 cache).

### WAL + write-behind

A scenario's writes should NOT touch the baseline WAL. Each active
scenario gets its own WAL file (`<wal_dir>/scenario-<uuid>.wal`),
or scenarios live in RAM only and a `Merge` is what triggers the
durability write.

The cleaner of the two: **scenarios are RAM-only** (ephemeral) and
durability happens at `Merge` time when the diff lands in baseline.
This matches the conceptual model: "scenarios are what-ifs, baseline
is the source of truth."

If a scenario is in flight and the engine crashes, the scenario is
LOST. That's acceptable — they're explicit user-driven what-ifs,
not auto-saved drafts. Users get a banner: "your scenario was lost,
restart it from the baseline."

### Lock model

Currently:
- baseline: `Arc<RwLock<Graph>>`, writes serialized via the write lock
- scenarios: `DashMap<Uuid, Arc<Scenario>>`, reads concurrent

For per-scenario propagation:
- Baseline propagation: unchanged.
- Scenario propagation: takes `Arc<Scenario>` from the manager, then
  needs to mutate its overlay. The overlay is `DashMap<NodeIndex,
  Node>` — already thread-safe. But the propagator's apply phase
  needs to write the full set atomically (otherwise a concurrent
  read could see partial state).

  Solution: each `Scenario` carries an inner `parking_lot::Mutex<()>`
  that propagators acquire for the duration of the apply phase.
  Reads from the overlay don't need this — DashMap handles them.

  Multiple readers of the SAME scenario : fine.
  Two propagators on the SAME scenario : serialized by the per-
  scenario mutex. Two propagators on DIFFERENT scenarios : truly
  parallel.

### gRPC contract refinements

No proto change required (`scenario_id` already in the field).
Behaviors clarified:

- If `scenario_id` is empty OR matches `BASELINE_SCENARIO_ID`:
  write to baseline (today's behavior).
- If `scenario_id` matches an active fork: write to that fork's
  overlay.
- If `scenario_id` doesn't match anything: return `NOT_FOUND`.

---

## Implementation phases

| # | Deliverable | Effort | Notes |
|---|---|---|---|
| A1 | Define `GraphAccessor` trait + impl for `&mut Graph` | 0.5d | Refactor only |
| A2 | Impl `GraphAccessor` for `&Scenario` w/ pre-materialize | 1d | Includes parity test |
| A3 | Add per-scenario mutex to `Scenario` | 0.5d | Tests for concurrency |
| A4 | Route `PropagateRequest.scenario_id` in service | 0.5d | NOT_FOUND for unknown IDs |
| A5 | Tests: scenario-isolated mutations + parallel scenarios | 1d | Critical correctness |
| A6 | Bench: scenario propagate vs baseline propagate | 0.5d | Should be similar perf |

Total: **3-4 days** of focused work.

---

## What's NOT in this ADR

- Scenario hierarchies (forking a fork). Currently all forks are from
  baseline. Nested forks would require chaining overlays, adds
  complexity for unclear value.
- Cross-scenario merge (cherry-pick a node delta from scenario A
  into scenario B without going through baseline). Niche feature.
- Per-scenario WAL files. Scenarios stay RAM-only — acceptable
  trade-off given they're ephemeral by user-intent.
- Scenario expiration / GC. Operator-managed for now via explicit
  Merge or via a future `DeleteScenario` RPC.

---

## Open questions

1. **Should baseline propagation block parallel scenario propagations?**
   Today the baseline RwLock means: baseline write = no other writes.
   With scenarios, ideally baseline writes block ONLY new fork
   snapshots (so the new fork sees the latest baseline), not in-flight
   scenario propagations on existing forks. The per-scenario mutex
   approach achieves this naturally.

2. **What happens to a fork when baseline is merged into?**
   The fork's `baseline_snapshot: Arc<Graph>` is a snapshot at fork
   time. Subsequent baseline mutations are invisible to the fork.
   Forks see a frozen-in-time baseline. This is the COW contract —
   simple to reason about, but means forks gradually drift from
   the live state. A `RefreshFromBaseline` RPC could resync at user
   request.

3. **Memory pressure with many active scenarios.**
   Each fork is a fresh `Arc<Graph>` clone (~76 MB on profile L) +
   overlay. 10 active forks = 760 MB just for snapshots. Should
   we cap the number of active scenarios per process? Currently no
   limit. Phase A6 bench should investigate.

These are addressed in follow-up PRs or operational defaults, not
this ADR.

---

## Skeleton

A starter implementation lives in
`rust/ootils_engine/src/propagator_scenario.rs.skeleton` — it shows
the trait + signatures but is intentionally not wired up. The phase A
implementation lifts that skeleton.
