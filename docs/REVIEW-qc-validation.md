# Ootils Supply Chain Planning Engine — QC Validation Review

**Document type:** QC / Architecture Audit  
**Date:** 2026-04-04  
**Reviewer:** Senior QC Architect (AI)  
**Scope:** Full architecture as documented in ADR-001 through ADR-005, EXPERT-dirty-flags-and-scenarios.md, PROPOSAL-engine-execution-model.md, PROOF-OF-ARCHITECTURE-V1.md, and the SQL schema in migration 001  
**Status:** ⚠️ BLOCKERs present — do not proceed to PoA execution without resolution of items flagged BLOCKER

---

## Executive Summary

The Ootils architecture is technically ambitious and intellectually coherent. The core ideas — incremental propagation with topological ordering, delta overlays for scenarios, node-type-driven temporal policies, and inline causal explanations — are sound. At PoA scale (100 SKUs × 5 locations × 5–6 scenarios), the system should work.

However, this review identifies **4 BLOCKERs** that represent cases where the architecture as currently documented contains gaps that could corrupt data, make recovery impossible, or produce silently wrong results in ways that would not be caught by the existing acceptance criteria. These must be resolved before PoA validation begins.

Beyond blockers, there are **7 RISKs** that are acceptable for PoA but will cause production failures if not addressed before V1 goes live. There are **9 DEBT items** that are deferred safely but should be tracked.

The most dangerous category of issues is **temporal model ambiguity**: the zone-transition job design has at least two failure modes that are not recoverable without manual intervention, and the stale detection mechanism has a logic gap that could silently present wrong scenario results to planners.

---

## Section 1 — Data Integrity

### 1.1 Dirty-nodes table vs. is_dirty cache sync

**Severity: RISK**

The architecture designates `dirty_nodes` as authoritative and `nodes.is_dirty` as a cache. The clearing sequence described in EXPERT-dirty-flags-and-scenarios.md is:

1. Recompute node N
2. Write computed attributes to `nodes`
3. Delete from `dirty_nodes WHERE calc_run_id = X AND node_id = N AND scenario_id = S`
4. Mark downstream dirty in `dirty_nodes`
5. After all dirty_nodes processed: clear `nodes.is_dirty` in a batch UPDATE

Steps 3 and 5 are not in the same transaction. If the process crashes between step 3 (dirty_nodes row deleted) and step 5 (is_dirty cleared), `nodes.is_dirty` remains `TRUE` while `dirty_nodes` has no row. At recovery, the crash-recovery scan queries `calc_runs WHERE status IN ('pending', 'running')`. It correctly re-derives dirty nodes from `dirty_nodes`. But the engine also reads `nodes.is_dirty` in the hot propagation path (index `idx_nodes_dirty` on `WHERE is_dirty = TRUE`). If the engine uses `nodes.is_dirty` as the dirty queue rather than `dirty_nodes`, it will recompute already-computed nodes from the stale cache. This produces double computation at best; if the node now has a different value due to concurrent events, it silently overwrites correct results.

**Exact failure condition:** Crash between step 3 (dirty_nodes row deleted) and step 5 (nodes.is_dirty cleared). Recovery resets calc_run to `pending`, re-derives dirty nodes from `dirty_nodes` (which is now empty for this node), but finds `nodes.is_dirty = TRUE` and re-queues the node unnecessarily — or worse, skips the node because dirty_nodes says clean.

**Resolution:** Decide at design time whether the hot propagation loop reads from `dirty_nodes` or from `nodes.is_dirty`. The two must never be used interchangeably in different parts of the code. Recommended: the propagation loop reads exclusively from the in-memory Python set (flushed from `dirty_nodes`); `nodes.is_dirty` is updated only as a side effect, never read by the engine. Document this explicitly and enforce in code review.

---

### 1.2 Dirty_nodes row deletion atomicity

**Severity: RISK**

"Clear the `dirty_nodes` entry for a node immediately after successful recompute" — the EXPERT document assumes the DELETE from `dirty_nodes` and the UPDATE to `nodes` happen atomically. The schema does not enforce this. In SQLite (the current storage), if the Python process crashes after writing the new computed attributes to `nodes` but before deleting from `dirty_nodes`, the node has a fresh computed value AND remains in `dirty_nodes`. On recovery, the engine will recompute it again. This is safe (the result will be the same — assuming idempotency), but only if the engine guarantees idempotency of recomputation. The architecture does not explicitly guarantee this. If the computed value depends on wall-clock time or an external state that has changed between the crash and recovery, the result will differ.

**Resolution:** Wrap the `nodes` attribute update and the `dirty_nodes` DELETE in a single SQLite transaction, always. Make idempotency an explicit engine invariant and test it.

---

### 1.3 Concurrent calc_runs on the same (node_id, scenario_id)

**Severity: BLOCKER**

The `dirty_nodes` table PK is `(calc_run_id, node_id, scenario_id)`. This means two different `calc_run_id`s can simultaneously hold dirty entries for the same `(node_id, scenario_id)`. The architecture does not address what happens when:

1. Event E1 arrives → creates calc_run R1 → marks node N dirty
2. Before R1 completes, Event E2 arrives → creates calc_run R2 → marks node N dirty again
3. R1 recomputes N, deletes its `dirty_nodes` row, writes new value
4. R2 recomputes N again — but now reads the state left by R1, which may already be final

At PoA scale with a single-threaded engine, this cannot happen in steady state. But the design explicitly says crash recovery replays from `triggered_by_event_id` and resumes from surviving dirty_nodes. If a crash leaves both R1 and R2 in `pending` state, recovery resumes both, and they race over the same nodes.

**More concretely:** The architecture describes the propagation as single-threaded but does not serialize calc_run creation. If the API accepts two events simultaneously (before the engine processes the first), two calc_runs are created. The architecture does not specify how the engine handles a queue of pending calc_runs — does it process them sequentially? Merge them? The answer matters enormously for correctness.

**Resolution required before PoA:**
- Explicitly document whether the engine serializes calc_runs (one at a time, queue-based) or merges them (collect all pending dirty nodes, run one combined pass).
- If serialized: add a constraint or advisory lock that prevents two calc_runs in `running` state for the same scenario simultaneously.
- If merged: change the dirty marking model so that all pending events for a scenario are collapsed into a single calc_run before execution starts.

---

### 1.4 is_dirty cache inconsistency guarantee

**Severity: DEBT**

The architecture states `nodes.is_dirty` is a cache, not authoritative. But the index `idx_nodes_dirty ON nodes (scenario_id, is_dirty) WHERE is_dirty = TRUE` is used for dirty-flag flush queries (per ADR-005). If the cache is stale (is_dirty = TRUE when dirty_nodes has no row), that query returns false positives. If is_dirty = FALSE when dirty_nodes has a row (possible after a non-transactional update), the query misses nodes. No reconciliation mechanism is defined.

**Resolution:** Explicitly prohibit the engine from reading `nodes.is_dirty` for propagation decisions. Make the index comment-documented as "for operator inspection only, not for engine reads."

---

## Section 2 — Crash Recovery

### 2.1 Mid-propagation crash recovery

**Severity: RISK**

The recovery procedure is: reset `running` calc_runs to `failed` then to `pending`, then resume from surviving `dirty_nodes` entries. The architecture says this is safe because "surviving entries after a crash = exactly the nodes that need recomputation."

This is correct *only if* dirty_nodes deletions and node attribute writes are atomic (see 1.2). If they are not, the surviving dirty_nodes entries may include nodes whose computed value is already correct. Recomputing them is safe only if the computation is deterministic and no external state has changed. If a new event arrived between crash and recovery (changing the state of an upstream node), the recovered computation will be correct because it re-reads current upstream state. This is actually fine — but it needs to be stated as an explicit invariant, not assumed.

**Resolution:** Document the invariant explicitly: "Recomputing a node that was already correctly computed is safe; the result will be identical to the first computation if no upstream state has changed, and will reflect new upstream state if it has." Write a test that verifies this.

---

### 2.2 Zone-transition job crash recovery

**Severity: BLOCKER**

The zone-transition jobs (weekly→daily on Monday 02:00 UTC; monthly→weekly on 1st of month 02:00 UTC) perform structural mutations: they replace one bucket node with N new nodes (7 for weekly→daily, 4–5 for monthly→weekly). The architecture says these jobs are idempotent ("safe to re-run if the job was missed").

**The idempotency claim is not implemented anywhere in the documents.** What does "safe to re-run" actually mean for this operation? If the job crashes after deleting the weekly bucket node but before creating the 7 daily nodes, the projection series has a gap: no node covers that week. Rerunning the job detects that the weekly bucket is gone (already deleted) and that no daily nodes exist for that week, then creates the 7 daily nodes — this appears safe. But:

1. If the weekly bucket deletion and daily node creation are not in the same transaction, a crash mid-creation leaves a partial gap (3 of 7 daily nodes created).
2. On re-run, the engine sees 3 existing daily nodes (and does not delete them, because they appear valid) and creates 4 more, ending up with 7. Safe only if the re-run detects and skips existing nodes by date. The documents do not describe this deduplication logic.
3. If edges (replenishes, consumes) pointing to the deleted weekly bucket node are not updated to point to the new daily nodes, the propagation graph is broken. Edge updates are not mentioned in the job description.
4. The `dirty_nodes` entries for the new daily nodes must be created and associated with a calc_run. If the job crashes between node creation and dirty_node insertion, the new nodes exist but are never computed.

**Resolution required before PoA:** The zone-transition job must be specified as a fully transactional unit: one DB transaction per (item, location) pair that atomically deletes the old bucket, creates new nodes, updates edges, inserts dirty_nodes entries. If partial failures are possible at the batch level (e.g., the job processes 250 of 500 pairs before crashing), the job must track its own progress in a dedicated table and resume from the last successfully committed pair.

---

### 2.3 Scenario_merge partial processing

**Severity: RISK**

The scenario_merge event is described as: for each node in S1 with an override, apply that override to the baseline node. This is a potentially large write (if S1 has 100 overrides, 100 baseline nodes must be updated). The document says this is "processed by normal propagation engine" via an event, implying it is handled in a single calc_run.

If the engine crashes after applying 50 of 100 overrides:
- The baseline is now in a hybrid state: some nodes have S1's values, others have pre-merge values.
- The calc_run is in `running` or `failed` state.
- Recovery resets it to `pending` and resumes from dirty_nodes.
- But which dirty_nodes survive? If the override writes and dirty_nodes inserts were done node-by-node (non-transactionally), surviving dirty_nodes entries reflect only the 50 nodes not yet processed. The 50 already-written baseline overrides are not dirty — the engine will not reprocess them. **The baseline is permanently corrupted.**

**Resolution:** The scenario_merge must be implemented as: (a) write all overrides in one atomic transaction before any baseline node is modified, or (b) keep a `scenario_merge_operations` log of all intended writes, check completion on recovery, and reapply incomplete writes. Option (a) is simpler and correct.

---

## Section 3 — Temporal Model Consistency

### 3.1 PO due on Sunday, weekly→daily transition on Monday

**Severity: RISK**

A PO with `due_date = 2026-05-03` (Sunday) is in the weekly zone and contributes to the weekly bucket `[2026-04-27, 2026-05-03]` (Mon–Sun). On Monday 2026-05-04, the weekly→daily transition runs. That bucket should be replaced with 7 daily buckets: Mon 04/27 through Sun 05/03. The PO with due_date = 05/03 should now contribute to the daily bucket `[2026-05-03, 2026-05-03]`.

**The risk:** If the transition job creates daily buckets with `time_span_start` as Monday through the following Sunday inclusive (i.e., `[2026-05-04, 2026-05-04]` through `[2026-05-10, 2026-05-10]`), the PO falls in the preceding week's bucket — but that bucket has already been transitioned and archived. The PO's contribution may be lost or assigned to the wrong bucket.

**The root cause:** The architecture does not specify whether the weekly→daily transition creates buckets for the *incoming* week (the week that just crossed into the daily zone) or the *outgoing* week (the week that just became past). This distinction matters when the PO date is at the end of the transitioning week.

**Resolution:** Explicitly document and test the bucket boundary logic: which calendar week does the Monday transition job act on? Write a deterministic test case with a PO due on a Sunday at the zone boundary and verify bucket assignment is correct.

---

### 3.2 Shortages spanning zone transition boundaries

**Severity: RISK**

A shortage exists from 2026-06-22 (Sunday, end of weekly zone at 90d) through 2026-06-29 (beginning of monthly zone). This shortage spans a zone boundary. In the daily zone, it is represented by daily Shortage nodes. In the monthly zone, it might be a monthly Shortage node that happens to start at the zone boundary. When the Monday transition runs and daily buckets approach this boundary, the shortage representation changes. The original shortage node may be archived (tied to the weekly bucket), and new shortage nodes are created for the daily buckets. The shortage's stable identity (`shortage-{item}-{loc}-{date}-{scenario}`) must survive this restructuring.

**What the architecture does not specify:** how do `impacts` edges (from Shortage nodes to CustomerOrderDemand nodes) survive zone transitions? If the Shortage node is deleted and recreated, all `impacts` edges to it are broken. CustomerOrderDemand nodes lose their causal reference.

**Resolution:** Define explicitly whether Shortage nodes are recreated or updated during zone transitions. If recreated, specify how `impacts` edges are migrated to the new nodes.

---

### 3.3 Bucket time_span_start before PLANNING_START

**Severity: RISK**

The architecture states: "The engine never re-plans past. No PI node exists with time_span_start < PLANNING_START." But it also says: "PLANNING_START = max(today, as_of_date)." The zone-transition job creates new daily buckets for the week that is now entering the daily zone. If the job runs at 02:00 UTC on Monday and the weekly bucket being split started on the preceding Monday (7 days ago), the split must create 7 daily buckets, but 7 days ago is in the past. Does the engine create `[2026-05-03, 2026-05-03]` (a past date) as a daily bucket? If PLANNING_START is recalculated on every job run as `today`, the first daily bucket of the transitioning week will often be a past date.

**Guard identified:** The PAST principle should prevent this. But the architecture does not specify the exact guard: is it enforced at the zone-transition job level (skip creating buckets before PLANNING_START) or at the engine level (refuse to compute nodes with time_span_start < PLANNING_START)? If it's only at the engine level, the node is created but never computed — a dangling node with no computed value.

**Resolution:** The zone-transition job must explicitly set the start of the first daily bucket to `max(PLANNING_START, original_weekly_bucket_start)`. Enforce this in code and test with a transition that runs mid-week.

---

### 3.4 Horizon Extension missed for 2 months

**Severity: RISK**

The monthly Horizon Extension job is append-only and idempotent. If missed for 2 months, the horizon ends at today + 16 months instead of today + 18 months. No structural damage. But:

1. Any demand data (CustomerOrderDemand, ForecastDemand) that falls beyond today + 16 months has no corresponding PI bucket. The contribution rule (`point_in_bucket`) maps source nodes to PI buckets. If no bucket exists, the demand's contribution is silently dropped — no shortage is detected for that period.
2. AI agents querying `/projection` for the full 18-month horizon will receive data through +16 months and see no data beyond that — potentially interpreted as zero projected inventory, which is neither correct nor flagged as missing.

**Resolution:** The engine should raise an alert (log warning + optional API status flag) when queried for a time range beyond the current horizon end. Implement `GET /horizon/status` that returns `{horizon_end, is_current, days_behind}`.

---

## Section 4 — Scenario Consistency

### 4.1 Baseline recompute while variant is in computing state

**Severity: BLOCKER**

Scenario lifecycle: `draft → computing → computed → stale → approved → archived`. The EXPERT document says variants should be marked `stale` when baseline recomputes with changed values. But the lifecycle check (Q4.7) runs "as a post-processing step after each baseline calc_run, before marking calc_runs.status = 'completed'."

What happens when:
1. Variant V1 is in `computing` state (its calc_run is `running`)
2. A new event triggers a baseline recompute
3. Baseline calc_run completes
4. Post-processing checks variants: V1 is `computing`, not `computed` — no stale check runs
5. V1's calc_run completes, sets V1 to `computed`
6. V1 is now `computed` but was computed against a baseline that was superseded during V1's own computation

**The result:** V1 shows results computed against the *old* baseline, but its status is `computed` — not `stale`. There is no subsequent trigger to re-evaluate V1's staleness. The planner sees `computed` and trusts the results. They are wrong.

**Exact mechanism of silent corruption:** The check for stale variants only runs at baseline calc_run completion. It misses variants that transition from `computing` to `computed` *after* the stale check ran.

**Resolution required before PoA:** Add a post-completion hook on variant calc_runs: when a variant transitions from `running` to `complete`, check whether the baseline has been recomputed since the variant's calc_run started (compare variant's `started_at` against baseline's most recent `completed_at`). If baseline was recomputed during variant computation, immediately transition the variant to `stale` rather than `computed`.

---

### 4.2 Variant computed against superseded baseline

**Severity: RISK**

The `scenarios.baseline_snapshot_id` mechanism is designed to detect this. But the EXPERT document's stale detection query (Q4.7 pseudoquery) compares node values from the current baseline against values from `baseline_snapshot_id`. This comparison requires accessing historical node values — but the schema does not store historical node values. `nodes` stores only the current state. Once a baseline recompute overwrites a node's attributes, the old value from `baseline_snapshot_id` is gone.

**The stale detection query cannot work as documented.** It compares `n_old` (from `baseline_snapshot_id` calc_run) against `n_new` (from the new baseline), but `n_old` is not retrievable because node attribute writes are destructive (no history table, no event sourcing of node attributes).

**Resolution:** Either (a) add a `node_history` table that retains the node attribute snapshot at each calc_run completion (expensive, but correct), or (b) change the stale detection mechanism to compare `scenarios.baseline_snapshot_id` against the current baseline's latest `calc_run_id` — if they differ, mark variant as `stale` unconditionally without comparing individual node values. Option (b) is conservative (marks stale even when no relevant node changed) but safe and implementable. Option (a) is precise but adds significant storage cost.

---

### 4.3 Scenario bleed prevention

**Severity: RISK**

The architecture states scenario isolation is enforced by namespacing all writes by `scenario_id`. The scenario resolution pattern uses `COALESCE(scenario_override, baseline_node)`. But the engine also writes new result nodes (ProjectedInventory, Shortage, pegged_to edges) during propagation. Are these writes correctly scoped to the variant scenario_id, or could a bug incorrectly write to `scenario_id = 'baseline'`?

The schema constraint: `edges.scenario_id` and `nodes.scenario_id` are both set at insert time. There is no DB-level constraint preventing a variant scenario's propagation from writing a node with `scenario_id = 'baseline'`. This must be a code-level invariant.

**The risk:** a code bug in the propagation engine (e.g., passing the wrong `scenario_id` argument when creating result nodes) would silently write variant results into the baseline, corrupting it irreversibly (since the overwrite is destructive and events are not node-attribute-level — you can't replay to recover the original baseline value).

**Resolution:** Add a DB trigger or application-level guard: when the engine creates or updates nodes during a variant calc_run, assert `node.scenario_id == calc_run.scenario_id`. Raise an exception if they mismatch. This is a defensive programming requirement, not just documentation.

---

### 4.4 Branch depth enforcement mechanism

**Severity: DEBT**

The architecture says "enforce at API level" with a `depth` column on scenarios (from EXPERT document: `ALTER TABLE scenarios ADD COLUMN depth INT NOT NULL DEFAULT 0`). But this column does not appear in the current SQL schema (`001_initial_schema.sql`). The scenarios table has `parent_scenario_id` but no `depth` column.

**The API enforcement** would need to traverse `parent_scenario_id` to count depth — O(depth) queries. For depth 2 this is trivial, but the enforcement is not automatic: it requires every scenario creation endpoint to explicitly check depth before inserting. There is no DB-level constraint preventing depth-3 creation.

**Resolution:** Add a CHECK constraint or trigger at the DB level: `CREATE TRIGGER prevent_deep_branch BEFORE INSERT ON scenarios...` that rejects inserts where depth > 2 (computed by counting parent chain). Alternatively, denormalize depth into the `depth` column and enforce `CHECK (depth <= 2)` in the schema.

---

## Section 5 — Testability

### 5.1 Dirty-flag system deterministic testability

**Rating: Adequate with conditions**

The dirty flag system is deterministically testable because:
- The in-memory Python set is initialized from `dirty_nodes` at run start
- Topological sort is deterministic given a fixed graph structure
- The delta check termination condition is deterministic given fixed node values

**Test recipe:**
1. Seed a graph with known topology
2. Mark specific nodes dirty via direct DB insert to `dirty_nodes`
3. Run the propagation engine
4. Assert exactly which nodes were recomputed (via `calc_runs.nodes_recalculated`) and which were skipped (via `nodes_unchanged`)
5. Assert the final node attribute values match expected values

**Condition:** The engine must expose a propagation trace (log of which nodes were processed, in which order, and whether they changed). Without this trace, it is impossible to verify that propagation stopped at the right nodes. The current schema tracks only aggregate counts (`nodes_recalculated`, `nodes_unchanged`) — insufficient for correctness testing.

**Resolution:** Add a `calc_run_node_log (calc_run_id, node_id, sequence_order, was_changed, computation_ms)` table, or at minimum, write a per-node log entry at DEBUG level that tests can capture.

---

### 5.2 Zone-transition roll testability

**Rating: Requires design work**

The zone-transition jobs are parameterized by `planning_date` in the function signatures (`def weekly_to_daily_transition(planning_date: date, db: Session)`). This is correct and enables deterministic testing without waiting for real calendar events.

**Test recipe:**
1. Initialize a graph with known weekly PI buckets at the zone boundary
2. Call `weekly_to_daily_transition(planning_date=<next_monday>, db=db)` directly in a test
3. Assert that weekly buckets within the transition range are replaced by 7 daily buckets each
4. Assert that edges previously pointing to weekly buckets now point to the corresponding daily buckets
5. Assert that dirty_nodes entries exist for all new daily nodes

**Gap:** The test recipe requires knowing the expected edge-update behavior, which is not yet specified in the architecture (see 3.2 above). Until edge migration during zone transitions is documented, the test cannot be written definitively.

**Testability score:** 7/10. The parameterized function design is correct. The missing specification of edge behavior during transitions is the blocking gap.

---

### 5.3 Scenario stale detection testability

**Rating: Currently untestable — depends on resolution of 4.2**

As documented in Section 4.2, the stale detection mechanism relies on comparing node values from `baseline_snapshot_id` against current node values. Since node history is not persisted, this comparison cannot be made and the mechanism cannot be implemented or tested.

If the stale detection is changed to the conservative approach (stale if `baseline_snapshot_id != current baseline calc_run_id`), then:

**Test recipe:**
1. Create variant V1 with `baseline_snapshot_id = R_old`
2. Run a new baseline calc_run R_new
3. Assert V1's status is now `stale` (transition happened in post-processing)
4. Assert V1's `baseline_snapshot_id` is updated to R_new after recomputation

**Testability score post-fix:** 9/10. Clean and deterministic.

---

## Section 6 — Operational Risk

### 6.1 Maximum propagation latency at PoA scale

**Scenario:** Single PO date change affecting 1 (item, location) pair in baseline scenario.

**Nodes affected:** ~115 PI nodes per (item, location) × 1 pair = 115 nodes, plus downstream Shortage nodes (~10–30). Total: ~150 dirty nodes.

**Estimated steps:**
- Event insert + dirty_nodes insert: 1–2ms
- calc_run creation: <1ms  
- In-memory topo sort (150 nodes, static DAG): <1ms
- 150 node recomputations (Python, SQLite reads/writes): ~50–200ms (0.3–1.3ms/node assuming 2–3 DB reads + 1 write per node)
- Explanation generation for new Shortage nodes: ~5–20ms
- `nodes.is_dirty` batch clear: ~5ms

**Estimated total:** 100–250ms for a single (item, location) pair change. Well within the 5-second SLA.

**Scenario for worst-case:** Event that affects all 500 (item, location) pairs (e.g., `policy_changed` on a global safety stock policy). ~57,500 dirty nodes. At 1ms/node, ~60 seconds — 12× above the 5-second SLA.

**Resolution:** Policy-change propagation must be specifically scoped. A global policy change should not trigger a single calc_run of 57,500 nodes. It should trigger parallel calc_runs per scenario (5–6 concurrent) with bounded subgraph expansion. At PoA scale with a single-threaded engine, this is a latency risk for broad-scope events. Document the expected max-scope event and its expected propagation time explicitly.

---

### 6.2 Zone-transition job cost at PoA vs. production scale

**PoA scale (500 (item, location) pairs):**
- Weekly→daily transition: 500 pairs × 1 weekly bucket → 7 daily nodes + edge updates = 3,500 new nodes, ~3,500 edge updates
- At 2ms per pair (delete + insert + edges): ~1 second total
- Within the 02:00 UTC window: trivially safe

**Production scale (10,000 pairs):**
- Weekly→daily transition: 10,000 × 1 bucket → 70,000 new nodes, ~70,000 edge updates
- At 2ms per pair (SQLite, single-threaded): ~20 seconds
- Monthly→weekly: similar scale, similar cost
- Total window: ~40 seconds at production scale — manageable but tight if jobs overlap

**Risk: Postgres (V2 target):** With proper batch inserts and indexed updates, this should scale to <10 seconds even at 10K pairs. But SQLite's single-writer constraint means these are serialized inserts — no batch parallelism. If production scale is attempted on SQLite, the zone-transition job could run for minutes.

**Resolution:** Implement zone-transition jobs using batch INSERT (executemany), not row-by-row. Test at 10K pair scale before V1 production.

---

### 6.3 Overlapping zone-transition jobs (1st of month on Monday)

**Severity: BLOCKER**

When the 1st of the month falls on a Monday (e.g., 2026-06-01), both scheduled jobs fire:
- 02:00 UTC: Monday job (weekly→daily transition)
- 02:00 UTC: 1st-of-month job (monthly→weekly transition + horizon extension)

The architecture does not specify whether these jobs are:
1. Serialized (one completes before the other starts)
2. Independent (can run concurrently)
3. Merged (detected at scheduling time and combined into a single job)

**The failure mode if they run concurrently:**
- Both jobs operate on the same `(item, location)` pairs
- Both modify PI node structure (one splits a weekly bucket into days; the other splits a monthly bucket into weeks)
- If the 1st-of-month job is splitting a monthly bucket into weeks at the same moment the weekly→daily job is trying to find that month's weekly buckets to split into days, the jobs are reading and writing overlapping nodes
- In SQLite (single-writer), concurrent writes will serialize via WAL, but the read-modify-write pattern may produce incorrect results: the monthly→weekly job creates weekly buckets, the weekly→daily job reads the old state (weekly bucket not yet created) and does nothing for that month — leaving the newly created weekly buckets unprocessed

**Resolution required before PoA:** When the 1st of month falls on Monday, run a single combined job: monthly→weekly first, then weekly→daily within the same scheduled execution. Detect this condition at scheduler configuration time and implement a combined entry point. Add a test that simulates this condition.

---

## Section 7 — Showstoppers Summary

### 🔴 BLOCKER — Must fix before PoA

| # | Issue | Location in Review | Risk if Not Fixed |
|---|-------|-------------------|-------------------|
| B1 | Concurrent calc_runs on same (node_id, scenario_id) — race on dirty state, no serialization guarantee | §1.3 | Silent data corruption in crash-recovery scenarios; double-computation or skipped recompute |
| B2 | Zone-transition job crash recovery is unspecified — partial structural mutations are not recoverable | §2.2 | Irreparable gap in PI node coverage after a Monday or 1st-of-month job crash |
| B3 | Baseline recompute while variant is `computing` — variant can complete as `computed` against superseded baseline, never marked stale | §4.1 | Planners make decisions on variant results that are silently wrong |
| B4 | Overlapping zone-transition jobs (1st-of-month on Monday) — concurrent structural mutations on same node set produce incorrect state | §6.3 | Corrupted PI structure; monthly bucket split twice or never fully split |

---

### 🟡 RISK — Must address before V1 production

| # | Issue | Location in Review | Risk if Not Fixed |
|---|-------|-------------------|-------------------|
| R1 | is_dirty cache / dirty_nodes desync — crash window between step 3 and step 5 of clearing sequence | §1.1 | False-positive dirty reads; double-computation on recovery |
| R2 | Scenario_merge partial processing — baseline corrupted in hybrid state if crash during merge | §2.3 | Irrecoverable baseline corruption; no replay possible |
| R3 | Stale detection mechanism is unimplementable as documented — node history not persisted | §4.2 | Variant staleness never detected; planners work from silently stale scenarios |
| R4 | Scenario bleed — no DB-level guard prevents variant propagation from writing to baseline | §4.3 | Baseline irreversibly corrupted by a code bug; undetectable until production incident |
| R5 | PO due Sunday at zone boundary — bucket assignment ambiguous at weekly→daily transition | §3.1 | Demand contribution lost or double-counted at zone boundary |
| R6 | Global policy change propagation latency — 57,500 node recompute at PoA scale worst case | §6.1 | 60-second propagation latency violates 5-second SLA |
| R7 | Horizon Extension missed — demand beyond horizon silently dropped, no alert | §3.4 | Undetected shortages in the 16–18-month planning horizon |

---

### 🔵 DEBT — Can defer to V2

| # | Issue | Location in Review | Consequence if Deferred |
|---|-------|-------------------|------------------------|
| D1 | is_dirty cache should be removed from propagation read path, documented as inspection-only | §1.4 | Maintenance burden; potential future misuse |
| D2 | Dirty_nodes row deletion and nodes attribute write should be in one transaction | §1.2 | Non-idempotent recovery in edge cases |
| D3 | Propagation trace table needed for correctness testing | §5.1 | Cannot verify propagation correctness at node level in tests |
| D4 | Branch depth enforcement is API-only, no DB constraint | §4.4 | Depth-3 scenarios can be created by bypassing the API |
| D5 | Edge migration during zone transitions is unspecified | §3.2 | impacts edges to Shortage nodes broken by zone transition; causal chain lost |
| D6 | PLANNING_START guard in zone-transition job not explicitly specified | §3.3 | Possible past-dated PI buckets created; engine refuses to compute them (dangling nodes) |
| D7 | Zone-transition jobs not implemented as batch inserts | §6.2 | Acceptable at PoA, will be too slow at production scale (10K pairs) |
| D8 | Shortage stable identity during zone restructuring not specified | §3.2 | Shortage tracking across transitions loses identity |
| D9 | `depth` column missing from current SQL schema (001_initial_schema.sql) | §4.4 | Branch depth enforcement cannot be implemented without schema update |

---

## Section 8 — Architecture Strengths (For Completeness)

These items are correctly designed and represent genuine architectural strengths. No changes needed.

- **Event-sourced audit log:** Insert-only events table with `triggered_by_event_id` on calc_runs provides a solid replay anchor. Crash recovery can always re-derive the correct dirty set from the originating event.
- **Node-type temporal policies:** Moving granularity from a global zone model to a per-node-type policy (ADR-002d final decision) is correct. It eliminates cross-type coupling and makes the policy configurable without structural changes.
- **Point_in_bucket contribution rule:** Correct business semantics for far-horizon planning. No proration eliminates a class of floating-point consistency bugs seen in competing APS implementations.
- **Topological sort across locations:** The PROPOSAL document correctly identifies the cross-location topo ordering bug as "the most common correctness bug in supply chain propagation engines." Calling it out explicitly suggests awareness of the failure mode.
- **Explanation assembly inline during allocation:** Building causal paths during the allocation pass (not post-hoc) avoids the brittle "reverse engineer why" problem. Root cause classification into 5 archetypes is appropriately scoped.
- **Scenario delta overlay:** Correct and efficient. No full copy per scenario. At PoA scale, the COALESCE resolution pattern costs one dict lookup per node — negligible.
- **SQLite for PoA:** The right call. Zero ops overhead, ACID, WAL mode, full FK enforcement. The upgrade path to Postgres is clean given the schema design.

---

## Section 9 — Recommended Pre-PoA Actions (Priority Order)

1. **[B4] Define combined zone-transition job for Monday=1st-of-month** — one combined job entry point, tested with a synthetic date
2. **[B1] Serialize calc_runs per scenario** — document and enforce: at most one `running` calc_run per scenario at any time; incoming events for a running scenario queue into the same or next calc_run
3. **[B3] Add post-completion stale check to variant calc_runs** — when a variant's calc_run completes, compare variant's `started_at` against baseline's last `completed_at`; mark stale if baseline moved during variant computation
4. **[B2] Define zone-transition job as transactional per (item, location)** — one DB transaction per pair; add a progress log table for job resume after crash
5. **[R3] Change stale detection to conservative mechanism** — `stale if baseline_snapshot_id != current baseline calc_run_id`; remove the unimplementable node-value comparison
6. **[R4] Add engine-level scenario_id assertion** — before any node write during propagation, assert `node.scenario_id == calc_run.scenario_id`

---

*Review complete. This document should be considered a living specification gap list. Each BLOCKER item should be closed with a corresponding architectural decision and test case before PoA execution begins.*
