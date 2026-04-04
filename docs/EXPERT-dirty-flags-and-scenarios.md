# Ootils — Expert Recommendations: Dirty Flags & Scenario State Persistence

**Author:** Architecture Review (AI)
**Date:** 2026-04-04
**Status:** Recommendation — requires human sign-off on flagged trade-offs

---

## Question 3 — Dirty Flag Strategy

### Recommended Design: `calc_run`-anchored dirty tracking with in-memory propagation graph and batch-commit checkpoints

#### Core Recommendation

**Do not rely solely on `is_dirty BOOLEAN` in `nodes`.** Replace it with a `dirty_nodes` table scoped to a `(calc_run_id, node_id, scenario_id)` triple. The `nodes.is_dirty` column can be retained as a fast read signal but is treated as a derived cache — not the authoritative dirty state.

```sql
dirty_nodes (
  calc_run_id  UUID NOT NULL REFERENCES calc_runs,
  node_id      UUID NOT NULL,
  scenario_id  UUID NOT NULL,
  marked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (calc_run_id, node_id, scenario_id)
)
```

---

### Q3.1 — When to mark nodes dirty

**Recommendation:** Mark dirty **transactionally at event ingestion**, inside the same Postgres transaction that inserts the event.

```
BEGIN;
  INSERT INTO events (...);
  INSERT INTO dirty_nodes (calc_run_id, node_id, scenario_id, ...) VALUES (...);
  UPDATE nodes SET is_dirty = TRUE WHERE node_id = <trigger_node_id> AND scenario_id = <sid>;
COMMIT;
```

The engine creates a `calc_run` row (status=`pending`) as part of this transaction. Dirty marking is synchronous with event insert — never deferred.

**Reasoning:** Separating event insert from dirty marking creates a race window. If the engine starts between the two, it sees a clean graph with a new event and skips recalculation. Transactional coupling is cheap and eliminates this.

**Failure mode avoided:** Ghost events — events that entered the system but whose downstream effects were never propagated because dirty marking happened in a separate step that failed silently.

---

### Q3.2 — When to clear dirty flags

**Recommendation:** Clear the `dirty_nodes` entry for a node **immediately after successful recompute of that node**, not after the full calc_run. Use `calc_runs.status` to track overall progress.

Sequence:
1. Recompute node N for `(calc_run_id, scenario_id)`.
2. Write new computed attributes to `nodes`.
3. Delete from `dirty_nodes WHERE calc_run_id = X AND node_id = N AND scenario_id = S`.
4. Mark downstream as dirty (insert into `dirty_nodes`) if delta check shows change.
5. After all dirty_nodes for a calc_run are processed: set `calc_runs.status = 'completed'`, clear `nodes.is_dirty` for affected rows in one batch UPDATE.

**Do not wait for human approval to clear dirty flags.** Human approval is a planning workflow concept; dirty = "needs recomputation", not "needs human review". These are orthogonal.

**Failure mode avoided:** If you wait for full calc_run completion to clear any flags, a crash midway leaves all previously-computed nodes still marked dirty. On restart, you'd recompute work already done. Per-node clearing allows safe resume.

---

### Q3.3 — Reference base: dirty relative to what?

**Recommendation:** A node is dirty relative to its **last successfully completed calc_run** for that `(node_id, scenario_id)` pair. Track this in `nodes` as:

```sql
nodes.last_calc_run_id UUID  -- FK to calc_runs, NULLable
```

A node is effectively dirty if:
- It exists in `dirty_nodes` for any `calc_run_id`, OR
- `nodes.last_calc_run_id` is NULL (never computed), OR
- The `calc_run` referenced by `last_calc_run_id` has status `failed`

**Reasoning:** Without a reference anchor, you can't distinguish "dirty because of new event" from "dirty because initial computation never ran." The `last_calc_run_id` gives you a clean baseline reference for delta checks and for the explanation engine ("this node changed vs. calc_run X").

---

### Q3.4 — Crash recovery

**Recommendation:** Use the `calc_runs.status` state machine as the recovery control surface:

```
pending → running → completed
                 ↘ failed
```

On engine startup:
1. Query `SELECT * FROM calc_runs WHERE status IN ('pending', 'running')`.
2. For `running` entries: treat as `failed` (crash happened). Reset to `pending`.
3. Re-derive dirty_nodes from the event that triggered the calc_run (the `triggered_by_event_id` FK is your replay anchor).
4. Resume propagation from the surviving `dirty_nodes` entries (those not yet cleared = not yet recomputed).

Because we clear `dirty_nodes` per-node as we go, surviving entries after a crash = exactly the nodes that need recomputation. **No full graph replay needed.**

**Failure mode avoided:** Corrupt partial state treated as valid. Without explicit `running → failed` transitions on restart, the engine might skip recovery entirely and serve stale results.

---

### Q3.5 — Performance: batching strategy

**Recommendation:** Use a **two-tier dirty tracking system**:
- **In-memory:** During a single propagation run, maintain the dirty frontier as a Python `set` of `(node_id, scenario_id)`. Use this for topological traversal — no DB round-trip per node.
- **Postgres:** Flush dirty_nodes inserts in **batches of 100–500** at checkpoint boundaries (e.g., after each topological "level" is processed), not after every node.

Concrete pattern:

```python
batch_dirty = []
for node in current_level:
    result = compute(node)
    if result != previous:
        batch_dirty.extend(downstream_nodes(node))
        persist_result(node, result)  # buffered
if batch_dirty:
    bulk_insert(dirty_nodes, batch_dirty)
    bulk_update(nodes, is_dirty=True, where=batch_dirty)
flush_persisted_results()  # single executemany
```

At PoA scale (100 SKUs × 5 locations × 5 scenarios = ~2,500–15,000 nodes), full in-memory propagation fits trivially. Postgres writes are for durability, not for computation.

**Trade-off to decide (human):** How large can the in-memory dirty set grow before you need to page to DB? For PoA this never matters. For production scale (100K+ nodes), you'd need a Redis-backed dirty set or Postgres advisory locks. Flag for V2.

---

### Q3.6 — Multi-scenario dirty flags

**Recommendation:** Dirty state is fully per `(node_id, scenario_id)`. The `dirty_nodes` table structure above already enforces this via PK.

Key rules:
- Marking baseline dirty does **not** automatically mark variant scenarios dirty.
- Variant scenarios are independently marked dirty when: (a) their own overrides change, or (b) the engine detects a baseline recomputation that affects nodes the variant inherits (see Q4.7 below for the coupling logic).
- The `is_dirty` column on `nodes` is per-row, and since `(node_id, scenario_id)` is unique per row, this works naturally.

One optimization: when a variant scenario's node has no override (inherits baseline), and baseline recomputes that node with **no change** (delta check passed), the variant node stays clean. Only propagate dirty to variants when baseline actually changes value.

---

## Question 4 — Scenario State Persistence Policy

### Recommended Design: Dual persistence (inputs + computed results), lifecycle state machine, max branch depth 2

---

### Q4.1 — What is persisted per scenario?

**Recommendation: Persist both input overrides AND computed results, separately.**

| Layer | Table | What's stored |
|---|---|---|
| Input overrides | `scenario_overrides` | User-specified changes to node attributes (demand forecast, lead time, safety stock, etc.) |
| Computed results | `nodes` (existing, scoped by scenario_id) | Full computed state for every node in the scenario |
| Explanation | `explanations` + `explanation_steps` | Why each result exists |

**Do not** store computed results only as diffs against baseline in `scenario_overrides`. That's tempting but fragile — it conflates input intent with computed output, making invalidation logic a nightmare.

**Reasoning:** The two layers serve different purposes. Overrides are user intent (stable, human-authored). Computed results are engine output (derived, invalidatable). Mixing them makes it impossible to re-run without reapplying all overrides from scratch, and breaks the audit trail.

**Failure mode avoided:** If you store results as diffs, a baseline recomputation requires you to "undo" the diff and "redo" with new baseline values. The math is fragile and error-prone. Full independent results per scenario allow clean invalidation.

**Storage cost:** At PoA scale, 100 SKUs × 5 locations × 18 time buckets × 6 scenarios ≈ 54,000 node-states. Trivial.

---

### Q4.2 — When are scenario results invalidated?

**Recommendation:** Use explicit invalidation triggers, not lazy staleness detection.

A scenario's computed results are invalidated when:

1. **A scenario override changes** → invalidate only the affected node and its downstream subgraph within the scenario.
2. **Baseline is recomputed** → for each variant scenario, check which nodes are *inherited* (no override). Mark those nodes dirty in the variant. **Only if** the baseline's new computed value differs from what was previously inherited.
3. **An upstream scenario (parent) changes** → same cascade as (2), scoped to the branch depth.

Implementation: add a `baseline_calc_run_id UUID` to each variant scenario row. On baseline recompute, compare new node values against the node values from `baseline_calc_run_id`. If different AND the variant inherits that node → mark dirty.

```sql
scenarios (
  scenario_id     UUID PK,
  parent_scenario_id UUID REFERENCES scenarios,  -- NULL = root/baseline
  baseline_calc_run_id UUID REFERENCES calc_runs, -- last baseline used for this variant
  status          TEXT,  -- see Q4.3
  ...
)
```

**Failure mode avoided:** Silent staleness — variants showing results computed against an old baseline without any indicator. Without explicit invalidation tracking, planners make decisions on stale scenario comparisons.

---

### Q4.3 — Scenario lifecycle

**Recommendation:** Enforce a strict lifecycle via a `status` column on `scenarios`:

```
draft → computing → computed → [approved | archived]
          ↓
        failed → draft (retry)
```

- **draft:** Created, overrides may be in progress, no computation triggered yet.
- **computing:** Calc run in progress. Read-only (no override changes allowed).
- **computed:** Latest calc_run completed successfully. Results are valid and current.
- **stale:** (Add this state) Results exist but are known-outdated (baseline changed, override modified while computing). Computed results are still readable but flagged.
- **approved:** Planner explicitly confirmed this scenario. Immutable. Eligible for merge into baseline.
- **archived:** Soft-deleted. Results retained for audit. No further computation.

**The `stale` state is important** — it lets you show users results while background recalculation is running, with a clear indicator. The alternative (blocking reads during recompute) kills the UX.

---

### Q4.4 — Scenario branching

**Recommendation:** Allow branching, but cap at **depth 2** (baseline → variant → sub-variant). Enforce at API level.

```
Baseline (S0)
  ├── S1 (demand surge)
  │   └── S1a (demand surge + expedite supplier)  ← max depth
  └── S2 (cost optimization)
      └── S2a (cost optimization + extended lead times)  ← max depth
```

**No depth-3+ branches.** Reason: invalidation cascades become exponentially complex. At depth 3, a baseline change requires checking 3 levels of inheritance, each with partial override coverage. The combinatorial logic is a bug farm.

**Invalidation cascade at depth 2:**
- Baseline recomputes → check S1, S2 for inherited node changes → mark stale/dirty.
- S1 recomputes → check S1a for inherited node changes → mark stale/dirty.
- S0 change does NOT directly trigger S1a recompute; it goes through S1 first.

**Trade-off to decide (human):** Depth 2 may feel restrictive for complex what-if trees. The alternative is allowing arbitrary depth with explicit "snapshot" semantics (a branch can freeze its parent reference, so parent changes don't propagate). This is significantly more complex to implement and explain to users. Recommend shipping depth-2 and revisiting.

---

### Q4.5 — Approved scenario merging into baseline

**Recommendation:** Scenario merge is a **first-class event**, not a schema operation.

Process:
1. Planner approves scenario S1 (status → `approved`).
2. Planner triggers "promote to baseline" action.
3. Engine creates a new `events` entry of type `scenario_merge` with `payload = { source_scenario_id: S1 }`.
4. Event processing: for each node in S1 that has an override, apply that override to the baseline node (update `nodes WHERE scenario_id = baseline_id`). Write a `scenario_overrides` entry for each change with `old_value` (baseline before merge).
5. Trigger a full baseline recalculation.
6. Original S1 scenario is archived (status → `archived`), not deleted.
7. All variant scenarios branched off baseline are marked `stale`.

**What this gives you:** Merge is auditable (event in the log), reversible (scenario_overrides old_value captured), and triggers normal propagation machinery — no special cases.

**Failure mode avoided:** Merge-as-schema-patch (directly overwriting baseline node attributes without an event) would leave no audit trail and bypass the propagation engine, leading to baseline nodes with no explanation for why their values changed.

---

### Q4.6 — Scenario cleanup

**Recommendation:** Two-tier cleanup policy:

**Tier 1 — Auto-archive (engine-driven):**
- Any `draft` scenario with no activity for 30 days → auto-archive.
- Any `stale` scenario that has been stale for 14 days without user action → auto-archive.
- Archive = set `status = 'archived'`, no data deletion.

**Tier 2 — Hard delete (human-triggered, with safeguard):**
- Archived scenarios can be hard-deleted after 90 days.
- Hard delete removes `nodes` rows for that scenario and `scenario_overrides`, but **retains** the `scenarios` row itself (tombstone), the `events` rows, and any `explanations` that reference a node still in the system.
- A soft-delete flag on `scenarios` is sufficient: `deleted_at TIMESTAMPTZ`.

**Audit trail preservation:** The `events` table is insert-only and never deleted. `calc_runs` rows are retained. The scenario's *intent* (what overrides were applied) is preserved in `scenario_overrides` even after node data is purged.

**Trade-off to decide (human):** How long to retain computed node data for archived scenarios? Storage is cheap at PoA scale but grows at production scale. Recommend parameterizing the 30/14/90 day thresholds as config rather than hardcoding.

---

### Q4.7 — Consistency: baseline recompute → which scenario results are stale?

**Recommendation:** Maintain a `scenarios.baseline_snapshot_id` FK pointing to the `calc_run_id` of the baseline computation this scenario was last computed against.

On any baseline calc_run completion:
1. For each active variant scenario (status = `computed`):
2. Compare: which nodes in this scenario are **inherited** (no override in `scenario_overrides`)?
3. For each inherited node: did the baseline calc_run change its value vs. `baseline_snapshot_id`?
4. If yes for any node: set scenario status → `stale`, mark affected nodes dirty in variant.
5. Optionally: trigger automatic recomputation of stale variants (configurable — some teams prefer manual trigger).

This check runs as a post-processing step after each baseline calc_run, before marking `calc_runs.status = 'completed'`.

```sql
-- Pseudo-query: find stale variants
SELECT s.scenario_id
FROM scenarios s
WHERE s.parent_scenario_id = <baseline_id>
  AND s.status = 'computed'
  AND EXISTS (
    SELECT 1 FROM nodes n_new
    JOIN nodes n_old ON n_old.node_id = n_new.node_id
      AND n_old.scenario_id = <baseline_id>
      -- n_old from baseline_snapshot_id calc_run
    WHERE n_new.scenario_id = <baseline_id>
      AND n_new.attributes != n_old.attributes  -- simplified
      AND NOT EXISTS (
        SELECT 1 FROM scenario_overrides so
        WHERE so.scenario_id = s.scenario_id
          AND so.node_id = n_new.node_id
          AND so.active = TRUE
      )
  );
```

**Failure mode avoided:** Variant scenario showing plan results that were computed against a 3-week-old baseline, with no indication to the planner. This is the most common silent data quality failure in scenario planning tools.

---

## Summary: Schema Additions Recommended

Beyond the existing schema, add:

```sql
-- Replace/augment nodes.is_dirty with:
dirty_nodes (
  calc_run_id  UUID NOT NULL REFERENCES calc_runs,
  node_id      UUID NOT NULL,
  scenario_id  UUID NOT NULL,
  marked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY  (calc_run_id, node_id, scenario_id)
);

-- Add to nodes table:
ALTER TABLE nodes ADD COLUMN last_calc_run_id UUID REFERENCES calc_runs;

-- Add to scenarios table:
ALTER TABLE scenarios ADD COLUMN parent_scenario_id UUID REFERENCES scenarios;
ALTER TABLE scenarios ADD COLUMN status TEXT NOT NULL DEFAULT 'draft';
ALTER TABLE scenarios ADD COLUMN baseline_snapshot_id UUID REFERENCES calc_runs;
ALTER TABLE scenarios ADD COLUMN deleted_at TIMESTAMPTZ;
ALTER TABLE scenarios ADD COLUMN depth INT NOT NULL DEFAULT 0;  -- enforce max 2
```

---

## Decision Checklist (Flagged Trade-offs)

| # | Decision | Options | Recommendation | Risk if deferred |
|---|---|---|---|---|
| 1 | In-memory dirty set size limit | Unlimited (PoA) vs. paged (prod) | Ship unlimited, revisit at 50K nodes | None at PoA scale |
| 2 | Auto-recompute stale variants | Auto vs. manual trigger | Start with manual, add auto as config flag | Planners see stale data silently if manual isn't clicked |
| 3 | Max scenario branch depth | 2 (recommended) vs. N | Cap at 2 | Cascade invalidation logic complexity explodes |
| 4 | Retention periods (30/14/90 days) | Configurable | Make config, not constants | Ops burden or storage cost |
| 5 | Merge = promote vs. copy | Promote S1 to baseline vs. fork S0 with S1 changes | Promote (event-sourced merge as described) | If promote: original S0 history becomes less accessible. Both approaches are valid. |

---

*End of recommendation document.*
