# ADR-012: Scenario Fork — Bulk INSERT…SELECT Path

**Status:** Accepted
**Date:** 2026-05-23
**Author:** Nicolas GOINEAU

---

## Context

`ScenarioManager.create_scenario` is the entry point used by `/v1/simulate`, MPS promotions, and every agent that branches the planning state. Until this ADR, it deep-copied parent data row by row:

1. `SELECT * FROM projection_series WHERE scenario_id = parent` → loop, one `INSERT` per row, build `old_series_id → new_series_id` dict.
2. `SELECT * FROM nodes WHERE scenario_id = parent AND active = TRUE` → loop, one `INSERT` per row, build `old_node_id → new_node_id` dict.
3. `SELECT * FROM edges WHERE scenario_id = parent AND active = TRUE` → loop, one `INSERT` per edge with the two endpoints remapped from the in-memory dict.
4. One orphan-edge integrity check.

This is O(N) in query count. The [bench harness](../scripts/bench_scenario_fork.py) measured the cost on a fresh PostgreSQL via the dev tunnel:

| Scale | Source rows (nodes + edges) | Wall time | Queries | Rows/sec |
|---|---|---|---|---|
| 50 items × 30 buckets | 1,550 + 1,500 | **12.2 s** | **3,105** | 250 |
| 200 items × 90 buckets | 18,200 + 18,000 | extrapolated ≥ 2 min | ≥ 36 K | ~250 |
| SMB (500 items × 90 buckets) | ≥ 600 K | extrapolated **~40 min** | ≥ 600 K | — |

REVIEW-2026-05 R10 flagged this. R10 originally framed the fix as "lazy / true copy-on-write" — child scenario writes a `scenarios` row only, all reads fall through to the parent via a scenario chain, writes materialise rows on demand. That model is correct but expensive: every `GraphStore` reader becomes scenario-chain-aware, every write becomes materialise-or-update, the diff path changes shape, and the `scenario_overrides` semantics need re-grounding. Risk of breaking the engine kernel is high.

---

## Decision

**Adopt bulk `INSERT…SELECT` as the immediate fix** (Phase 1). Keep the explicit-copy semantics; reduce the query count from O(N) to a small constant via temp mapping tables, leaving every downstream reader unchanged.

The fork sequence becomes:

```sql
-- Mapping tables (drop on commit so they cannot leak between forks)
CREATE TEMP TABLE _series_map (old_id UUID PRIMARY KEY,
                               new_id UUID NOT NULL DEFAULT gen_random_uuid())
    ON COMMIT DROP;
INSERT INTO _series_map (old_id)
    SELECT series_id FROM projection_series WHERE scenario_id = $parent;

CREATE TEMP TABLE _node_map (old_id UUID PRIMARY KEY,
                             new_id UUID NOT NULL DEFAULT gen_random_uuid())
    ON COMMIT DROP;
INSERT INTO _node_map (old_id)
    SELECT node_id FROM nodes WHERE scenario_id = $parent AND active = TRUE;

-- Three bulk INSERT…SELECTs
INSERT INTO projection_series (…) SELECT … FROM projection_series ps
    JOIN _series_map m ON m.old_id = ps.series_id …;
INSERT INTO nodes (…) SELECT … FROM nodes n
    JOIN _node_map m ON m.old_id = n.node_id
    LEFT JOIN _series_map sm ON sm.old_id = n.projection_series_id …;
INSERT INTO edges (…) SELECT … FROM edges e
    JOIN _node_map mf ON mf.old_id = e.from_node_id
    JOIN _node_map mt ON mt.old_id = e.to_node_id …;

-- Existing orphan-edge integrity check unchanged
```

Edges whose endpoints are missing from `_node_map` are dropped by the inner JOIN — same semantic as the previous in-app `node_id_map.get()` filter.

The caller's `dict[str(old_series_id), new_series_id]` return value is preserved (the new code reads it back from `_series_map` with one `SELECT`) so `_copy_nodes` can keep its existing signature.

The "true lazy CoW" model is **deferred to a follow-up ADR** (working title: ADR-013-scenario-lazy-cow). It needs:

- A scenario-chain helper visible to every `GraphStore` reader.
- A `WITH RECURSIVE` or per-call list of ancestor ids in `WHERE scenario_id = ANY(…)`.
- A materialise-or-update wrapper around every kernel write.
- A migration if we choose to denormalise the chain.

None of that is in scope here.

---

## Consequences

### Positive

- **Measured 27.5× speedup** at the bench scale (12.2 s → 0.44 s), **scaling improvement is ~O(N) → ~O(1) in query count**:
  - 1,550 nodes + 1,500 edges: 3,105 → **11** queries, 12.2 s → 0.44 s.
  - 18,200 nodes + 18,000 edges: extrapolated ≥36 K → **11** queries, ≥2 min → 4.15 s.
- **Zero downstream change.** `GraphStore`, the propagator, the diff path, `apply_override`, simulate endpoints — none of them know the fork got faster.
- **Same data shape.** Child scenario rows are byte-for-byte equivalent to the previous deep-copy output.
- **Orphan-edge integrity check still runs** at the end. Aborts the transaction if any edge in the new scenario points outside the copied set.

### Negative / dette

- The cost of forking is still **O(N) in storage** — each fork still doubles the relevant rows. Lazy CoW would drop this to O(overrides). The SCALABILITY.md note about scenario-data growth remains accurate; this ADR only reduces the *time* of a fork, not its *footprint*.
- Two temp tables (`_series_map`, `_node_map`) leak into the caller's transaction scope. They are declared `ON COMMIT DROP` so a normal `COMMIT` cleans them up, but a long-lived caller transaction sees them. We treat that as acceptable — `create_scenario` is the only path that creates them, and the caller is expected to commit shortly after.
- The bulk INSERT does not log per-row outcomes. The previous code logged "skipping edge X" when an endpoint was outside the source set; now those edges silently fail to materialise. The orphan check at the end still catches the data-integrity case, just without the per-row trace.

### Reste à faire

- **ADR-013-scenario-lazy-cow** — the true lazy model. Needs scenario-chain helpers in `GraphStore`, a kernel-write materialise wrapper, and migration of the diff path.
- Add the bench numbers to `docs/SCALABILITY.md` once we have a re-measurement at SMB scale.
- Consider compressing the orphan-check into a single CTE inside the third `INSERT` for one less round trip (optimisation, low priority).

---

## Code references

- Refactor: `src/ootils_core/engine/scenario/manager.py:_copy_projection_series`, `_copy_nodes`
- Bench: `scripts/bench_scenario_fork.py`
- Tests:
  - Bulk-path contract: `tests/test_coverage_gaps.py::TestScenarioManagerGaps::test_create_scenario_uses_bulk_sql_path`
  - Constant-query budget: `tests/test_coverage_gaps.py::TestScenarioManagerGaps::test_create_scenario_bulk_path_uses_constant_query_count`
  - End-to-end: `tests/test_m5_scenarios.py` (unit), live PG via the tunnel (manual)
- Review source: `docs/REVIEW-2026-05.md` (R10)
