# ADR-011: Scenario Retention Policy

**Status:** Accepted
**Date:** 2026-05-21
**Author:** Nicolas GOINEAU

---

## Context

The 2026-05 architecture review flagged "missing garbage collection for orphan scenarios" as a critical risk: scenarios are created by deep-copying ~600K nodes and ~1.5M edges from a parent, so each fork costs ~60 MB at 500 items. If scenarios were hard-deleted without cleanup, orphaned `nodes` / `edges` rows would accumulate and inflate the `nodes` and `edges` tables forever.

Investigation surfaced an existing architectural decision documented in migration `015_schema_integrity.sql` (lines 7–10):

> NOTE: ON DELETE CASCADE on scenarios intentionally NOT added.
> Ootils uses soft-delete pattern (status='archived'). Hard-deleting a scenario
> should be a deliberate admin operation with explicit table cleanup.

This intent was carried by PostgreSQL's default `NO ACTION` on every FK pointing at `scenarios(scenario_id)` — silent at the schema level and easy to overlook.

Three options were considered:

| Option | Behavior on `DELETE FROM scenarios` | Trade-off |
|---|---|---|
| A — `ON DELETE RESTRICT` everywhere | Raises FK violation. Soft-delete via `status='archived'` remains the only path. | Codifies existing intent. Bloat risk remains, but is bounded by archive cadence, not deletion. |
| B — `ON DELETE CASCADE` everywhere | Cascades to all child tables (nodes, edges, audit, projections). | Violates the migration 015 decision. Risk of accidental destructive delete of audit history. |
| C — Hybrid (CASCADE on regenerable, RESTRICT on audit) | Splits children into two classes. | Most "correct" model, but requires per-table classification and a real ADR on what's audit vs. derived. Deferred. |

A separate bug was also surfaced during investigation: migration `021_mrp_lot_sizing_params.sql:21` declares `mrp_runs.scenario_id UUID NOT NULL` but omits `REFERENCES scenarios(scenario_id)`, leaving the table referentially unprotected.

---

## Decision

**Option A is adopted as the baseline retention policy.** All foreign keys pointing at `scenarios(scenario_id)` use `ON DELETE RESTRICT`. The missing FK on `mrp_runs.scenario_id` is added with the same policy.

Hard-delete of a scenario is intentionally impossible through the API. The `DELETE /v1/scenarios/{id}` endpoint performs a soft-delete (sets `status='archived'`). Administrative hard-delete requires a deliberate, documented procedure that explicitly deletes from each child table in the correct order.

**The bloat risk identified in the review remains open**, but is reframed: it is no longer "orphan rows accumulate after delete" (impossible now), but "archived scenarios retain their full deep-copied subgraph". A long-term cleanup strategy — scheduled purge, admin endpoint, or accepted unbounded retention — is left to a follow-up ADR once the operational picture is clearer (volumes, audit requirements, regulatory constraints).

The hybrid model (option C) is not rejected; it is deferred until the cleanup strategy ADR forces a decision on what "regenerable" means in this codebase.

---

## Consequences

### Positive
- The retention intent is now visible in the schema (`pg_constraint` shows `RESTRICT` explicitly).
- A future contributor reading the schema understands the policy without needing to read migration 015's comment.
- `mrp_runs.scenario_id` is now referentially protected.
- Any code path that tries to hard-delete a scenario fails fast with a clear FK violation, rather than silently leaving orphans (which `NO ACTION` would also do, but less explicitly).

### Negative / dette
- The storage bloat risk for archived scenarios is acknowledged but not solved.
- The hybrid model is deferred — when the cleanup ADR lands, some of these FKs may flip to CASCADE.
- No CI guardrail yet prevents a future migration from re-introducing a FK without `ON DELETE` clause.

### Reste à faire
- New ADR — `ADR-NNN-scenario-archive-cleanup.md` — addressing what to do with archived scenarios over time. Should answer: (1) does anyone need to read an archived scenario after N days/months? (2) how is "regenerable" defined? (3) admin endpoint vs. scheduled job?
- Integration test (added in this PR) verifying that `DELETE FROM scenarios WHERE scenario_id = X` raises `ForeignKeyViolation` when X is referenced.
- Lint / migration template enforcing `ON DELETE` on every new FK (lower priority).

---

## Code references

- Migration: `src/ootils_core/db/migrations/032_scenario_fk_retention.sql`
- Prior intent: `src/ootils_core/db/migrations/015_schema_integrity.sql:7-10`
- Bug fixed: `src/ootils_core/db/migrations/021_mrp_lot_sizing_params.sql:21`
- Test: `tests/integration/test_scenario_fk_retention.py`
- Review source: `docs/REVIEW-2026-05.md`
