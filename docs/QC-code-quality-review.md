# QC Code Quality Review — Ootils Core V1

**Date:** 2026-04-05  
**Reviewer:** Senior QC Engineer (automated subagent)  
**Scope:** Sprint 1 — focused code review, no docs/ADRs

---

## Summary

| Severity | Count |
|----------|-------|
| 🔴 BLOCKER | 3 |
| 🟡 HIGH | 5 |
| 🟢 MEDIUM | 5 |
| ⚪ LOW | 2 |

---

## 🔴 BLOCKERS

---

### BLK-1 — `seed_demo_data.py` inserts into `shortages` table that doesn't exist

**File:** `scripts/seed_demo_data.py`  
**Lines:** ~220–260 (`_seed_shortages` function)

**Problem:**  
`_seed_shortages()` executes:
```sql
INSERT INTO shortages (shortage_id, scenario_id, pi_node_id, ...)
VALUES (...)
```
The `shortages` table is **never defined** in migration 001 or 002. Running the seed script will crash immediately with:
```
psycopg.errors.UndefinedTable: relation "shortages" does not exist
```
This is also referenced from `ShortageDetector.get_active_shortages()` and `ShortageDetector.detect()` in the propagator — if those methods hit the same table, the propagation engine will also crash on any scenario with shortages.

**Fix:** Create a migration for the `shortages` table (it's clearly a planned table — `ShortageDetector` references it throughout). Add to migration 002 or create 003.

---

### BLK-2 — `events.py` router allows `scenario_merge` event type not in DB CHECK constraint

**File:** `src/ootils_core/api/routers/events.py`  
**Lines:** ~28–40 (`VALID_EVENT_TYPES` set)

**Problem:**  
The router defines:
```python
VALID_EVENT_TYPES = {
    ...
    # From migration 006 CHECK constraint extension
    "scenario_merge",
}
```
Only migrations 001 and 002 exist. Migration 006 does not exist. The `events.event_type` column in migration 002 has a CHECK constraint:
```sql
CHECK (event_type IN ('supply_date_changed', 'supply_qty_changed', ... 'test_event'))
```
`scenario_merge` is absent. Any API call submitting `event_type: "scenario_merge"` will pass the Python-side validation but fail at the DB INSERT with a `CheckViolation` (HTTP 500). The fake migration reference is a code smell indicating this was added without the corresponding DB migration.

**Fix:** Either remove `scenario_merge` from `VALID_EVENT_TYPES` until migration 006 exists, or add it to the migration 002 CHECK constraint immediately.

---

### BLK-3 — `docker-compose.yml` uses `--reload` in production command

**File:** `docker-compose.yml`  
**Line:** ~25

**Problem:**  
```yaml
command: uvicorn ootils_core.api.app:app --host 0.0.0.0 --port 8000 --reload
```
`--reload` is a **development-only** flag that starts a file-watcher subprocess. In production containers:
1. Uvicorn spawns a watchdog process that continuously polls the filesystem.
2. Any transient inotify/filesystem event can trigger a full worker restart — mid-request state is dropped.
3. The `--reload` worker does not handle SIGTERM gracefully in all configurations.
4. The Dockerfile CMD does NOT have `--reload`, but docker-compose overrides it — production deployments using this compose file will get the dev flag.

**Fix:** Remove `--reload` from the `command:` in `docker-compose.yml`. Use a separate `docker-compose.override.yml` for dev with `--reload`.

---

## 🟡 HIGH

---

### HIGH-1 — `store.py` `update_node_closing_stock` says it clears `is_dirty` but doesn't

**File:** `src/ootils_core/engine/kernel/graph/store.py`  
**Lines:** ~280–300 (`update_node_closing_stock`)

**Problem:**  
Docstring says:
> "Also clears is_dirty so downstream propagation knows this node is fresh."

The SQL does NOT include `is_dirty = FALSE`:
```python
self._conn.execute(
    """
    UPDATE nodes
    SET closing_stock = %s,
        updated_at    = %s
    WHERE node_id = %s AND scenario_id = %s
    """,
    ...
)
```
Any PI node updated via the allocation path (`update_node_closing_stock`) remains `is_dirty = TRUE` in the DB. The propagation engine checks `is_dirty` to decide what to recompute. A node that's been allocated from but not marked clean will appear dirty on the next incremental run, triggering a redundant (and potentially incorrect) re-projection that overwrites the allocation result.

**Fix:** Add `is_dirty = FALSE` to the UPDATE statement.

---

### HIGH-2 — `events.py` returns a fake `calc_run_id` not stored in the database

**File:** `src/ootils_core/api/routers/events.py`  
**Lines:** ~70–90

**Problem:**  
```python
event_id = uuid4()
calc_run_id = uuid4()   # ← randomly generated, never stored
...
return EventResponse(
    ...
    calc_run_id=calc_run_id,  # ← client can never poll this
)
```
The `calc_run_id` returned to clients is a freshly generated UUID that is never inserted into `calc_runs`. Any client that uses this ID to poll for status (e.g., `GET /calc_runs/{calc_run_id}`) will receive a 404. The API contract is broken — the response implies an actionable run ID.

**Fix:** Either don't return a `calc_run_id` until a real run has been created (async via queue), or remove the field from `EventResponse` and document that it's fire-and-forget.

---

### HIGH-3 — Advisory lock uses 32-bit hash — scenario UUID collisions possible

**File:** `src/ootils_core/engine/orchestration/calc_run.py`  
**Lines:** ~35, ~90, ~115

**Problem:**  
```python
db.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (str(scenario_id),))
```
`hashtext()` returns `int32` (4 billion distinct values). With N scenarios, collision probability grows as ~N²/2³². At 100k scenarios the probability of any pair colliding is ~1.2%. Colliding scenarios will falsely block each other's calc runs — scenario A's run prevents scenario B from starting even though they're independent. This is a data-correctness issue under load.

**Fix:** Use `pg_try_advisory_lock(bigint)` with a stable 64-bit hash, e.g., `('x' || encode(scenario_id::text::bytea, 'hex'))::bit(64)::bigint` or store a deterministic int64 per scenario.

---

### HIGH-4 — `Dockerfile` installs `[dev]` extras in production image

**File:** `Dockerfile`  
**Line:** ~10

**Problem:**  
```dockerfile
RUN pip install --no-cache-dir -e ".[dev]"
```
This installs all development dependencies (pytest, hypothesis, ruff, mypy, etc.) into the production container. Consequences:
- Bloated image (significant MB overhead).
- Test frameworks are callable from within the production container.
- `pip install -e` (editable mode) adds a `.pth` file that depends on the source tree being present — acceptable here since `COPY src/` is done, but fragile if the image is ever used without the full source.

**Fix:** Change to `RUN pip install --no-cache-dir .` (non-editable, no dev extras) for production. Use a multi-stage build or separate `[dev]` install for CI.

---

### HIGH-5 — Failed propagation run loses its audit record on transaction rollback

**File:** `src/ootils_core/engine/orchestration/propagator.py`  
**Lines:** ~85–100 (exception handler in `process_event`)

**Problem:**  
```python
except Exception as exc:
    db.execute("ROLLBACK TO SAVEPOINT propagation_start")
    self._calc_run_mgr.fail_calc_run(calc_run, str(exc), db)
    raise
```
`start_calc_run` inserts a `calc_runs` row **before** the savepoint. `ROLLBACK TO SAVEPOINT` does not undo that insert. But `fail_calc_run` writes `UPDATE calc_runs SET status='failed'...` — and when `raise` propagates up to the caller's `OotilsDB.conn()` context manager, it calls `connection.rollback()`, which rolls back **the entire outer transaction**, including both the INSERT and the UPDATE. Result: no `calc_run` record survives in the database; the failure is unauditable and unobservable.

Note: `pg_advisory_unlock` inside `fail_calc_run` IS durable (advisory locks are non-transactional), so the lock IS released correctly. Only the audit trail is lost.

**Fix:** Either commit the `calc_run` status change in a separate savepoint after rollback, or use `autocommit` mode for the status update, or restructure so `fail_calc_run` runs outside the rolled-back transaction.

---

## 🟢 MEDIUM

---

### MED-1 — `connection.py` migration error handler too broad — can swallow real failures

**File:** `src/ootils_core/db/connection.py`  
**Lines:** ~60–68

**Problem:**  
```python
except Exception as e:
    if "already exists" not in str(e):
        raise
```
Any exception whose string representation anywhere contains "already exists" is silently swallowed. A real error — e.g., a FK violation on a seed INSERT that mentions an existing constraint — could be suppressed. If psycopg3 sends a multi-statement file via `PQexec` and the first statement raises "already exists", the entire remainder of the file is skipped silently.

**Fix:** Catch specifically `psycopg.errors.DuplicateTable` or `psycopg.errors.DuplicateObject`. Or split the SQL into individual statements and handle each.

---

### MED-2 — `dirty.py` `mark_dirty` accepts unused `db` parameter

**File:** `src/ootils_core/engine/kernel/graph/dirty.py`  
**Lines:** ~35–45

**Problem:**  
```python
def mark_dirty(self, node_ids, scenario_id, calc_run_id, db) -> None:
    """Mark node_ids as dirty in memory. Does NOT write to Postgres."""
```
`db` is accepted but never used. Every call site must provide it. This creates a misleading API — callers assume `db` is used (contrast: `clear_dirty` and `flush_to_postgres` both actually use `db`). Future maintainers will struggle to understand why `mark_dirty` needs a DB connection.

**Fix:** Remove the `db` parameter from `mark_dirty` (it's a no-op). Update all call sites.

---

### MED-3 — `seed_demo_data.py` uses hardcoded credentials as default

**File:** `scripts/seed_demo_data.py`  
**Line:** ~35

**Problem:**  
```python
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://ootils:ootils@localhost:5432/ootils_dev"
)
```
If `DATABASE_URL` is not set, the script connects with hardcoded username `ootils` and password `ootils`. This is committed to source control. While a seed script, if the same credentials are used in any deployed environment (accidental copy-paste or CI), they become a known credential.

**Fix:** Remove the default fallback or replace with `postgresql:///ootils_dev` (Unix socket, no creds). Fail loudly if `DATABASE_URL` is not set.

---

### MED-4 — `Dockerfile` runs container as root

**File:** `Dockerfile`

**Problem:**  
No `USER` directive exists. The container runs as root. A compromised Python process has full container root access, which can simplify container escape attacks.

**Fix:** Add:
```dockerfile
RUN adduser --disabled-password --no-create-home ootils
USER ootils
```

---

### MED-5 — `docker-compose.yml` defines no resource limits

**File:** `docker-compose.yml`

**Problem:**  
Neither `api` nor `postgres` services have `mem_limit`, `cpus`, or `deploy.resources` constraints. A runaway calc job or memory leak can OOM-kill the entire host.

**Fix:** Add `deploy.resources.limits` (compose v3) or `mem_limit`/`cpus` (legacy) for both services.

---

## ⚪ LOW

---

### LOW-1 — `projection.py` double-converts `source_qty` to `Decimal`

**File:** `src/ootils_core/engine/kernel/calc/projection.py`  
**Lines:** ~45, ~90

The caller converts `supply_qty` to `Decimal(str(supply_qty))` before passing to `apply_contribution_rule`, which then calls `Decimal(str(source_qty))` again. Harmless but wasteful in a hot path.

---

### LOW-2 — `seed_demo_data.py` uses `ON CONFLICT DO NOTHING` without explicit conflict target

**File:** `scripts/seed_demo_data.py`

`ON CONFLICT DO NOTHING` without a conflict target silently suppresses conflicts on **any** constraint, including unexpected FK/unique violations. Prefer `ON CONFLICT (item_id) DO NOTHING` etc. to be explicit.

---

## Top 3 Most Critical Findings

| Rank | Issue | Severity | Impact |
|------|-------|----------|--------|
| 1 | **`shortages` table missing from migrations** (`seed_demo_data.py`) | 🔴 BLOCKER | Seed script crashes; demo is non-functional; `ShortageDetector` likely crashes too |
| 2 | **`scenario_merge` event type missing from DB CHECK constraint** (`events.py`) | 🔴 BLOCKER | Any `scenario_merge` submission → HTTP 500; silent contract lie in code |
| 3 | **`update_node_closing_stock` doesn't clear `is_dirty`** (`store.py`) | 🟡 HIGH | Allocated nodes stay dirty → over-propagation, allocation results overwritten by engine on next run |

---

## Files Reviewed

1. `src/ootils_core/db/migrations/001_initial_schema.sql` — clean (no-op)
2. `src/ootils_core/db/migrations/002_sprint1_schema.sql` — well-structured; `shortages` table missing
3. `src/ootils_core/db/connection.py` — migration handler too broad
4. `src/ootils_core/engine/kernel/graph/store.py` — `is_dirty` clear bug
5. `src/ootils_core/engine/kernel/graph/dirty.py` — unused `db` parameter
6. `src/ootils_core/engine/kernel/calc/projection.py` — clean
7. `src/ootils_core/engine/orchestration/propagator.py` — rollback audit loss
8. `src/ootils_core/engine/orchestration/calc_run.py` — hash collision risk
9. `src/ootils_core/api/app.py` — clean
10. `src/ootils_core/api/routers/events.py` — fake calc_run_id, scenario_merge blocker
11. `src/ootils_core/api/routers/explain.py` — clean
12. `src/ootils_core/api/routers/graph.py` — clean
13. `src/ootils_core/api/routers/issues.py` — clean
14. `src/ootils_core/api/routers/projection.py` — clean
15. `src/ootils_core/api/routers/simulate.py` — clean
16. `docker-compose.yml` — `--reload` in production
17. `Dockerfile` — dev deps in prod, runs as root
18. `scripts/seed_demo_data.py` — `shortages` table crash, hardcoded creds
19. `tests/test_sprint1.py` — generally solid; integration cleanup not guarded by try/finally
