# ADR-006: QC Blockers Resolution

**Status:** Accepted  
**Date:** 2026-04-04  
**Author:** Architecture Review + Nicolas GOINEAU  
**Context:** Four BLOCKERs identified by independent QC review (REVIEW-qc-validation.md). All resolved before Sprint 1.

---

## B1 — Race Condition on Concurrent calc_runs

### Problem
Two events arriving before the engine processes the first can cause two calc_runs to race on the same `(node_id, scenario_id)` during crash recovery. The last writer wins, but dirty_nodes from the first run may already be cleared. Result: silently corrupted state.

### Resolution

**Two combined mechanisms:**

**1. Per-scenario serialization via Postgres advisory lock**

Before starting a calc_run, the engine acquires a Postgres advisory lock on the `scenario_id`. Only one calc_run per scenario can run at a time. A second attempt returns `None` — the event stays `pending` and is retried on the next engine cycle.

```python
def start_calc_run(scenario_id: UUID, db: Session) -> Optional[CalcRun]:
    locked = db.execute(
        "SELECT pg_try_advisory_lock(hashtext(:sid))",
        {"sid": str(scenario_id)}
    ).scalar()
    if not locked:
        return None  # already running, retry later

    pending_events = db.query(Event).filter(
        Event.scenario_id == scenario_id,
        Event.processed == False
    ).order_by(Event.created_at).all()

    run = CalcRun(
        scenario_id=scenario_id,
        triggered_by_event_ids=[e.id for e in pending_events],
        status='running'
    )
    db.add(run)
    return run
```

Note: Postgres advisory locks are session-scoped. On engine restart, all locks are automatically released — recovery is always clean.

**2. Event coalescing**

Before starting a calc_run, all pending events for the same scenario are coalesced into a single run. This reduces unnecessary sequential runs on burst imports.

### Schema change
```sql
-- calc_runs: triggered_by_event_id → triggered_by_event_ids
ALTER TABLE calc_runs ADD COLUMN triggered_by_event_ids UUID[];
-- (triggered_by_event_id kept for backward compat, deprecated)
```

### Guarantees
- One active calc_run per scenario at any time
- No race possible — advisory lock is atomic
- Recovery always clean (session-scoped locks released on restart)
- Burst imports produce one coalesced run, not N sequential runs

---

## B2 — Zone-Transition Job Crash Recovery

### Problem
The weekly→daily and monthly→weekly jobs perform structural mutations (archive bucket, create N new nodes, rewire edges). A mid-job crash leaves a coverage gap. Naive re-run has undefined behavior.

### Resolution

**Three combined mechanisms:**

**1. Per-series atomic transaction**

Each (item, location) pair is processed in its own Postgres transaction. Archive + create + rewire = atomic. Either all three operations succeed or none.

```python
for series in projection_series:
    with db.begin_nested():  # savepoint per series
        _archive_bucket(series, old_bucket, db)
        _create_new_buckets(series, old_bucket, db)
        _rewire_edges(series, old_bucket, new_buckets, db)
        _mark_dirty(new_buckets, db)
```

**2. Progress tracking in `zone_transition_runs` table**

```sql
CREATE TABLE zone_transition_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type        TEXT NOT NULL,        -- 'weekly_to_daily' | 'monthly_to_weekly' | 'combined'
    transition_date DATE NOT NULL,
    status          TEXT NOT NULL DEFAULT 'running',  -- running | completed | failed
    series_total    INT,
    series_done     INT DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    UNIQUE(job_type, transition_date)     -- enforces idempotence
);
```

**3. Idempotent resume per series**

On restart, the job checks `zone_transition_runs` for this `(job_type, transition_date)`:
- `completed` → skip (already done)
- `running` → resume from first unprocessed series
- Missing → start fresh

Per-series atomicity makes partial states impossible: a series is either fully done or not started.

### Guarantees
- No PI coverage gap possible (per-series atomicity)
- Strict idempotence (`UNIQUE(job_type, transition_date)`)
- Clean resume after crash — restarts from last unprocessed series
- Safe to re-run N times with no side effects

---

## B3 — Variant Completing as `computed` Against Superseded Baseline

### Problem
If a baseline recompute completes while a variant's calc_run is `running`, stale detection runs before the variant finishes. The variant later completes as `computed` — not `stale` — against the superseded baseline. Planners see wrong results with a clean status.

### Resolution

**Version check at variant calc_run finalization:**

At finalization, the variant checks whether the baseline has changed since its run started.

```python
def finalize_calc_run(run: CalcRun, db: Session):
    scenario = db.query(Scenario).get(run.scenario_id)
    baseline = db.query(Scenario).filter_by(is_baseline=True).one()
    current_baseline_snapshot = baseline.last_completed_calc_run_id

    if scenario.baseline_snapshot_id != current_baseline_snapshot:
        # Baseline changed during our run → we are stale
        run.status = 'completed_stale'
        scenario.status = 'stale'
    else:
        run.status = 'completed'
        scenario.status = 'computed'

    db.add(run)
    db.add(scenario)
    db.commit()
```

The check is atomic with run finalization (same transaction). A variant can never transition to `computed` against a superseded baseline.

### Schema change
```sql
-- Add 'completed_stale' to calc_runs status values
-- Distinguishes "run technically succeeded but result is stale" from 'failed'
COMMENT ON COLUMN calc_runs.status IS
    'pending | running | completed | completed_stale | failed';
```

### Guarantees
- Variant can never show `computed` status against a superseded baseline
- Planner always sees the correct status immediately
- `completed_stale` is distinguishable from `failed` — the computation succeeded, the result is just outdated

---

## B4 — Overlapping Zone-Transition Jobs (1st of Month Falls on Monday)

### Problem
When the 1st of the month falls on a Monday (~12 times/year), both jobs fire at 02:00 UTC:
- Monday job: weekly→daily (archives weekly buckets, creates 7 daily)
- 1st of month job: monthly→weekly (archives monthly buckets, creates 4-5 weekly) + Horizon Extension

Both modify the PI structure of the same projection series simultaneously. Without serialization, they corrupt each other's edge rewiring and dirty_nodes.

### Resolution

**Single combined job on 1st-Monday, with global advisory lock:**

```python
def run_zone_transition_jobs(today: date, db: Session):
    is_monday = today.weekday() == 0
    is_first_of_month = today.day == 1

    if is_monday and is_first_of_month:
        _run_combined_transition(today, db)
    elif is_monday:
        _weekly_to_daily_transition(today, db)
    elif is_first_of_month:
        _monthly_to_weekly_transition(today, db)
        _horizon_extension(today, db)

def _run_combined_transition(today: date, db: Session):
    """
    Execution order on 1st-Monday:
    1. monthly→weekly first (creates new weekly buckets)
    2. weekly→daily second (some new weekly buckets may immediately enter daily zone)
    3. Horizon Extension last (append-only, no structural conflict)
    """
    locked = db.execute(
        "SELECT pg_try_advisory_lock(hashtext('zone_transition'))"
    ).scalar()
    if not locked:
        raise RuntimeError("Zone transition already running — aborting")

    try:
        _monthly_to_weekly_transition(today, db)
        _weekly_to_daily_transition(today, db)
        _horizon_extension(today, db)
    finally:
        db.execute("SELECT pg_advisory_unlock(hashtext('zone_transition'))")
```

The same `'zone_transition'` advisory lock is acquired by all zone-transition jobs (not just the combined case), preventing manual operator triggers from running concurrently with a scheduled job.

### Execution order rationale
1. monthly→weekly first: new weekly buckets must exist before weekly→daily can evaluate them
2. weekly→daily second: operates on the (now updated) weekly bucket set
3. Horizon Extension last: append-only, no structural conflict with either transition

### Guarantees
- Exactly one zone-transition process runs at any time (global advisory lock)
- Deterministic execution order on 1st-Monday
- Each sub-job remains independently idempotent (B2 solution applies)
- Safe manual re-runs (lock prevents concurrent execution, idempotence prevents duplicate work)

---

## Summary of Schema Changes

```sql
-- B1: event coalescing
ALTER TABLE calc_runs ADD COLUMN triggered_by_event_ids UUID[];

-- B2: zone transition progress tracking
CREATE TABLE zone_transition_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type        TEXT NOT NULL,
    transition_date DATE NOT NULL,
    status          TEXT NOT NULL DEFAULT 'running',
    series_total    INT,
    series_done     INT DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    UNIQUE(job_type, transition_date)
);

-- B3: completed_stale status (documented, enforced in application layer)
COMMENT ON COLUMN calc_runs.status IS
    'pending | running | completed | completed_stale | failed';
```

---

## Open Issues (Not Blockers — Tracked Separately)

- **Stale detection implementability (R3 from QC review):** The stale detection mechanism relies on comparing historical node values. The schema currently retains the last computed value per node but not prior values. This is sufficient for snapshot-ID-based stale detection (B3 solution) but insufficient for value-level comparison. Full value-history tracking is deferred to V1 — for PoA, snapshot-ID comparison is adequate.

- **Scenario bleed (R4 from QC review):** No DB-level enforcement prevents writes to baseline nodes scoped with a variant scenario_id. This is enforced by application-layer convention only. A DB-level CHECK or trigger will be added in V1.
