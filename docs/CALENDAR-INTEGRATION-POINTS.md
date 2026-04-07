# Calendar Integration Points

> Document tracking locations in the engine where `timedelta`-based date
> arithmetic should eventually be replaced by `add_working_days()` /
> `add_working_days_sync()` from `ootils_core.engine.kernel.calc.calendar`.

## Status

- `add_working_days_sync()` and `add_working_days()` are available as of
  `feat/calendars`.
- The API endpoints (`POST /v1/ingest/calendars`, `GET /v1/calendars/*`,
  `POST /v1/calendars/working-days`) are live.
- Engine integration is **best-effort / future work** — no calendar-aware
  date arithmetic was found in the active engine compute paths (lead_time_days
  is used only for statistical safety-stock calculations, not for scheduling).

---

## Audit of `timedelta` usage in engine

### `src/ootils_core/engine/orchestration/propagator.py`

| Line | Expression | Purpose | Calendar-aware? |
|------|-----------|---------|-----------------|
| 107 | `max(old_date, new_date) + timedelta(days=365)` | Propagation window upper bound | No — administrative cap, not a delivery date. Not a candidate. |
| 110 | `old_date + timedelta(days=365)` | Same | No |
| 113 | `new_date + timedelta(days=365)` | Same | No |

### `src/ootils_core/engine/kernel/temporal/bridge.py`

| Line | Expression | Purpose | Calendar-aware? |
|------|-----------|---------|-----------------|
| 77  | `d - timedelta(days=d.weekday())` | Floor to Monday for bucket alignment | No — time-grain arithmetic, not delivery scheduling. |
| 99  | `bucket_start + timedelta(days=1)` | Next day bucket | No |
| 101 | `bucket_start + timedelta(weeks=1)` | Next week bucket | No |

### `src/ootils_core/engine/kernel/temporal/zone_transition.py`

| Line | Expression | Purpose | Calendar-aware? |
|------|-----------|---------|-----------------|
| 45  | `today + timedelta(weeks=daily_horizon_weeks)` | Horizon cutoff | No |
| 359 | `current + timedelta(days=1)` | Day iteration | No |
| 426, 429, 434 | Week arithmetic | Bucket boundaries | No |

### `src/ootils_core/engine/policies.py`

Uses `lead_time_days` as a **scalar float** for statistical formulas
(safety stock, reorder point). Not a date computation — not a candidate.

### `src/ootils_core/engine/decision_engine.py`

Uses `lead_time_days` for scoring / comparison only — no calendar date arithmetic.

---

## Where to integrate when scheduling is added

When the engine gains an explicit `planned_delivery_date` calculation
(e.g., `order_date + lead_time_days → delivery_date`), replace with:

```python
from ootils_core.engine.kernel.calc.calendar import add_working_days_sync

planned_delivery_date = add_working_days_sync(
    conn=db,
    location_id=destination_location_id,
    start_date=order_date,
    n=lead_time_days,
)
```

Candidate locations (not yet implemented as of 2026-04-07):
- `engine/orchestration/calc_run.py` — if/when replenishment scheduling is added
- `engine/supplier_selection.py` — if earliest-delivery-date scoring is added

---

## ADR reference

- ADR-009: Import pipeline & operational calendars
- Convention: **absence = working day** (safe-by-default)
