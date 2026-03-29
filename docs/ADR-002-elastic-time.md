# ADR-002: Object-Local Time (Elastic Time Model)

**Status:** Accepted  
**Date:** 2026-03-29  
**Author:** Nicolas GOINEAU

---

## Context

Every existing APS system imposes a **global time axis** on the entire planning model:
- SAP IBP: fiscal periods
- Kinaxis: configurable buckets (weekly, monthly) — but global
- Blue Yonder: fixed granularity per module

This creates a fundamental problem: **real supply chains are temporally heterogeneous.**

| Object | Real-world time representation |
|--------|-------------------------------|
| Purchase Order | Exact date (e.g., April 15, 2026) |
| Sales Forecast | Monthly bucket (e.g., April 2026 = 300 units) |
| Capacity | Weekly rhythm (e.g., week 15 = 400 hours) |
| S&OP Plan | Monthly / quarterly |
| Projected Stock | Daily snapshot |
| Safety Stock Policy | No time dimension — always applicable |

When a system forces all of these into the same bucket (e.g., weekly), it:
1. **Loses precision** on exact-date events (a PO due April 15 becomes "week 15")
2. **Requires arbitrary disaggregation** of monthly forecasts into weekly (prorated? front-loaded?)
3. **Creates false alignment** — the system pretends everything is synchronized when it's not
4. **Breaks AI agent queries** — an agent asking "what is the available stock on April 15 at 14:00?" cannot get a meaningful answer from a weekly-bucket system

---

## Decision

**Time is a property of the object, not a global axis.**

Each node carries its own temporal properties:

```
node {
  time_grain: "day" | "week" | "month" | "quarter" | "exact_datetime"
  time_ref: date or bucket anchor
  time_span_start: date
  time_span_end: date
}
```

### The Temporal Bridge

A dedicated engine component — the **Temporal Bridge** — handles all cross-granularity operations:

**Split** (coarse → fine)
```
monthly_forecast → daily_demand[]
Mode: FLAT (qty/days) | FRONT_LOAD | HISTORICAL_PROFILE
```

**Aggregate** (fine → coarse)
```
daily_projected_stock[] → weekly_view
```

**Align** (reconcile two different grains)
```
monthly_forecast ∩ weekly_capacity → daily_netting_window
```

**Coverage window** (determine consumption period)
```
order due Apr 15 → consumes forecast from Apr 1–15
```

### Forecast Consumption Rule

A critical business rule: **confirmed orders consume forecast** to avoid double-counting.

```
daily_demand(t) = max(0, forecast_daily(t) - confirmed_orders(t))
```

This rule lives in the Temporal Bridge, not in SQL.

---

## Consequences

**Positive:**
- The model reflects the real world — no information loss from forced bucketing
- AI agents can query at any granularity they need
- Planners can work at their natural horizon without system-imposed constraints
- Multi-granularity S&OP (weekly operational + monthly tactical + quarterly strategic) on the same engine

**Negative / Trade-offs:**
- Significantly more complex than a uniform bucket model
- The Temporal Bridge is a critical component that must be correct — errors here propagate everywhere
- Disaggregation rules must be configurable per item/location/segment

**Mitigations:**
- Temporal Bridge is isolated as a separate, well-tested module
- Default rule is FLAT (safe and simple) — more sophisticated rules are opt-in
- All temporal operations are deterministic and traceable

---

## Alternatives Considered

### Option A: Global daily grain (rejected)
Store everything at the day level. Simple but: (a) destroys monthly forecast semantics, (b) creates massive data volume for long horizons, (c) doesn't reflect how planners actually think.

### Option B: Configurable global grain (rejected)
Let the user choose weekly or monthly. This is what APS vendors do. It shifts the problem to the user and still loses precision on cross-granularity objects.

### Option C: Multiple parallel grids (rejected)
Maintain a daily grid, a weekly grid, and a monthly grid simultaneously. Creates synchronization complexity and multiple sources of truth.

---

## References
- [Digital twin-based multi-granularity synchronisation — Zhang et al. (2024)](../BIBLIOGRAPHY.md)
- [VIATRA framework — Varró et al. (2016)](../BIBLIOGRAPHY.md)
