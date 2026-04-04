# Backlog — Calendar Architecture

**Created:** 2026-04-04  
**Status:** Noted — not designed, not scheduled  
**Source:** Architecture discussion with Nico

---

## Two distinct calendar categories

### Category 1 — Reporting Calendars (presentation layer only)

Handled by the TemporalBridge at query time. Zero impact on the computation engine.

| Calendar type | Description |
|---------------|-------------|
| Gregorian | Default ISO calendar |
| Fiscal month/year | Configurable start date (e.g., Feb 1 for retail) |
| 4-4-5 | Retail calendar: Q1 = 4w + 4w + 5w |
| 5-4-4 | Retail calendar variant |
| 4-5-4 | Retail calendar variant |
| Quarter | Aggregation view (3 months) |
| Half-year | Aggregation view (6 months) |

Implementation: `calendars` config table mapping reporting periods to ISO buckets. Bridge uses `calendar=` query parameter. Multiple simultaneous calendars supported (US fiscal + French calendaire).

---

### Category 2 — Operational Calendars (computation engine)

**These affect the planning engine directly.** They determine valid dates for supply and demand operations.

| Calendar type | Impact on engine |
|---------------|-----------------|
| Working days | Lead time calculation in business days vs. calendar days |
| Plant production calendar | Valid start/end dates for WorkOrders |
| Supplier delivery calendar | Valid receipt dates for POs (no delivery on weekends/holidays) |
| Warehouse receiving calendar | Valid receipt dates at a location |
| Shipping calendar | Valid ship dates for TransferSupply and CustomerOrder |
| Bank holidays | Per-country, affects PO payment terms and delivery date calculation |
| Customer calendar | Customer-specific receiving windows |

**Architectural implication:** Operational calendars are not a reporting concern. They must be integrated into:
- Due date / start date calculation for supply nodes
- Lead time offset computation in the TemporalBridge (when computing coverage windows)
- Feasibility checks during scenario simulation ("can this PO arrive on this date?")

---

## Design notes (not decided)

- Operational calendars are likely modeled as a `Calendar` node type in the graph (linked via `governed_by` edge to locations, suppliers, resources)
- A `Calendar` node contains a set of valid/invalid date rules (working days, exceptions, holidays)
- The computation kernel consults the calendar when computing `effective_date` for supply arrivals and demand need dates
- Multiple calendars can apply to the same operation (supplier calendar AND receiving calendar AND bank holidays)
- Calendar conflicts (supplier can ship, but warehouse can't receive) must be resolved by taking the most restrictive applicable calendar

---

## Priority

- **Reporting calendars:** Low priority for PoA. Add when reporting is needed.
- **Operational calendars:** Medium priority for V1. Required for accurate lead time calculation in real scenarios. Can be simplified for PoA (assume all days are working days) but must be designed properly before V1 goes live.

---

## Open questions (for future discussion)

1. Should calendars be a node type in the graph (first-class) or a configuration table (external to the graph)?
2. How do multiple overlapping calendars resolve conflicts (most restrictive? priority order?)?
3. Are working-day lead times stored as a property of the Policy node or the Supplier node?
4. How does the engine handle a PO whose due_date lands on a non-working day — auto-adjust to next working day, or flag for planner review?
