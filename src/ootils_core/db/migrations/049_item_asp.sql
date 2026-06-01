-- ============================================================
-- Migration 049 — item_asp: derived Average Selling Price (rolling T12M)
-- ============================================================
-- ASP is a DERIVED metric, NOT a maintained static price:
--   ASP = trailing-12-month SUM(value_ext) / SUM(ordered_quantity)
--         over ASP-ELIGIBLE demand (demand_history.counts_for_asp = TRUE),
--         per (item, org).
--
-- ASP-eligible excludes warranty ($0), intercompany & drop-ship (billed
-- at-cost, not list), no-bill/invoice-only, and returns — i.e. it is the
-- real customer selling price. Recomputed periodically (scripts/compute_asp.py),
-- never hand-maintained.
--
-- Used as the units <-> $ bridge:
--   - Shipping Plan: Ad'hoc $ target -> units to ship  (docs/DESIGN-shipping-plan.md)
--   - Demand valuation: forecast $ = forecast units x current ASP
--
-- Per (item, org) because the two operating companies (PPS = USA / PCC =
-- Canada) price in different currencies; ASP is in the org's order currency.
-- (MVP granularity = item x org; extensible later to x channel/region.)
--
-- Additive new table; idempotent.
-- ============================================================

CREATE TABLE IF NOT EXISTS item_asp (
    item_id       UUID        NOT NULL REFERENCES items(item_id) ON DELETE RESTRICT,
    org_id        TEXT        NOT NULL,
    asp           NUMERIC,                 -- value_12m / units_12m
    units_12m     NUMERIC,
    value_12m     NUMERIC,
    window_start  DATE,
    window_end    DATE,
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (item_id, org_id)
);

COMMENT ON TABLE item_asp IS
    'Derived Average Selling Price per (item, org): trailing-12-month '
    'SUM(value_ext)/SUM(ordered_quantity) over ASP-eligible demand '
    '(counts_for_asp). Recomputed by scripts/compute_asp.py; the units<->$ '
    'bridge for the shipping plan and demand valuation. Currency = org order ccy.';

CREATE INDEX IF NOT EXISTS idx_item_asp_org ON item_asp (org_id);
