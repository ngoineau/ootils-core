-- ============================================================
-- Migration 050 — returns_history: the returns series (separate from demand)
-- ============================================================
-- Per the sign rule (memory/demand-business-rule-booking): demand =
-- POSITIVE quantities only; NEGATIVE quantities are RETURNS and are NEVER
-- netted into demand. demand_history holds only positive demand; this table
-- holds the returns (the negative-quantity lines), as a SEPARATE series.
--
-- Used for:
--   - the NET of the shipping-plan Ad'hoc  (net $ = shipments $ − returns $,
--     at company level / month) — docs/DESIGN-shipping-plan.md
--   - return-rate analytics per item (cf. T_DTK.RETURN_PERCENTAGE)
--
-- Magnitudes stored POSITIVE (return_quantity / return_value ≥ 0); the
-- consumer subtracts. Additive new table; idempotent.
-- ============================================================

CREATE TABLE IF NOT EXISTS returns_history (
    id              BIGSERIAL   NOT NULL PRIMARY KEY,
    item_id         UUID        REFERENCES items(item_id) ON DELETE RESTRICT,
    item_code       TEXT        NOT NULL,
    org_id          TEXT,
    return_date     DATE,                    -- booked date of the credit / return line
    return_quantity NUMERIC,                 -- positive magnitude
    return_value    NUMERIC,                 -- positive magnitude ($)
    line_type       TEXT,
    warehouse_id    TEXT,
    ship_state      TEXT,
    channel         TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE returns_history IS
    'Returns series (negative-quantity sales lines), kept SEPARATE from '
    'demand_history per the sign rule (returns are never netted into demand). '
    'Magnitudes stored positive. Feeds the net of the shipping-plan Ad''hoc '
    '(shipments $ − returns $) and per-item return-rate analytics.';

CREATE INDEX IF NOT EXISTS idx_returns_history_item_date ON returns_history (item_id, return_date);
CREATE INDEX IF NOT EXISTS idx_returns_history_org_date  ON returns_history (org_id, return_date);
