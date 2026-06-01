-- ============================================================
-- Migration 048 — demand_history: org_id (entity) + order_type (buy program)
-- ============================================================
-- Two additive, nullable columns on demand_history (no data change):
--
--   org_id     — operating company from the ERP (ORG_ID):
--                'PPS' = USA (+ intl), 'PCC' = Canada. Canada is an
--                independent end-market; the `CN *` line/order types are PCC.
--                Needed to (a) forecast per company and (b) avoid the
--                multi-entity double-count: intercompany PPS->PCC replenishes
--                Canada, so the SAME demand appears as PCC end-customer demand
--                AND as the interco order. End-demand forecast filters
--                fulfillment != 'inter_entity'; the DRP generates interco as
--                dependent demand. See memory/demand-business-rule-booking.
--
--   order_type — the ERP ORDER_TYPE, which carries the BUY PROGRAM
--                (SPRING BUY / SUMMER BUY / EARLY BUY / CN EARLY BUY /
--                LESLIES FWD BUY ...) alongside STANDARD VISTA etc. The buy
--                programs ARE the aggregate seasonality; segmenting by program
--                isolates clean, near-deterministic-timing signals.
--
-- Additive nullable columns: no backfill needed at DDL time (the re-ingest
-- repopulates demand_history). Idempotent.
-- ============================================================

ALTER TABLE demand_history ADD COLUMN IF NOT EXISTS org_id     TEXT;
ALTER TABLE demand_history ADD COLUMN IF NOT EXISTS order_type TEXT;

COMMENT ON COLUMN demand_history.org_id IS
    'Operating company (ERP ORG_ID): PPS = USA (+intl), PCC = Canada. '
    'Forecast/DRP run per company. Canada end-demand vs PPS->PCC intercompany '
    'must not be double-counted (see fulfillment=inter_entity).';

COMMENT ON COLUMN demand_history.order_type IS
    'ERP ORDER_TYPE — carries the buy program (SPRING/SUMMER/EARLY BUY, '
    'FWD BUY, CN *) plus STANDARD VISTA etc. Buy programs drive the seasonality; '
    'forecast segments by (org_id, buy program).';

-- Common access paths: by company over time, and by order_type (program).
CREATE INDEX IF NOT EXISTS idx_demand_history_org_booked
    ON demand_history (org_id, booked_date);
CREATE INDEX IF NOT EXISTS idx_demand_history_order_type
    ON demand_history (order_type);
