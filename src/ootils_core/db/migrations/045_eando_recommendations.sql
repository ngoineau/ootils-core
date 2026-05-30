-- ============================================================
-- Migration 045 — Excess & Obsolete disposition recommendations
-- ============================================================
-- The E&O Watcher turns the read-only E&O valuation into governed, actionable
-- dispositions: per item sitting beyond its coverage threshold, a proposed action
-- (stop buying, review for disposition, hold to burn down…) at L1 DRAFT for human
-- approval. Frees working capital — the mirror image of the shortage tower.
--
-- Confidence is data-quality aware: an "obsolete" item still being replenished,
-- or one with no cost, is flagged NEEDS_DATA_REVIEW / LOW rather than asserting a
-- scrap. Reuses agent_runs; same state machine + Decision Ladder as the fleet.
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS eando_recommendations (
    eando_recommendation_id UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name              TEXT        NOT NULL,
    agent_run_id            UUID        NOT NULL REFERENCES agent_runs(agent_run_id),
    scenario_id             UUID        NOT NULL,

    item_id                 UUID        NOT NULL,
    item_external_id        TEXT        NOT NULL,

    classification          TEXT        NOT NULL CHECK (classification IN ('EXCESS','OBSOLETE')),
    on_hand                 NUMERIC     NOT NULL,
    annual_demand           NUMERIC,                 -- 0 for obsolete
    coverage_months         NUMERIC,                 -- NULL = infinite (no demand)
    excess_units            NUMERIC     NOT NULL,     -- on-hand beyond the threshold
    excess_value            NUMERIC,                 -- excess_units × cost; NULL if unpriced
    currency                TEXT,

    disposition             TEXT        NOT NULL
                            CHECK (disposition IN ('STOP_BUY','REVIEW','REDEPLOY','RETURN_SUPPLIER',
                                                   'DISCOUNT_SELL','SCRAP','HOLD')),

    decision_level          TEXT        NOT NULL DEFAULT 'L1'
                            CHECK (decision_level IN ('L0','L1','L2','L3','L4')),
    status                  TEXT        NOT NULL DEFAULT 'DRAFT'
                            CHECK (status IN ('DRAFT','REVIEWED','APPROVED','REJECTED','APPLIED','EXPIRED')),
    confidence              TEXT        NOT NULL DEFAULT 'MEDIUM'
                            CHECK (confidence IN ('HIGH','MEDIUM','LOW','NEEDS_DATA_REVIEW')),

    evidence                JSONB,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_eando_status ON eando_recommendations (status);
CREATE INDEX IF NOT EXISTS ix_eando_class  ON eando_recommendations (classification, status);
CREATE INDEX IF NOT EXISTS ix_eando_value  ON eando_recommendations (excess_value DESC);
CREATE INDEX IF NOT EXISTS ix_eando_item   ON eando_recommendations (item_id);
CREATE INDEX IF NOT EXISTS ix_eando_run    ON eando_recommendations (agent_run_id);

COMMIT;
