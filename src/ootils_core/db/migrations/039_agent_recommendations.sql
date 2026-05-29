-- ============================================================
-- Migration 039 — Agent recommendations + work ledger
-- ============================================================
-- Substrate for the agent fleet (North Star): persistent, governed,
-- auditable recommendations produced by watcher/scenario agents.
--
-- Decision Ladder (strategy doc §5): agents produce L1 DRAFT
-- recommendations only. State machine governs promotion to
-- APPROVED / APPLIED — those transitions require human approval.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- agent_runs — work ledger: every agent execution is logged
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_runs (
    agent_run_id  UUID         NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name    TEXT         NOT NULL,
    scenario_id   UUID         NOT NULL,
    status        TEXT         NOT NULL DEFAULT 'RUNNING'
                  CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    started_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    -- JSONB carve-out (CLAUDE.md policy): run metrics + scope are a
    -- diagnostic/forensic payload of unbounded shape — counts, timings,
    -- data-quality flags. Not business-queryable columns.
    metrics       JSONB,
    notes         TEXT
);
CREATE INDEX IF NOT EXISTS ix_agent_runs_name ON agent_runs (agent_name, started_at DESC);

-- ------------------------------------------------------------
-- recommendations — agent-produced supply actions, governed by
-- a state machine. Typed columns for everything business-queryable;
-- only the evidence trail is JSONB.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recommendations (
    recommendation_id    UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name           TEXT        NOT NULL,
    agent_run_id         UUID        NOT NULL REFERENCES agent_runs(agent_run_id),
    scenario_id          UUID        NOT NULL,

    -- What
    item_id              UUID        NOT NULL,
    item_external_id     TEXT        NOT NULL,
    shortage_date        DATE        NOT NULL,
    deficit_qty          NUMERIC     NOT NULL,
    recommended_qty      NUMERIC     NOT NULL,
    estimated_cost       NUMERIC,
    currency             TEXT,

    -- Who from
    supplier_id          UUID,
    supplier_external_id TEXT,
    lead_time_days       INTEGER,

    -- Timing / urgency
    runway_days          INTEGER,
    margin_days          INTEGER,
    action               TEXT        NOT NULL
                         CHECK (action IN ('EXPEDITE', 'ORDER_RUSH', 'ORDER_NOW')),

    -- Governance
    decision_level       TEXT        NOT NULL DEFAULT 'L1'
                         CHECK (decision_level IN ('L0','L1','L2','L3','L4')),
    status               TEXT        NOT NULL DEFAULT 'DRAFT'
                         CHECK (status IN ('DRAFT','REVIEWED','APPROVED','REJECTED','APPLIED','EXPIRED')),
    confidence           TEXT        NOT NULL DEFAULT 'MEDIUM'
                         CHECK (confidence IN ('HIGH','MEDIUM','LOW','NEEDS_DATA_REVIEW')),

    -- JSONB carve-out (CLAUDE.md policy): forensic evidence trail for
    -- explainability — the full computation that justifies this reco
    -- (deficit, lead time, runway, supplier choice reason). Unbounded
    -- diagnostic shape, not business-queryable columns.
    evidence             JSONB,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_reco_status     ON recommendations (status);
CREATE INDEX IF NOT EXISTS ix_reco_agent_run  ON recommendations (agent_run_id);
CREATE INDEX IF NOT EXISTS ix_reco_item       ON recommendations (item_id);
CREATE INDEX IF NOT EXISTS ix_reco_action     ON recommendations (action, status);

COMMIT;
