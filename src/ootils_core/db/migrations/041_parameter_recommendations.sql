-- ============================================================
-- Migration 041 — Parameter recommendations (param-piloting fleet)
-- ============================================================
-- The "Pilotage des paramètres" agent family (North Star / AGENT-FLEET
-- catalog) does NOT produce supply actions — it proposes ADJUSTMENTS to
-- planning parameters (lot-size rule, MOQ, order multiple, POQ periods…).
--
-- Negotiated supplier terms (MOQ, multiple) are the DEFAULT and are always
-- respected by the engine. These agents observe the realized plan and
-- propose tuning as L1 DRAFT recommendations into a governed queue — a human
-- approves before any parameter is changed. Same Decision Ladder, same audit
-- backbone as procurement recos, but a distinct typed shape.
--
-- Reuses agent_runs (migration 039) as the work ledger.
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS parameter_recommendations (
    parameter_recommendation_id UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name                  TEXT        NOT NULL,
    agent_run_id                UUID        NOT NULL REFERENCES agent_runs(agent_run_id),
    scenario_id                 UUID        NOT NULL,

    -- What entity is being tuned
    item_id                     UUID        NOT NULL,
    item_external_id            TEXT        NOT NULL,

    -- The proposed change (parameter-agnostic: values stored as text)
    parameter                   TEXT        NOT NULL,   -- e.g. moq / order_multiple / lot_size_rule / lot_size_poq_periods
    current_value               TEXT,
    proposed_value              TEXT        NOT NULL,
    change_type                 TEXT        NOT NULL
                                CHECK (change_type IN ('RENEGOTIATE_MOQ','REVIEW_MULTIPLE','SET_LOT_RULE','DATA_REVIEW')),
    rationale_code              TEXT        NOT NULL,   -- MOQ_EXCESS_WOS / LFL_TOO_FREQUENT / MULTIPLE_OVERHANG / SPORADIC_DEMAND …

    -- Quantified justification (business-queryable)
    weeks_of_supply             NUMERIC,    -- weeks one order under current policy covers
    annual_demand               NUMERIC,
    est_inventory_impact_units  NUMERIC,    -- signed cycle-stock change if applied (neg = reduction)

    -- Governance (same ladder + state machine as procurement recos)
    decision_level              TEXT        NOT NULL DEFAULT 'L1'
                                CHECK (decision_level IN ('L0','L1','L2','L3','L4')),
    status                      TEXT        NOT NULL DEFAULT 'DRAFT'
                                CHECK (status IN ('DRAFT','REVIEWED','APPROVED','REJECTED','APPLIED','EXPIRED')),
    confidence                  TEXT        NOT NULL DEFAULT 'MEDIUM'
                                CHECK (confidence IN ('HIGH','MEDIUM','LOW','NEEDS_DATA_REVIEW')),

    -- JSONB carve-out (CLAUDE.md policy): forensic evidence trail —
    -- the realized-plan computation that justifies the proposal
    -- (order count, avg order qty, MOQ-binding share, demand stability).
    evidence                    JSONB,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_param_reco_status  ON parameter_recommendations (status);
CREATE INDEX IF NOT EXISTS ix_param_reco_run     ON parameter_recommendations (agent_run_id);
CREATE INDEX IF NOT EXISTS ix_param_reco_item    ON parameter_recommendations (item_id);
CREATE INDEX IF NOT EXISTS ix_param_reco_change  ON parameter_recommendations (change_type, status);

-- Event-sourced audit, mirroring recommendation_transitions (migration 040)
CREATE TABLE IF NOT EXISTS parameter_recommendation_transitions (
    transition_id               UUID         NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    parameter_recommendation_id UUID         NOT NULL REFERENCES parameter_recommendations(parameter_recommendation_id),
    from_status                 TEXT,
    to_status                   TEXT         NOT NULL,
    actor                       TEXT         NOT NULL,
    actor_kind                  TEXT         NOT NULL DEFAULT 'human'
                                CHECK (actor_kind IN ('human','agent')),
    reason                      TEXT,
    created_at                  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_param_trans_reco  ON parameter_recommendation_transitions (parameter_recommendation_id, created_at);
CREATE INDEX IF NOT EXISTS ix_param_trans_actor ON parameter_recommendation_transitions (actor, created_at DESC);

COMMIT;
