-- ============================================================
-- Migration 040 — Recommendation state-machine transitions (audit)
-- ============================================================
-- Every status change on a recommendation is logged here: who, when,
-- from→to, why. This is the audit backbone of the human control room
-- (strategy doc §8.4 — "every agent write must be auditable", and the
-- approval workflow for L3+ decisions).
--
-- `recommendations.status` is the denormalized current state (fast to
-- query); this table is the full event-sourced history.
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS recommendation_transitions (
    transition_id      UUID         NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    recommendation_id  UUID         NOT NULL REFERENCES recommendations(recommendation_id),
    from_status        TEXT,
    to_status          TEXT         NOT NULL,
    actor              TEXT         NOT NULL,     -- username, or agent name
    actor_kind         TEXT         NOT NULL DEFAULT 'human'
                       CHECK (actor_kind IN ('human', 'agent')),
    reason             TEXT,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_reco_trans_reco ON recommendation_transitions (recommendation_id, created_at);
CREATE INDEX IF NOT EXISTS ix_reco_trans_actor ON recommendation_transitions (actor, created_at DESC);

COMMIT;
