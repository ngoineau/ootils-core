-- ============================================================
-- Migration 052 — scenario_promotions audit table
-- ============================================================
-- Promoting a scenario to baseline is an L3+ decision (Decision
-- Ladder, strategy doc §5) that patches baseline nodes — until now
-- it left no audit trail at all. One row per successful promote
-- (POST /v1/scenarios/{id}/promote, chantier #341b):
--   who (promoted_by), when (promoted_at), how much (override_count),
--   whether divergence was checked (conflict_checked — always TRUE for
--   the API path; column exists so legacy/backfilled rows can say FALSE),
--   and how many sibling scenarios were logically invalidated.
--
-- Typed columns only, no JSONB (per CLAUDE.md carve-out policy).
-- The promote event itself reuses the existing 'scenario_merge'
-- event_type (migration 006 CHECK) — no CHECK change needed here.
-- Conflicting promotes write NOTHING (409 + PromoteConflictError),
-- so this table records successes only.
-- ============================================================

CREATE TABLE IF NOT EXISTS scenario_promotions (
    promotion_id         UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id          UUID        NOT NULL REFERENCES scenarios(scenario_id),
    promoted_by          TEXT        NOT NULL,           -- human actor (L3+ gate)
    promoted_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason               TEXT,                           -- optional justification
    override_count       INTEGER     NOT NULL DEFAULT 0, -- overrides replayed on baseline
    conflict_checked     BOOLEAN     NOT NULL DEFAULT TRUE,
    siblings_invalidated INTEGER     NOT NULL DEFAULT 0  -- active same-parent scenarios now stale
);

CREATE INDEX IF NOT EXISTS idx_scenario_promotions_scenario
    ON scenario_promotions (scenario_id);

CREATE INDEX IF NOT EXISTS idx_scenario_promotions_promoted_at
    ON scenario_promotions (promoted_at);
