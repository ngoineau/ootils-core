-- Migration 004: Sprint M3 — Explainability
-- Root cause chain persistence for every planning result.
-- Architecture: no JSONB — all fields are typed columns (ADR-004).

-- ---------------------------------------------------------------------------
-- Table: explanations
-- One row per shortage/result event that has a causal explanation.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS explanations (
    explanation_id      UUID        PRIMARY KEY,
    calc_run_id         UUID        NOT NULL REFERENCES calc_runs(calc_run_id),
    target_node_id      UUID        NOT NULL REFERENCES nodes(node_id),
    target_type         TEXT        NOT NULL,               -- 'Shortage', 'ProjectedInventory', …
    root_cause_node_id  UUID        REFERENCES nodes(node_id),  -- nullable
    summary             TEXT        NOT NULL,               -- 1-line plain English
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Table: causal_steps
-- Ordered steps in the causal chain for an explanation.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS causal_steps (
    step_id         UUID        PRIMARY KEY,
    explanation_id  UUID        NOT NULL REFERENCES explanations(explanation_id),
    step            INTEGER     NOT NULL,
    node_id         UUID,                   -- nullable (e.g. policy check step)
    node_type       TEXT,
    edge_type       TEXT,
    fact            TEXT        NOT NULL,   -- human-readable description
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_explanations_calc_run_id
    ON explanations (calc_run_id);

CREATE INDEX IF NOT EXISTS idx_explanations_target_node_id
    ON explanations (target_node_id);

CREATE INDEX IF NOT EXISTS idx_causal_steps_explanation_id
    ON causal_steps (explanation_id);
