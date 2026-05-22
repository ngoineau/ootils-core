-- ============================================================
-- Migration 012: DQ Agent V1 — agent runs + enriched issues
-- ============================================================
-- JSONB carve-out (see REVIEW-2026-05 R7):
--   `dq_agent_runs.summary` stores the agent's per-run diagnostic payload:
--   counts (total_rows, issues_count, critical_count, warning_count,
--   stat_issues, temporal_issues, affected_items_count), the LLM
--   availability flag, and an unbounded list of priority actions emitted
--   by the LLM. The "no JSONB for business data" rule in CLAUDE.md
--   explicitly carves out diagnostic / staging payloads. This column is
--   the canonical diagnostic case the rule was designed for and is the
--   only acceptable JSONB use in the dq_agent subsystem.
-- ============================================================

-- Table tracking each DQ Agent execution
CREATE TABLE IF NOT EXISTS dq_agent_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID NOT NULL REFERENCES ingest_batches(batch_id),
    status          TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    model_used      TEXT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    summary         JSONB,  -- diagnostic payload (see header comment + REVIEW-2026-05 R7)
    llm_narrative   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dq_agent_runs_batch_id
    ON dq_agent_runs (batch_id);
CREATE INDEX IF NOT EXISTS idx_dq_agent_runs_status
    ON dq_agent_runs (status, created_at DESC);

-- Enrich data_quality_issues with agent columns
ALTER TABLE data_quality_issues
    ADD COLUMN IF NOT EXISTS impact_score   NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS agent_run_id   UUID REFERENCES dq_agent_runs(run_id),
    ADD COLUMN IF NOT EXISTS llm_explanation TEXT,
    ADD COLUMN IF NOT EXISTS llm_suggestion  TEXT;

CREATE INDEX IF NOT EXISTS idx_dq_issues_agent_run_id
    ON data_quality_issues (agent_run_id);
CREATE INDEX IF NOT EXISTS idx_dq_issues_impact_score
    ON data_quality_issues (impact_score DESC NULLS LAST);
