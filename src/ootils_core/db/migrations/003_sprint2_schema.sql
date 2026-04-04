-- ============================================================
-- Ootils Core — Migration 003: Sprint 2 Schema Corrections
--
-- Fixes zone_transition_runs table:
-- The 002 schema defined column names that don't match the
-- ZoneTransitionEngine implementation (Sprint 2).
-- This migration drops and recreates the table with the correct schema.
--
-- 002 had: transition_run_id, scenario_id, series_id, affected_start, affected_end
-- Correct:  id, job_type, transition_date, series_total, series_done
--
-- Safe to run on a fresh DB (zone_transition_runs has no inbound FK references).
-- ============================================================

-- Drop the misaligned table from migration 002
DROP TABLE IF EXISTS zone_transition_runs;

-- Recreate with schema matching ZoneTransitionEngine
CREATE TABLE IF NOT EXISTS zone_transition_runs (
    id                  UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Job classification: which type of zone boundary transition
    job_type            TEXT        NOT NULL
                        CHECK (job_type IN ('weekly_to_daily', 'monthly_to_weekly')),

    -- The calendar date on which this transition ran (Monday for w→d, 1st for m→w)
    transition_date     DATE        NOT NULL,

    -- Idempotency key: composite f"{job_type}:{series_id}:{as_of_date}"
    -- UNIQUE ensures at most one completed run per (job, series, date)
    idempotency_key     TEXT        NOT NULL,

    -- State machine
    status              TEXT        NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'completed', 'failed')),

    -- Progress counters
    series_total        INTEGER,               -- Number of series processed (NULL until complete)
    series_done         INTEGER     NOT NULL DEFAULT 0,

    -- Timing
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ,
    error_message       TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (idempotency_key)
);

-- Index: monitor pending/running runs by job type and date
CREATE INDEX IF NOT EXISTS idx_zone_transition_job_date
    ON zone_transition_runs (job_type, transition_date, status);

CREATE INDEX IF NOT EXISTS idx_zone_transition_idempotency
    ON zone_transition_runs (idempotency_key);
