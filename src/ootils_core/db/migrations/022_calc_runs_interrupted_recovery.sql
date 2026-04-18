-- 022_calc_runs_interrupted_recovery.sql
-- Add explicit 'interrupted' calc_run status for crash recovery / startup replay.

ALTER TABLE calc_runs
    DROP CONSTRAINT IF EXISTS calc_runs_status_check;

ALTER TABLE calc_runs
    ADD CONSTRAINT calc_runs_status_check
    CHECK (status IN ('pending', 'running', 'completed', 'completed_stale', 'interrupted', 'failed'));
