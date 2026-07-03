-- ============================================================
-- Migration 055 — Pyramide backtest accuracy metrics (axis D, PR-D3)
-- ============================================================
-- Persists the rolling-origin backtest report (pyramide/accuracy.py,
-- AccuracyReport) of the model that PRODUCED a run's values, so agents
-- and planners can rank forecasts without re-running the backtest.
--
-- Row semantics — LIGNES-PAR-HORIZON (pilot arbitration):
--   * horizon IS NULL  -> the all-horizons AGGREGATE row (pooled
--     mase/wape/smape/bias/coverage of the report);
--   * horizon = h >= 1 -> the per-horizon row. The report only carries
--     RESIDUALS per horizon (actual - forecast), not the actuals — so
--     only the residual-derivable metrics are populated per horizon
--     (bias = -mean(residuals_h), counts). mase/wape/smape/coverage
--     need the per-horizon actuals and therefore stay NULL on horizon
--     rows: we persist what the report really contains, nothing more.
--
-- NULL semantics (None-honest contract of accuracy.py): a NULL metric
-- means "not computable on this data" (e.g. WAPE with zero total
-- demand, MASE with constant history, coverage without evaluated
-- intervals) — NEVER a masked 0. Consumers must treat NULL as "not
-- comparable".
--
-- UNIQUE NULLS NOT DISTINCT: plain UNIQUE treats NULLs as distinct in
-- Postgres, so two aggregate rows (horizon NULL) for the same run would
-- both be accepted. NULLS NOT DISTINCT (PG15+; the project runs PG16)
-- makes (run_id, NULL) collide — exactly one aggregate row per run.
--
-- Write path: repository.persist_accuracy_metrics does DELETE + INSERT
-- per run_id (full-set replace), so re-persisting a run is idempotent.
-- A run without a backtest report writes NO row (absence = no honest
-- backtest existed, e.g. ENSEMBLE_STAT blend or external backend).
--
-- Idempotence: IF NOT EXISTS everywhere (repo migration policy).
-- No JSONB. Typed columns only.
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS pyramide_accuracy_metrics (
    metric_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES pyramide_runs(run_id) ON DELETE CASCADE,
    -- NULL = all-horizons aggregate row; h >= 1 = per-horizon row.
    horizon         INT NULL CHECK (horizon IS NULL OR horizon >= 1),
    mase            NUMERIC NULL,
    wape            NUMERIC NULL,
    smape           NUMERIC NULL,
    bias            NUMERIC NULL,
    coverage        NUMERIC NULL,
    n_cutoffs       INT NOT NULL CHECK (n_cutoffs >= 0),
    n_observations  INT NOT NULL CHECK (n_observations >= 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- NULLS NOT DISTINCT: dedupe the aggregate (horizon NULL) row too.
    CONSTRAINT pyramide_accuracy_metrics_run_horizon_uniq
        UNIQUE NULLS NOT DISTINCT (run_id, horizon)
);

CREATE INDEX IF NOT EXISTS idx_pyramide_accuracy_metrics_run
    ON pyramide_accuracy_metrics (run_id);

COMMENT ON TABLE pyramide_accuracy_metrics IS
    'Rolling-origin backtest metrics of the model that produced a Pyramide '
    'run (accuracy.AccuracyReport). One aggregate row (horizon NULL) + one '
    'row per horizon step. NULL metric = not computable on this data '
    '(None-honest contract) — never a masked 0. Runs without a backtest '
    'report have no rows.';
COMMENT ON COLUMN pyramide_accuracy_metrics.horizon IS
    'NULL = all-horizons aggregate; h >= 1 = metrics of forecast step h.';
COMMENT ON COLUMN pyramide_accuracy_metrics.mase IS
    'Mean Absolute Scaled Error (per-cutoff naive scaling). NULL when no '
    'cutoff had a usable denominator, or on per-horizon rows (needs the '
    'per-horizon actuals, which the report does not carry).';
COMMENT ON COLUMN pyramide_accuracy_metrics.wape IS
    'sum|a-f| / sum|a| pooled over evaluated pairs. NULL when total actual '
    'demand is zero, or on per-horizon rows (needs the actuals).';
COMMENT ON COLUMN pyramide_accuracy_metrics.smape IS
    'Symmetric MAPE ratio in [0,2]. NULL on per-horizon rows (needs the '
    'actuals).';
COMMENT ON COLUMN pyramide_accuracy_metrics.bias IS
    'Mean signed error forecast - actual; POSITIVE = over-forecast (the '
    'stock-critical direction). On per-horizon rows: -mean(residuals_h), '
    'since report residuals are actual - forecast.';
COMMENT ON COLUMN pyramide_accuracy_metrics.coverage IS
    'Share of actuals inside prediction intervals. NULL for rolling-origin '
    'point-forecast backtests (no intervals were evaluated).';
COMMENT ON COLUMN pyramide_accuracy_metrics.n_cutoffs IS
    'Aggregate row: number of forecast origins evaluated. Per-horizon row: '
    'number of cutoffs that reached this horizon (= residual count).';
COMMENT ON COLUMN pyramide_accuracy_metrics.n_observations IS
    'Number of (actual, forecast) pairs behind the row''s metrics.';

COMMIT;
