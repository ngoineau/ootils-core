-- ============================================================
-- Migration 054 — Pyramide reconciliation provenance (axis A, PR3)
-- ============================================================
-- pyramide_runs.recon_method was dead since migration 038 (stored but
-- never consumed). PR3 makes it REAL: it now carries the reconciliation
-- method EFFECTIVELY applied by the hierarchy layer
-- (src/ootils_core/pyramide/hierarchy/reconcile.py):
--   * 'middleout'            — deterministic middle-out by historical
--                              proportions (the guaranteed core path);
--   * 'mintrace_wls_shrink'  — optional MinT with shrinkage covariance
--                              via hierarchicalforecast (new value);
--   * 'none'                 — single-series leaf runs (no
--                              reconciliation applied — the honest
--                              value for the standalone runner);
--   * 'mintrace_wls', 'bottomup', 'topdown' stay accepted for
--     compatibility with rows written since 038.
--
-- Also repairs a latent 038 gap: SUPPORTED_METHODS gained 'SEASONAL'
-- (PR1) and forecasts / forecast_values CHECKs were extended then, but
-- pyramide_runs.method / pyramide_snapshots.method were not — a leaf
-- run with method='SEASONAL' would fail its INSERT. The hierarchical
-- runner persists method into both tables, so the gap is closed here.
--
-- Idempotence: DROP CONSTRAINT IF EXISTS + ADD (deterministic
-- re-create) — a re-run must not fail (repo migration policy).
-- No JSONB. Typed columns only.
-- ============================================================

BEGIN;

ALTER TABLE pyramide_runs DROP CONSTRAINT IF EXISTS pyramide_runs_recon_method_check;
ALTER TABLE pyramide_runs ADD CONSTRAINT pyramide_runs_recon_method_check CHECK (
    recon_method IN (
        'mintrace_wls', 'mintrace_wls_shrink',
        'bottomup', 'topdown', 'middleout', 'none'
    )
);

ALTER TABLE pyramide_runs DROP CONSTRAINT IF EXISTS pyramide_runs_method_check;
ALTER TABLE pyramide_runs ADD CONSTRAINT pyramide_runs_method_check CHECK (
    method IN (
        'MA', 'EXP_SMOOTHING', 'CROSTON', 'SEASONAL',
        'AUTO_SELECT', 'ENSEMBLE_STAT',
        'STAT_AUTOETS', 'STAT_AUTOARIMA',
        'ML_LGBM', 'FM_CHRONOS', 'FM_MOIRAI'
    )
);

ALTER TABLE pyramide_snapshots DROP CONSTRAINT IF EXISTS pyramide_snapshots_method_check;
ALTER TABLE pyramide_snapshots ADD CONSTRAINT pyramide_snapshots_method_check CHECK (
    method IN (
        'MA', 'EXP_SMOOTHING', 'CROSTON', 'SEASONAL',
        'AUTO_SELECT', 'ENSEMBLE_STAT',
        'STAT_AUTOETS', 'STAT_AUTOARIMA',
        'ML_LGBM', 'FM_CHRONOS', 'FM_MOIRAI'
    )
);

COMMENT ON COLUMN pyramide_runs.recon_method IS
    'Reconciliation method EFFECTIVELY applied to the run (never the '
    'rejected request): middleout = deterministic core, '
    'mintrace_wls_shrink = optional MinT edge, none = single-series leaf '
    'run. Made real by migration 054 (PR3).';

COMMIT;
