-- ============================================================
-- Migration 057 — Pyramide method CHECK hygiene (axis B, PR-B0)
-- ============================================================
-- Moirai (Salesforce) = cc-by-nc-4.0, EXCLU commercialement (décision
-- verrouillée 2026-05-31) — toléré en DB pour l'historique, REJETÉ par
-- l'application (retiré de SUPPORTED_METHODS / EXTERNAL_METHODS ; une
-- requête API avec FM_MOIRAI reçoit le 422 standard des méthodes
-- inconnues).
--
-- État constaté avant cette migration :
--   * 038 avait créé pyramide_runs.method / pyramide_snapshots.method
--     SANS 'SEASONAL' (divergence avec forecasts.method) ;
--   * 054 a déjà réparé cette divergence (SEASONAL ajouté aux deux
--     CHECKs pyramide) — on ne la refait pas ici.
-- Cette migration re-affirme donc les quatre CHECKs method sur la MÊME
-- liste canonique (celle de forecasts, SEASONAL inclus, FM_MOIRAI
-- conservé pour les lignes historiques) et documente la décision de
-- licence au niveau colonne. Une base neuve qui rejoue 038→057 comme
-- une base existante qui n'applique que 057 aboutissent au même état.
--
-- Idempotence : DROP CONSTRAINT IF EXISTS + ADD (re-création
-- déterministe) — un re-run ne doit pas échouer (politique migrations).
-- No JSONB. Typed columns only.
-- ============================================================

BEGIN;

ALTER TABLE forecasts DROP CONSTRAINT IF EXISTS forecasts_method_check;
ALTER TABLE forecasts ADD CONSTRAINT forecasts_method_check CHECK (
    method IN (
        'MA', 'EXP_SMOOTHING', 'CROSTON', 'SEASONAL',
        'AUTO_SELECT', 'ENSEMBLE_STAT',
        'STAT_AUTOETS', 'STAT_AUTOARIMA',
        'ML_LGBM', 'FM_CHRONOS', 'FM_MOIRAI'
    )
);

ALTER TABLE forecast_values DROP CONSTRAINT IF EXISTS forecast_values_method_check;
ALTER TABLE forecast_values ADD CONSTRAINT forecast_values_method_check CHECK (
    method IN (
        'MA', 'EXP_SMOOTHING', 'CROSTON', 'SEASONAL',
        'AUTO_SELECT', 'ENSEMBLE_STAT',
        'STAT_AUTOETS', 'STAT_AUTOARIMA',
        'ML_LGBM', 'FM_CHRONOS', 'FM_MOIRAI'
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

COMMENT ON COLUMN forecasts.method IS
    'Forecast method. FM_MOIRAI tolerated for historical rows only: '
    'Moirai (Salesforce) is cc-by-nc-4.0, commercially EXCLUDED '
    '(decision locked 2026-05-31); the application rejects it (422).';
COMMENT ON COLUMN forecast_values.method IS
    'Forecast method. FM_MOIRAI tolerated for historical rows only: '
    'Moirai (Salesforce) is cc-by-nc-4.0, commercially EXCLUDED '
    '(decision locked 2026-05-31); the application rejects it (422).';
COMMENT ON COLUMN pyramide_runs.method IS
    'Forecast method. FM_MOIRAI tolerated for historical rows only: '
    'Moirai (Salesforce) is cc-by-nc-4.0, commercially EXCLUDED '
    '(decision locked 2026-05-31); the application rejects it (422).';
COMMENT ON COLUMN pyramide_snapshots.method IS
    'Forecast method. FM_MOIRAI tolerated for historical rows only: '
    'Moirai (Salesforce) is cc-by-nc-4.0, commercially EXCLUDED '
    '(decision locked 2026-05-31); the application rejects it (422).';

COMMIT;
