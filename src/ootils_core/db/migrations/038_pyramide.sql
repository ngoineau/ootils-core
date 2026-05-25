-- ============================================================
-- Ootils Core - Migration 038: Pyramide forecast boundary
-- ============================================================
-- Adds typed metadata tables for the Pyramide forecasting layer.
-- Pyramide writes stochastic run metadata here, while deterministic
-- consumption remains fenced behind immutable forecast_values artifacts.
-- ============================================================

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

CREATE TABLE IF NOT EXISTS pyramide_runs (
    run_id                 UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    forecast_id            UUID        NOT NULL UNIQUE REFERENCES forecasts(forecast_id) ON DELETE CASCADE,
    item_id                UUID        NOT NULL REFERENCES items(item_id),
    location_id            UUID        NOT NULL REFERENCES locations(location_id),
    scenario_id            UUID        NOT NULL REFERENCES scenarios(scenario_id),
    horizon_start          DATE        NOT NULL,
    horizon_end            DATE        NOT NULL,
    granularity            TEXT        NOT NULL CHECK (granularity IN ('daily', 'weekly', 'monthly')),
    method                 TEXT        NOT NULL CHECK (method IN (
        'MA', 'EXP_SMOOTHING', 'CROSTON',
        'AUTO_SELECT', 'ENSEMBLE_STAT',
        'STAT_AUTOETS', 'STAT_AUTOARIMA',
        'ML_LGBM', 'FM_CHRONOS', 'FM_MOIRAI'
    )),
    model_strategy         TEXT        NOT NULL DEFAULT 'stat' CHECK (model_strategy IN ('auto', 'stat', 'ml', 'fm', 'hybrid')),
    recon_method           TEXT        NOT NULL DEFAULT 'bottomup' CHECK (recon_method IN ('mintrace_wls', 'bottomup', 'topdown', 'middleout', 'none')),
    random_seed            BIGINT      NOT NULL DEFAULT 0,
    code_version           TEXT        NOT NULL DEFAULT 'local',
    selected_model         TEXT        NOT NULL DEFAULT 'AUTO_SELECT',
    engine_backend         TEXT        NOT NULL DEFAULT 'internal:auto_select',
    source_history_count   INTEGER     NOT NULL CHECK (source_history_count >= 0),
    status                 TEXT        NOT NULL DEFAULT 'generated' CHECK (status IN ('generated', 'committed', 'failed')),
    deterministic_artifact TEXT        NOT NULL DEFAULT 'forecast_values',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    committed_at           TIMESTAMPTZ,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_pyramide_run_horizon CHECK (horizon_end >= horizon_start)
);

CREATE INDEX IF NOT EXISTS idx_pyramide_runs_forecast ON pyramide_runs (forecast_id);
CREATE INDEX IF NOT EXISTS idx_pyramide_runs_item_location ON pyramide_runs (item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_pyramide_runs_scenario ON pyramide_runs (scenario_id);
CREATE INDEX IF NOT EXISTS idx_pyramide_runs_status ON pyramide_runs (status);
CREATE INDEX IF NOT EXISTS idx_pyramide_runs_created_at ON pyramide_runs (created_at DESC);

CREATE TABLE IF NOT EXISTS pyramide_snapshots (
    snapshot_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id             UUID        NOT NULL UNIQUE REFERENCES pyramide_runs(run_id) ON DELETE CASCADE,
    forecast_id         UUID        NOT NULL UNIQUE REFERENCES forecasts(forecast_id) ON DELETE CASCADE,
    item_id             UUID        NOT NULL REFERENCES items(item_id),
    location_id         UUID        NOT NULL REFERENCES locations(location_id),
    scenario_id         UUID        NOT NULL REFERENCES scenarios(scenario_id),
    horizon_start       DATE        NOT NULL,
    horizon_end         DATE        NOT NULL,
    granularity         TEXT        NOT NULL CHECK (granularity IN ('daily', 'weekly', 'monthly')),
    method              TEXT        NOT NULL CHECK (method IN (
        'MA', 'EXP_SMOOTHING', 'CROSTON',
        'AUTO_SELECT', 'ENSEMBLE_STAT',
        'STAT_AUTOETS', 'STAT_AUTOARIMA',
        'ML_LGBM', 'FM_CHRONOS', 'FM_MOIRAI'
    )),
    frozen_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    value_count         INTEGER     NOT NULL CHECK (value_count >= 0),
    total_quantity      NUMERIC(18,6) NOT NULL DEFAULT 0,
    immutable_artifact  TEXT        NOT NULL DEFAULT 'forecast_values',

    CONSTRAINT chk_pyramide_snapshot_horizon CHECK (horizon_end >= horizon_start),
    CONSTRAINT chk_pyramide_snapshot_total CHECK (total_quantity >= 0)
);

CREATE INDEX IF NOT EXISTS idx_pyramide_snapshots_run ON pyramide_snapshots (run_id);
CREATE INDEX IF NOT EXISTS idx_pyramide_snapshots_forecast ON pyramide_snapshots (forecast_id);
CREATE INDEX IF NOT EXISTS idx_pyramide_snapshots_item_location ON pyramide_snapshots (item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_pyramide_snapshots_frozen_at ON pyramide_snapshots (frozen_at DESC);

CREATE TABLE IF NOT EXISTS pyramide_snapshot_demand_nodes (
    snapshot_id     UUID        NOT NULL REFERENCES pyramide_snapshots(snapshot_id) ON DELETE CASCADE,
    value_id        UUID        NOT NULL REFERENCES forecast_values(value_id) ON DELETE CASCADE,
    demand_node_id  UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (snapshot_id, value_id),
    UNIQUE (demand_node_id)
);

CREATE INDEX IF NOT EXISTS idx_pyramide_snapshot_demand_nodes_node
    ON pyramide_snapshot_demand_nodes (demand_node_id);

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'pyramide_runs'
    ) THEN
        DROP TRIGGER IF EXISTS trg_pyramide_runs_updated_at ON pyramide_runs;
        CREATE TRIGGER trg_pyramide_runs_updated_at
            BEFORE UPDATE ON pyramide_runs
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

COMMENT ON TABLE pyramide_runs IS 'Typed stochastic forecast run metadata for the Pyramide layer';
COMMENT ON COLUMN pyramide_runs.deterministic_artifact IS 'Fence handed to the deterministic engine; currently forecast_values';
COMMENT ON TABLE pyramide_snapshots IS 'Immutable Pyramide forecast snapshot headers referencing frozen forecast_values';
COMMENT ON COLUMN pyramide_snapshots.immutable_artifact IS 'Business artifact containing the frozen values for this snapshot';
COMMENT ON TABLE pyramide_snapshot_demand_nodes IS 'Idempotent mapping from committed Pyramide snapshot values to deterministic ForecastDemand nodes';
