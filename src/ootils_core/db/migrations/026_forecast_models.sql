-- ============================================================
-- Ootils Core — Migration 026: Forecast models
-- FORECAST-001: Data Model Forecast & Adjustments
-- ============================================================
-- Creates tables for statistical forecasting with adjustments support.
-- Integrates with existing graph architecture via forecasted_demand_for edge.
-- ============================================================

-- ============================================================
-- 1. Table forecasts — forecast headers per item/location/horizon
-- ============================================================

CREATE TABLE IF NOT EXISTS forecasts (
    forecast_id     UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id         UUID        NOT NULL REFERENCES items(item_id),
    location_id     UUID        NOT NULL REFERENCES locations(location_id),
    scenario_id     UUID        NOT NULL REFERENCES scenarios(scenario_id),
    horizon_start   DATE        NOT NULL,
    horizon_end     DATE        NOT NULL,
    granularity     TEXT        NOT NULL CHECK (granularity IN ('daily', 'weekly', 'monthly')),
    method          TEXT        NOT NULL CHECK (method IN ('MA', 'EXP_SMOOTHING', 'CROSTON', 'SEASONAL')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_forecast_horizon CHECK (horizon_end >= horizon_start)
);

CREATE INDEX IF NOT EXISTS idx_forecasts_item_location ON forecasts (item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_forecasts_scenario ON forecasts (scenario_id);
CREATE INDEX IF NOT EXISTS idx_forecasts_horizon ON forecasts (horizon_start, horizon_end);
CREATE INDEX IF NOT EXISTS idx_forecasts_granularity ON forecasts (granularity);


-- ============================================================
-- 2. Table forecast_values — individual forecast buckets
-- ============================================================

CREATE TABLE IF NOT EXISTS forecast_values (
    value_id                  UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    forecast_id               UUID        NOT NULL REFERENCES forecasts(forecast_id) ON DELETE CASCADE,
    forecast_date             DATE        NOT NULL,
    quantity                  NUMERIC(18,6) NOT NULL,
    method                    TEXT        NOT NULL CHECK (method IN ('MA', 'EXP_SMOOTHING', 'CROSTON', 'SEASONAL')),
    confidence_interval_lower NUMERIC(18,6),
    confidence_interval_upper NUMERIC(18,6),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_forecast_value_positive CHECK (quantity >= 0)
);

CREATE INDEX IF NOT EXISTS idx_forecast_values_forecast ON forecast_values (forecast_id);
CREATE INDEX IF NOT EXISTS idx_forecast_values_date ON forecast_values (forecast_date);
CREATE INDEX IF NOT EXISTS idx_forecast_values_method ON forecast_values (method);

-- Composite index for efficient lookups by item/location/date range via forecast join
CREATE INDEX IF NOT EXISTS idx_forecast_values_forecast_date ON forecast_values (forecast_id, forecast_date);


-- ============================================================
-- 3. Table forecast_adjustments — manual/programmatic overrides
-- ============================================================

CREATE TABLE IF NOT EXISTS forecast_adjustments (
    adjustment_id   UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    forecast_id     UUID        NOT NULL REFERENCES forecasts(forecast_id) ON DELETE CASCADE,
    value_id        UUID        REFERENCES forecast_values(value_id) ON DELETE CASCADE,
    adjustment_type TEXT        NOT NULL CHECK (adjustment_type IN ('manual', 'promotion', 'seasonality', 'event')),
    delta           NUMERIC(18,6) NOT NULL,
    delta_percent   NUMERIC(6,4),
    reason          TEXT,
    user_id         TEXT,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_adjustment_delta CHECK (
        (delta IS NOT NULL AND delta_percent IS NULL) OR
        (delta IS NULL AND delta_percent IS NOT NULL) OR
        (delta IS NOT NULL AND delta_percent IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_forecast_adjustments_forecast ON forecast_adjustments (forecast_id);
CREATE INDEX IF NOT EXISTS idx_forecast_adjustments_value ON forecast_adjustments (value_id);
CREATE INDEX IF NOT EXISTS idx_forecast_adjustments_type ON forecast_adjustments (adjustment_type);
CREATE INDEX IF NOT EXISTS idx_forecast_adjustments_user ON forecast_adjustments (user_id);


-- ============================================================
-- 4. Add 'forecasted_demand_for' edge type to edges.edge_type
-- ============================================================

DO $$
DECLARE
    v_constraint_def TEXT;
BEGIN
    SELECT pg_get_constraintdef(c.oid)
    INTO v_constraint_def
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE c.contype = 'c'
      AND c.conname LIKE '%edge_type%'
      AND t.relname = 'edges'
      AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
    LIMIT 1;

    IF v_constraint_def IS NOT NULL AND v_constraint_def NOT LIKE '%forecasted_demand_for%' THEN
        ALTER TABLE edges DROP CONSTRAINT IF EXISTS edges_edge_type_check;
        ALTER TABLE edges ADD CONSTRAINT edges_edge_type_check CHECK (
            edge_type IN (
                'replenishes', 'feeds_forward', 'consumes', 'depends_on',
                'transfers_to', 'pegged_to', 'governed_by',
                'transfers', 'requires', 'substitutes',
                'fulfills', 'produces', 'ghost_member',
                'bom_component', 'consumes_resource',
                'forecasted_demand_for'
            )
        );
    ELSIF v_constraint_def IS NULL THEN
        ALTER TABLE edges ADD CONSTRAINT edges_edge_type_check CHECK (
            edge_type IN (
                'replenishes', 'feeds_forward', 'consumes', 'depends_on',
                'transfers_to', 'pegged_to', 'governed_by',
                'transfers', 'requires', 'substitutes',
                'fulfills', 'produces', 'ghost_member',
                'bom_component', 'consumes_resource',
                'forecasted_demand_for'
            )
        );
    END IF;
END $$;


-- ============================================================
-- 5. Add updated_at trigger for forecasts table
-- ============================================================

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'forecasts') THEN
        DROP TRIGGER IF EXISTS trg_forecasts_updated_at ON forecasts;
        CREATE TRIGGER trg_forecasts_updated_at
            BEFORE UPDATE ON forecasts
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;


-- ============================================================
-- Comments
-- ============================================================

COMMENT ON TABLE forecasts IS 'Statistical forecast headers for item/location/horizon combinations';
COMMENT ON COLUMN forecasts.granularity IS 'Time granularity: daily, weekly, or monthly';
COMMENT ON COLUMN forecasts.method IS 'Statistical method used: MA, EXP_SMOOTHING, CROSTON, or SEASONAL';

COMMENT ON TABLE forecast_values IS 'Individual forecast quantities for each date bucket within a forecast horizon';
COMMENT ON COLUMN forecast_values.confidence_interval_lower IS 'Lower bound of confidence interval (optional)';
COMMENT ON COLUMN forecast_values.confidence_interval_upper IS 'Upper bound of confidence interval (optional)';

COMMENT ON TABLE forecast_adjustments IS 'Manual or programmatic adjustments applied to forecasts';
COMMENT ON COLUMN forecast_adjustments.adjustment_type IS 'Type of adjustment: manual, promotion, seasonality, or event';
COMMENT ON COLUMN forecast_adjustments.delta IS 'Absolute adjustment quantity (can be negative)';
COMMENT ON COLUMN forecast_adjustments.delta_percent IS 'Percentage adjustment (alternative to delta)';
COMMENT ON COLUMN forecast_adjustments.value_id IS 'NULL if adjustment applies to entire forecast, otherwise specific value';

COMMENT ON COLUMN edges.edge_type IS 'Added forecasted_demand_for: links ForecastDemand nodes to ProjectedInventory nodes';
