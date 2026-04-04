-- Migration 005: M4 Shortage Detection
-- Creates the shortages table with severity scoring and explanation linkage.

CREATE TABLE IF NOT EXISTS shortages (
    shortage_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id     UUID        NOT NULL REFERENCES scenarios(scenario_id),
    pi_node_id      UUID        NOT NULL REFERENCES nodes(node_id),
    item_id         UUID        REFERENCES items(item_id),
    location_id     UUID        REFERENCES locations(location_id),
    shortage_date   DATE        NOT NULL,
    shortage_qty    NUMERIC     NOT NULL,
    severity_score  NUMERIC     NOT NULL DEFAULT 0,
    explanation_id  UUID        REFERENCES explanations(explanation_id),
    calc_run_id     UUID        NOT NULL REFERENCES calc_runs(calc_run_id),
    status          TEXT        NOT NULL DEFAULT 'active'
                                CHECK (status IN ('active', 'resolved')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Unique constraint to support ON CONFLICT upsert in the detector
CREATE UNIQUE INDEX IF NOT EXISTS shortages_pi_node_calc_run_uidx
    ON shortages (pi_node_id, calc_run_id);

-- Query indexes
CREATE INDEX IF NOT EXISTS shortages_scenario_id_idx
    ON shortages (scenario_id);

CREATE INDEX IF NOT EXISTS shortages_pi_node_id_idx
    ON shortages (pi_node_id);

CREATE INDEX IF NOT EXISTS shortages_shortage_date_idx
    ON shortages (shortage_date);

CREATE INDEX IF NOT EXISTS shortages_status_idx
    ON shortages (status);
