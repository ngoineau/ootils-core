-- Demo run audit/history
--
-- JSONB carve-out (see CLAUDE.md "no JSONB for business data — explicit
-- carve-outs for diagnostic/staging payloads"):
--   `demo_runs.artifact` captures the full per-step output payload of a
--   demo execution (e.g. forecast snapshot, MPS draft, ATP probe). The
--   shape changes between demo flows and demo evolutions; the table is a
--   forensic audit trail, never queried as business data. Typed columns
--   above cover everything that is queried (status, counts, durations).
--   This is one of the three documented carve-outs alongside
--   `dq_agent_runs.summary` (mig 012) and `mrp_runs.errors`/`warnings`
--   (mig 021).

CREATE TABLE IF NOT EXISTS demo_runs (
    demo_run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    demo_name TEXT NOT NULL DEFAULT 'phase1',
    status TEXT NOT NULL CHECK (status IN ('ok', 'error')),
    item_external_id TEXT,
    location_external_id TEXT,
    forecast_total NUMERIC(18,6),
    forecast_buckets INTEGER,
    mps_nodes_created INTEGER,
    mps_total_demand NUMERIC(18,6),
    approval_status TEXT,
    mrp_status TEXT,
    planned_supplies_created INTEGER,
    crp_planned_orders_count INTEGER,
    crp_work_centers_count INTEGER,
    crp_load_profiles INTEGER,
    atp_requested_quantity NUMERIC(18,6),
    atp_quantity_available NUMERIC(18,6),
    atp_buckets INTEGER,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    artifact JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_demo_runs_name_created ON demo_runs(demo_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_demo_runs_status_created ON demo_runs(status, created_at DESC);
