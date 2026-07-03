-- ============================================================
-- Migration 058 — Pyramide routing provenance (axis B, PR-B1)
-- ============================================================
-- The head/tail router (src/ootils_core/pyramide/routing.py, spec
-- docs/DESIGN-pyramide-forecasting.md §5) decides a forecast METHOD and
-- a forecast LEVEL per series. Governance requirement of the spec:
-- "chaque forecast porte sa provenance (méthode, niveau, pourquoi ce
-- routage)". These three typed columns carry it on pyramide_runs.
--
-- NULL = the run was NOT routed: its method was requested explicitly by
-- the caller (the historical behaviour, which PR-B1 does not change —
-- routing is opt-in). All-or-nothing CHECK: a routed run always carries
-- the three fields together (RoutingDecision is method + level +
-- reason), a partial provenance would be unauditable.
--
-- routed_method is intentionally UNCHECKED against the method catalogue:
-- the router's vocabulary is a superset of the executable methods (e.g.
-- 'TWIN' is named in B1, wired in B2) and the routed method is what the
-- router RECOMMENDED, not necessarily what ran (pyramide_runs.method
-- stays the executed contract, with its own CHECK from migration 054).
--
-- Idempotence (repo migration policy: a re-run must not fail):
--   * ADD COLUMN IF NOT EXISTS          — no-op on re-run.
--   * DROP CONSTRAINT IF EXISTS + ADD   — deterministic re-create.
-- No JSONB. Typed columns only.
-- ============================================================

BEGIN;

ALTER TABLE pyramide_runs ADD COLUMN IF NOT EXISTS routed_method  TEXT;
ALTER TABLE pyramide_runs ADD COLUMN IF NOT EXISTS routed_level   TEXT;
ALTER TABLE pyramide_runs ADD COLUMN IF NOT EXISTS routing_reason TEXT;

ALTER TABLE pyramide_runs DROP CONSTRAINT IF EXISTS chk_pyramide_runs_routed_level;
ALTER TABLE pyramide_runs ADD CONSTRAINT chk_pyramide_runs_routed_level CHECK (
    routed_level IS NULL OR routed_level IN ('leaf', 'aggregate')
);

ALTER TABLE pyramide_runs DROP CONSTRAINT IF EXISTS chk_pyramide_runs_routing_all_or_none;
ALTER TABLE pyramide_runs ADD CONSTRAINT chk_pyramide_runs_routing_all_or_none CHECK (
    (routed_method IS NULL AND routed_level IS NULL AND routing_reason IS NULL)
    OR
    (routed_method IS NOT NULL AND routed_level IS NOT NULL AND routing_reason IS NOT NULL)
);

COMMENT ON COLUMN pyramide_runs.routed_method IS
    'Method RECOMMENDED by the head/tail router (spec §5). NULL = run not '
    'routed (method requested explicitly by the caller). May name a '
    'routing-vocabulary method not yet executable (e.g. TWIN, wired in B2).';
COMMENT ON COLUMN pyramide_runs.routed_level IS
    'Forecast level the router chose: leaf (forecast the series itself) or '
    'aggregate (forecast the parent node + disaggregate). NULL = run not routed.';
COMMENT ON COLUMN pyramide_runs.routing_reason IS
    'Short auditable sentence explaining the routing branch, with the '
    'feature values that triggered it (ADR-004). NULL = run not routed.';

COMMIT;
