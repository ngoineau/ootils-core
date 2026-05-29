-- ============================================================
-- Migration 043 — pyramide scenario FKs → ON DELETE RESTRICT
-- ============================================================
-- 032_scenario_fk_retention converted every scenario_id FK to ON DELETE
-- RESTRICT, but 038_pyramide created pyramide_runs / pyramide_snapshots
-- AFTERWARDS, so their scenario_id FKs kept the default NO ACTION and slipped
-- past the governance net. The schema-governance test
-- (tests/integration/test_scenario_fk_retention.py::test_all_scenario_fks_are_restrict)
-- requires ON DELETE RESTRICT on every scenario_id FK so a scenario can never be
-- deleted out from under rows that reference it. Bring these two into line.
-- ============================================================

BEGIN;

ALTER TABLE pyramide_runs      DROP CONSTRAINT IF EXISTS pyramide_runs_scenario_id_fkey;
ALTER TABLE pyramide_runs      ADD  CONSTRAINT pyramide_runs_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenarios(scenario_id) ON DELETE RESTRICT;

ALTER TABLE pyramide_snapshots DROP CONSTRAINT IF EXISTS pyramide_snapshots_scenario_id_fkey;
ALTER TABLE pyramide_snapshots ADD  CONSTRAINT pyramide_snapshots_scenario_id_fkey
    FOREIGN KEY (scenario_id) REFERENCES scenarios(scenario_id) ON DELETE RESTRICT;

COMMIT;
