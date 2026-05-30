-- ============================================================
-- Migration 044 — Data-quality findings (DQ watcher fleet)
-- ============================================================
-- The engine is only as correct as its inputs. This session repeatedly hit
-- SILENT data defects that produced plausible-but-wrong plans (multi-location
-- forecast loss, 11% unvalued purchase volume, 87% past-due demand from stale
-- ERP dates, 1322 mis-flagged make parents). DQ agents turn those silent gaps
-- into visible, business-impact-ranked, governed work.
--
-- A DQ finding is a third shape (neither a procurement reco nor a parameter
-- tweak): "this datum is missing/stale/contradictory, and here's how much it
-- weighs". Reuses agent_runs as the work ledger; same Decision Ladder spirit —
-- agents detect and surface, humans (or interface owners) remediate.
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS dq_findings (
    dq_finding_id       UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name          TEXT        NOT NULL,
    agent_run_id        UUID        NOT NULL REFERENCES agent_runs(agent_run_id),
    scenario_id         UUID        NOT NULL,

    rule_code           TEXT        NOT NULL,   -- MISSING_COST / STALE_DEMAND / NO_SUPPLIER / ORPHAN_MAKE_FLAG / EXPIRED_SUPPLIER_TERM / MAKE_WITHOUT_BOM …
    entity_type         TEXT        NOT NULL,   -- item / supplier / supplier_item / node
    entity_id           UUID,
    entity_external_id  TEXT,

    severity            TEXT        NOT NULL DEFAULT 'MEDIUM'
                        CHECK (severity IN ('HIGH','MEDIUM','LOW')),
    description         TEXT        NOT NULL,
    -- business impact, so findings rank by weight not by count
    impact_metric       TEXT,                   -- planned_volume_units / past_due_demand_qty / count
    impact_value        NUMERIC,
    suggested_action    TEXT,

    status              TEXT        NOT NULL DEFAULT 'OPEN'
                        CHECK (status IN ('OPEN','ACKNOWLEDGED','RESOLVED','IGNORED','SUPERSEDED')),

    -- JSONB carve-out (CLAUDE.md): forensic detail behind the finding.
    evidence            JSONB,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_dq_status  ON dq_findings (status);
CREATE INDEX IF NOT EXISTS ix_dq_rule    ON dq_findings (rule_code, status);
CREATE INDEX IF NOT EXISTS ix_dq_entity  ON dq_findings (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_dq_impact  ON dq_findings (impact_value DESC);
CREATE INDEX IF NOT EXISTS ix_dq_run     ON dq_findings (agent_run_id);

COMMIT;
