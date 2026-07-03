-- ============================================================
-- Migration 056 — Pyramide run stale-demand provenance (axis D, PR-D4)
-- ============================================================
-- ADR-023: a Pyramide run executed while the demand_history ingest age
-- exceeded the freshness SLA (request parameter, pilot default 7 days)
-- must CARRY that fact in its own provenance — not only in the
-- dq_findings STALE_DEMAND row emitted alongside. Agents ranking or
-- committing runs gate on this flag without joining dq_findings.
--
-- stale_demand is a typed BOOLEAN (no JSONB): it is business-queryable
-- ("which runs were produced on stale demand?"), so per CLAUDE.md it
-- gets a real column. The forensic detail (ingest age, SLA, coverage
-- lag) lives in the paired dq_findings.evidence carve-out.
--
-- DEFAULT FALSE: pre-existing runs and write paths that do not measure
-- freshness (hierarchy persist_series_run, for now) stay honest — FALSE
-- means "not proven stale at run time", never "proven fresh".
--
-- Idempotence: ADD COLUMN IF NOT EXISTS (repo migration policy).
-- ============================================================

BEGIN;

ALTER TABLE pyramide_runs
    ADD COLUMN IF NOT EXISTS stale_demand BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN pyramide_runs.stale_demand IS
    'TRUE when the run was generated while demand_history ingest age exceeded the freshness SLA (ADR-023). Paired with a dq_findings STALE_DEMAND row carrying the evidence.';

COMMIT;
