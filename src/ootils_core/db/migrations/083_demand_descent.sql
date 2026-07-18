-- ============================================================
-- Migration 083 — DESC-1 PR-A: demand descent schema (national → DC)
-- ============================================================
-- Chantier DESC-1 (see the approved plan, ADR-043 to come). The pilot's
-- business model, discovered on the 18/07 first real load: demand is
-- PLANNED nationally (a pooled forecast/order channel, pooled safety
-- stock — a local shortage routes to whichever DC has stock) and EXECUTED
-- per distribution centre (orders dispatched by US state to PAT/DCW/DAL,
-- purchase orders posed on whichever DC's projection shows the need). The
-- per-site DRP echelon (migration 029) already exists and runs idle for
-- lack of localised demand; this migration lands the schema that lets a
-- later PR (DESC-1 PR-B, "activate the link") materialise per-DC demand
-- nodes from the national channel. This migration is SCHEMA-ONLY — no
-- descent run, no engine code, no reader change. No existing object is
-- altered; every table here is brand-new and additive (rolling-safe).
--
-- FOUR tables, one model:
--   1. demand_split_pct     — the % of national demand routed to each DC,
--                             per item. Scenario-scoped WITH baseline
--                             fallback (see below) so a fork can test an
--                             alternate split policy without touching the
--                             baseline percentages.
--   2. state_dc_routing     — the EXECUTION dispatch table (US state →
--                             DC), ERP-sourced (per the pilot's MIXTE
--                             arbitration 18/07: ERP extracts state→DC,
--                             Ootils computes the % from history). Global
--                             reference data, not scenario-scoped.
--   3. item_dc_eligibility  — which DCs a given item may be stocked/
--                             ordered at. Global reference data, not
--                             scenario-scoped.
--   4. demand_descent_lines — the provenance ledger: for every descent
--                             run, which national node was split, into
--                             which per-DC node, at what %, for what qty.
--                             Read-side audit trail; never re-derives a
--                             number, only records what a run already
--                             computed.
--
-- WHY NO NEW node_type / NO read-path change (per the plan): the descent
-- run (PR-B) writes ordinary ForecastDemand/CustomerOrderDemand nodes on
-- REAL DC locations and deactivates (active=FALSE) the national source
-- nodes; every existing reader (DRP loader, MRP loader, projection,
-- shortage detection) already understands those node types on any
-- location. This migration only lands the four tables that FEED that
-- future run — it does not touch `nodes`/`edges` at all.
--
-- ============================================================
-- FK POLICY (every reference explicit — Postgres' default FK action is
-- NO ACTION, never relied upon implicitly in this repo's modern
-- migrations, cf. 067/069/070/072):
-- ============================================================
--   demand_split_pct.scenario_id -> scenarios(scenario_id)
--       ON DELETE RESTRICT, NULLABLE. NULL is a first-class value here,
--       not "no scenario associated": it means "the baseline/default split
--       row", read by EVERY scenario unless that scenario has its own
--       override row (the resolver, `engine/descent/shares.py`, is future
--       PR-B/PR-A2 scope — this migration only lands the storage). This is
--       a deliberate two-tier design collapsed into ONE table rather than
--       a separate master-data table + scenario_planning_overrides-style
--       overlay (migration 060): demand_split_pct has no other home for
--       its baseline percentages, so the NULL row IS the baseline data,
--       not a pointer to the real baseline scenario UUID
--       ('00000000-0000-0000-0000-000000000001', migration 002's seed).
--       ON DELETE RESTRICT is EXPLICIT despite the column being nullable —
--       Postgres FK checks simply skip NULL values, so nullability and
--       the delete-action clause are orthogonal; the repo-wide guard
--       (test_scenario_fk_retention, migration 032) asserts confdeltype=
--       'r' on EVERY FK referencing scenarios(scenario_id) regardless of
--       nullability, and this FK must satisfy it.
--   demand_split_pct.item_id / .dc_location_id -> items / locations
--       ON DELETE RESTRICT. Reference data in this schema is never hard-
--       deleted in normal operation (status flag instead, migration 002)
--       — RESTRICT is the safe default, matching the dominant convention
--       in the newer migrations (067, 072) rather than 060's CASCADE
--       (060's CASCADE was specific to an ephemeral scenario override
--       row; a split-% row is itself durable planning data, closer in
--       kind to a snapshot or a drift verdict than to a what-if override).
--   demand_split_pct.source_calc_run_id -> calc_runs(calc_run_id)
--       ON DELETE SET NULL, NULLABLE. Traceability nicety (which run
--       computed this %), mirroring 072's pyramide_run_id: the % value
--       itself is frozen on this row and must survive the calc_run being
--       garbage-collected/purged.
--   state_dc_routing.dc_location_id / item_dc_eligibility.item_id,
--   .dc_location_id -> locations / items ON DELETE RESTRICT. Same
--       reference-data reasoning as above.
--   demand_descent_lines.descent_run_id -> calc_runs(calc_run_id)
--       ON DELETE RESTRICT. The ledger's own run coordinate; a ledger row
--       must not outlive silently losing its run.
--   demand_descent_lines.source_node_id / .derived_node_id -> nodes
--       (node_id) ON DELETE RESTRICT. Provenance audit: the ledger must
--       never dangle to a vanished node. In this schema nodes are not
--       organically hard-deleted (only PURGE-1's scenario-purge sweep,
--       migration 076, deletes node rows for archived+TTL-elapsed
--       scenarios) — same RESTRICT-by-default posture as `explanations`/
--       `causal_steps` (migration 004), which reference nodes(node_id) the
--       same way. FOLLOW-UP (out of THIS migration's scope, flagged for
--       the PURGE-1 owner): if a scenario carrying descent ledger rows is
--       ever purged, `demand_descent_lines` needs an FK-ordered position
--       in PURGE_WHITELIST (engine/maintenance/purge.py) ahead of `nodes`,
--       exactly as `explanations`/`causal_steps` already are (whitelist
--       entries 1+6) — otherwise the node delete will hit this RESTRICT
--       and fail.
--   demand_descent_lines.item_id / .dc_location_id -> items / locations
--       ON DELETE RESTRICT. Denormalised alongside the node FKs (same
--       pattern as `shortages.item_id`/`location_id` sitting next to
--       `shortages.pi_node_id`, migration 005) so the ledger can be
--       queried/reported without joining `nodes`.
--
-- SCENARIO SCOPE OF demand_descent_lines — EXPLICIT scenario_id COLUMN
-- (revised at PR-A review time; differs from recommendation_outcomes'
-- migration-069 reasoning FOR A REASON). recommendation_outcomes is
-- baseline-only by nature (an outcome is an OBSERVED fact — ADR-030), so
-- fork purges never touch it and inheriting the scenario transitively is
-- free. Descent lines are the opposite: a FORK's descent run (testing
-- alternative split percentages — the whole point of forkability here)
-- writes ledger lines whose source/derived node FKs are ON DELETE
-- RESTRICT — without an explicit, sweepable scenario_id column, PURGE-1's
-- FK-ordered fork purge would hit those RESTRICTs on `nodes` and the
-- whole purge would fail. The explicit column (NULL = baseline run) makes
-- the table part of the standard PURGE_WHITELIST mechanism (swept BEFORE
-- `nodes` in the deletion order), at the accepted cost of a redundant-
-- but-guarded scenario reference (kept consistent by PR-B's writer, which
-- stamps it from the calc_run's own scenario in the same transaction).
--
-- ============================================================
-- UNIQUENESS — NULLS NOT DISTINCT, not a partial index (a deliberate
-- deviation from the literal "partial unique index for scenario_id IS
-- NULL" brief, documented here):
-- ============================================================
-- demand_split_pct has TWO independently-nullable columns in its natural
-- key: scenario_id (NULL = baseline, see above) AND season_bucket (V1
-- ships annual-only, i.e. ALWAYS NULL for every row today — the column
-- exists ready for V2, per the plan). A plain UNIQUE(scenario_id, item_id,
-- dc_location_id, season_bucket), or even a single partial unique index
-- scoped to "WHERE scenario_id IS NULL", would NOT prevent two baseline,
-- annual (scenario_id NULL, season_bucket NULL) rows for the same
-- (item, dc) — Postgres treats every NULL as distinct from every other
-- NULL under a UNIQUE index, partial or not, so the season_bucket NULL
-- alone (independently of scenario_id) already escapes a plain or
-- scenario-only-partial constraint. This is exactly the V1 common case
-- (scenario_id NULL AND season_bucket NULL together), so a partial index
-- on scenario_id alone would silently under-protect it. PG16's
-- UNIQUE ... NULLS NOT DISTINCT constraint (used identically for the same
-- "NULL is a meaningful key value, not a wildcard" reason in migration
-- 060's scenario_planning_overrides) solves BOTH nullable columns AT ONCE
-- with one constraint instead of stacking multiple partial indexes that
-- could drift apart under a future edit — the same trade-off 060's header
-- already recorded. Chosen here for the identical reason.
--
-- The instruction's separately-requested "index (scenario_id, item_id)"
-- is NOT added as a second, standalone index: the NULLS NOT DISTINCT
-- constraint above already creates a btree index whose two LEADING
-- columns are (scenario_id, item_id) — a query filtering on exactly those
-- two columns already gets an efficient leftmost-prefix index scan from
-- it. A second, narrower index over the same leading columns would be
-- pure redundancy (extra WAL on every INSERT/UPDATE, no query benefit) —
-- the kind of duplicate index this repo's other migrations (e.g. 070's
-- header: "the forward lookup is already served by the UNIQUE key's
-- implicit index") explicitly avoid. This comment IS the requested
-- "index (scenario_id, item_id)", implemented as the constraint's own
-- index rather than a redundant twin.
--
-- ============================================================
-- IDEMPOTENCE (mandatory, per migration 063's canonical header — the
-- runner in db/connection.py wraps this whole file in ONE transaction and
-- ABORTS on any error without swallowing "already exists"; a re-attempt
-- after a fixed failure re-runs every statement from scratch):
--   * CREATE TABLE IF NOT EXISTS  — no-op on re-run, and inline
--     constraints (PK/FK/CHECK/UNIQUE) are created/skipped atomically with
--     the table, needing no separate guard (cf. 070's header).
--   * CREATE INDEX IF NOT EXISTS  — no-op on re-run.
-- Nothing here uses ALTER TABLE ADD CONSTRAINT on a pre-existing table, so
-- no DO $$ / pg_constraint guard is needed (that pattern is reserved for
-- widening an existing CHECK, cf. migration 076).
--
-- TYPE NOTES:
--   * qty_source / qty_derived (demand_descent_lines) are NUMERIC(18,6) —
--     the canonical scaled quantity precision already used across the
--     engine (MRP buckets 021, forecast 026, DRP 029, snapshots 067,
--     outcomes 069) — rather than a bare unscaled NUMERIC, for
--     consistency with every other quantity column a descent-adjacent
--     reader might JOIN against.
--   * pct / pct_applied are NUMERIC(9,8) (not NUMERIC(18,6)): these are
--     ratios in (0, 1], not quantities — 8 fractional digits is ample
--     headroom for a Σ=1 split across many DCs without rounding drift,
--     matching the plan's "Σ=1 garanti" requirement for the pure shares
--     engine (engine/descent/shares.py, a later PR).
--   * confidence (demand_split_pct) is NUMERIC(4,3) as specified, with a
--     CHECK bounding it to [0, 1] (ADR-023's convention, cf. migration
--     072's confidence CHECK) — NUMERIC(4,3) alone permits up to 9.999,
--     the CHECK closes that gap defensively.
--   * item_dc_eligibility.source DEFAULT 'derived' NOT NULL: the plan's
--     assumed V1 default ("éligibilité : dérivée (historique ∪ lanes)
--     jusqu'à l'extrait ERP") — a row inserted by the cold-start derivation
--     path needs no caller-supplied value.
--
-- No JSONB anywhere in this migration — every column here is typed,
-- business-queryable planning/config/audit data, not a diagnostic
-- payload.
--
-- ref: DESC-1 PR-A (plan doc), ADR-043 (to come), ADR-020 (DRP), ADR-021
-- (two shortage truths — the pooled/per-site convergence this chantier
-- completes), ADR-025 (scenario overlay pattern this table's baseline-
-- fallback design mirrors).
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- 1. demand_split_pct — national → DC routing shares (planning)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS demand_split_pct (
    split_id            UUID          PRIMARY KEY DEFAULT gen_random_uuid(),

    -- NULL = baseline/default split, read by every scenario unless
    -- overridden. Non-NULL = this ONE scenario's override. See header.
    scenario_id         UUID          REFERENCES scenarios(scenario_id)
                                       ON DELETE RESTRICT,

    item_id              UUID          NOT NULL
                                       REFERENCES items(item_id)
                                       ON DELETE RESTRICT,
    dc_location_id       UUID          NOT NULL
                                       REFERENCES locations(location_id)
                                       ON DELETE RESTRICT,

    -- V1 ships annual-only (always NULL); column is ready for a V2
    -- seasonal split without a later migration. See header uniqueness note.
    season_bucket        TEXT,

    pct                  NUMERIC(9,8)  NOT NULL
                                       CHECK (pct > 0 AND pct <= 1),

    method                TEXT         NOT NULL
                                       CHECK (method IN ('history', 'equal_split', 'manual')),

    -- Distinct from `method`: `method` records the ORIGIN of the value
    -- ('history' = calibrated from state-level history, 'equal_split' =
    -- cold-start fallback, 'manual' = hand-entered from the start);
    -- manual_override flags that a HUMAN has since hand-adjusted a value
    -- that may originally have been computed (e.g. method='history' but a
    -- planner has overridden the calibrated number). The two are
    -- independent by design — no CHECK links them.
    manual_override       BOOLEAN      NOT NULL DEFAULT FALSE,

    -- Deterministic [0,1] confidence (ADR-023 world), nullable — a cold-
    -- start equal_split row may carry none. NUMERIC(4,3) as specified;
    -- CHECK closes the [0,9.999] gap the bare precision would allow.
    confidence            NUMERIC(4,3) CHECK (confidence IS NULL
                                          OR (confidence >= 0 AND confidence <= 1)),

    -- Freshness of the underlying history this % was calibrated from.
    -- NULL for equal_split/manual rows that have no history basis.
    freshness_date        DATE,

    -- Traceability nicety: which calc_run computed this %. SET NULL on
    -- purge — the % value is frozen on this row regardless. See FK POLICY.
    source_calc_run_id    UUID         REFERENCES calc_runs(calc_run_id)
                                       ON DELETE SET NULL,

    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One split row per (scenario incl. NULL-as-baseline, item, DC,
    -- season_bucket incl. NULL-as-annual). NULLS NOT DISTINCT is
    -- mandatory here — see header "UNIQUENESS" note for why a plain
    -- UNIQUE or a scenario-only partial index would under-protect this.
    CONSTRAINT demand_split_pct_natural_key
        UNIQUE NULLS NOT DISTINCT (scenario_id, item_id, dc_location_id, season_bucket)
);

COMMENT ON TABLE demand_split_pct IS
    'DESC-1: the % of an item''s national demand routed to each DC '
    '(planning-side of the national/per-site model). scenario_id NULL = '
    'baseline/default, read by every scenario unless overridden; '
    'season_bucket NULL = V1 annual split (column ready for V2 '
    'seasonality). One row per (scenario, item, dc, season) via a '
    'NULLS NOT DISTINCT unique key.';

COMMENT ON COLUMN demand_split_pct.scenario_id IS
    'NULL = baseline/default split row, read by every scenario absent its '
    'own override; non-NULL = a single scenario''s override. FK ON DELETE '
    'RESTRICT (mandatory, guard test_scenario_fk_retention) regardless of '
    'nullability.';

COMMENT ON COLUMN demand_split_pct.season_bucket IS
    'NULL in V1 (annual split only). Ready for a V2 seasonal key without '
    'a further migration.';

COMMENT ON COLUMN demand_split_pct.pct IS
    'Share of national demand routed to this DC, in (0, 1]. NUMERIC(9,8): '
    'a ratio, not a quantity — ample fractional headroom for a Sigma=1 '
    'split across many DCs (engine/descent/shares.py, later PR).';

COMMENT ON COLUMN demand_split_pct.method IS
    'Origin of the % value: history (calibrated from state-level history), '
    'equal_split (cold-start fallback, flagged never silent), or manual '
    '(hand-entered). Independent of manual_override (see column comment '
    'there).';

COMMENT ON COLUMN demand_split_pct.manual_override IS
    'TRUE when a human has hand-adjusted a value regardless of its '
    'original `method` (e.g. a calibrated history value since overridden). '
    'Independent of `method`.';

COMMENT ON COLUMN demand_split_pct.source_calc_run_id IS
    'The calc_run that computed this %, if any. ON DELETE SET NULL: the % '
    'value is frozen on this row and survives the run being purged — the '
    'pointer is a traceability nicety, not load-bearing.';

-- ------------------------------------------------------------
-- 2. state_dc_routing — execution dispatch (US state -> DC)
-- ------------------------------------------------------------
-- ERP-sourced (pilot arbitration 18/07, MIXTE rule: ERP extracts this
-- table, Ootils computes demand_split_pct's %). A state may legitimately
-- route to more than one DC (e.g. primary + backup) — the UNIQUE key is
-- per (state, dc), not per state alone, so multiple rows per state are a
-- real, supported shape.
CREATE TABLE IF NOT EXISTS state_dc_routing (
    routing_id       UUID          PRIMARY KEY DEFAULT gen_random_uuid(),

    state_code       TEXT          NOT NULL
                                    CHECK (state_code ~ '^[A-Z]{2}$'),

    dc_location_id   UUID          NOT NULL
                                    REFERENCES locations(location_id)
                                    ON DELETE RESTRICT,

    -- When this routing rule took effect. NULL = always effective (no
    -- known start date, e.g. bootstrap/legacy rows from the initial ERP
    -- extract).
    effective_from   DATE,

    active            BOOLEAN      NOT NULL DEFAULT TRUE,

    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (state_code, dc_location_id)
);

COMMENT ON TABLE state_dc_routing IS
    'DESC-1: execution-side dispatch of a US state to the DC(s) it orders '
    'from (ERP-sourced, pilot arbitration 18/07). A state may route to '
    'more than one DC — UNIQUE is per (state, dc), not per state.';

COMMENT ON COLUMN state_dc_routing.state_code IS
    'Two-letter uppercase US state code, e.g. ''CA''. CHECK enforces the '
    'format at write time.';

COMMENT ON COLUMN state_dc_routing.effective_from IS
    'When this routing rule took effect; NULL = always effective (no '
    'known start date, e.g. bootstrap/legacy ERP extract rows).';

-- ------------------------------------------------------------
-- 3. item_dc_eligibility — which DCs may stock/order a given item
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS item_dc_eligibility (
    eligibility_id   UUID          PRIMARY KEY DEFAULT gen_random_uuid(),

    item_id           UUID         NOT NULL
                                    REFERENCES items(item_id)
                                    ON DELETE RESTRICT,
    dc_location_id     UUID        NOT NULL
                                    REFERENCES locations(location_id)
                                    ON DELETE RESTRICT,

    eligible            BOOLEAN    NOT NULL DEFAULT TRUE,

    -- 'derived' is the V1 cold-start default (per the plan: eligibility is
    -- derived from history UNION lanes until the ERP extract lands).
    source                TEXT     NOT NULL DEFAULT 'derived'
                                    CHECK (source IN ('erp', 'derived', 'manual')),

    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (item_id, dc_location_id)
);

COMMENT ON TABLE item_dc_eligibility IS
    'DESC-1: whether an item may be stocked/ordered at a given DC. V1 '
    'defaults to source=''derived'' (history UNION lanes) until the ERP '
    'eligibility extract lands (source=''erp'').';

COMMENT ON COLUMN item_dc_eligibility.source IS
    'Origin of the eligibility flag: erp (authoritative extract), derived '
    '(V1 cold-start, history UNION lanes), manual (hand-entered).';

-- ------------------------------------------------------------
-- 4. demand_descent_lines — provenance ledger of a descent run
-- ------------------------------------------------------------
-- No scenario_id column (inherited via descent_run_id/source_node_id/
-- derived_node_id — see header "SCENARIO SCOPE" note).
CREATE TABLE IF NOT EXISTS demand_descent_lines (
    line_id            UUID          PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Explicit, sweepable scenario scope (NULL = baseline run). See the
    -- header's SCENARIO SCOPE section: a fork's descent lines must be
    -- purgeable by the standard PURGE_WHITELIST sweep BEFORE `nodes`,
    -- or the RESTRICT node FKs below would block the fork purge.
    scenario_id        UUID          REFERENCES scenarios(scenario_id)
                                     ON DELETE RESTRICT,

    -- The calc_run that performed this descent (DESC-1 PR-B, future).
    descent_run_id     UUID          NOT NULL
                                     REFERENCES calc_runs(calc_run_id)
                                     ON DELETE RESTRICT,

    -- The national node this line split FROM, deactivated (active=FALSE)
    -- by the descent run once split (anti-double-count, audit retained).
    source_node_id      UUID         NOT NULL
                                     REFERENCES nodes(node_id)
                                     ON DELETE RESTRICT,

    -- The per-DC node this line split INTO.
    derived_node_id       UUID       NOT NULL
                                     REFERENCES nodes(node_id)
                                     ON DELETE RESTRICT,

    -- Denormalised alongside the node FKs so the ledger is queryable/
    -- reportable without a join to `nodes` (mirrors shortages.item_id/
    -- location_id sitting next to shortages.pi_node_id, migration 005).
    item_id                 UUID     NOT NULL
                                     REFERENCES items(item_id)
                                     ON DELETE RESTRICT,
    dc_location_id           UUID    NOT NULL
                                     REFERENCES locations(location_id)
                                     ON DELETE RESTRICT,

    -- The % actually applied for this line (may differ from the current
    -- demand_split_pct.pct if that row is edited after this run executed
    -- — this ledger freezes what was applied AT RUN TIME).
    pct_applied                NUMERIC(9,8)  NOT NULL
                                     CHECK (pct_applied > 0 AND pct_applied <= 1),

    -- Canonical scaled quantity precision (see header TYPE NOTES).
    qty_source                  NUMERIC(18,6) NOT NULL,
    qty_derived                   NUMERIC(18,6) NOT NULL,

    created_at                     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE demand_descent_lines IS
    'DESC-1: provenance ledger of a demand-descent run — for every line, '
    'which national node was split, into which per-DC node, at what % '
    '(frozen at run time), for what qty. No scenario_id column: inherited '
    'via descent_run_id -> calc_runs.scenario_id and source_node_id/'
    'derived_node_id -> nodes.scenario_id (mirrors recommendation_outcomes, '
    'migration 069). Read-only audit trail — never re-derives a number.';

COMMENT ON COLUMN demand_descent_lines.source_node_id IS
    'The national demand node this line split from; the descent run '
    'deactivates it (active=FALSE) once split, anti-double-count, audit '
    'retained. ON DELETE RESTRICT — see this migration''s header FOLLOW-UP '
    'note on PURGE_WHITELIST ordering.';

COMMENT ON COLUMN demand_descent_lines.derived_node_id IS
    'The per-DC demand node this line split into (ordinary ForecastDemand/'
    'CustomerOrderDemand node type, no new node_type introduced).';

COMMENT ON COLUMN demand_descent_lines.pct_applied IS
    'The % actually applied at run time — frozen even if demand_split_pct '
    'is edited afterward.';

-- ------------------------------------------------------------
-- Indexes
-- ------------------------------------------------------------
-- demand_split_pct: FK completeness / reverse lookups. (scenario_id,
-- item_id) is deliberately NOT duplicated here — see header UNIQUENESS
-- note (already served by the NULLS NOT DISTINCT constraint's own index).
CREATE INDEX IF NOT EXISTS idx_demand_split_pct_dc
    ON demand_split_pct (dc_location_id);
CREATE INDEX IF NOT EXISTS idx_demand_split_pct_source_run
    ON demand_split_pct (source_calc_run_id);

-- state_dc_routing: reverse lookup ("which states route to this DC") +
-- FK completeness. Forward lookup (state -> DC) is already served by the
-- UNIQUE (state_code, dc_location_id) key's implicit index.
CREATE INDEX IF NOT EXISTS idx_state_dc_routing_dc
    ON state_dc_routing (dc_location_id);

-- item_dc_eligibility: reverse lookup ("which items are eligible at this
-- DC") + FK completeness. Forward lookup (item -> DC) is already served
-- by the UNIQUE (item_id, dc_location_id) key's implicit index.
CREATE INDEX IF NOT EXISTS idx_item_dc_eligibility_dc
    ON item_dc_eligibility (dc_location_id);

-- demand_descent_lines: per-run listing (as requested) + per-source-node
-- lookup (as requested, "trace what this national node was split into")
-- + per-derived-node lookup (the natural reverse direction once nodes
-- exist, "trace where this DC node came from" — FK completeness).
CREATE INDEX IF NOT EXISTS idx_demand_descent_lines_scenario
    ON demand_descent_lines (scenario_id);
CREATE INDEX IF NOT EXISTS idx_demand_descent_lines_run
    ON demand_descent_lines (descent_run_id);
CREATE INDEX IF NOT EXISTS idx_demand_descent_lines_source_node
    ON demand_descent_lines (source_node_id);
CREATE INDEX IF NOT EXISTS idx_demand_descent_lines_derived_node
    ON demand_descent_lines (derived_node_id);

COMMIT;
