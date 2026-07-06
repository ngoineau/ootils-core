-- ============================================================
-- Migration 070 — Location aliases (N source-system codes per site)
-- ============================================================
-- Chantier #414. ADR-031 ("Location aliases — multi-code site
-- resolution") — à venir.
--
-- WHAT (role): a site (locations, migration 002) can be known by MANY
-- codes depending on the source system feeding it. One physical
-- warehouse may carry an alpha code in one feed and a numeric ERP code
-- in another. locations.external_id (migration 007) holds exactly ONE
-- canonical code per site and is UNIQUE — it cannot express this
-- many-to-one reality. location_aliases is the generic correspondence
-- table: 0..N alternate codes point back to one location_id, tagged by
-- the source system they originate from. It is NOT a pilot-specific
-- hack; the pilot case (demand_history.warehouse_id carrying numeric ERP
-- codes '87'/'286' while locations.external_id carries alpha 'DAL'/'CAN')
-- is merely the first consumer of a business-generic capability.
--
-- UNIQUENESS SCOPE — per source_system, DELIBERATELY (the key decision):
-- the UNIQUE key is (alias, source_system), NOT (alias) alone. This is NOT
-- because the same literal code may legitimately mean two different sites
-- in two different systems — it may NOT: the applicative invariant is
-- ONE code -> EXACTLY ONE site, ACROSS ALL SOURCE SYSTEMS.
-- demand_history.warehouse_id (the pilot's read path) carries no
-- source-system tag at all, so resolution at read time
-- (_warehouse_codes_subquery, UNION locations.external_id ∪
-- location_aliases) is itself system-agnostic; two sites sharing a code
-- under two different systems would double-count demand, not merely be an
-- odd data point. The per-system key exists for a DIFFERENT, narrower
-- reason: it lets ONE SAME site declare the SAME code from SEVERAL
-- systems (e.g. both an old and a new ERP feed independently reporting '87'
-- for the one Dallas warehouse) without a spurious UNIQUE violation on
-- (alias) alone — legitimate multiplicity is PER SITE, not per code.
-- Enforcing "one code, one site, all systems" as a single-table DB
-- constraint is not possible here (a check spanning every source_system's
-- rows for the same alias, plus locations.external_id, on every write is
-- not expressible as a single UNIQUE); the ingest layer owns that stronger
-- invariant instead (see #414/ADR-031 — always ingest for location_aliases
-- writes, never a direct SQL INSERT, precisely because direct SQL bypasses
-- this guard). The DB guarantees only intra-table, per-system uniqueness;
-- ingest guarantees the cross-system, cross-table "one site" invariant on
-- top of it. 🎯 A pilot may later tighten this to GLOBAL uniqueness (drop
-- source_system from the key, or add a UNIQUE (alias) partial/whole) if a
-- single-system deployment wants the DB itself to carry the stricter
-- invariant — the schema is written so that tightening is additive.
--
-- source_system NOT NULL DEFAULT '_default' — NEVER nullable (critical):
-- in PostgreSQL two NULLs do NOT collide under a UNIQUE constraint
-- (NULL <> NULL). A nullable source_system would therefore let the SAME
-- alias string be inserted twice with NULL system and both rows survive
-- the UNIQUE (alias, source_system) key — i.e. one alias silently
-- resolving to two different sites, the exact corruption this table
-- exists to prevent. The NOT NULL DEFAULT '_default' sentinel makes the
-- "unspecified system" case a real, collidable value: two '_default'
-- rows for the same alias DO conflict and the second is rejected.
--
-- FK ON DELETE RESTRICT — EXPLICIT (project convention): deleting a
-- location that still owns aliases is refused, so a live alias can never
-- dangle to a vanished site. PostgreSQL's default FK action is NO ACTION,
-- which is NOT written out here on purpose being replaced by the explicit
-- RESTRICT — the same discipline the repo-wide test_scenario_fk_retention
-- guard enforces on scenario FKs (migration 032): retention intent is
-- always spelled out, never left implicit.
--
-- ALIAS HYGIENE — CHECK (alias <> '' AND btrim(alias) = alias): ERP codes
-- arrive from TSV feeds where a stray leading/trailing space or an empty
-- cell is a real risk. An empty alias is meaningless; a space-padded
-- alias (' 87') would fail to match the trimmed lookup key ('87') and
-- silently never resolve. The CHECK rejects both at write time so a
-- malformed code fails loudly at ingest instead of becoming a phantom,
-- never-matching row (fail-loudly over silent wrong answers).
--
-- IDEMPOTENCE (repo migration policy — the runner in db/connection.py
-- wraps each file in its own transaction and, on ANY error, ROLLS BACK
-- and ABORTS; it does NOT swallow "already exists", and re-runs the whole
-- file from scratch at the next boot after a failed partial run): every
-- statement re-runs as a clean no-op. CREATE TABLE IF NOT EXISTS skips
-- the table AND all its inline constraints (PK, FK, CHECK, UNIQUE) on
-- re-run — inline table-level constraints need no separate DO $$ guard
-- because they are created/skipped atomically with the table itself (the
-- DO $$ + pg_constraint guard, cf. migration 007/063, is only needed for
-- constraints added to a PRE-EXISTING table via ALTER TABLE ADD
-- CONSTRAINT, which this migration does not do). CREATE INDEX IF NOT
-- EXISTS is a no-op on re-run.
--
-- No JSONB. Typed columns only. No trigger. No seed data.
--
-- Rolling-safe: brand-new, additive table — no existing reader depends on
-- it yet, no existing object is altered.
--
-- ref: ADR-031 (location aliases), #414.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- location_aliases: 0..N alternate source-system codes per site
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS location_aliases (
    alias_id       UUID          PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The site this alias resolves to. ON DELETE RESTRICT (explicit, not
    -- the NO ACTION default): a location owning live aliases cannot be
    -- deleted out from under them (project retention convention, cf.
    -- test_scenario_fk_retention / migration 032).
    location_id    UUID          NOT NULL
                                 REFERENCES locations(location_id)
                                 ON DELETE RESTRICT,

    -- The alternate code as it appears in a source feed. Must be non-empty
    -- and un-padded (see CHECK) so it matches the trimmed lookup key.
    alias          TEXT          NOT NULL,

    -- Origin system of this code. NEVER nullable: NULL source_system would
    -- escape the UNIQUE key (NULL <> NULL) and let one alias resolve to two
    -- sites. '_default' is the collidable sentinel for "unspecified system".
    source_system  TEXT          NOT NULL DEFAULT '_default',

    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- Hygiene for TSV-sourced ERP codes: reject empty and space-padded
    -- aliases at write time (a padded ' 87' would never match '87').
    CONSTRAINT location_aliases_alias_nonblank
        CHECK (alias <> '' AND btrim(alias) = alias),

    -- Uniqueness scoped PER source system (see header): this lets ONE SAME
    -- site declare the same code from several systems without a spurious
    -- violation. The stronger invariant "one code -> one site, ALL systems"
    -- (demand_history.warehouse_id is system-agnostic on read) is NOT
    -- expressible as a single-table constraint here — it is enforced at
    -- ingest, not here.
    UNIQUE (alias, source_system)
);

COMMENT ON TABLE location_aliases IS
    'Generic correspondence of 0..N source-system codes to one site '
    '(locations, migration 002) — #414, ADR-031. Application resolution '
    'reads the UNION locations.external_id ∪ location_aliases; the '
    'cross-site anti-collision guard lives at ingest, not in a cross-table '
    'DB constraint. Uniqueness is scoped per source_system.';

COMMENT ON COLUMN location_aliases.location_id IS
    'Site this alias resolves to. ON DELETE RESTRICT (explicit): cannot '
    'delete a location that still owns aliases — no dangling live alias.';

COMMENT ON COLUMN location_aliases.alias IS
    'Alternate code as it appears in a source feed (e.g. numeric ERP '
    'warehouse code). Non-empty and un-padded by CHECK so it matches the '
    'trimmed lookup key.';

COMMENT ON COLUMN location_aliases.source_system IS
    'Origin system of this code; part of the UNIQUE key. NEVER nullable — '
    'a NULL would escape the UNIQUE constraint (NULL <> NULL in PostgreSQL) '
    'and let one alias resolve to two sites. ''_default'' is the collidable '
    'sentinel for an unspecified system.';

-- ------------------------------------------------------------
-- Index
-- ------------------------------------------------------------
-- Inverse lookup ("all aliases of this site") and the FK's supporting
-- index. The forward lookup (alias -> site) is already served by the
-- UNIQUE (alias, source_system) key's implicit index.
CREATE INDEX IF NOT EXISTS idx_location_aliases_location
    ON location_aliases (location_id);

COMMIT;
