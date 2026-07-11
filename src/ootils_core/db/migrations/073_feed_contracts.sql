-- ============================================================
-- Migration 073 — feed_contracts (INT-1 PR1, governed-ingest doctrine)
-- ============================================================
-- Chantier INT-1 PR1. `feed_contracts` is the typed, VERSIONED registry of
-- what a source feed (on-hand, open purchase orders, open work orders, ...)
-- IS ALLOWED TO LOOK LIKE before it is trusted by a daily run: its entity
-- mapping, physical format, business key, mandatory columns, cadence,
-- arrival window, owner, criticality and volume guards. It is the DB half
-- of the contract; the YAML files under config/feed-contracts/*.yaml are
-- the pilot-editable source of truth, loaded into this table by
-- scripts/load_feed_contracts.py (interfaces/contracts.py) — see
-- ADR-037 (docs/ADR-037-daily-run-and-governed-ingest.md) for the full
-- decision record, including the supersede of ADR-013 D4 (governed-ingest
-- option (a): auto-approval iff DQ green AND all guards green; a red guard
-- escalates to an L3 human webhook instead of blocking the whole run).
--
-- SCOPE OF THIS PR: registry only. Nothing here reads or writes at ingest
-- time yet (that runtime wiring — daily_runs, guard evaluation, the
-- auto-approve/escalate decision — lands in PR2/PR3). Deliberately NO FK
-- from a `daily_runs` table: it does not exist yet.
--
-- WHY VERSIONED, APPEND-ONLY PER VERSION (never UPDATE an existing
-- version's content): a contract changing shape (a new mandatory column, a
-- widened volume guard, a new owner) is itself a fact worth keeping —
-- silently rewriting version N would erase the history of what a daily run
-- actually validated against on a given day. The loader's contract
-- (interfaces/contracts.py, not enforced here in SQL) is: identical parsed
-- YAML content vs. the current active version => a traced no-op (nothing
-- inserted); any diff => version+1 inserted; an existing version row's
-- content columns are NEVER UPDATEd. The exception is bookkeeping, not
-- content: `active` may be flipped (see below) and `updated_at` bumped
-- with it — the same mutable-metadata-vs-immutable-payload split already
-- documented for `events.processed` (CLAUDE.md, migration 002/006/051).
--
-- "ACTIVE" SEMANTICS + THE PARTIAL INDEX: `active` marks the version
-- currently in effect for its feed_key — AT MOST ONE per feed_key, enforced
-- by a UNIQUE partial index (not just an app-level invariant): when the
-- loader inserts a new version it first flips the previous active row's
-- `active` to FALSE (bookkeeping UPDATE), then inserts the new row with
-- `active = TRUE`. get_active_contract(feed_key) is then a plain
-- `WHERE feed_key = $1 AND active = TRUE` — guaranteed 0-or-1 rows by the
-- index, no ORDER BY/LIMIT race. Zero active rows for a feed_key means the
-- feed is currently disabled/retired (None-honest: the reader returns
-- None, it does not fall back to the latest inactive version).
--
-- entity_type CHECK — DELIBERATELY the exact same enum as
-- ingest_batches_entity_type_check, as it stands after migrations
-- 023 -> 035 -> 036 (verified before writing this list; DO NOT invent new
-- values here — a feed_contract's entity_type is a claim about which real
-- ingest_batches.entity_type the feed lands as, so the two enums must stay
-- in lockstep; widen ingest_batches' CHECK first if a new entity type is
-- ever needed, then mirror it here in a follow-up migration):
--   'items', 'locations', 'suppliers', 'supplier_items', 'purchase_orders',
--   'customer_orders', 'forecasts', 'work_orders', 'transfers', 'on_hand',
--   'resources', 'planning_params', 'routings'.
-- The 3 PR1 seed contracts use 'on_hand', 'purchase_orders' and
-- 'work_orders' — all three already present, no enum widening needed.
--
-- format CHECK — same 4-value universe as staging.uploads.file_format
-- (migration 033, ADR-013): 'tsv', 'csv', 'xlsx', 'json'.
--
-- load_mode CHECK — 'full' ONLY in V1. This is a deliberate fail-loudly
-- trap: delta-load semantics (change-only feeds) are explicitly OUT of
-- PR1's scope and arrive in a V2 migration that widens this CHECK. A
-- contract YAML that claims load_mode: delta today must be REJECTED by the
-- Pydantic loader before it ever reaches this table, and if one somehow
-- did, this CHECK constraint is the second, DB-level line of defense.
--
-- criticality CHECK — 'blocking' | 'advisory'. Doctrine link (ADR-037,
-- governed-ingest option (a)): a 'blocking' feed missing/late or failing
-- its volume guard blocks the daily run's auto-approval outright; an
-- 'advisory' feed doing the same only downgrades confidence / raises an
-- escalation, it does not block. (open-work-orders.yaml is the PR1
-- 'advisory' example: not every pilot customer runs work orders.)
--
-- key_columns / mandatory_columns — TEXT[] NOT NULL, each CHECKed
-- non-empty (cardinality > 0): a contract with zero key columns or zero
-- mandatory columns is a contract that validates nothing, which is a
-- config bug worth failing loudly on rather than silently accepting.
--
-- volume_guard_min_rows / volume_guard_max_pct_delta — nullable
-- (None-honest): not every feed needs a volume guard in V1; PR2/3 decide
-- what "no guard configured" means operationally, this table just refuses
-- to fabricate a default threshold.
--
-- depends_on — TEXT[] of feed_keys, NOT a FK. feed_key alone is not a
-- unique key here (only (feed_key, version) is — a feed_key spans many
-- versions), so a real FK cannot target it; the referential integrity of
-- "this feed_key actually exists" is validated by the Python loader against
-- the set of known feed_keys, the same non-DB-enforced pattern already used
-- for location_aliases' cross-site invariant (ADR-031, migration 070
-- header). NOT NULL DEFAULT '{}' (not NULL-as-absence): "no upstream
-- dependency" is a known fact about the feed, not missing data.
--
-- JSONB: NONE in this table. Every field is a typed, business-queryable
-- column (CLAUDE.md JSONB carve-out policy does not apply — a feed
-- contract is exactly the kind of bounded-shape config the carve-out
-- excludes).
--
-- FK to scenarios: not applicable. A feed contract is a global ingest-time
-- config object, not scenario-scoped data.
--
-- IDEMPOTENCE (repo migration policy — the runner in db/connection.py wraps
-- each file in its own transaction and, on ANY error, ROLLS BACK and
-- ABORTS; it does NOT swallow "already exists"): every statement re-runs as
-- a clean no-op — CREATE TABLE/INDEX IF NOT EXISTS. See migration 063's
-- header for the canonical defensive-idempotence pattern this follows.
--
-- Rolling-safe: brand-new, additive table — no reader depends on it yet
-- (the loader/CLI landing in this same PR is the first reader/writer), no
-- existing object is altered.
--
-- ref: INT-1 PR1 (#449), ADR-037.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- feed_contracts — versioned registry of what a source feed must look like
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS feed_contracts (
    feed_contract_id            UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Stable identifier across versions, e.g. 'on-hand',
    -- 'open-purchase-orders', 'open-work-orders'. Non-blank/un-padded, same
    -- hygiene as location_aliases.alias (migration 070).
    feed_key                    TEXT        NOT NULL,

    -- Monotone version per feed_key, entirely app-assigned (no DEFAULT/
    -- sequence): the loader computes current_max(version)+1 itself so it
    -- can decide, in the SAME transaction, whether the new content differs
    -- from the current active row before minting a version number at all.
    version                     INTEGER     NOT NULL CHECK (version >= 1),

    -- Which real ingest_batches.entity_type this feed lands as. See header
    -- for the exact enum provenance (023 -> 035 -> 036).
    entity_type                 TEXT        NOT NULL CHECK (entity_type IN (
                                     'items', 'locations', 'suppliers', 'supplier_items',
                                     'purchase_orders', 'customer_orders', 'forecasts',
                                     'work_orders', 'transfers', 'on_hand', 'resources',
                                     'planning_params', 'routings'
                                 )),

    -- Free-text origin system name (e.g. 'SAP', 'NetSuite', 'manual-xlsx').
    -- No enum: unlike entity_type this is not a closed vocabulary the
    -- engine branches on, same free-text treatment as
    -- ingest_batches.source_system (migration 007) and
    -- location_aliases.source_system (migration 070).
    source_system                TEXT        NOT NULL,

    -- Physical file format, same universe as staging.uploads.file_format
    -- (ADR-013, migration 033).
    format                       TEXT        NOT NULL CHECK (format IN (
                                     'tsv', 'csv', 'xlsx', 'json'
                                 )),

    -- Business key columns identifying a row (e.g. {item_code, site_code}
    -- for on-hand). Non-empty by CHECK — a contract with no key defines no
    -- matching semantics, which is a config error, not a valid V1 state.
    key_columns                  TEXT[]      NOT NULL CHECK (cardinality(key_columns) > 0),

    -- Columns that must be non-null/non-blank on every row for the feed to
    -- pass its DQ gate. Non-empty by CHECK, same rationale as key_columns.
    mandatory_columns             TEXT[]      NOT NULL CHECK (cardinality(mandatory_columns) > 0),

    -- V1 ONLY admits full reloads. See header: fail-loudly trap against a
    -- delta contract slipping in before V2 defines delta semantics.
    load_mode                    TEXT        NOT NULL DEFAULT 'full'
                                 CHECK (load_mode IN ('full')),

    -- Expected schedule, cron text (interpretation lives in the PR2/3
    -- runtime, not validated here beyond being present).
    cadence                       TEXT        NOT NULL,

    -- How late the feed is allowed to arrive after its cadence tick before
    -- a daily run treats it as missing. Strictly positive: a zero/negative
    -- window is not a real tolerance.
    arrival_window_minutes        INTEGER     NOT NULL CHECK (arrival_window_minutes > 0),

    -- Human/team accountable for this feed's content and freshness — who
    -- the L3 escalation webhook (ADR-037) ultimately names.
    owner                         TEXT        NOT NULL,

    -- 'blocking' feeds gate the daily run's auto-approval outright;
    -- 'advisory' feeds only downgrade confidence / escalate. See header.
    criticality                   TEXT        NOT NULL CHECK (criticality IN ('blocking', 'advisory')),

    -- Volume guards — BOTH nullable (None-honest): not every feed needs
    -- one configured in V1; runtime semantics of "no guard" are PR2/3
    -- territory, this table just refuses to invent a default.
    volume_guard_min_rows         INTEGER     CHECK (volume_guard_min_rows IS NULL OR volume_guard_min_rows >= 0),
    -- Fraction (0.20 = 20%), NOT a percent integer — matches ADR-013 D4's
    -- deletion-ratio safeguard vocabulary (staging.transform_runs.forced_approval,
    -- migration 033) that this guard generalizes. NUMERIC(5,4) covers up to
    -- a 999.99% swing threshold, ample headroom above the legal [0,1] range
    -- most guards will actually use.
    volume_guard_max_pct_delta    NUMERIC(5, 4) CHECK (volume_guard_max_pct_delta IS NULL OR volume_guard_max_pct_delta >= 0),

    -- Upstream feed_keys this feed's daily-run ordering depends on. NOT a
    -- FK (feed_key alone isn't unique — see header); validated by the
    -- Python loader. NOT NULL DEFAULT '{}': "no dependency" is known, not
    -- missing.
    depends_on                    TEXT[]      NOT NULL DEFAULT '{}'::TEXT[],

    -- Whether THIS VERSION is the one currently in effect for its
    -- feed_key. At most one TRUE per feed_key, enforced by the partial
    -- unique index below — see header for the full "active" semantics.
    active                        BOOLEAN     NOT NULL DEFAULT TRUE,

    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped on bookkeeping-only mutation (the `active` flip on
    -- supersede/retire) — content columns themselves are never UPDATEd
    -- (append-only per version, see header).
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT feed_contracts_feed_key_nonblank
        CHECK (feed_key <> '' AND btrim(feed_key) = feed_key),

    -- One version number, once, per feed_key.
    CONSTRAINT uq_feed_contracts_feed_key_version UNIQUE (feed_key, version)
);

COMMENT ON TABLE feed_contracts IS
    'Versioned, append-only-per-version registry of source-feed contracts '
    '(INT-1 PR1, ADR-037). YAML under config/feed-contracts/*.yaml is the '
    'pilot-editable source of truth; scripts/load_feed_contracts.py loads it '
    'here idempotently (identical content => traced no-op, any diff => a '
    'new version row, never an UPDATE of an existing version''s content). '
    'PR1 is registry-only: nothing yet reads this table at ingest time '
    '(daily_runs + guard evaluation land in PR2/PR3).';

COMMENT ON COLUMN feed_contracts.feed_key IS
    'Stable identifier across versions (e.g. ''on-hand''). Non-blank/'
    'un-padded by CHECK, same hygiene as location_aliases.alias (070).';

COMMENT ON COLUMN feed_contracts.version IS
    'Monotone per feed_key, app-assigned (no sequence/DEFAULT) so the '
    'loader can decide content-identical-=>no-op BEFORE minting a version.';

COMMENT ON COLUMN feed_contracts.entity_type IS
    'Which real ingest_batches.entity_type this feed lands as. CHECK is '
    'DELIBERATELY the same enum as ingest_batches_entity_type_check after '
    'migrations 023/035/036 — keep the two in lockstep (widen 023/035/036''s '
    'constraint first, then mirror here).';

COMMENT ON COLUMN feed_contracts.load_mode IS
    'V1 admits ONLY ''full''. Fail-loudly trap: delta semantics are out of '
    'PR1 scope and arrive via a V2 migration widening this CHECK.';

COMMENT ON COLUMN feed_contracts.criticality IS
    '''blocking'' gates the daily run''s governed auto-approval outright; '
    '''advisory'' only downgrades confidence / escalates (ADR-037 option a).';

COMMENT ON COLUMN feed_contracts.volume_guard_max_pct_delta IS
    'Fraction (0.20 = 20%), not a percent integer. Nullable: not every feed '
    'configures a volume guard in V1 (None-honest, no fabricated default).';

COMMENT ON COLUMN feed_contracts.depends_on IS
    'Upstream feed_keys this feed''s ordering depends on. NOT a FK — '
    'feed_key alone isn''t unique (only (feed_key, version) is); validated '
    'by the Python loader, same non-DB-enforced pattern as ADR-031''s '
    'location_aliases cross-site invariant. NOT NULL DEFAULT ''{}'': ''no '
    'dependency'' is a known fact, not missing data.';

COMMENT ON COLUMN feed_contracts.active IS
    'Whether THIS VERSION is currently in effect for its feed_key. At most '
    'one TRUE per feed_key — enforced by the partial UNIQUE index below, '
    'not just an app-level invariant. Zero active rows = feed currently '
    'disabled/retired; get_active_contract() returns None, no fallback to '
    'the latest inactive version.';

COMMENT ON COLUMN feed_contracts.updated_at IS
    'Bumped only on bookkeeping mutation (the `active` flip when a new '
    'version supersedes this one) — content columns are append-only per '
    'version and never UPDATEd, mirroring events.processed''s documented '
    'mutable-metadata-vs-immutable-payload split (CLAUDE.md).';

-- ------------------------------------------------------------
-- Indexes
-- ------------------------------------------------------------
-- Version history scan for a feed_key ("show me every version of
-- on-hand, newest first").
CREATE INDEX IF NOT EXISTS idx_feed_contracts_feed_key_version
    ON feed_contracts (feed_key, version DESC);

-- THE active-contract lookup path (get_active_contract(feed_key)) AND the
-- DB-level guarantee of "at most one active version per feed_key" — the
-- "index partiel actif" the plan calls for. A plain WHERE feed_key = $1 AND
-- active = TRUE is guaranteed 0-or-1 rows by this constraint, no ORDER BY/
-- LIMIT race needed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_feed_contracts_active_per_feed
    ON feed_contracts (feed_key)
    WHERE active;

COMMIT;
