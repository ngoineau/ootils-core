-- ============================================================
-- Migration 064 — API tokens registry + scopes, audit binding (#392)
-- ============================================================
-- Chantier #392 "agent enterprise floor" PR1. ADR-029 (à venir):
-- "Cryptographic actor identity — the Decision Ladder's actor_kind is
-- derived from the presented token, never self-declared by the request
-- body."
--
-- THE GOVERNANCE HOLE THIS CLOSES: today auth (api/auth.py) validates a
-- single shared OOTILS_API_TOKEN and any caller asserts its own actor
-- kind (agent / human / service) in the request payload. That means the
-- #341 approval state machine — which gates L3+ actions on "a human, not
-- an agent, approved this" — trusts a self-declared field. A compromised
-- or buggy agent could stamp itself 'human' and walk an irreversible
-- CANCEL past the human-only gate. api_tokens makes actor_kind a property
-- of the CREDENTIAL: it is set once, by an operator, at token-issue time
-- (scripts/issue_agent_token.py, PR2) and read back cryptographically from
-- the presented token's hash. The request body can no longer influence
-- who the caller is. This is the substrate the #341 machine and the
-- Decision Ladder (L0-L4) become genuinely enforceable on.
--
-- WHY SHA-256 WITH NO KDF (deliberate, not an oversight): a password KDF
-- (bcrypt/argon2/scrypt) exists to slow brute force against LOW-entropy
-- human secrets. Our tokens are NOT human secrets — issue_agent_token.py
-- (PR2) mints them from 32 bytes of os.urandom (256 bits of entropy),
-- rendered as `ootk_<base>`. A 256-bit random string is not brute-forceable
-- regardless of hash speed, so a slow KDF buys zero security here while
-- adding per-request CPU on the hot auth path (every API call hashes the
-- bearer to look it up). Plain SHA-256 hex is the correct, standard choice
-- for high-entropy API keys (same rationale GitHub/Stripe-class keys use a
-- fast hash + a prefix). The CLEARTEXT token is shown exactly ONCE at
-- issuance and never stored — only token_hash (lookup key) and
-- token_prefix (human-readable, non-secret) live in the DB. A leak of this
-- table therefore leaks no usable credential.
--
-- WHY scopes IS TEXT[] (not JSONB, not a join table):
--   * NOT JSONB — the repo doctrine (CLAUDE.md, "no JSONB for business
--     data") reserves JSONB for unbounded-shape diagnostic payloads. A
--     scope set is a flat list of short enum-like strings with a KNOWN
--     shape; that is exactly what a native TEXT[] models, with real
--     array operators (`'shortage:read' = ANY(scopes)`) and GIN-indexable
--     containment. TEXT[] is a typed column, not a JSONB carve-out.
--   * NOT a token_scopes join table — a scope grant has no attributes of
--     its own (no grant time, no grantor, no per-scope expiry in V1); it
--     is pure set membership on the token. A join table would add a second
--     table, a second FK, and a multi-row read on the hot auth path to
--     reconstruct a set we can carry inline. If scopes ever grow
--     attributes, promoting to a join table is a clean forward migration;
--     V1 does not pay that cost.
--   * NO CHECK ON THE ARRAY CONTENTS — the set of valid scope strings is
--     validated in APPLICATION code (the auth layer's whitelist), NOT by a
--     SQL CHECK. A `CHECK (scopes <@ ARRAY[...])` would FREEZE the whole
--     scope vocabulary into the schema: every new capability scope would
--     require a new migration to widen the CHECK, coupling scope evolution
--     to DDL and to the migration runner's ordering. Keeping the whitelist
--     in Python lets scopes evolve with the code that enforces them, at
--     the same review boundary, with no schema churn. The DB stores the
--     grant; the app decides what a grant is allowed to contain.
--
-- WHY actor_kind IS DENORMALISED INTO api_request_log: the audit trail
-- must stay readable FOREVER, including after a token is hard-deleted. The
-- FK api_request_log.token_id → api_tokens is ON DELETE SET NULL (audit
-- rows survive token deletion, they are not cascaded away), so token_id
-- alone cannot answer "was this call made by an agent or a human?" once
-- the token row is gone. Copying actor_kind onto each audit row at write
-- time freezes that fact at the moment of the call — the historically
-- correct value, immune to later token deletion OR to a token being
-- re-issued with a different kind under a recycled id. Denormalisation is
-- the right call for an immutable audit log: the log records what was true
-- then, not what the live registry says now.
--
-- WHY recommendation_transitions.actor_kind IS ALSO WIDENED HERE (security-
-- review defect #6): 'service' becomes a first-class api_tokens.actor_kind
-- in THIS migration, but recommendation_transitions (migration 040) still
-- CHECKs actor_kind IN ('human', 'agent') only. Without this fix, a fully
-- authorised 'service' token performing a legitimate transition (e.g.
-- REVIEWED) would trip that stale CHECK on INSERT and surface as a raw
-- CheckViolation -> opaque 500 on a request that should have succeeded.
-- The migration that introduces a new actor_kind value is the correct,
-- single place to keep every actor_kind CHECK across the schema in sync —
-- see section (3) below for the introspection-based, name-safe widening
-- (mirrors migration 034's pattern for retiring an anonymous CHECK).
--
-- Idempotence (repo migration policy — the runner in db/connection.py
-- wraps each file in its own transaction and, on ANY error, ROLLS BACK and
-- ABORTS; it does NOT swallow "already exists"): every statement is
-- written to re-run as a clean no-op.
--   * CREATE TABLE IF NOT EXISTS       — skips the table on re-run.
--   * ADD COLUMN IF NOT EXISTS         — skips each audit column on re-run.
--   * CREATE INDEX IF NOT EXISTS       — no-op on re-run.
--   * DO $$ ... pg_constraint lookup + DROP CONSTRAINT IF EXISTS + ADD —
--     section (3): finds the OLD (possibly anonymous) actor_kind CHECK by
--     introspection rather than assuming its auto-generated name, drops it,
--     then (re)creates the canonically-named, widened CHECK. The
--     introspection query itself excludes the canonical name from its
--     match, so once the widened CHECK is in place a re-run finds nothing
--     left to drop — a clean no-op, exactly like section (1) and (2).
-- No JSONB. Typed columns and a native TEXT[] only.
--
-- ref: ADR-029 (cryptographic actor identity), #392.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- (1) API tokens registry
-- ------------------------------------------------------------
-- One row per issued credential. The cleartext token is NEVER stored: only
-- its SHA-256 hex (lookup + uniqueness) and a non-secret human-readable
-- prefix. actor_kind is the cryptographic source of the Decision Ladder —
-- set at issuance, never taken from a request body.
CREATE TABLE IF NOT EXISTS api_tokens (
    token_id      UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT        NOT NULL,                      -- human label: "shortage-watcher", "pilote-ngo"
    actor_kind    TEXT        NOT NULL CHECK (actor_kind IN ('agent', 'human', 'service')),
    token_hash    TEXT        NOT NULL UNIQUE,               -- SHA-256 hex of the full token; cleartext never stored
    token_prefix  TEXT        NOT NULL,                      -- ~12 leading chars in clear ("ootk_XXXXXXX") for lookup + readable audit
    scopes        TEXT[]      NOT NULL DEFAULT '{}',         -- native array; valid values whitelisted in app code (see header: no CHECK on contents)
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ,                               -- NULL = no expiry
    revoked_at    TIMESTAMPTZ,                               -- NULL = live; set = kill-switched
    last_used_at  TIMESTAMPTZ,                               -- NULL = never presented
    rate_per_min  INTEGER     CHECK (rate_per_min IS NULL OR rate_per_min > 0)  -- NULL = no per-token rate cap
);

COMMENT ON TABLE api_tokens IS
    'Issued API credentials registry (#392, ADR-029). Cleartext token is '
    'never stored — only token_hash (SHA-256 hex, lookup/uniqueness) and '
    'token_prefix (non-secret, readable). actor_kind is the cryptographic '
    'source of the Decision Ladder actor identity, set at issuance by an '
    'operator, never self-declared by a request body — the #341 human-only '
    'approval gate becomes enforceable on it.';

COMMENT ON COLUMN api_tokens.actor_kind IS
    'agent | human | service. Cryptographically bound to the credential at '
    'issuance; the #341 approval state machine and the Decision Ladder '
    '(L0-L4) derive the caller''s kind from this, NOT from the request '
    'payload. Kept in sync with the auth layer''s VALID_ACTOR_KINDS.';

COMMENT ON COLUMN api_tokens.token_hash IS
    'SHA-256 hex of the full cleartext token. No KDF by design: tokens are '
    '256-bit os.urandom (issue_agent_token.py, PR2), not low-entropy human '
    'secrets, so a fast hash is both sufficient and correct on the hot auth '
    'path. Cleartext is shown once at issuance and never persisted.';

COMMENT ON COLUMN api_tokens.token_prefix IS
    'First ~12 chars of the token in clear ("ootk_XXXXXXX"). Non-secret: '
    'safe to log and display for lookup + human-readable audit. Not unique '
    '(collisions on the short prefix are possible); token_hash is the '
    'unique identity.';

COMMENT ON COLUMN api_tokens.scopes IS
    'Granted scopes as a native TEXT[] (not JSONB, not a join table — see '
    'migration header). The set of valid scope strings is validated in '
    'application code, never by a SQL CHECK, so scopes evolve with the code '
    'that enforces them without schema churn.';

-- Lookup / audit index on the readable prefix. NON-unique on purpose:
-- token_hash carries uniqueness; two tokens may share a short prefix.
CREATE INDEX IF NOT EXISTS idx_api_tokens_prefix
    ON api_tokens (token_prefix);

-- ------------------------------------------------------------
-- (2) Bind the audit log (migration 023) to the issuing token
-- ------------------------------------------------------------
-- Both columns are NULLABLE and default-free: pre-existing api_request_log
-- rows keep NULL, and the current INSERT in api/app.py (which names its
-- columns explicitly) is unaffected — a strictly additive, rolling-deploy-
-- safe extension. The backend-dev wires the values in api/ separately.
--
-- token_id → api_tokens(token_id) ON DELETE SET NULL: the audit trail must
-- OUTLIVE a hard-deleted token. Deleting a token nulls the reference on old
-- audit rows rather than cascading them away — the record of "a call
-- happened" is never erased by credential lifecycle.
ALTER TABLE api_request_log
    ADD COLUMN IF NOT EXISTS token_id UUID
        REFERENCES api_tokens (token_id) ON DELETE SET NULL;

-- actor_kind is DENORMALISED here on purpose (see header): once token_id is
-- nulled by a token delete, this frozen copy still answers "agent or human
-- made this call?" — the historically correct value, immune to later token
-- deletion or id recycling. No FK, no CHECK: this mirrors whatever
-- api_tokens.actor_kind was at call time; it is an immutable audit fact,
-- not a live-validated field.
ALTER TABLE api_request_log
    ADD COLUMN IF NOT EXISTS actor_kind TEXT;

COMMENT ON COLUMN api_request_log.token_id IS
    'Issuing api_tokens.token_id for this request (#392). ON DELETE SET '
    'NULL — audit rows survive a hard token delete; the reference is '
    'cleared, the audit row is kept.';

COMMENT ON COLUMN api_request_log.actor_kind IS
    'Denormalised copy of the token''s actor_kind at call time (#392). '
    'Frozen so the audit stays answerable ("agent vs human") even after '
    'token_id is nulled by a token delete. No FK / no CHECK: it is an '
    'immutable audit fact, not a live-validated field.';

-- ------------------------------------------------------------
-- (3) Align recommendation_transitions.actor_kind with the new vocabulary
-- ------------------------------------------------------------
-- Security-review defect #6: this migration makes 'service' a first-class
-- api_tokens.actor_kind (whitelisted in code, allowed through the kill
-- switch), but recommendation_transitions (migration 040) still CHECKs
-- actor_kind IN ('human', 'agent') only. A valid 'service' token
-- performing an authorised transition (e.g. REVIEWED) hits transition_one's
-- INSERT, trips that stale CHECK, and surfaces as a raw CheckViolation ->
-- opaque 500 on a request that should have succeeded. Fix at the source:
-- the migration that introduces 'service' as a valid actor is the correct
-- place to widen every actor_kind CHECK to accept it.
--
-- The migration 040 constraint is an UNNAMED inline CHECK
-- (`actor_kind TEXT ... CHECK (actor_kind IN ('human', 'agent'))`), so
-- Postgres auto-assigned it a name. Rather than hardcode the assumed
-- `<table>_<column>_check` auto-name (recommendation_transitions_
-- actor_kind_check) and risk a silent no-op DROP IF EXISTS on a
-- differently-named constraint, we discover it by INTROSPECTION —
-- pg_constraint filtered on this table + contype='c' (CHECK) + the
-- constraint definition mentioning actor_kind — the same robust pattern
-- migration 034 uses to retire an anonymous CHECK on resources.resource_
-- type. The `conname <> 'recommendation_transitions_actor_kind_check'`
-- guard makes this loop-safe on re-run: once the canonical name is in
-- place, the introspection finds nothing left to drop (the one match IS
-- the named constraint we just added) and the block is a clean no-op —
-- matching 034's own idempotence guard exactly.
DO $$
DECLARE
    old_constraint_name TEXT;
BEGIN
    SELECT conname INTO old_constraint_name
    FROM pg_constraint
    WHERE conrelid = 'recommendation_transitions'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%actor_kind%'
      AND conname <> 'recommendation_transitions_actor_kind_check'
    LIMIT 1;

    IF old_constraint_name IS NOT NULL THEN
        EXECUTE 'ALTER TABLE recommendation_transitions DROP CONSTRAINT '
            || quote_ident(old_constraint_name);
    END IF;
END $$;

ALTER TABLE recommendation_transitions DROP CONSTRAINT IF EXISTS recommendation_transitions_actor_kind_check;
ALTER TABLE recommendation_transitions ADD CONSTRAINT recommendation_transitions_actor_kind_check
    CHECK (actor_kind IN ('human', 'agent', 'service'));

COMMENT ON COLUMN recommendation_transitions.actor_kind IS
    'human | agent | service (#392). Widened from the migration-040 '
    'human/agent-only CHECK so a valid service-actor_kind api_tokens '
    'credential (#392) can record a governed transition without tripping a '
    'stale CheckViolation. Kept in sync with api_tokens.actor_kind''s '
    'vocabulary.';

COMMIT;
