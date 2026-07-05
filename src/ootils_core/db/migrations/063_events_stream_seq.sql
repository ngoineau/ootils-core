-- ============================================================
-- Migration 063 — events.stream_seq keyset cursor + LISTEN/NOTIFY wakeup (#391)
-- ============================================================
-- Chantier #391 StreamChanges. ADR-027 (à venir) : "StreamChanges — a
-- replayable keyset cursor over `events` woken by LISTEN/NOTIFY".
--
-- WHY a new column at all: the `events` table (migration 002) has NO
-- monotone ordering key an SSE consumer can resume from.
--   * event_id is a v4 UUID (gen_random_uuid) — random, NOT orderable.
--   * created_at is TIMESTAMPTZ DEFAULT now() — collides at microsecond
--     resolution under batch inserts, so it cannot be a strict cursor
--     (two rows can share the same instant → a `> last_seen_ts` resume
--     would skip or duplicate rows).
-- stream_seq (BIGINT GENERATED ALWAYS AS IDENTITY) gives every event a
-- strictly-increasing per-insert integer: THE replayable cursor. Consumers
-- resume with `WHERE scenario_id = $1 AND stream_seq > $last_seen` — a
-- keyset scan, no OFFSET, no polling of the payload.
--
-- MONOTONE BUT NOT GAP-FREE (contract, do not "fix"): an IDENTITY sequence
-- advances on every INSERT attempt, so a rolled-back transaction BURNS its
-- value and leaves a hole (…, 41, 43, …). This is fine and intended.
-- Consumers MUST treat stream_seq as an opaque high-water mark compared
-- with `>` only — never as a count, never as `last + 1`, never
-- gap-checked. "I have up to N" means "give me stream_seq > N", nothing
-- about how many rows that is.
--
-- HISTORICAL BACKFILL ORDER IS NOT created_at ORDER (documented so nobody
-- reads history as a timeline via stream_seq): adding an IDENTITY column
-- rewrites the table and assigns stream_seq to the PRE-EXISTING rows in
-- whatever physical/heap order PG16 walks them during the rewrite — which
-- is NOT guaranteed to follow created_at. Only events inserted AFTER this
-- migration get a stream_seq aligned with their insertion order. For the
-- replay/stream contract this is a non-issue (a fresh subscriber starts
-- from the current high-water mark and only ever moves forward); it only
-- means "ORDER BY stream_seq" over historical rows is not a chronological
-- sort. ADR-027 records this explicitly.
--
-- THE NOTIFY TRIGGER IS A LOSSY WAKEUP, NOT THE TRUTH: events_stream_notify
-- fires pg_notify('ootils_events', <scenario_id>) after each insert purely
-- so a listening consumer wakes immediately instead of poll-sleeping.
-- NOTIFY can be missed (consumer not yet LISTENing, connection bounce,
-- reconnect gap) and its 8 KB payload cap forbids shipping the event body —
-- so the payload is the scenario_id ONLY. The authoritative, replayable
-- source of every delta is the keyset SELECT on stream_seq; the wakeup just
-- collapses latency. A consumer that (re)connects always does one catch-up
-- SELECT (stream_seq > last_seen) BEFORE trusting notifications, so a
-- dropped NOTIFY costs latency, never correctness.
--
-- Idempotence: this migration must re-run as a clean no-op. The runner
-- (db/connection.py) wraps each file in its own transaction and, on ANY
-- error, rolls back and ABORTS (it does NOT swallow "already exists") — so
-- every statement is written defensively:
--   * ADD COLUMN IF NOT EXISTS         — skips the column (and its IDENTITY
--     sequence + table rewrite) on re-run.
--   * CREATE INDEX IF NOT EXISTS       — no-op on re-run.
--   * CREATE OR REPLACE FUNCTION       — re-defines harmlessly.
--   * DROP TRIGGER IF EXISTS + CREATE  — deterministic re-create (PG has no
--     CREATE OR REPLACE TRIGGER before PG14; DROP-then-CREATE is the
--     portable, replay-safe idiom, matching migration 016's pattern).
--
-- events stays IMMUTABLE on its payload: this migration adds a monotone
-- bookkeeping column + an index + an AFTER-INSERT notify trigger only. No
-- UPDATE, no payload change (ADR-005 D2 payload-immutability is untouched;
-- stream_seq is assigned once at insert and never rewritten).
--
-- ref: ADR-027 (StreamChanges), #391.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- (1) Replayable keyset cursor column
-- ------------------------------------------------------------
-- GENERATED ALWAYS AS IDENTITY (not a plain BIGSERIAL/DEFAULT): the value
-- is engine-owned, cannot be supplied or overwritten by an INSERT, which is
-- exactly what a trustworthy cursor needs. PG16 backfills every existing
-- row during the table rewrite triggered by this ADD COLUMN (acceptable at
-- the current demo scale; see the header note on backfill ordering).
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS stream_seq BIGINT GENERATED ALWAYS AS IDENTITY;

COMMENT ON COLUMN events.stream_seq IS
    'Monotone per-insert cursor for StreamChanges (#391, ADR-027). '
    'Strictly increasing but NOT gap-free (a rolled-back INSERT burns its '
    'value) — consumers resume with `WHERE scenario_id = $1 AND stream_seq '
    '> $last_seen` and compare with `>` ONLY, never as a count or last+1. '
    'Historical rows were backfilled in table-rewrite order, which is NOT '
    'created_at order; only post-migration events have a stream_seq aligned '
    'with insertion order. The keyset SELECT is the replayable truth; the '
    'events_stream_notify trigger is only a lossy latency-cutting wakeup.';

-- ------------------------------------------------------------
-- (2) Scenario-scoped keyset index
-- ------------------------------------------------------------
-- Every stream subscriber reads ONE scenario's tail: (scenario_id,
-- stream_seq) makes `scenario_id = $1 AND stream_seq > $last_seen ORDER BY
-- stream_seq` an index-only range scan — no sort, no full-column scan. Not
-- partial: a resuming consumer may legitimately reach back to any
-- stream_seq, so the whole range must be indexed.
CREATE INDEX IF NOT EXISTS idx_events_stream_seq
    ON events (scenario_id, stream_seq);

-- ------------------------------------------------------------
-- (3) LISTEN/NOTIFY wakeup trigger
-- ------------------------------------------------------------
-- AFTER INSERT: fire a NOTIFY carrying the scenario_id so a listening
-- consumer wakes and runs its keyset catch-up SELECT. scenario_id is
-- NOT NULL on events (migration 002), so NEW.scenario_id::text is always a
-- non-empty payload. RETURN NULL is correct for an AFTER trigger (its
-- return value is ignored; NULL is the conventional signal). The payload is
-- the scenario_id ONLY — never the event body (8 KB NOTIFY cap; the cursor
-- fetches rows, the wakeup just says "this scenario has something new").
CREATE OR REPLACE FUNCTION ootils_notify_event() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('ootils_events', NEW.scenario_id::text);
    RETURN NULL;
END;
$$;

COMMENT ON FUNCTION ootils_notify_event() IS
    'StreamChanges wakeup (#391, ADR-027): AFTER INSERT on events, NOTIFY '
    'channel ''ootils_events'' with scenario_id as payload. Lossy by design '
    '(a missed NOTIFY costs latency, not correctness — the keyset SELECT on '
    'events.stream_seq is the replayable truth). Payload is scenario_id '
    'only (8 KB NOTIFY cap; never the event body).';

DROP TRIGGER IF EXISTS events_stream_notify ON events;
CREATE TRIGGER events_stream_notify
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION ootils_notify_event();

COMMIT;
