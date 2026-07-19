-- ============================================================
-- Migration 087 — invariant_violations: the runtime exactitude net
-- ============================================================
-- Chantier « moteur d'exception », volet INVARIANTS RUNTIME (CHANTIER 1).
--
-- DOCTRINE — le filet FoundationDB (« teste une fois » vs « vrai en
-- continu »). A unit/integration test proves an invariant held ONCE, on the
-- one state that test happened to build. The interesting failures are the
-- ones NO test thought to build: a migration backfill that skewed a chain, a
-- watcher that wrote a half-split, an engine regression that stopped netting.
-- FoundationDB's answer (and ours here) is to state the business invariants
-- ONCE, declaratively, as a query that must return ZERO rows over the LIVE
-- data at all times — then assert its emptiness continuously (in every
-- integration module's teardown, see tests/integration/conftest.py). The view
-- is the single source of truth for "what must never be true"; the fixture is
-- the tripwire. A violation row is not a test artefact — it is a real,
-- committed state that contradicts a business law the engine claims to uphold.
--
-- This view is READ-ONLY and SIDE-EFFECT-FREE: one row per detected
-- violation, never an action. It is deliberately cheap to eyeball (a DBA can
-- SELECT * FROM invariant_violations at any moment) and cheap to assert
-- (empty == healthy). It writes nothing, ADR-021's `shortages` ownership is
-- untouched, no `events` row is emitted — a pure diagnostic lens.
--
-- COLUMNS (stable contract — every UNION branch matches these types):
--   invariant   TEXT  — the violated law's stable name (snake_case).
--   node_id     TEXT  — the offending node (NULL for table-level laws d/e).
--   detail      TEXT  — a human-readable "got X, expected Y" (values inlined).
--   scenario_id UUID  — the fork/baseline the violation lives in (NULL where
--                       the row itself is baseline/annual, e.g. a baseline
--                       demand_split_pct group).
--   severity    TEXT  — 'error' for every V1 law (all are hard breaks).
--
-- THE FIVE V1 INVARIANTS (columns verified against migrations 002/083):
--
--  (a) pi_projection_balance — a COMPUTED ProjectedInventory bucket must
--      satisfy closing_stock = opening_stock + inflows - outflows (tolerance
--      1e-6). This is the projection identity PROPAGATE_SQL writes verbatim
--      (propagator_sql.py: closing_stock = opening + inflows - outflows), so
--      for real engine output it holds by construction and the row NEVER
--      fires — until a regression breaks it. GATED on last_calc_run_id IS NOT
--      NULL ON PURPOSE: this is the "was produced by the engine" marker
--      (PROPAGATE_SQL stamps last_calc_run_id in the same UPDATE). Direct
--      test fixtures that seed only a closing_stock and leave opening/inflows/
--      outflows at 0 (a legitimate shortage-detection fixture — the numbers
--      that matter for THAT test are set, the rest are placeholders) carry a
--      NULL last_calc_run_id and are correctly OUT of scope: the balance law
--      is about what the engine computed, not about every fixture's placeholder
--      arithmetic. The all-four-NOT-NULL guard is belt-and-braces on top.
--
--  (b) pi_chain_continuity — along an ACTIVE feeds_forward edge (migration
--      019: PI[n].closing_stock feeds PI[n+1].opening_stock, weight 1.0),
--      the downstream opening_stock must equal the upstream closing_stock
--      (tolerance 1e-6). Same "chain break" the propagator's window function
--      makes impossible for computed series — so, like (a), gated on BOTH
--      endpoints' last_calc_run_id IS NOT NULL to scope it to engine output
--      and exclude fixtures that wire a feeds_forward edge across
--      deliberately non-chaining placeholder buckets.
--
--  (c) pi_stockout_flag_coherence — a PURE physical-sign law, INDEPENDENT of
--      safety_scope (ADR-021 amendment, DESC-1 PR-C): a COMPUTED active PI
--      whose closing_stock is meaningfully negative (< -1e-6) MUST carry
--      has_shortage = TRUE (PROPAGATE_SQL writes has_shortage = closing_stock
--      < -1e-9 in the same UPDATE — so, like (a)/(b), for real engine output
--      it holds by construction). We do NOT assert the converse
--      (has_shortage=TRUE with closing_stock >= 0): that is LEGITIMATE under a
--      safety-stock shortage (closing positive but below safety), and in
--      'national' safety_scope the per-site threshold is deliberately NULLed —
--      so the "positive-closing-yet-flagged" direction is scope-dependent and
--      NOT a violation. Only "physically short but not flagged" is unambiguous
--      on every scope. GATED on last_calc_run_id IS NOT NULL, uniformly with
--      (a)/(b): the has_shortage flag only claims to reflect the closing sign
--      once the ENGINE has computed the bucket. A raw fixture that seeds a
--      negative closing and leaves has_shortage at its FALSE default (a
--      legitimate allocation/graph fixture) never claimed to be computed and
--      is correctly out of scope — while an engine regression that stopped
--      flagging a real stockout (last_calc_run_id set) is still caught, and
--      the is_stocking amendment is preserved: a virtual-channel PI is still
--      computed (projection runs everywhere) and MUST keep has_shortage=TRUE
--      even though its `shortages`-table row is suppressed
--      (test_is_stocking_integration).
--
--  (d) demand_split_pct_sums_to_one — for each (scenario_id, item_id,
--      season_bucket) group in demand_split_pct, the per-DC shares must sum
--      to 1 (tolerance 1e-8). scenario_id NULL (baseline) and season_bucket
--      NULL (V1 annual) are FIRST-CLASS key values here (migration 083's
--      NULLS NOT DISTINCT key), so GROUP BY folds them into one group each —
--      exactly the Σ=1 law the pure shares engine guarantees. A lone
--      partial-split row (e.g. a single 0.625 seeded to prove a CHECK/
--      idempotence, never a real plan) is a genuine violation of the law and
--      must be exempted at the test site, not silently tolerated here.
--
--  (e) demand_descent_mass_conservation — a demand-descent run must preserve
--      mass per split source node: SUM(qty_derived) over a source_node_id's
--      ledger lines must equal that source's qty_source (tolerance 1e-6).
--      Grouped per (source_node_id, scenario_id); qty_source is denormalised
--      identically on every line of a source (migration 083), so MAX() is its
--      unambiguous representative. Mirrors the run test's own per-source
--      conservation assertion (test_demand_descent_run_integration).
--
-- IDEMPOTENCE (migration 063 header doctrine — the runner wraps this file in
-- ONE transaction and does NOT swallow "already exists"): CREATE OR REPLACE
-- VIEW is the trivially-idempotent form — a re-run replaces the definition
-- with the identical text, a clean no-op. No DROP needed (nothing depends on
-- this view); a future migration that must change the COLUMN SET will DROP
-- VIEW IF EXISTS first, per PG's replace-can't-change-columns rule.
--
-- No JSONB: every column is a typed diagnostic scalar. This is a VIEW, not a
-- table — it stores nothing, so the JSONB carve-out policy does not apply.
--
-- ref: chantier moteur-d'exception CHANTIER 1 (INVARIANTS RUNTIME);
--      migration 002 (nodes/edges), migration 019 (feeds_forward),
--      migration 083 (demand_split_pct / demand_descent_lines),
--      ADR-021 (shortage truth + safety_scope amendment), propagator_sql.py
--      (PROPAGATE_SQL projection identity this net guards).
-- ============================================================

CREATE OR REPLACE VIEW invariant_violations AS

-- (a) projection balance — closing = opening + inflows - outflows
SELECT
    'pi_projection_balance'::text AS invariant,
    n.node_id::text               AS node_id,
    format(
        'closing_stock=%s != opening_stock=%s + inflows=%s - outflows=%s (delta=%s)',
        n.closing_stock, n.opening_stock, n.inflows, n.outflows,
        n.closing_stock - (n.opening_stock + n.inflows - n.outflows)
    )                             AS detail,
    n.scenario_id                 AS scenario_id,
    'error'::text                 AS severity
FROM nodes n
WHERE n.node_type = 'ProjectedInventory'
  AND n.active = TRUE
  AND n.last_calc_run_id IS NOT NULL
  AND n.opening_stock IS NOT NULL
  AND n.inflows       IS NOT NULL
  AND n.outflows      IS NOT NULL
  AND n.closing_stock IS NOT NULL
  AND abs(n.closing_stock - (n.opening_stock + n.inflows - n.outflows)) > 0.000001

UNION ALL

-- (b) chain continuity — opening(bucket) = closing(previous bucket)
SELECT
    'pi_chain_continuity'::text,
    n_to.node_id::text,
    format(
        'opening_stock=%s (bucket_sequence=%s) != previous closing_stock=%s from node %s (delta=%s)',
        n_to.opening_stock, n_to.bucket_sequence, n_from.closing_stock,
        n_from.node_id, n_to.opening_stock - n_from.closing_stock
    ),
    n_to.scenario_id,
    'error'::text
FROM edges e
JOIN nodes n_from ON n_from.node_id = e.from_node_id
JOIN nodes n_to   ON n_to.node_id   = e.to_node_id
WHERE e.edge_type = 'feeds_forward'
  AND e.active = TRUE
  AND n_from.node_type = 'ProjectedInventory'
  AND n_to.node_type   = 'ProjectedInventory'
  AND n_from.active = TRUE
  AND n_to.active   = TRUE
  AND n_from.last_calc_run_id IS NOT NULL
  AND n_to.last_calc_run_id   IS NOT NULL
  AND n_from.closing_stock IS NOT NULL
  AND n_to.opening_stock   IS NOT NULL
  AND abs(n_to.opening_stock - n_from.closing_stock) > 0.000001

UNION ALL

-- (c) stockout-flag coherence — physically negative closing MUST be flagged
SELECT
    'pi_stockout_flag_coherence'::text,
    n.node_id::text,
    format(
        'closing_stock=%s < 0 but has_shortage=FALSE (physical stockout not flagged)',
        n.closing_stock
    ),
    n.scenario_id,
    'error'::text
FROM nodes n
WHERE n.node_type = 'ProjectedInventory'
  AND n.active = TRUE
  AND n.last_calc_run_id IS NOT NULL
  AND n.closing_stock IS NOT NULL
  AND n.closing_stock < -0.000001
  AND n.has_shortage = FALSE

UNION ALL

-- (d) demand split shares sum to 1 per (scenario, item, season)
SELECT
    'demand_split_pct_sums_to_one'::text,
    NULL::text,
    format(
        'SUM(pct)=%s != 1 for item_id=%s season_bucket=%s (%s DC rows)',
        SUM(dsp.pct), dsp.item_id, COALESCE(dsp.season_bucket, '<annual>'), COUNT(*)
    ),
    dsp.scenario_id,
    'error'::text
FROM demand_split_pct dsp
GROUP BY dsp.scenario_id, dsp.item_id, dsp.season_bucket
HAVING abs(SUM(dsp.pct) - 1) > 0.00000001

UNION ALL

-- (e) demand descent preserves mass per split source node
SELECT
    'demand_descent_mass_conservation'::text,
    ddl.source_node_id::text,
    format(
        'SUM(qty_derived)=%s != qty_source=%s for source_node_id=%s (%s lines, delta=%s)',
        SUM(ddl.qty_derived), MAX(ddl.qty_source), ddl.source_node_id, COUNT(*),
        SUM(ddl.qty_derived) - MAX(ddl.qty_source)
    ),
    ddl.scenario_id,
    'error'::text
FROM demand_descent_lines ddl
GROUP BY ddl.source_node_id, ddl.scenario_id
HAVING abs(SUM(ddl.qty_derived) - MAX(ddl.qty_source)) > 0.000001;

COMMENT ON VIEW invariant_violations IS
    'Runtime exactitude net (chantier moteur-d''exception, CHANTIER 1): one '
    'row per LIVE business-invariant violation — projection balance, '
    'feeds_forward chain continuity, physical stockout-flag coherence, '
    'demand-split Σ=1, and demand-descent mass conservation. MUST be empty at '
    'all times; asserted continuously in every integration module teardown '
    '(tests/integration/conftest.py). Read-only, side-effect-free — never '
    'writes shortages/events. See the migration 087 header for the full '
    'doctrine and per-invariant rationale.';
