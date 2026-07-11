"""
tests/integration/test_shortages_outcome_index_integration.py — migration 075
(PERF-1 PR-A): the `idx_shortages_scenario_item_loc_active` partial index that
serves `_load_observed_shortages` (engine/outcome/evaluator.py), against a real
PostgreSQL database (no mocks — CLAUDE.md).

Covered contracts:

  1. Schema guarantee: the index exists in pg_indexes with the exact expected
     shape — btree key (scenario_id, item_id, location_id, severity_score DESC,
     shortage_date), partial predicate WHERE status = 'active', NOT unique —
     and migration 075 replaced nothing: every pre-existing shortages index
     (005 + 014) is still present.
  2. Usability proof — the index's FORM, never the planner's preference: 2
     fork scenarios x ~3,800 seeded `shortages` rows each (bulk server-side
     INSERT ... SELECT, same row shape as the existing outcome-test seeds),
     then EXPLAIN (FORMAT JSON) of the VERBATIM `_load_observed_shortages`
     query with every COMPETING access path neutralized -> the plan is an
     ordered Index Scan on the 075 index, with NO Seq Scan, NO Sort node
     (the Sort-free plan is the entire point of the index: the DISTINCT ON
     ordering comes straight off the btree), and scenario_id in the Index
     Cond — not demoted to a Filter.

     Why "neutralize competitors" instead of "pin GUCs and expect the
     planner to pick 075" (the strategy this module first shipped with, and
     which failed in CI): at this deliberately small volume (~7.6k rows),
     even with SET LOCAL enable_seqscan = off AND enable_bitmapscan = off,
     the planner LEGITIMATELY prefers shortages_status_idx + an explicit
     Sort — the 075 path implies ~1.8k random-order heap fetches (priced at
     random_page_cost = 4) and costs MORE than a narrower index plus an
     in-memory sort at toy scale. That choice flips with volume, statistics
     and server version (at the pilot's 175k rows the 075 index wins), so
     asserting the planner's preference is a volume-sensitive non-proof.
     What IS deterministic is the index's SHAPE: with the competing indexes
     transactionally dropped (DROP INDEX is transactional in PostgreSQL;
     the drops live inside a SAVEPOINT rolled back immediately after the
     probe, so nothing leaks whatever the fixture's commit semantics) and
     seq/bitmap paths priced out, a plan that still needed a Sort on top of
     the 075 index — or pushed scenario_id into a Filter — would fail the
     assertions. That Sort-free ordered scan is exactly the property that
     makes the index win at pilot volume.

     Droppability, verified against the migrations: `shortages_pkey` is
     constraint-backed (not DROP INDEX-able, and useless to this query, so
     it stays); `shortages_pi_node_calc_run_uidx` is a plain CREATE UNIQUE
     INDEX (005 — no pg_constraint rides it), so it is discovered and
     dropped like the rest. Discovery is dynamic from pg_index, excluding
     constraint-backed indexes — a future migration adding another shortages
     index can never silently re-introduce a competitor into this probe.
  3. Truth consistency: the neutralized-path (index-forced) plan returns
     byte-identical rows to the real query executed BEFORE the savepoint
     under the default planner with all indexes present (the true contract:
     the index changes the access path, never the result set), and the
     DISTINCT ON semantics over the seeded volume are the expected ones
     (per (item, location): max severity_score among ACTIVE rows dated <=
     as_of, tie-broken by earliest shortage_date).
  4. Query-shape guard: the verbatim SQL copy this test EXPLAINs is asserted
     (whitespace-normalized) to still be the literal query inside
     `evaluator._load_observed_shortages` — if the hot query drifts from the
     indexed shape, this fails loudly instead of the EXPLAIN silently proving
     a stale query.
  5. Migration 075 idempotence: re-executing the file verbatim (twice) on an
     already-migrated DB is a clean no-op (defensive-idempotence contract,
     migration 063 header; the runner never swallows "already exists").

Dates are fixed constants (no CURRENT_DATE / wall clock): the seeded calendar
and the derived expectations are fully deterministic. Every read accesses
columns BY NAME (dict_row), never positionally.
"""
from __future__ import annotations

import inspect
import json
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from ootils_core.engine.outcome import evaluator as outcome_evaluator

from .conftest import requires_db

pytestmark = requires_db

_REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_075 = (
    _REPO_ROOT / "src" / "ootils_core" / "db" / "migrations"
    / "075_shortages_outcome_index.sql"
)

INDEX_NAME = "idx_shortages_scenario_item_loc_active"

# Every shortages index that predates migration 075 (005 + 014) — the
# migration ADDS a third active-partial index, it replaces nothing.
PRE_EXISTING_SHORTAGES_INDEXES = {
    "shortages_pkey",
    "shortages_pi_node_calc_run_uidx",
    "shortages_scenario_id_idx",
    "shortages_pi_node_id_idx",
    "shortages_shortage_date_idx",
    "shortages_status_idx",
    "idx_shortages_scenario_active",
    "idx_shortages_item_active",
}

# Copied VERBATIM from evaluator.py::_load_observed_shortages (the exact query
# migration 075 was built for). test_verbatim_copy_matches_evaluator_source
# guards this copy against drift.
OBSERVED_SHORTAGES_SQL = """
        SELECT DISTINCT ON (item_id, location_id)
               item_id, location_id, shortage_date, shortage_qty, severity_score
        FROM shortages
        WHERE scenario_id = %s
          AND status = 'active'
          AND shortage_date <= %s
          AND item_id IS NOT NULL
        ORDER BY item_id, location_id, severity_score DESC, shortage_date
        """

# ---------------------------------------------------------------------------
# Seeded-volume geometry (all fixed => all expectations derivable by hand).
#
# Per scenario: N_ITEMS x N_LOCATIONS coordinates x N_DAYS consecutive dates
# from BASE_DATE, one shortages row (and its own ProjectedInventory node —
# `shortages_pi_node_calc_run_uidx` is UNIQUE on (pi_node_id, calc_run_id))
# per coordinate-day, plus N_NULL_ITEM_ROWS active rows with item_id IS NULL
# (real on the pilot base; excluded by the query's `item_id IS NOT NULL`).
#
# Row at day-offset d:  severity_score = 100 + d * 13   (distinct, increasing)
#                       status = 'resolved' when d % 5 == 4, else 'active'
#
# With AS_OF = BASE_DATE + 14, the in-window offsets are 0..14; offsets
# {4, 9, 14} are resolved, so the DISTINCT ON winner for EVERY coordinate is
# offset 13: severity 100 + 13*13 = 269, date BASE_DATE + 13.
# ---------------------------------------------------------------------------
N_ITEMS = 30
N_LOCATIONS = 5
N_DAYS = 25
N_NULL_ITEM_ROWS = 25
ROWS_PER_SCENARIO = N_ITEMS * N_LOCATIONS * N_DAYS + N_NULL_ITEM_ROWS  # 3775
N_COORDINATES = N_ITEMS * N_LOCATIONS  # 150

BASE_DATE = date(2026, 1, 5)
AS_OF = BASE_DATE + timedelta(days=14)
EXPECTED_WINNER_SEVERITY = 100 + 13 * 13  # 269
EXPECTED_WINNER_DATE = BASE_DATE + timedelta(days=13)


# ---------------------------------------------------------------------------
# Bulk seed (server-side, one round-trip per scenario) — same row shape as
# test_outcome_integration.py's _seed_shortage, at volume.
# ---------------------------------------------------------------------------


_BULK_SEED_SQL = """
    WITH coords AS (
        SELECT i.item_id, l.location_id, gs.day_off
        FROM unnest(%(item_ids)s::uuid[]) AS i(item_id)
        CROSS JOIN unnest(%(location_ids)s::uuid[]) AS l(location_id)
        CROSS JOIN generate_series(0, %(n_days)s - 1) AS gs(day_off)
    ),
    pi_nodes AS (
        INSERT INTO nodes (node_id, node_type, scenario_id, item_id,
                           location_id, time_grain, time_ref, active)
        SELECT gen_random_uuid(), 'ProjectedInventory', %(scenario_id)s,
               item_id, location_id, 'exact_date',
               %(base_date)s::date + day_off, TRUE
        FROM coords
        RETURNING node_id, item_id, location_id, time_ref
    )
    INSERT INTO shortages (scenario_id, pi_node_id, item_id, location_id,
                           shortage_date, shortage_qty, severity_score,
                           calc_run_id, status)
    SELECT %(scenario_id)s,
           node_id,
           item_id,
           location_id,
           time_ref,
           50,
           100 + (time_ref - %(base_date)s::date) * 13,
           %(calc_run_id)s,
           CASE WHEN (time_ref - %(base_date)s::date) %% 5 = 4
                THEN 'resolved' ELSE 'active' END
    FROM pi_nodes
"""

_NULL_ITEM_SEED_SQL = """
    WITH gs AS (
        SELECT generate_series(0, %(n_rows)s - 1) AS day_off
    ),
    pi_nodes AS (
        INSERT INTO nodes (node_id, node_type, scenario_id, item_id,
                           location_id, time_grain, time_ref, active)
        SELECT gen_random_uuid(), 'ProjectedInventory', %(scenario_id)s,
               NULL, %(location_id)s, 'exact_date',
               %(base_date)s::date + day_off, TRUE
        FROM gs
        RETURNING node_id, location_id, time_ref
    )
    INSERT INTO shortages (scenario_id, pi_node_id, item_id, location_id,
                           shortage_date, shortage_qty, severity_score,
                           calc_run_id, status)
    SELECT %(scenario_id)s, node_id, NULL, location_id, time_ref,
           5, 42, %(calc_run_id)s, 'active'
    FROM pi_nodes
"""


def _seed_one_scenario(c, *, name: str, item_ids, location_ids):
    scenario_id = c.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
        "VALUES (%s, %s, FALSE, 'active') RETURNING scenario_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["scenario_id"]
    calc_run_id = c.execute(
        "INSERT INTO calc_runs (calc_run_id, scenario_id, status) "
        "VALUES (%s, %s, 'completed') RETURNING calc_run_id",
        (uuid4(), scenario_id),
    ).fetchone()["calc_run_id"]
    c.execute(
        _BULK_SEED_SQL,
        {
            "item_ids": item_ids,
            "location_ids": location_ids,
            "n_days": N_DAYS,
            "scenario_id": scenario_id,
            "calc_run_id": calc_run_id,
            "base_date": BASE_DATE,
        },
    )
    c.execute(
        _NULL_ITEM_SEED_SQL,
        {
            "n_rows": N_NULL_ITEM_ROWS,
            "scenario_id": scenario_id,
            "location_id": location_ids[0],
            "calc_run_id": calc_run_id,
            "base_date": BASE_DATE,
        },
    )
    return scenario_id


@pytest.fixture(scope="module")
def seeded_shortages(migrated_db):
    """Module-scoped bulk seed: 2 fork scenarios x ROWS_PER_SCENARIO shortages
    rows over SHARED items/locations (the pilot-base shape migration 075 was
    written for: one table carrying several scenarios' rows, discriminated by
    the index's leading scenario_id column). Committed once; swept by the
    module teardown's DROP of all public tables (conftest.migrated_db)."""
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        item_ids = [
            r["item_id"]
            for r in c.execute(
                "INSERT INTO items (item_id, name) "
                "SELECT gen_random_uuid(), 'perf075-item-' || g "
                "FROM generate_series(1, %s) AS g RETURNING item_id",
                (N_ITEMS,),
            ).fetchall()
        ]
        location_ids = [
            r["location_id"]
            for r in c.execute(
                "INSERT INTO locations (location_id, name) "
                "SELECT gen_random_uuid(), 'perf075-loc-' || g "
                "FROM generate_series(1, %s) AS g RETURNING location_id",
                (N_LOCATIONS,),
            ).fetchall()
        ]
        scenario_a = _seed_one_scenario(
            c, name="perf075-fork-a", item_ids=item_ids, location_ids=location_ids
        )
        scenario_b = _seed_one_scenario(
            c, name="perf075-fork-b", item_ids=item_ids, location_ids=location_ids
        )
        c.commit()

    # Fresh stats so the planner prices real row counts, not defaults.
    with psycopg.connect(migrated_db, autocommit=True) as c:
        c.execute("ANALYZE shortages")

    return {
        "scenario_a": scenario_a,
        "scenario_b": scenario_b,
        "item_ids": item_ids,
        "location_ids": location_ids,
    }


# ---------------------------------------------------------------------------
# EXPLAIN helpers
# ---------------------------------------------------------------------------


def _explain_json(conn, query: str, params) -> dict:
    """EXPLAIN (FORMAT JSON) of a parameterized query; returns the root Plan
    node. psycopg's server-side binding parameterizes the inner statement, so
    the planner prices it exactly as the evaluator's own execute() would."""
    row = conn.execute("EXPLAIN (FORMAT JSON) " + query, params).fetchone()
    plan = row["QUERY PLAN"]
    if isinstance(plan, str):  # depending on server/driver json adaptation
        plan = json.loads(plan)
    return plan[0]["Plan"]


def _walk(plan: dict):
    yield plan
    for child in plan.get("Plans") or []:
        yield from _walk(child)


# Every DROP INDEX-able index on shortages EXCEPT the one under proof:
# constraint-backed indexes (shortages_pkey) are excluded because DROP INDEX
# would refuse them — and none of them serves this query anyway. Dynamic on
# purpose: a future migration adding a shortages index can never silently
# re-introduce a competing access path into the probe below.
_DROPPABLE_COMPETITOR_INDEXES_SQL = """
    SELECT c.relname AS indexname
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    JOIN pg_class t ON t.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
      AND t.relname = 'shortages'
      AND c.relname <> %(keep)s
      AND NOT EXISTS (
          SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid
      )
    ORDER BY c.relname
"""


@contextmanager
def _competitors_neutralized(conn):
    """SAVEPOINT-scoped probe environment: transactionally DROP every
    competing shortages index and price Seq/Bitmap Scan paths out, so the
    ONLY remaining way to serve `_load_observed_shortages` is the 075 index
    (or the constraint-backed shortages_pkey, whose full-scan-plus-Sort plan
    the assertions would reject). See module docstring, point 2, for why
    proving the index's FORM this way is deterministic while proving the
    planner's PREFERENCE at CI volume is not.

    Everything is undone by ROLLBACK TO SAVEPOINT — DROP INDEX is
    transactional in PostgreSQL, and SET LOCAL issued after a savepoint is
    likewise cancelled by rolling back to it — so no state leaks to the rest
    of the transaction, whatever the `conn` fixture commits or rolls back
    afterwards."""
    # The `conn` fixture is autocommit=False (tests/integration/conftest.py),
    # so the execute below opens (or joins) a transaction and SAVEPOINT is
    # always legal. If the fixture ever flips to autocommit, fail loudly
    # instead of silently probing a DB whose indexes we just really dropped.
    assert conn.autocommit is False, (
        "_competitors_neutralized requires a transactional connection: "
        "with autocommit the DROP INDEX below would be permanent"
    )
    conn.execute("SAVEPOINT idx_proof")
    try:
        competitors = [
            r["indexname"]
            for r in conn.execute(
                _DROPPABLE_COMPETITOR_INDEXES_SQL, {"keep": INDEX_NAME}
            ).fetchall()
        ]
        # Sanity on the discovery query itself: every known droppable
        # pre-existing index must be in the drop set (shortages_pkey is the
        # single constraint-backed one). An empty/short list here would mean
        # the probe below proves nothing.
        expected_min = PRE_EXISTING_SHORTAGES_INDEXES - {"shortages_pkey"}
        assert expected_min <= set(competitors), sorted(competitors)
        assert INDEX_NAME not in competitors

        for name in competitors:
            conn.execute(
                sql.SQL("DROP INDEX {}").format(sql.Identifier("public", name))
            )
        # Belt and braces on top of the drops: even a Seq Scan / Bitmap Scan
        # over the surviving indexes must be priced out, not just beaten.
        conn.execute("SET LOCAL enable_seqscan = off")
        conn.execute("SET LOCAL enable_bitmapscan = off")
        yield
    finally:
        conn.execute("ROLLBACK TO SAVEPOINT idx_proof")


def _norm(s: str) -> str:
    return " ".join(s.split())


# ---------------------------------------------------------------------------
# 1. Schema guarantee — the index exists with the expected definition
# ---------------------------------------------------------------------------


class TestMigration075Schema:
    def test_index_present_with_expected_definition(self, conn):
        rows = conn.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = 'shortages'"
        ).fetchall()
        by_name = {r["indexname"]: r["indexdef"] for r in rows}
        assert INDEX_NAME in by_name, sorted(by_name)

        indexdef = by_name[INDEX_NAME]
        # Exact key: WHERE-equality column first, then the DISTINCT ON
        # grouping columns, then the tie-break sort columns — in order.
        assert (
            "USING btree (scenario_id, item_id, location_id, "
            "severity_score DESC, shortage_date)" in indexdef
        ), indexdef
        # Partial predicate (pg_get_indexdef renders the text cast).
        assert "WHERE (status = 'active'::text)" in indexdef, indexdef
        # A plain query index — never unique (the detector's upsert identity
        # is shortages_pi_node_calc_run_uidx, untouched).
        assert "UNIQUE" not in indexdef, indexdef

    def test_075_added_never_replaced(self, conn):
        """Migration 075's header promises addition, not replacement: every
        pre-existing shortages index (005 + 014) must still be present."""
        names = {
            r["indexname"]
            for r in conn.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = 'public' AND tablename = 'shortages'"
            ).fetchall()
        }
        missing = PRE_EXISTING_SHORTAGES_INDEXES - names
        assert not missing, f"replaced/dropped by mistake: {sorted(missing)}"


# ---------------------------------------------------------------------------
# 2. Query-shape guard — the verbatim copy tracks evaluator.py
# ---------------------------------------------------------------------------


class TestQueryShapeGuard:
    def test_verbatim_copy_matches_evaluator_source(self):
        """The EXPLAIN below proves the index against THIS module's copy of
        the query; this guard proves the copy is still the literal SQL inside
        `_load_observed_shortages`. If the hot query drifts (new predicate,
        changed ORDER BY), this fails loudly and the index shape must be
        re-derived — instead of the EXPLAIN silently green-lighting a stale
        query."""
        source = inspect.getsource(outcome_evaluator._load_observed_shortages)
        assert _norm(OBSERVED_SHORTAGES_SQL) in _norm(source), (
            "evaluator._load_observed_shortages no longer contains the exact "
            "query migration 075 was built for — update OBSERVED_SHORTAGES_SQL "
            "AND re-check the index shape against the new query"
        )


# ---------------------------------------------------------------------------
# 3. Usability proof at volume — EXPLAIN + result consistency
# ---------------------------------------------------------------------------


class TestIndexUsability:
    def test_seeded_volume_and_scoping(self, conn, seeded_shortages):
        """The seed actually put a few thousand rows per scenario in place —
        the EXPLAIN below is exercised at volume, not on an empty table."""
        for key in ("scenario_a", "scenario_b"):
            n = conn.execute(
                "SELECT COUNT(*) AS n FROM shortages WHERE scenario_id = %s",
                (seeded_shortages[key],),
            ).fetchone()["n"]
            assert n == ROWS_PER_SCENARIO

    def test_explain_references_index_no_seqscan_no_sort(
        self, conn, seeded_shortages
    ):
        """The core PERF-1 PR-A proof — the index's FORM: the exact
        `_load_observed_shortages` query is servable by
        idx_shortages_scenario_item_loc_active as an ordered index scan —
        no Seq Scan (the 35-minute pilot symptom), no Sort node (the
        DISTINCT ON ordering comes straight off the btree: equality on the
        leading scenario_id column makes the trailing (item_id, location_id,
        severity_score DESC, shortage_date) columns provide the ORDER BY),
        and scenario_id served as an Index Cond, never a Filter. Competing
        access paths are neutralized first — see module docstring, point 2,
        for why the planner's unassisted preference at CI volume proves
        nothing about this index."""
        with _competitors_neutralized(conn):
            plan = _explain_json(
                conn,
                OBSERVED_SHORTAGES_SQL,
                (seeded_shortages["scenario_a"], AS_OF),
            )
        nodes = list(_walk(plan))

        index_nodes = [n for n in nodes if n.get("Index Name") == INDEX_NAME]
        assert index_nodes, json.dumps(plan, indent=2)

        seq_scans = [n for n in nodes if n.get("Node Type") == "Seq Scan"]
        assert not seq_scans, json.dumps(plan, indent=2)

        # "Sort" and "Incremental Sort" both betray an index that cannot
        # deliver the DISTINCT ON ordering by itself.
        sorts = [n for n in nodes if "Sort" in n.get("Node Type", "")]
        assert not sorts, json.dumps(plan, indent=2)

        # The WHERE equality must ride the index (leading-column match),
        # not get re-checked row-by-row after the fetch. The deparsed
        # condition renders the bound parameter as either `$1` or a literal
        # depending on server/driver — asserting on the column name covers
        # both.
        idx = index_nodes[0]
        assert "scenario_id" in idx.get("Index Cond", ""), json.dumps(
            plan, indent=2
        )
        assert "scenario_id" not in (idx.get("Filter") or ""), json.dumps(
            plan, indent=2
        )

    def test_forced_index_rows_equal_default_plan_rows(
        self, conn, seeded_shortages
    ):
        """Truth consistency — the real contract: the partial index returns
        byte-identical rows to whatever plan the default planner picks with
        every index present. Both sides EXECUTE the real query (not just
        EXPLAIN it): default plan first, OUTSIDE the savepoint; index-forced
        plan second, inside `_competitors_neutralized` where the 075 index
        is the only remaining access path. The index must change the access
        path, never the result set."""
        params = (seeded_shortages["scenario_a"], AS_OF)

        default_rows = conn.execute(OBSERVED_SHORTAGES_SQL, params).fetchall()

        with _competitors_neutralized(conn):
            # Belt and braces: only compare if the probe really engaged the
            # index for this execution.
            plan = _explain_json(conn, OBSERVED_SHORTAGES_SQL, params)
            assert any(
                n.get("Index Name") == INDEX_NAME for n in _walk(plan)
            ), json.dumps(plan, indent=2)
            forced_rows = conn.execute(OBSERVED_SHORTAGES_SQL, params).fetchall()

        assert forced_rows == default_rows
        assert len(forced_rows) == N_COORDINATES

    def test_distinct_on_semantics_over_seeded_volume(
        self, conn, seeded_shortages
    ):
        """The seeded calendar makes the DISTINCT ON winner derivable by hand
        (module docstring): for EVERY coordinate the max-severity ACTIVE row
        dated <= AS_OF is day-offset 13. Resolved rows (offsets 4/9/14),
        rows past the window (15..24) and NULL-item rows must all lose."""
        rows = conn.execute(
            OBSERVED_SHORTAGES_SQL, (seeded_shortages["scenario_a"], AS_OF)
        ).fetchall()

        assert len(rows) == N_COORDINATES
        assert all(r["item_id"] is not None for r in rows)
        assert {r["severity_score"] for r in rows} == {EXPECTED_WINNER_SEVERITY}
        assert {r["shortage_date"] for r in rows} == {EXPECTED_WINNER_DATE}
        # One row per coordinate, and exactly the seeded coordinate grid.
        coords = {(r["item_id"], r["location_id"]) for r in rows}
        assert len(coords) == N_COORDINATES
        expected = {
            (i, loc)
            for i in seeded_shortages["item_ids"]
            for loc in seeded_shortages["location_ids"]
        }
        assert coords == expected


# ---------------------------------------------------------------------------
# 4. Migration 075 idempotence at re-run
# ---------------------------------------------------------------------------


class TestMigration075Idempotent:
    def test_reexecuting_075_sql_is_noop(self, migrated_db, conn):
        """Defensive-idempotence contract (migration 063 header; the runner in
        db/connection.py never swallows 'already exists'): re-running the file
        verbatim on an already-migrated DB — twice — must not raise. The file
        carries its own BEGIN/COMMIT, so it runs on a fresh autocommit
        connection (mirrors test_reexecuting_073_sql_is_noop)."""
        sql_text = MIGRATION_075.read_text(encoding="utf-8")
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            raw.execute(sql_text)  # 2nd application overall
            raw.execute(sql_text)  # and a 3rd — still a clean no-op

        names = {
            r["indexname"]
            for r in conn.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = 'public' AND tablename = 'shortages'"
            ).fetchall()
        }
        assert INDEX_NAME in names
