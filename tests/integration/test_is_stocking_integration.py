"""
tests/integration/test_is_stocking_integration.py — DB-backed tests of the
`locations.is_stocking` shortage-DETECTION gate (migration 081, PR-B — plan
modélisation 2026-07-17, ADR-021 amendment) against a real PostgreSQL — no
mocks (CLAUDE.md).

Business context: the first real ERP load exposes ~9 400 dollar-valued phantom
shortages on virtual demand channels (USA/CAN/ICO) that carry real forecast/CO
demand but ZERO supply of any kind. `locations.is_stocking = FALSE` gates
shortage DETECTION only — the ProjectedInventory PROJECTION stays computed
everywhere (explainability, ADR-004).

Covers:

  1. Non-stocking channel (is_stocking=FALSE), demand with zero supply:
     the PI buckets ARE computed — the negative closing_stock is VISIBLE on
     the node (has_shortage/shortage_qty projection fields included) — but
     ZERO `shortages` rows materialize. On BOTH engines: SqlPropagationEngine
     (SHORTAGES_SQL's new `locations` LEFT JOIN + COALESCE guard) and
     PropagationEngine (location_stocking_cache preload →
     detect_with_params(is_stocking=False)).
  2. The witness: an IDENTICAL seed with is_stocking=TRUE yields exactly the
     'stockout' row the FALSE case suppressed — the flag is the only
     differentiator. Business values are pinned exactly (qty / $ severity /
     date), so the two parametrized engines must also agree with each other:
     this doubles as the SQL/Python parity check on is_stocking.
  3. Migration 081 idempotence: triple execution overall (#1 = the migrated_db
     boot; #2 and #3 re-run the file verbatim), mirroring
     test_daily_runs_integration.test_reexecuting_078_sql_is_noop — like 078
     (and unlike 080's bare INSERT...SELECT) the file carries its own
     BEGIN/COMMIT, so the re-runs go through a fresh autocommit connection.
     A committed FALSE row survives un-reset, no duplicate column, DEFAULT
     TRUE for new rows, COMMENT intact.

ISOLATION (the committed-seed lesson, cf. test_ingest_retraction_integration's
neutralizing finalizer): every committed seed is neutralized by a finalizer
registered BEFORE the first commit — DEACTIVATION only (edges/nodes
active=FALSE, shortages resolved, items obsolete, the location's is_stocking
restored to the default-behaviour TRUE), NEVER a DELETE — no cascade can take
innocent rows with it. The module-scoped migrated_db teardown drops the schema
afterwards as the backstop.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.kernel.calc.projection import ProjectionKernel
from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.engine.kernel.graph.traversal import GraphTraversal
from ootils_core.engine.kernel.shortage.detector import ShortageDetector
from ootils_core.engine.orchestration.calc_run import CalcRunManager
from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine

from .conftest import requires_db

pytestmark = requires_db

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

# One CO, one daily bucket, zero supply → closing = -DEMAND_QTY. The item is
# deliberately unpriced and has NO item_planning_params row: the only possible
# shortage class is 'stockout' (never below_safety_stock), the unit-cost proxy
# is 1 and the bucket is 1 day, so severity_score == DEMAND_QTY exactly —
# single-valued assertions that both engines must hit bit-identically.
DEMAND_QTY = Decimal("40")

_REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_081 = (
    _REPO_ROOT / "src" / "ootils_core" / "db" / "migrations"
    / "081_location_is_stocking.sql"
)

ENGINES = [
    pytest.param(SqlPropagationEngine, id="sql"),
    pytest.param(PropagationEngine, id="python"),
]


# ---------------------------------------------------------------------------
# Seed: one virtual demand-only channel (the USA/CAN/ICO phantom topology)
# ---------------------------------------------------------------------------


def _seed_demand_only_channel(conn, *, is_stocking: bool) -> dict:
    """A location carrying ONE CustomerOrderDemand (qty DEMAND_QTY, today)
    consuming a single bucket-0 daily PI, and ZERO supply of any kind — no
    OnHandSupply, no PO/WO/transfer, no planning params. The projection must
    land at closing_stock = -DEMAND_QTY regardless of `is_stocking`; only the
    `shortages` materialization is at stake."""
    today = date.today()
    location_id, item_id, series_id = uuid4(), uuid4(), uuid4()
    pi_id, demand_id = uuid4(), uuid4()

    conn.execute(
        "INSERT INTO locations (location_id, name, is_stocking) VALUES (%s, %s, %s)",
        (location_id, f"IS-STOCKING-LOC-{uuid4()}", is_stocking),
    )
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"IS-STOCKING-ITEM-{uuid4()}"),
    )
    conn.execute(
        """
        INSERT INTO projection_series
            (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (series_id, item_id, location_id, BASELINE, today, today + timedelta(days=1)),
    )
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             time_grain, time_span_start, time_span_end,
             projection_series_id, bucket_sequence, is_dirty, active)
        VALUES (%s, 'ProjectedInventory', %s, %s, %s,
                'day', %s, %s, %s, 0, TRUE, TRUE)
        """,
        (pi_id, BASELINE, item_id, location_id,
         today, today + timedelta(days=1), series_id),
    )
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             quantity, qty_uom, time_grain, time_ref, is_dirty, active)
        VALUES (%s, 'CustomerOrderDemand', %s, %s, %s, %s, 'EA',
                'exact_date', %s, FALSE, TRUE)
        """,
        (demand_id, BASELINE, item_id, location_id, DEMAND_QTY, today),
    )
    conn.execute(
        """
        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
        VALUES (%s, 'consumes', %s, %s, %s, TRUE)
        """,
        (uuid4(), demand_id, pi_id, BASELINE),
    )
    return {
        "location_id": location_id,
        "item_id": item_id,
        "pi_id": pi_id,
        "today": today,
    }


def _register_neutralizer(request, dsn: str, seed: dict) -> None:
    """Register BEFORE the first commit (isolation lesson). Deactivation only,
    scoped strictly to this test's own uuids — never a DELETE. Runs on its own
    autocommit connection so it cannot depend on the test's `conn` state."""
    location_id, item_id = seed["location_id"], seed["item_id"]

    def _sweep():
        try:
            with psycopg.connect(dsn, autocommit=True) as c:
                c.execute(
                    """
                    UPDATE edges SET active = FALSE
                    WHERE scenario_id = %s AND to_node_id IN (
                        SELECT node_id FROM nodes WHERE location_id = %s)
                    """,
                    (BASELINE, location_id),
                )
                c.execute(
                    "UPDATE shortages SET status = 'resolved' "
                    "WHERE location_id = %s AND status = 'active'",
                    (location_id,),
                )
                c.execute(
                    "UPDATE nodes SET active = FALSE, is_dirty = FALSE "
                    "WHERE location_id = %s",
                    (location_id,),
                )
                c.execute(
                    "UPDATE items SET status = 'obsolete' WHERE item_id = %s",
                    (item_id,),
                )
                # Back to the default-behaviour state — no later test can be
                # perturbed by a leftover non-stocking location.
                c.execute(
                    "UPDATE locations SET is_stocking = TRUE WHERE location_id = %s",
                    (location_id,),
                )
        except Exception:
            pass  # best-effort — migrated_db teardown is the backstop

    request.addfinalizer(_sweep)


# ---------------------------------------------------------------------------
# Engine drivers + readers (same shape as test_param_overlay_propagation /
# test_rust_parity: direct `_propagate` over this seed's dirty PI set)
# ---------------------------------------------------------------------------


def _build_engine(cls, conn):
    store = GraphStore(conn)
    return cls(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        shortage_detector=ShortageDetector(),
    )


def _propagate_seed(engine, conn, seed: dict) -> UUID:
    """Start a calc_run on baseline, mark the seed's PI dirty, run the
    engine's _propagate, resolve stale shortages, close the run. Returns the
    calc_run_id."""
    pi_id = seed["pi_id"]
    calc_mgr = CalcRunManager()
    calc_run = calc_mgr.start_calc_run(scenario_id=BASELINE, event_ids=[], db=conn)
    assert calc_run is not None, "could not acquire advisory lock for baseline"
    dirty = DirtyFlagManager()
    dirty.mark_dirty({pi_id}, BASELINE, calc_run.calc_run_id, conn)
    dirty.flush_to_postgres(calc_run.calc_run_id, BASELINE, conn)

    engine._propagate(calc_run, {pi_id}, conn)
    engine._shortage_detector.resolve_stale(
        scenario_id=BASELINE, calc_run_id=calc_run.calc_run_id, db=conn
    )
    conn.execute(
        "UPDATE calc_runs SET status = 'completed', completed_at = now() "
        "WHERE calc_run_id = %s",
        (calc_run.calc_run_id,),
    )
    conn.execute("SELECT pg_advisory_unlock(hashtext(%s)::bigint)", (str(BASELINE),))
    conn.commit()
    return calc_run.calc_run_id


def _pi_row(conn, pi_id: UUID) -> dict:
    row = conn.execute(
        """
        SELECT opening_stock, inflows, outflows, closing_stock,
               has_shortage, shortage_qty
        FROM nodes WHERE node_id = %s
        """,
        (pi_id,),
    ).fetchone()
    assert row is not None
    return row


def _shortage_rows(conn, location_id: UUID) -> list[dict]:
    """EVERY shortages row for this location, in ANY status — the non-stocking
    invariant is 'not one row was ever written', not 'no active row'."""
    return conn.execute(
        """
        SELECT status, severity_class, shortage_qty, severity_score, shortage_date
        FROM shortages WHERE location_id = %s
        """,
        (location_id,),
    ).fetchall()


# ===========================================================================
# 1. Non-stocking channel: PI computed (negative closing VISIBLE), ZERO
#    shortages rows — both engines
# ===========================================================================


@pytest.mark.parametrize("engine_cls", ENGINES)
def test_non_stocking_location_pi_computed_but_zero_shortage_rows(
    conn, migrated_db, request, engine_cls
):
    """is_stocking=FALSE gates DETECTION only: the projection runs and the
    negative closing stock stays fully visible/explainable on the PI node
    (including the node-level has_shortage/shortage_qty projection fields,
    which are NOT gated), but the `shortages` table gets nothing."""
    seed = _seed_demand_only_channel(conn, is_stocking=False)
    _register_neutralizer(request, migrated_db, seed)
    conn.commit()

    engine = _build_engine(engine_cls, conn)
    _propagate_seed(engine, conn, seed)

    pi = _pi_row(conn, seed["pi_id"])
    # PROJECTION computed and honest — the phantom is visible on the node...
    assert pi["closing_stock"] is not None, "projection must run on a non-stocking location"
    assert Decimal(str(pi["closing_stock"])) == -DEMAND_QTY
    assert Decimal(str(pi["outflows"])) == DEMAND_QTY
    assert Decimal(str(pi["inflows"])) == Decimal("0")
    assert pi["has_shortage"] is True, (
        "node-level has_shortage is a PROJECTION field — is_stocking must not gate it"
    )
    assert Decimal(str(pi["shortage_qty"])) == DEMAND_QTY
    # ... but DETECTION is gated: not one shortages row, in any status.
    assert _shortage_rows(conn, seed["location_id"]) == [], (
        f"{engine_cls.__name__}: a non-stocking location materialized a shortages row"
    )


# ===========================================================================
# 2. Witness: identical seed, is_stocking=TRUE → the stockout row appears —
#    both engines, business values pinned exactly (doubles as parity)
# ===========================================================================


@pytest.mark.parametrize("engine_cls", ENGINES)
def test_stocking_location_identical_seed_yields_stockout_row(
    conn, migrated_db, request, engine_cls
):
    seed = _seed_demand_only_channel(conn, is_stocking=True)
    _register_neutralizer(request, migrated_db, seed)
    conn.commit()

    engine = _build_engine(engine_cls, conn)
    _propagate_seed(engine, conn, seed)

    pi = _pi_row(conn, seed["pi_id"])
    assert Decimal(str(pi["closing_stock"])) == -DEMAND_QTY  # same projection as case 1

    rows = _shortage_rows(conn, seed["location_id"])
    assert len(rows) == 1, (
        f"{engine_cls.__name__}: expected exactly the one stockout row, got {rows!r}"
    )
    row = rows[0]
    assert row["status"] == "active"
    assert row["severity_class"] == "stockout"
    assert Decimal(str(row["shortage_qty"])) == DEMAND_QTY
    # 1-day bucket x unpriced item (unit-cost proxy 1) → severity == qty.
    # Pinned exactly so BOTH parametrized engines must agree on the $ value.
    assert Decimal(str(row["severity_score"])) == DEMAND_QTY
    assert row["shortage_date"] == seed["today"]


# ===========================================================================
# 3. Migration 081 idempotence — triple execution (pattern of 078/080)
# ===========================================================================


class TestMigration081Idempotent:
    def test_triple_execution_preserves_schema_and_values(
        self, migrated_db, conn, request
    ):
        """Defensive-idempotence contract (migration 063 header; the runner in
        db/connection.py does NOT swallow 'already exists'): triple execution
        overall — #1 was the migrated_db boot, #2 and #3 re-run the file
        verbatim below. Like 078 the file carries its own BEGIN/COMMIT, so the
        re-runs go through a fresh autocommit connection (the one adaptation
        080 did not need).

        This test COMMITS (the re-executed file must see the row), so the
        residue is neutralized by a finalizer registered BEFORE the commit —
        deactivation-style: is_stocking restored to TRUE, never a DELETE."""
        loc_id = uuid4()

        def _sweep():
            try:
                with psycopg.connect(migrated_db, autocommit=True) as c:
                    c.execute(
                        "UPDATE locations SET is_stocking = TRUE WHERE location_id = %s",
                        (loc_id,),
                    )
            except Exception:
                pass  # best-effort — migrated_db teardown is the backstop

        request.addfinalizer(_sweep)

        # Committed non-default row: proves the re-runs preserve VALUES (a
        # botched re-add of the column would reset it to the DEFAULT).
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            raw.execute(
                "INSERT INTO locations (location_id, name, is_stocking) "
                "VALUES (%s, %s, FALSE)",
                (loc_id, f"MIG081-IDEM-{uuid4()}"),
            )

        sql_text = MIGRATION_081.read_text(encoding="utf-8")
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            raw.execute(sql_text)  # execution #2
            raw.execute(sql_text)  # execution #3 — still a clean no-op

        # The committed FALSE row survived both re-runs, un-reset.
        row = conn.execute(
            "SELECT is_stocking FROM locations WHERE location_id = %s", (loc_id,)
        ).fetchone()
        assert row is not None
        assert row["is_stocking"] is False

        # Exactly ONE is_stocking column — boolean, NOT NULL, DEFAULT true.
        cols = conn.execute(
            "SELECT data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'locations' "
            "AND column_name = 'is_stocking'"
        ).fetchall()
        assert len(cols) == 1, "ADD COLUMN IF NOT EXISTS duplicated the column?"
        assert cols[0]["data_type"] == "boolean"
        assert cols[0]["is_nullable"] == "NO"
        assert cols[0]["column_default"] == "true"

        # DEFAULT TRUE actually applies to a new row (on the rollback `conn`
        # fixture — this insert leaves no residue).
        default_row = conn.execute(
            "INSERT INTO locations (location_id, name) VALUES (%s, %s) "
            "RETURNING is_stocking",
            (uuid4(), f"MIG081-DEFAULT-{uuid4()}"),
        ).fetchone()
        assert default_row["is_stocking"] is True

        # COMMENT survived the re-runs (COMMENT ON replaces, never errors).
        comment = conn.execute(
            """
            SELECT col_description(a.attrelid, a.attnum) AS c
            FROM pg_attribute a
            WHERE a.attrelid = 'public.locations'::regclass
              AND a.attname = 'is_stocking'
            """
        ).fetchone()["c"]
        assert comment is not None
        assert "shortage DETECTION" in comment

    def test_bootstrap_rerun_is_idempotent(self, migrated_db):
        """A second OotilsDB() on an already-migrated DB (the exact boot path)
        is a no-op — 081 is tracked in schema_migrations and skipped."""
        from ootils_core.db.connection import OotilsDB

        OotilsDB(migrated_db)

        with psycopg.connect(migrated_db, row_factory=dict_row) as c:
            applied = c.execute(
                "SELECT COUNT(*) AS n FROM schema_migrations WHERE version LIKE '081%'"
            ).fetchone()["n"]
            assert applied == 1
            n_cols = c.execute(
                "SELECT COUNT(*) AS n FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'locations' "
                "AND column_name = 'is_stocking'"
            ).fetchone()["n"]
            assert n_cols == 1
