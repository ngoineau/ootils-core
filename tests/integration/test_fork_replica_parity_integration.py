"""
tests/integration/test_fork_replica_parity_integration.py — ADR-040.

Fork FK-trigger derogation against a real PostgreSQL (no mocks):

  - PARITY: the same source scenario forked through the fast path
    (session_replication_role = 'replica') and through the forced fallback
    (triggers-on) yields STRICTLY identical content — row counts and md5
    checksums over the business columns with a deterministic ORDER BY, for
    nodes, edges and projection_series. Fresh UUIDs (node_id, edge_id,
    series_id, scenario_id) and timestamps are excluded by design: they
    differ between any two forks, fast or slow.
  - COMPENSATORY CHECK, dangling item_id: a source node whose item_id
    references no items row (injected with replica role, FK triggers off —
    the most robust corruption vector: no SQL-string surgery on the copy
    query) must make create_scenario RAISE on the fast path. The disabled
    FK triggers would have let the copy through silently; the set-based
    check is what fires. After rollback, NO partial scenario remains.
  - COMPENSATORY CHECK, cross-scenario projection_series_id: a source node
    referencing an EXISTING series of another scenario is FK-valid, so no
    FK trigger — fast OR slow path — would ever catch it; only the
    scenario-scoped set-based check does (the review-requested hardening).

Unit-level contract tests (statement ordering, savepoint dance, warning,
error propagation) live in tests/test_fork_replica_role.py.
"""
from __future__ import annotations

from datetime import date
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

BASELINE = "00000000-0000-0000-0000-000000000001"
H_START = date(2026, 8, 1)
H_END = date(2026, 9, 1)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def _connect(dsn: str, *, autocommit: bool = False):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(dsn, row_factory=dict_row, autocommit=autocommit)


def _replica_privilege_available(dsn: str) -> bool:
    """True iff the test role may SET session_replication_role (superuser,
    or PG15+ GRANT SET ON PARAMETER). Gates the fast-path-dependent tests:
    without the privilege the fast path is unreachable and a 'parity' or
    'derogation' assertion would be vacuous."""
    import psycopg

    with _connect(dsn) as conn:
        try:
            conn.execute("SET LOCAL session_replication_role = 'replica'")
            return True
        except psycopg.errors.InsufficientPrivilege:
            return False
        finally:
            conn.rollback()


# ---------------------------------------------------------------------------
# Seed: one dedicated source scenario with series, nodes and edges
# ---------------------------------------------------------------------------


class _Seed:
    """A self-contained source scenario: 2 items x 1 location, 2 projection
    series, 6 nodes (2 supplies + 2x2 chained PI buckets), 4 edges."""

    NODE_COUNT = 6
    EDGE_COUNT = 4
    SERIES_COUNT = 2

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.item_ids = [str(uuid4()), str(uuid4())]
        self.location_id = str(uuid4())
        self.source_scenario_id = str(uuid4())
        self.forked_scenario_ids: list[str] = []

        with _connect(dsn, autocommit=True) as conn:
            for i, item_id in enumerate(self.item_ids):
                conn.execute(
                    "INSERT INTO items (item_id, name) VALUES (%s, %s)",
                    (item_id, f"FORK-PARITY-ITEM-{i}"),
                )
            conn.execute(
                "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
                (self.location_id, "FORK-PARITY-DC"),
            )
            conn.execute(
                "INSERT INTO scenarios "
                "(scenario_id, name, parent_scenario_id, is_baseline, status) "
                "VALUES (%s, 'fork-parity-source', %s, FALSE, 'active')",
                (self.source_scenario_id, BASELINE),
            )
            for i, item_id in enumerate(self.item_ids):
                series_id = str(uuid4())
                conn.execute(
                    "INSERT INTO projection_series "
                    "(series_id, item_id, location_id, scenario_id, "
                    " horizon_start, horizon_end) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        series_id,
                        item_id,
                        self.location_id,
                        self.source_scenario_id,
                        H_START,
                        H_END,
                    ),
                )
                supply_id, pi0_id, pi1_id = uuid4(), uuid4(), uuid4()
                conn.execute(
                    "INSERT INTO nodes "
                    "(node_id, node_type, scenario_id, item_id, location_id, "
                    " quantity, qty_uom, time_grain, time_ref, active) "
                    "VALUES (%s, 'PurchaseOrderSupply', %s, %s, %s, %s, 'EA', "
                    "        'day', %s, TRUE)",
                    (
                        supply_id,
                        self.source_scenario_id,
                        item_id,
                        self.location_id,
                        100 + i,
                        H_START,
                    ),
                )
                for bucket, (node_id, closing, shortage) in enumerate(
                    [(pi0_id, 40 + i, False), (pi1_id, -10 - i, True)]
                ):
                    conn.execute(
                        "INSERT INTO nodes "
                        "(node_id, node_type, scenario_id, item_id, location_id, "
                        " time_grain, time_span_start, time_span_end, "
                        " projection_series_id, bucket_sequence, "
                        " opening_stock, inflows, outflows, closing_stock, "
                        " has_shortage, shortage_qty, active) "
                        "VALUES (%s, 'ProjectedInventory', %s, %s, %s, 'day', "
                        "        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)",
                        (
                            node_id,
                            self.source_scenario_id,
                            item_id,
                            self.location_id,
                            H_START,
                            H_END,
                            series_id,
                            bucket,
                            50 + i,
                            100 + i,
                            110 + i,
                            closing,
                            shortage,
                            10 + i if shortage else 0,
                        ),
                    )
                conn.execute(
                    "INSERT INTO edges "
                    "(edge_type, from_node_id, to_node_id, scenario_id, priority) "
                    "VALUES ('replenishes', %s, %s, %s, %s)",
                    (supply_id, pi0_id, self.source_scenario_id, i),
                )
                conn.execute(
                    "INSERT INTO edges "
                    "(edge_type, from_node_id, to_node_id, scenario_id) "
                    "VALUES ('feeds_forward', %s, %s, %s)",
                    (pi0_id, pi1_id, self.source_scenario_id),
                )

    def cleanup(self) -> None:
        all_ids = self.forked_scenario_ids + [self.source_scenario_id]
        with _connect(self.dsn, autocommit=True) as conn:
            conn.execute(
                "DELETE FROM edges WHERE scenario_id = ANY(%s::uuid[])", (all_ids,)
            )
            conn.execute(
                "DELETE FROM nodes WHERE scenario_id = ANY(%s::uuid[])", (all_ids,)
            )
            conn.execute(
                "DELETE FROM projection_series WHERE scenario_id = ANY(%s::uuid[])",
                (all_ids,),
            )
            # Children (forks) before their parent (the source scenario).
            conn.execute(
                "DELETE FROM scenarios WHERE scenario_id = ANY(%s::uuid[])",
                (self.forked_scenario_ids,),
            )
            conn.execute(
                "DELETE FROM scenarios WHERE scenario_id = %s",
                (self.source_scenario_id,),
            )
            conn.execute(
                "DELETE FROM locations WHERE location_id = %s", (self.location_id,)
            )
            conn.execute(
                "DELETE FROM items WHERE item_id = ANY(%s::uuid[])", (self.item_ids,)
            )


@pytest.fixture(scope="module")
def seed(migrated_db):
    s = _Seed(migrated_db)
    yield s
    s.cleanup()


# ---------------------------------------------------------------------------
# Content signatures — counts + md5 over business columns, deterministic order
# ---------------------------------------------------------------------------
#
# Identity columns (node_id/edge_id/series_id/scenario_id — fresh UUIDs on
# every fork), audit timestamps, and is_dirty (reset to FALSE by the copy)
# are excluded. NULLs are made explicit ('<null>') so 'a,<null>,b' can never
# collide with 'a,b'. string_agg(... ORDER BY row_text) makes the checksum
# independent of physical row order.

_NODES_SIG_SQL = """
SELECT COUNT(*) AS n,
       COALESCE(md5(string_agg(row_text, '|' ORDER BY row_text)), '<empty>') AS checksum
FROM (
    SELECT concat_ws(',',
        n.node_type,
        COALESCE(n.item_id::text, '<null>'),
        COALESCE(n.location_id::text, '<null>'),
        COALESCE(n.quantity::text, '<null>'),
        COALESCE(n.qty_uom, '<null>'),
        COALESCE(n.time_grain, '<null>'),
        COALESCE(n.time_ref::text, '<null>'),
        COALESCE(n.time_span_start::text, '<null>'),
        COALESCE(n.time_span_end::text, '<null>'),
        COALESCE(n.bucket_sequence::text, '<null>'),
        COALESCE(n.opening_stock::text, '<null>'),
        COALESCE(n.inflows::text, '<null>'),
        COALESCE(n.outflows::text, '<null>'),
        COALESCE(n.closing_stock::text, '<null>'),
        n.has_shortage::text,
        n.shortage_qty::text,
        n.has_exact_date_inputs::text,
        n.has_week_inputs::text,
        n.has_month_inputs::text
    ) AS row_text
    FROM nodes n
    WHERE n.scenario_id = %s AND n.active = TRUE
) t
"""

# Edge endpoints are remapped UUIDs — identify them by the endpoint node's
# business key instead.
_EDGES_SIG_SQL = """
SELECT COUNT(*) AS n,
       COALESCE(md5(string_agg(row_text, '|' ORDER BY row_text)), '<empty>') AS checksum
FROM (
    SELECT concat_ws(',',
        e.edge_type,
        e.priority::text,
        e.weight_ratio::text,
        COALESCE(e.effective_start::text, '<null>'),
        COALESCE(e.effective_end::text, '<null>'),
        nf.node_type,
        COALESCE(nf.item_id::text, '<null>'),
        COALESCE(nf.location_id::text, '<null>'),
        COALESCE(nf.bucket_sequence::text, '<null>'),
        COALESCE(nf.time_ref::text, '<null>'),
        nt.node_type,
        COALESCE(nt.item_id::text, '<null>'),
        COALESCE(nt.location_id::text, '<null>'),
        COALESCE(nt.bucket_sequence::text, '<null>'),
        COALESCE(nt.time_ref::text, '<null>')
    ) AS row_text
    FROM edges e
    JOIN nodes nf ON nf.node_id = e.from_node_id
    JOIN nodes nt ON nt.node_id = e.to_node_id
    WHERE e.scenario_id = %s AND e.active = TRUE
) t
"""

_SERIES_SIG_SQL = """
SELECT COUNT(*) AS n,
       COALESCE(md5(string_agg(row_text, '|' ORDER BY row_text)), '<empty>') AS checksum
FROM (
    SELECT concat_ws(',',
        ps.item_id::text,
        COALESCE(ps.location_id::text, '<null>'),
        ps.horizon_start::text,
        ps.horizon_end::text
    ) AS row_text
    FROM projection_series ps
    WHERE ps.scenario_id = %s
) t
"""


def _scenario_signature(conn, scenario_id) -> dict[str, tuple[int, str]]:
    sig: dict[str, tuple[int, str]] = {}
    for label, sql in (
        ("nodes", _NODES_SIG_SQL),
        ("edges", _EDGES_SIG_SQL),
        ("series", _SERIES_SIG_SQL),
    ):
        row = conn.execute(sql, (scenario_id,)).fetchone()
        sig[label] = (int(row["n"]), row["checksum"])
    return sig


# ---------------------------------------------------------------------------
# Parity: fast path vs forced fallback
# ---------------------------------------------------------------------------


class TestForkPathParity:
    def test_fast_and_fallback_forks_have_identical_content(
        self, seed, migrated_db, monkeypatch
    ):
        if not _replica_privilege_available(migrated_db):
            pytest.skip(
                "test role lacks SET privilege on session_replication_role — "
                "the fast path is unreachable, parity would be vacuous"
            )

        from ootils_core.engine.scenario import manager as manager_module

        mgr = manager_module.ScenarioManager()
        source = UUID(seed.source_scenario_id)

        # Fork 1 — fast path (privilege verified above, so the SET succeeds
        # and the two bulk INSERTs run with FK triggers off).
        with _connect(migrated_db) as conn:
            fast = mgr.create_scenario("fork-parity-fast", source, conn)
            conn.commit()
        seed.forked_scenario_ids.append(str(fast.scenario_id))

        # Fork 2 — forced fallback: replace the SET attempt with the exact
        # observable effect of an InsufficientPrivilege denial (the same
        # savepoint dance the real handler performs, then False), which
        # sends _copy_nodes down the triggers-on slow path.
        def _denied(db):
            db.execute("SAVEPOINT scenario_fork_replica_role")
            db.execute("ROLLBACK TO SAVEPOINT scenario_fork_replica_role")
            db.execute("RELEASE SAVEPOINT scenario_fork_replica_role")
            return False

        monkeypatch.setattr(
            manager_module, "_enable_replica_role_for_fork", _denied
        )
        with _connect(migrated_db) as conn:
            slow = mgr.create_scenario("fork-parity-fallback", source, conn)
            conn.commit()
        seed.forked_scenario_ids.append(str(slow.scenario_id))

        with _connect(migrated_db) as conn:
            sig_fast = _scenario_signature(conn, fast.scenario_id)
            sig_slow = _scenario_signature(conn, slow.scenario_id)
            sig_source = _scenario_signature(conn, source)

        # Strict parity: same counts, same business-column checksums.
        assert sig_fast == sig_slow, (
            f"fast fork {fast.scenario_id} and fallback fork {slow.scenario_id} "
            f"diverged:\nfast={sig_fast}\nslow={sig_slow}"
        )
        # Sanity: the forks actually carry the seeded content (not two
        # identical empty copies) and match the source business content.
        assert sig_fast["nodes"][0] == seed.NODE_COUNT
        assert sig_fast["edges"][0] == seed.EDGE_COUNT
        assert sig_fast["series"][0] == seed.SERIES_COUNT
        assert sig_fast == sig_source


# ---------------------------------------------------------------------------
# Compensatory check actually fires — and nothing partial survives
# ---------------------------------------------------------------------------


class TestCompensatoryCheckFires:
    def test_dangling_item_fk_raises_on_fast_path_and_rolls_back_clean(
        self, seed, migrated_db
    ):
        """Corruption vector: a source node with a nonexistent item_id,
        injected with FK triggers off (replica role) in the SAME
        never-committed transaction as the fork. On the fast path the two
        bulk INSERTs copy it without FK validation — the set-based
        compensatory check is the only line of defense, and must RAISE."""
        if not _replica_privilege_available(migrated_db):
            pytest.skip(
                "test role lacks SET privilege on session_replication_role — "
                "cannot inject the corruption nor reach the fast path"
            )

        from ootils_core.engine.scenario.manager import ScenarioManager

        fork_name = f"fork-corrupt-item-{uuid4().hex[:8]}"
        with _connect(migrated_db) as conn:
            conn.execute("SET LOCAL session_replication_role = 'replica'")
            conn.execute(
                "INSERT INTO nodes "
                "(node_id, node_type, scenario_id, item_id, quantity, "
                " time_grain, time_ref, active) "
                "VALUES (%s, 'PurchaseOrderSupply', %s, %s, 1, 'day', %s, TRUE)",
                (uuid4(), seed.source_scenario_id, uuid4(), H_START),
            )
            conn.execute("SET LOCAL session_replication_role = 'origin'")

            with pytest.raises(
                RuntimeError,
                match="dangling item_id/location_id/projection_series_id",
            ):
                ScenarioManager().create_scenario(
                    fork_name, UUID(seed.source_scenario_id), conn
                )
            conn.rollback()

        self._assert_no_partial_scenario(migrated_db, seed, fork_name)

    def test_cross_scenario_series_reference_raises_and_rolls_back_clean(
        self, seed, migrated_db
    ):
        """Hardening proof (review point 3): a source node referencing an
        EXISTING projection_series of ANOTHER scenario is FK-valid — no FK
        trigger, fast or slow path, would ever reject it. Only the
        scenario-scoped compensatory check catches the cross-scenario leak.
        No replica privilege needed: the corrupt row satisfies every FK, and
        the check runs unconditionally on both paths."""
        from ootils_core.engine.scenario.manager import ScenarioManager

        fork_name = f"fork-corrupt-series-{uuid4().hex[:8]}"
        with _connect(migrated_db) as conn:
            # A series in ANOTHER scenario (the baseline) — never committed.
            alien_series_id = uuid4()
            conn.execute(
                "INSERT INTO projection_series "
                "(series_id, item_id, location_id, scenario_id, "
                " horizon_start, horizon_end) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    alien_series_id,
                    seed.item_ids[0],
                    seed.location_id,
                    BASELINE,
                    H_START,
                    H_END,
                ),
            )
            # A source-scenario PI node pointing at it: FK-valid, wrong scope.
            conn.execute(
                "INSERT INTO nodes "
                "(node_id, node_type, scenario_id, item_id, location_id, "
                " time_grain, time_span_start, time_span_end, "
                " projection_series_id, bucket_sequence, active) "
                "VALUES (%s, 'ProjectedInventory', %s, %s, %s, 'day', %s, %s, "
                "        %s, 99, TRUE)",
                (
                    uuid4(),
                    seed.source_scenario_id,
                    seed.item_ids[0],
                    seed.location_id,
                    H_START,
                    H_END,
                    alien_series_id,
                ),
            )

            with pytest.raises(
                RuntimeError,
                match="dangling item_id/location_id/projection_series_id",
            ):
                ScenarioManager().create_scenario(
                    fork_name, UUID(seed.source_scenario_id), conn
                )
            conn.rollback()

        self._assert_no_partial_scenario(migrated_db, seed, fork_name)

    @staticmethod
    def _assert_no_partial_scenario(dsn: str, seed: _Seed, fork_name: str) -> None:
        """After the rollback, the failed fork left NOTHING behind and the
        source scenario is exactly its seeded self again."""
        with _connect(dsn) as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM scenarios WHERE name = %s",
                (fork_name,),
            ).fetchone()
            assert row["cnt"] == 0, f"partial scenario row survived for {fork_name}"

            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM nodes WHERE scenario_id = %s",
                (seed.source_scenario_id,),
            ).fetchone()
            assert row["cnt"] == seed.NODE_COUNT, (
                "the injected corruption (or fork debris) survived the rollback"
            )

            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM projection_series "
                "WHERE scenario_id = %s",
                (seed.source_scenario_id,),
            ).fetchone()
            assert row["cnt"] == seed.SERIES_COUNT
